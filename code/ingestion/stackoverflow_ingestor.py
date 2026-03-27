import os
import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

# Config
SO_API_KEY  = os.getenv("SO_API_KEY")
BRONZE_PATH = Path("data/bronze/stackoverflow")
LOGS_PATH   = Path("data/logs")

BASE_URL = "https://api.stackexchange.com/2.3"

# Increased to 2s to avoid 429 Too Many Requests
REQUEST_DELAY = 2.0

TARGET_PACKAGES = [
    {"pypi": "pandas",       "github": "pandas-dev/pandas",         "so_tag": "pandas"},
    {"pypi": "requests",     "github": "psf/requests",              "so_tag": "python-requests"},
    {"pypi": "fastapi",      "github": "tiangolo/fastapi",          "so_tag": "fastapi"},
    {"pypi": "numpy",        "github": "numpy/numpy",               "so_tag": "numpy"},
    {"pypi": "flask",        "github": "pallets/flask",             "so_tag": "flask"},
    {"pypi": "django",       "github": "django/django",             "so_tag": "django"},
    {"pypi": "scikit-learn", "github": "scikit-learn/scikit-learn", "so_tag": "scikit-learn"},
    {"pypi": "torch",        "github": "pytorch/pytorch",           "so_tag": "pytorch"},
]

# Logging setup

LOGS_PATH.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOGS_PATH / "stackoverflow_ingestor.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def log_event(event_type: str, package: str, status: str, details: dict):
    """Structured JSON log, feeds pipeline stats dashboard later."""
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event":     event_type,
        "package":   package,
        "status":    status,
        **details,
    }
    log_file = LOGS_PATH / "stackoverflow_ingestor_structured.jsonl"
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


# HTTP helper
def _get(endpoint: str, params: dict = None) -> dict:
    url = f"{BASE_URL}/{endpoint}"

    base_params = {
        "site": "stackoverflow",
        "key":  SO_API_KEY,
    }
    if params:
        base_params.update(params)

    response = requests.get(url, params=base_params, timeout=15)
    response.raise_for_status()
    data = response.json()

    backoff = data.get("backoff", 0)
    if backoff:
        logger.warning(f"SE API requested backoff of {backoff}s sleeping.")
        time.sleep(backoff)

    quota_remaining = data.get("quota_remaining", "?")
    if isinstance(quota_remaining, int) and quota_remaining < 50:
        logger.warning(f"Stack Exchange quota low: {quota_remaining} remaining today.")

    time.sleep(REQUEST_DELAY)
    return data

# Save helper
def _save(subfolder: str, filename: str, data: dict):
    """Save raw API response to Bronze layer with ingestion envelope."""
    dest = BRONZE_PATH / subfolder
    dest.mkdir(parents=True, exist_ok=True)
    filepath = dest / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved -> {filepath}")
    return filepath


# Ingestion functions
def ingest_questions(pkg: dict, pages: int = 3):
    """
    Fetch questions tagged with this package's SO tag.
    Fetches up to 300 questions sorted by votes.
    The body field contains raw HTML — this is the unstructured data component.
    Returns the list of question IDs so answers can be fetched for them.
    """
    pypi   = pkg["pypi"]
    so_tag = pkg["so_tag"]

    all_questions = []
    has_more      = True
    page          = 1

    while has_more and page <= pages:
        try:
            data = _get("questions", params={
                "tagged":   so_tag,
                "sort":     "votes",
                "order":    "desc",
                "pagesize": 100,
                "page":     page,
                "filter": "withbody",
            })

            questions = data.get("items", [])
            all_questions.extend(questions)
            has_more = data.get("has_more", False)

            logger.info(f"[{pypi}] questions page {page}: {len(questions)} items")
            page += 1

        except requests.HTTPError as e:
            logger.error(f"[{pypi}] questions page {page} failed: {e}")
            log_event("questions", pypi, "error", {"page": page, "error": str(e)})
            break

    question_ids = [q["question_id"] for q in all_questions if "question_id" in q]

    if all_questions:
        envelope = {
            "_ingested_at":   datetime.now(timezone.utc).isoformat(),
            "_source":        "stackexchange_api_v2.3",
            "_pypi_package":  pypi,
            "_so_tag":        so_tag,
            "_endpoint":      f"{BASE_URL}/questions",
            "_total_fetched": len(all_questions),
            "questions":      all_questions,
        }

        filename = f"{pypi}.json"
        filepath = _save("questions", filename, envelope)

        answered = sum(1 for q in all_questions if q.get("is_answered"))
        log_event("questions", pypi, "success", {
            "file":             str(filepath),
            "bytes":            os.path.getsize(filepath),
            "question_count":   len(all_questions),
            "answered_count":   answered,
            "unanswered_count": len(all_questions) - answered,
        })

    return question_ids


def ingest_answers(pkg: dict, question_ids: list,
                   max_pages_per_batch: int = 2):
    """
    Fetch answers for specific questions using /questions/{ids}/answers.
    The /answers endpoint ignores the ``tagged`` param, so we must provide
    explicit question IDs to get tag-relevant answers.
    Answer bodies are the primary input for sentiment analysis later.
    """
    pypi   = pkg["pypi"]
    so_tag = pkg["so_tag"]

    if not question_ids:
        logger.warning(f"[{pypi}] No question IDs — skipping answer ingestion.")
        return

    all_answers = []
    batch_size  = 100  # SE API max per vectorised request

    for batch_start in range(0, len(question_ids), batch_size):
        batch    = question_ids[batch_start:batch_start + batch_size]
        ids_str  = ";".join(str(qid) for qid in batch)
        batch_no = batch_start // batch_size + 1

        has_more = True
        page     = 1

        while has_more and page <= max_pages_per_batch:
            try:
                data = _get(f"questions/{ids_str}/answers", params={
                    "sort":     "votes",
                    "order":    "desc",
                    "pagesize": 100,
                    "page":     page,
                    "filter":   "withbody",
                })

                answers = data.get("items", [])
                all_answers.extend(answers)
                has_more = data.get("has_more", False)

                logger.info(
                    f"[{pypi}] answers batch {batch_no} page {page}: "
                    f"{len(answers)} items"
                )
                page += 1

            except requests.HTTPError as e:
                logger.error(f"[{pypi}] answers batch {batch_no} page {page} failed: {e}")
                log_event("answers", pypi, "error", {
                    "batch": batch_no, "page": page, "error": str(e),
                })
                break

    if all_answers:
        envelope = {
            "_ingested_at":        datetime.now(timezone.utc).isoformat(),
            "_source":             "stackexchange_api_v2.3",
            "_pypi_package":       pypi,
            "_so_tag":             so_tag,
            "_endpoint":           f"{BASE_URL}/questions/{{ids}}/answers",
            "_total_fetched":      len(all_answers),
            "_question_ids_count": len(question_ids),
            "answers":             all_answers,
        }

        filename = f"{pypi}.json"
        filepath = _save("answers", filename, envelope)

        avg_score = (
            sum(a.get("score", 0) for a in all_answers) / len(all_answers)
            if all_answers else 0
        )
        log_event("answers", pypi, "success", {
            "file":         str(filepath),
            "bytes":        os.path.getsize(filepath),
            "answer_count": len(all_answers),
            "avg_score":    round(avg_score, 2),
        })


def ingest_tag_info(pkg: dict):
    """
    Fetch metadata about the SO tag — total question count, synonyms.
    Small structured payload, no pagination needed.
    """
    pypi   = pkg["pypi"]
    so_tag = pkg["so_tag"]

    try:
        data = _get(f"tags/{so_tag}/info", params={})

        envelope = {
            "_ingested_at":  datetime.now(timezone.utc).isoformat(),
            "_source":       "stackexchange_api_v2.3",
            "_pypi_package": pypi,
            "_so_tag":       so_tag,
            "_endpoint":     f"{BASE_URL}/tags/{so_tag}/info",
            "data":          data,
        }

        filename = f"{pypi}.json"
        filepath = _save("tag_info", filename, envelope)

        items = data.get("items", [{}])
        count = items[0].get("count", 0) if items else 0

        log_event("tag_info", pypi, "success", {
            "file":            str(filepath),
            "total_questions": count,
        })

    except requests.HTTPError as e:
        logger.error(f"[{pypi}] tag info failed: {e}")
        log_event("tag_info", pypi, "error", {"error": str(e)})


# Main
def run():
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info(f"Stack Overflow Bronze ingestion started | run={run_ts}")

    stats = {"success": 0, "error": 0}

    for pkg in TARGET_PACKAGES:
        pypi = pkg["pypi"]
        logger.info(f"[LOG] Processing: {pypi}")

        try:
            ingest_tag_info(pkg)
            question_ids = ingest_questions(pkg, pages=3)
            ingest_answers(pkg, question_ids=question_ids)
            stats["success"] += 1
        except Exception as e:
            logger.error(f"[{pypi}] unexpected error: {e}")
            log_event("package_run", pypi, "error", {"error": str(e)})
            stats["error"] += 1

        # Longer pause between packages to avoid 429s
        time.sleep(3)

    logger.info("--------------------------------------------------------------------")
    logger.info(f"Run complete | success={stats['success']} errors={stats['error']}")
    logger.info("---------------------------------------------------------------------")
    log_event("run_complete", "all", "success", stats)


if __name__ == "__main__":
    if not SO_API_KEY:
        raise EnvironmentError("SO_API_KEY not set in .env")
    run()