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

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
BRONZE_PATH   = Path("data/bronze/github")
LOGS_PATH     = Path("data/logs")

TARGET_PACKAGES = [
    {"pypi": "pandas",       "github": "pandas-dev/pandas",              "so_tag": "pandas"},
    {"pypi": "requests",     "github": "psf/requests",                   "so_tag": "python-requests"},
    {"pypi": "fastapi",      "github": "tiangolo/fastapi",               "so_tag": "fastapi"},
    {"pypi": "numpy",        "github": "numpy/numpy",                    "so_tag": "numpy"},
    {"pypi": "flask",        "github": "pallets/flask",                  "so_tag": "flask"},
    {"pypi": "django",       "github": "django/django",                  "so_tag": "django"},
    {"pypi": "scikit-learn", "github": "scikit-learn/scikit-learn",      "so_tag": "scikit-learn"},
    {"pypi": "torch",        "github": "pytorch/pytorch",                "so_tag": "pytorch"},
]

# Logging setup

LOGS_PATH.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOGS_PATH / "github_ingestor.log", encoding="utf-8"),
        logging.StreamHandler(stream=open(os.devnull, 'w')),  # suppress console
    ],
)
logger = logging.getLogger(__name__)


def log_event(event_type: str, package: str, status: str, details: dict):
    """Write a structured JSON log entry to be used for pipeline stats dashboard later."""
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event":     event_type,
        "package":   package,
        "status":    status,
        **details,
    }
    log_file = LOGS_PATH / "github_ingestor_structured.jsonl"
    with open(log_file, "a") as f:
        f.write(json.dumps(entry) + "\n")


# HTTP helper

def _get(url: str, params: dict = None) -> requests.Response:
    """
    Authenticated GET with rate-limit awareness.
    Blocks and retries once if we're about to hit the ceiling.
    """
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept":        "application/vnd.github+json",
    }
    response = requests.get(url, headers=headers, params=params, timeout=15)

    remaining = int(response.headers.get("X-RateLimit-Remaining", 1))
    reset_at   = int(response.headers.get("X-RateLimit-Reset",     0))

    if remaining < 10:
        wait = max(0, reset_at - int(time.time())) + 5
        logger.warning(f"Rate limit low ({remaining} left). Sleeping {wait}s.")
        time.sleep(wait)

    response.raise_for_status()
    return response


# Save helper

def _save(subfolder: str, filename: str, data: dict):
    """Dump raw API response as JSON to the Bronze layer, never overwriting old runs."""
    dest = BRONZE_PATH / subfolder
    dest.mkdir(parents=True, exist_ok=True)
    filepath = dest / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved -> {filepath}")
    return filepath


# Ingestion functions

def ingest_repo_metadata(pkg: dict, date_stamp: str) -> dict | None:
    """
    Fetch repository metadata (stars, forks, language, license,
    open issues, contributor count, description, topics, created_at).

    Saves the raw GitHub API response unchanged.
    Returns a summary dict for logging; None on failure.
    """
    repo  = pkg["github"]
    pypi  = pkg["pypi"]
    url   = f"https://api.github.com/repos/{repo}"

    try:
        response = _get(url)
        raw      = response.json()

        # Attach our own ingestion metadata so Bronze records are self-describing
        envelope = {
            "_ingested_at":  datetime.now(timezone.utc).isoformat(),
            "_source":       "github_rest_api",
            "_pypi_package": pypi,
            "_endpoint":     url,
            "data":          raw,
        }

        filename = f"{pypi}_{date_stamp}.json"
        filepath = _save("repos", filename, envelope)

        log_event("repo_metadata", pypi, "success", {
            "file":       str(filepath),
            "bytes":      os.path.getsize(filepath),
            "stars":      raw.get("stargazers_count"),
            "forks":      raw.get("forks_count"),
            "open_issues": raw.get("open_issues_count"),
        })
        return raw

    except requests.HTTPError as e:
        logger.error(f"[{pypi}] repo metadata failed: {e}")
        log_event("repo_metadata", pypi, "error", {"error": str(e)})
        return None


def ingest_readme(pkg: dict, date_stamp: str):
    """
    Fetch the raw README as plain text.

    The GitHub /readme endpoint returns the file content base64-encoded;
    we decode it here so Silver doesn't have to, but we still store the
    full raw API envelope for auditability.
    """
    repo  = pkg["github"]
    pypi  = pkg["pypi"]
    url   = f"https://api.github.com/repos/{repo}/readme"

    try:
        response  = _get(url)
        raw       = response.json()

        # Decode base64 content to readable Markdown
        import base64
        content_b64  = raw.get("content", "")
        readme_text  = base64.b64decode(content_b64).decode("utf-8", errors="replace")

        envelope = {
            "_ingested_at":  datetime.now(timezone.utc).isoformat(),
            "_source":       "github_rest_api",
            "_pypi_package": pypi,
            "_endpoint":     url,
            "raw_api_response": raw,   # original envelope preserved
            "readme_text":   readme_text,   # decoded for convenience
        }

        filename = f"{pypi}_{date_stamp}.json"
        filepath = _save("readmes", filename, envelope)

        log_event("readme", pypi, "success", {
            "file":           str(filepath),
            "readme_chars":   len(readme_text),
            "readme_encoding": raw.get("encoding"),
        })

    except requests.HTTPError as e:
        logger.error(f"[{pypi}] README fetch failed: {e}")
        log_event("readme", pypi, "error", {"error": str(e)})


def ingest_events(date_stamp: str, per_page: int = 100):
    """
    Poll GitHub's public Events API once.

    The Events API returns the ~300 most recent public events across GitHub.
    We save the entire page as a single timestamped file. In production this
    would be called every 60s by a scheduler; here it's one snapshot.

    Useful event types we care about: WatchEvent (star), PushEvent,
    PullRequestEvent, IssuesEvent, ForkEvent.
    """
    url = "https://api.github.com/events"

    try:
        response = _get(url, params={"per_page": per_page})
        events   = response.json()

        envelope = {
            "_ingested_at": datetime.now(timezone.utc).isoformat(),
            "_source":      "github_events_api",
            "_endpoint":    url,
            "_event_count": len(events),
            "events":       events,
        }

        filename = f"events_{date_stamp}.json"
        filepath = _save("events", filename, envelope)

        # Count event types for the structured log
        type_counts = {}
        for ev in events:
            t = ev.get("type", "unknown")
            type_counts[t] = type_counts.get(t, 0) + 1

        log_event("events_poll", "global", "success", {
            "file":         str(filepath),
            "event_count":  len(events),
            "type_breakdown": type_counts,
        })

    except requests.HTTPError as e:
        logger.error(f"Events API failed: {e}")
        log_event("events_poll", "global", "error", {"error": str(e)})


def ingest_contributors(pkg: dict, date_stamp: str):
    """
    Fetch top-100 contributors for a repo.
    Stored separately from repo metadata, contributor data changes
    more slowly and is expensive (counts toward rate limit).
    """
    repo  = pkg["github"]
    pypi  = pkg["pypi"]
    url   = f"https://api.github.com/repos/{repo}/contributors"

    try:
        response     = _get(url, params={"per_page": 100, "anon": "false"})
        contributors = response.json()

        envelope = {
            "_ingested_at":  datetime.now(timezone.utc).isoformat(),
            "_source":       "github_rest_api",
            "_pypi_package": pypi,
            "_endpoint":     url,
            "contributors":  contributors,
        }

        filename = f"{pypi}_{date_stamp}.json"
        filepath = _save("contributors", filename, envelope)

        log_event("contributors", pypi, "success", {
            "file":              str(filepath),
            "contributor_count": len(contributors),
        })

    except requests.HTTPError as e:
        logger.error(f"[{pypi}] contributors failed: {e}")
        log_event("contributors", pypi, "error", {"error": str(e)})


# Main

def run():
    date_stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logging.info("-" * 60)
    logger.info(f" GitHub Bronze ingestion started | run={date_stamp}")
    logging.info("-" * 60)

    stats = {"success": 0, "error": 0}

    for pkg in TARGET_PACKAGES:
        pypi = pkg["pypi"]
        logger.info(f"[LOG] Processing: {pypi}")

        result = ingest_repo_metadata(pkg, date_stamp)
        if result:
            stats["success"] += 1
        else:
            stats["error"] += 1

        ingest_readme(pkg, date_stamp)
        ingest_contributors(pkg, date_stamp)

        # Be polite to the API between packages
        time.sleep(1)

    # One events snapshot per full run
    ingest_events(date_stamp)

    logging.info("-" * 60)
    logger.info(
        f"Run complete | success={stats['success']} errors={stats['error']}"
    )
    logging.info("-" * 60)
    log_event("run_complete", "all", "success", stats)


if __name__ == "__main__":
    if not GITHUB_TOKEN:
        raise EnvironmentError("GITHUB_TOKEN not set in .env")
    run()