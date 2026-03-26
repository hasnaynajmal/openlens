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

BRONZE_PATH = Path("data/bronze/pypi")
LOGS_PATH   = Path("data/logs")

PYPI_BASE_URL      = "https://pypi.org/pypi"
PYPISTATS_BASE_URL = "https://pypistats.org/api/packages"

# PyPI and pypistats have no auth and no strict rate limit
# but we still sleep to be polite
REQUEST_DELAY = 1.0

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
        logging.FileHandler(LOGS_PATH / "pypi_ingestor.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def log_event(event_type: str, package: str, status: str, details: dict):
    """Structured JSON log entry — feeds pipeline stats dashboard later."""
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event":     event_type,
        "package":   package,
        "status":    status,
        **details,
    }
    log_file = LOGS_PATH / "pypi_ingestor_structured.jsonl"
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


# HTTP helper

def _get(url: str) -> dict:
    """Simple GET — no auth needed for either PyPI or pypistats."""
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    time.sleep(REQUEST_DELAY)
    return response.json()


# Save helper

def _save(subfolder: str, filename: str, data: dict):
    dest = BRONZE_PATH / subfolder
    dest.mkdir(parents=True, exist_ok=True)
    filepath = dest / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved -> {filepath}")
    return filepath


# Ingestion functions

def ingest_package_metadata(pkg: dict):
    """
    Fetch package metadata from pypi.org/pypi/{package}/json.

    Returns structured data: author, license, classifiers, dependencies,
    release history, description, homepage URL, requires_python, and more.
    No authentication needed — CDN cached, effectively unlimited.
    """
    pypi = pkg["pypi"]
    url  = f"{PYPI_BASE_URL}/{pypi}/json"

    try:
        raw = _get(url)

        envelope = {
            "_ingested_at":  datetime.now(timezone.utc).isoformat(),
            "_source":       "pypi_json_api",
            "_pypi_package": pypi,
            "_endpoint":     url,
            "data":          raw,
        }

        filename = f"{pypi}.json"
        filepath = _save("metadata", filename, envelope)

        info = raw.get("info", {})
        releases = raw.get("releases", {})

        log_event("package_metadata", pypi, "success", {
            "file":            str(filepath),
            "bytes":           os.path.getsize(filepath),
            "latest_version":  info.get("version"),
            "license":         info.get("license"),
            "requires_python": info.get("requires_python"),
            "release_count":   len(releases),
            "author":          info.get("author"),
        })

    except requests.HTTPError as e:
        logger.error(f"[{pypi}] metadata failed: {e}")
        log_event("package_metadata", pypi, "error", {"error": str(e)})


def ingest_download_stats_recent(pkg: dict):
    """
    Fetch recent download counts from pypistats.org.
    Returns downloads for last day, last week, and last month.
    """
    pypi = pkg["pypi"]
    url  = f"{PYPISTATS_BASE_URL}/{pypi}/recent"

    try:
        raw = _get(url)

        envelope = {
            "_ingested_at":  datetime.now(timezone.utc).isoformat(),
            "_source":       "pypistats_api",
            "_pypi_package": pypi,
            "_endpoint":     url,
            "data":          raw,
        }

        filename = f"{pypi}.json"
        filepath = _save("downloads_recent", filename, envelope)

        data = raw.get("data", {})
        log_event("downloads_recent", pypi, "success", {
            "file":       str(filepath),
            "last_day":   data.get("last_day"),
            "last_week":  data.get("last_week"),
            "last_month": data.get("last_month"),
        })

    except requests.HTTPError as e:
        logger.error(f"[{pypi}] recent downloads failed: {e}")
        log_event("downloads_recent", pypi, "error", {"error": str(e)})


def ingest_download_stats_overall(pkg: dict):
    """
    Fetch overall download history from pypistats.org.
    Returns total all-time downloads broken down with and without mirrors.
    Useful for calculating long-term download trend.
    """
    pypi = pkg["pypi"]
    url  = f"{PYPISTATS_BASE_URL}/{pypi}/overall"

    try:
        raw = _get(url)

        envelope = {
            "_ingested_at":  datetime.now(timezone.utc).isoformat(),
            "_source":       "pypistats_api",
            "_pypi_package": pypi,
            "_endpoint":     url,
            "data":          raw,
        }

        filename = f"{pypi}.json"
        filepath = _save("downloads_overall", filename, envelope)

        # Sum total downloads across all rows in the response
        rows = raw.get("data", [])
        total = sum(r.get("downloads", 0) for r in rows if r.get("category") == "without_mirrors")

        log_event("downloads_overall", pypi, "success", {
            "file":            str(filepath),
            "bytes":           os.path.getsize(filepath),
            "total_downloads": total,
            "row_count":       len(rows),
        })

    except requests.HTTPError as e:
        logger.error(f"[{pypi}] overall downloads failed: {e}")
        log_event("downloads_overall", pypi, "error", {"error": str(e)})


# Main

def run():
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info(f"PyPI Bronze ingestion started | run={run_ts}")

    stats = {"success": 0, "error": 0}

    for pkg in TARGET_PACKAGES:
        pypi = pkg["pypi"]
        logger.info(f"--- Processing: {pypi} ---")

        try:
            ingest_package_metadata(pkg)
            ingest_download_stats_recent(pkg)
            ingest_download_stats_overall(pkg)
            stats["success"] += 1
        except Exception as e:
            logger.error(f"[{pypi}] unexpected error: {e}")
            log_event("package_run", pypi, "error", {"error": str(e)})
            stats["error"] += 1

        time.sleep(1)

    logger.info("--------------------------------------------------------------------")
    logger.info(f"Run complete | success={stats['success']} errors={stats['error']}")
    logger.info("--------------------------------------------------------------------")
    log_event("run_complete", "all", "success", stats)


if __name__ == "__main__":
    run()