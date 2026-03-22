# Pipeline Architecture

## Overview

The Developer Ecosystem Analytics Pipeline collects data from public APIs and produces a unified **package health score** for popular Python packages. It follows a medallion architecture (Bronze → Silver → Gold) on Azure Databricks with Delta Lake storage.

The core problem: a Python package might have many GitHub stars but poor community support, declining downloads, or no recent maintenance. No single platform tells the full story. This pipeline will combine signals from GitHub, Stack Overflow, and PyPI into one score per package.

> **Current status:** Bronze layer ingestion from GitHub is implemented and working. Stack Overflow and PyPI ingestion, and the Silver/Gold layers, are in progress.

---

## Medallion Architecture

```
Raw APIs  →  Bronze Layer  →  Silver Layer  →  Gold Layer  →  Outputs
             (raw JSON)        (cleaned)        (aggregated)   (API + Dashboard)
```

This document currently covers the **Bronze layer only**.

---

## Bronze Layer: GitHub Ingestion

The Bronze layer stores the exact API response for every request, completely unchanged. Nothing is transformed or cleaned at this stage. Every saved file is timestamped so reruns append new files rather than overwriting old ones — this preserves a full audit trail and supports data lineage tracking.

### Folder structure

```
data/bronze/
  github/
    repos/          ← repository metadata per package per run
    readmes/        ← decoded Markdown README content
    contributors/   ← top-100 contributors per repo
    events/         ← public events feed snapshots
```

### Ingestion script

**`code/ingestion/github_ingestor.py`**

Four functions run per execution:

| Function | Endpoint | What it collects |
|---|---|---|
| `ingest_repo_metadata()` | `GET /repos/{owner}/{repo}` | Stars, forks, language, license, open issues, description, topics |
| `ingest_readme()` | `GET /repos/{owner}/{repo}/readme` | Full README decoded from base64 to plain Markdown |
| `ingest_contributors()` | `GET /repos/{owner}/{repo}/contributors` | Top-100 contributors with commit counts |
| `ingest_events()` | `GET /events` | Snapshot of GitHub public events feed (runs once per full run) |

### File envelope format

Every Bronze file wraps the raw API response in an envelope with ingestion metadata:

```json
{
  "_ingested_at": "2025-03-22T10:00:00Z",
  "_source": "github_rest_api",
  "_pypi_package": "pandas",
  "_endpoint": "https://api.github.com/repos/pandas-dev/pandas",
  "data": {
    "...raw GitHub API response..."
  }
}
```

The underscore-prefixed fields are added by our pipeline — not part of the original API response. They form the basis of data lineage tracking in later layers.

The README file additionally includes a `readme_text` field with the decoded Markdown, so downstream stages do not need to handle base64 decoding.

### Target packages

The pipeline currently tracks 8 Python packages, defined in `config.py`:

| PyPI name | GitHub repo | Stack Overflow tag |
|---|---|---|
| pandas | pandas-dev/pandas | pandas |
| requests | psf/requests | python-requests |
| fastapi | tiangolo/fastapi | fastapi |
| numpy | numpy/numpy | numpy |
| flask | pallets/flask | flask |
| django | django/django | django |
| scikit-learn | scikit-learn/scikit-learn | scikit-learn |
| pytorch | pytorch/pytorch | pytorch |

Adding a new package requires only a new entry in `TARGET_PACKAGES` in `config.py`. No other code changes are needed.

### Logging

Each run produces two log outputs:

- `data/logs/github_ingestor.log` — human-readable log for debugging
- `data/logs/github_ingestor_structured.jsonl` — one JSON object per event, used for pipeline statistics

Example structured log entry:

```json
{
  "timestamp": "2025-03-22T10:00:05Z",
  "event": "repo_metadata",
  "package": "pandas",
  "status": "success",
  "file": "data/bronze/github/repos/pandas_20250322_100000.json",
  "bytes": 14823,
  "stars": 43000,
  "forks": 17800,
  "open_issues": 3500
}
```

### Rate limit handling

GitHub allows 5,000 requests per hour with a token. Each full run uses approximately 33 API calls (4 calls × 8 packages + 1 events call). The ingestor reads the `X-RateLimit-Remaining` header after every request and pauses automatically if fewer than 10 requests remain.

---

## What comes next

The following layers are planned and will be documented as they are built:

- **Stack Overflow ingestion** Questions and answers with raw HTML bodies (unstructured data)
- **PyPI ingestion** Package metadata and download statistics
- **Silver layer** ETL, deduplication, cross-source linking, metadata profiling
- **Gold layer** Health score aggregation, ML sentiment analysis
- **Outputs** FastAPI REST endpoint, Streamlit dashboard
