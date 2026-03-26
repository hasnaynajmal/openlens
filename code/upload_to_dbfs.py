"""
Upload local bronze data to Databricks DBFS (dbfs:/FileStore/bronze/).

Usage:
    python code/upload_to_dbfs.py                  # upload all bronze data
    python code/upload_to_dbfs.py stackoverflow     # upload only stackoverflow subfolder

Requires DATABRICKS_HOST and DATABRICKS_TOKEN in .env (same as databricks-connect).
"""

import io
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

LOCAL_BRONZE = Path("data/bronze")
DBFS_PREFIX = "/FileStore/bronze"


def upload_bronze(subfolder: str | None = None):
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()

    root = LOCAL_BRONZE / subfolder if subfolder else LOCAL_BRONZE
    if not root.exists():
        print(f"ERROR: local path {root} does not exist")
        sys.exit(1)

    uploaded, skipped, errors = 0, 0, 0

    for local_file in sorted(root.rglob("*.json")):
        rel = local_file.relative_to(LOCAL_BRONZE)
        dbfs_path = f"{DBFS_PREFIX}/{rel.as_posix()}"

        try:
            data = local_file.read_bytes()
            w.dbfs.upload(dbfs_path, io.BytesIO(data), overwrite=True)
            uploaded += 1
            print(f"  OK  {dbfs_path}  ({len(data):,} bytes)")
        except Exception as e:
            errors += 1
            print(f"  FAIL  {dbfs_path}  — {e}")

    print(f"\nDone: {uploaded} uploaded, {skipped} skipped, {errors} errors")


if __name__ == "__main__":
    sub = sys.argv[1] if len(sys.argv) > 1 else None
    upload_bronze(sub)
