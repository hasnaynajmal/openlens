"""Thin wrapper around the OpenLens FastAPI."""
import os
import requests

_BASE = os.getenv("OPENLENS_API_URL", "http://127.0.0.1:8000")


def _get(path: str) -> dict | list:
    resp = requests.get(f"{_BASE}{path}", timeout=10)
    resp.raise_for_status()
    return resp.json()


def get_leaderboard() -> list[dict]:
    return _get("/scores/leaderboard")


def get_packages() -> list[dict]:
    return _get("/packages")


def get_package(name: str) -> dict:
    return _get(f"/packages/{name}")


def api_health() -> dict:
    return _get("/health")
