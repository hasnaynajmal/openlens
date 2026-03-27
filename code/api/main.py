from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException

from db import get_spark
from models import HealthScore, PackageDetail, PackageSummary, SentimentDetail

# ----------------------------------------------------
# Load environment variables from project root .env
# ----------------------------------------------------
for _d in [Path(__file__).resolve().parents[2], *Path(__file__).resolve().parents]:
    _env = _d / ".env"
    if _env.is_file():
        load_dotenv(_env)
        break

# ----------------------------------------------------
# Module-level caches (populated at startup)
# ----------------------------------------------------
_scores_df: pd.DataFrame = pd.DataFrame()
_sentiment_df: pd.DataFrame = pd.DataFrame()

# ----------------------------------------------------
# Lifespan: init Spark + cache gold tables once at startup
# ----------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scores_df, _sentiment_df

    spark = get_spark()

    _scores_df = (
        spark.table("ddc_databricks.gold.package_health_scores")
        .toPandas()
    )
    _sentiment_df = (
        spark.table("ddc_databricks.gold.package_sentiment")
        .select(
            "package_name",
            "so_question_sentiment_avg",
            "so_answer_sentiment_avg",
            "readme_sentiment_compound",
            "pypi_desc_sentiment_compound",
            "overall_sentiment",
        )
        .toPandas()
    )

    yield  # application runs here


# ----------------------------------------------------
# App
# ----------------------------------------------------
app = FastAPI(
    title="OpenLens API",
    version="1.0.0",
    lifespan=lifespan,
)

# ----------------------------------------------------
# Helper
# ----------------------------------------------------
def _row_to_health_score(row: pd.Series) -> HealthScore:
    return HealthScore(
        package_name=row["package_name"],
        github_score=float(row["github_score"]),
        pypi_score=float(row["pypi_score"]),
        community_score=float(row["community_score"]),
        sentiment_score=float(row["sentiment_score"]),
        overall_health_score=float(row["overall_health_score"]),
        health_tier=str(row["health_tier"]),
        scored_at=pd.Timestamp(row["scored_at"]).to_pydatetime(),
    )

def _row_to_sentiment(row: pd.Series) -> SentimentDetail:
    def _f(val):
        return float(val) if pd.notna(val) else None

    return SentimentDetail(
        package_name=row["package_name"],
        so_question_sentiment_avg=_f(row["so_question_sentiment_avg"]),
        so_answer_sentiment_avg=_f(row["so_answer_sentiment_avg"]),
        readme_sentiment_compound=_f(row["readme_sentiment_compound"]),
        pypi_desc_sentiment_compound=_f(row["pypi_desc_sentiment_compound"]),
        overall_sentiment=_f(row["overall_sentiment"]),
    )

# Endpoints
# ----------------------------------------------------
@app.get("/health", tags=["meta"])
def health_check():
    """API liveness check."""
    return {
        "status": "ok",
        "packages_cached": len(_scores_df),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/packages", response_model=List[PackageSummary], tags=["packages"])
def list_packages():
    """
    List all tracked packages ordered by overall health score (descending).
    """
    if _scores_df.empty:
        return []

    ranked = _scores_df.sort_values("overall_health_score", ascending=False)
    return [
        PackageSummary(
            package_name=row["package_name"],
            overall_health_score=float(row["overall_health_score"]),
            health_tier=str(row["health_tier"]),
        )
        for _, row in ranked.iterrows()
    ]


@app.get("/packages/{name}", response_model=PackageDetail, tags=["packages"])
def get_package(name: str):
    """
    Full health score + sentiment breakdown for a single package.
    """
    score_rows = _scores_df[_scores_df["package_name"] == name]
    if score_rows.empty:
        raise HTTPException(status_code=404, detail=f"Package '{name}' not found.")

    sent_rows = _sentiment_df[_sentiment_df["package_name"] == name]
    sentiment = (
        _row_to_sentiment(sent_rows.iloc[0])
        if not sent_rows.empty
        else SentimentDetail(package_name=name)
    )

    return PackageDetail(
        scores=_row_to_health_score(score_rows.iloc[0]),
        sentiment=sentiment,
    )


@app.get("/packages/{name}/scores", response_model=HealthScore, tags=["packages"])
def get_package_scores(name: str):
    """
    Health score breakdown (github, pypi, community, sentiment, overall, tier).
    """
    rows = _scores_df[_scores_df["package_name"] == name]
    if rows.empty:
        raise HTTPException(status_code=404, detail=f"Package '{name}' not found.")
    return _row_to_health_score(rows.iloc[0])


@app.get("/packages/{name}/sentiment", response_model=SentimentDetail, tags=["packages"])
def get_package_sentiment(name: str):
    """
    Sentiment breakdown across SO questions, SO answers, README, and PyPI description.
    """
    rows = _sentiment_df[_sentiment_df["package_name"] == name]
    if rows.empty:
        raise HTTPException(status_code=404, detail=f"Package '{name}' not found.")
    return _row_to_sentiment(rows.iloc[0])


@app.get("/scores/leaderboard", response_model=List[HealthScore], tags=["scores"])
def leaderboard():
    """
    All packages with full score breakdown, ordered by overall health score.
    """
    if _scores_df.empty:
        return []
    return [
        _row_to_health_score(row)
        for _, row in _scores_df.sort_values(
            "overall_health_score", ascending=False
        ).iterrows()
    ]