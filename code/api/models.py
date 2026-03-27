from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel

class PackageSummary(BaseModel):
    """Lightweight summary returned by the package list endpoint."""
    package_name: str
    overall_health_score: float
    health_tier: str

class HealthScore(BaseModel):
    """Full health score row from gold.package_health_scores."""
    package_name: str
    github_score: float
    pypi_score: float
    community_score: float
    sentiment_score: float
    overall_health_score: float
    health_tier: str
    scored_at: datetime

class SentimentDetail(BaseModel):
    """Sentiment breakdown from gold.package_sentiment."""
    package_name: str
    so_question_sentiment_avg: Optional[float] = None
    so_answer_sentiment_avg: Optional[float] = None
    readme_sentiment_compound: Optional[float] = None
    pypi_desc_sentiment_compound: Optional[float] = None
    overall_sentiment: Optional[float] = None

class PackageDetail(BaseModel):
    """Combined health scores + sentiment for a single package."""
    scores: HealthScore
    sentiment: SentimentDetail
