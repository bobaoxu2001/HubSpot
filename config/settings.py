"""
Centralised configuration for the AI Brand Visibility Intelligence Engine.

All secrets are loaded from environment variables.  Defaults are safe for local
development with a dockerised PostgreSQL instance.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, List


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class DatabaseConfig:
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    dbname: str = os.getenv("POSTGRES_DB", "brand_visibility")
    user: str = os.getenv("POSTGRES_USER", "bv_admin")
    password: str = os.getenv("POSTGRES_PASSWORD", "")

    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.dbname}"
        )


# ---------------------------------------------------------------------------
# LLM API keys & model identifiers
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class LLMConfig:
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    anthropic_api_key: str = os.getenv("ANTHROPIC_API_KEY", "")
    perplexity_api_key: str = os.getenv("PERPLEXITY_API_KEY", "")

    # Model identifiers used when dispatching queries
    models: Dict[str, str] = field(default_factory=lambda: {
        "chatgpt": os.getenv("OPENAI_MODEL", "gpt-4o"),
        "claude": os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514"),
        "perplexity": os.getenv("PERPLEXITY_MODEL", "sonar-pro"),
    })

    # Per-model request timeout (seconds)
    timeout: int = int(os.getenv("LLM_TIMEOUT", "120"))

    # Max concurrent requests per LLM provider
    max_concurrency: int = int(os.getenv("LLM_MAX_CONCURRENCY", "5"))


# ---------------------------------------------------------------------------
# Brand configuration
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class BrandConfig:
    primary_brand: str = "HubSpot"
    competitors: List[str] = field(default_factory=lambda: [
        "Salesforce",
        "Zoho",
        "Pipedrive",
        "Marketo",
        "ActiveCampaign",
    ])


# ---------------------------------------------------------------------------
# Visibility scoring weights (AISOV formula)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ScoringConfig:
    weight_mention_rate: float = float(os.getenv("W_MENTION", "0.30"))
    weight_rank_score: float = float(os.getenv("W_RANK", "0.25"))
    weight_sentiment: float = float(os.getenv("W_SENTIMENT", "0.25"))
    weight_recommendation: float = float(os.getenv("W_RECOMMENDATION", "0.20"))


# ---------------------------------------------------------------------------
# Clustering
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ClusteringConfig:
    algorithm: str = os.getenv("CLUSTER_ALGO", "hdbscan")  # hdbscan | kmeans
    n_clusters: int = int(os.getenv("CLUSTER_K", "6"))
    min_cluster_size: int = int(os.getenv("CLUSTER_MIN_SIZE", "5"))
    embedding_model: str = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")


# ---------------------------------------------------------------------------
# Aggregate settings object
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Settings:
    db: DatabaseConfig = field(default_factory=DatabaseConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    brand: BrandConfig = field(default_factory=BrandConfig)
    scoring: ScoringConfig = field(default_factory=ScoringConfig)
    clustering: ClusteringConfig = field(default_factory=ClusteringConfig)

    # Pipeline behaviour
    batch_size: int = int(os.getenv("PIPELINE_BATCH_SIZE", "10"))
    retry_limit: int = int(os.getenv("PIPELINE_RETRIES", "3"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


def get_settings() -> Settings:
    """Return a singleton-like settings instance."""
    return Settings()
