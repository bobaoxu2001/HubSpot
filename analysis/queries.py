"""
Analytics SQL queries — pre-built queries that power dashboards, reports,
and the strategy generator.

Each function returns a list of dicts from the warehouse.  All filtering
is parameterised to prevent SQL injection.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from data_pipeline.database import fetch_all


# ---------------------------------------------------------------------------
# 1. Mention rate by LLM
# ---------------------------------------------------------------------------

def mention_rate_by_llm(
    brand: str = "HubSpot",
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """
    Percentage of responses that mention the brand, grouped by LLM.

    Returns rows: {llm_name, total_responses, mentions, mention_rate}
    """
    date_filter = ""
    params: list = [brand]
    if start and end:
        date_filter = "AND m.created_at::date BETWEEN %s AND %s"
        params.extend([start, end])

    return fetch_all(
        f"""
        SELECT
            r.llm_name,
            COUNT(*)                                           AS total_responses,
            SUM(CASE WHEN m.brand_mentioned THEN 1 ELSE 0 END) AS mentions,
            ROUND(
                SUM(CASE WHEN m.brand_mentioned THEN 1 ELSE 0 END)::numeric
                / NULLIF(COUNT(*), 0), 4
            )                                                   AS mention_rate
        FROM brand_visibility_metrics m
        JOIN llm_responses r ON r.response_id = m.response_id
        WHERE m.brand_name = %s {date_filter}
        GROUP BY r.llm_name
        ORDER BY mention_rate DESC
        """,
        params,
    )


# ---------------------------------------------------------------------------
# 2. Visibility by intent category
# ---------------------------------------------------------------------------

def visibility_by_intent(
    brand: str = "HubSpot",
) -> List[Dict[str, Any]]:
    """
    AISOV components broken down by prompt intent category.

    Returns rows: {intent_category, mention_rate, avg_rank_score,
                   positive_ratio, rec_strength, sample_size}
    """
    return fetch_all(
        """
        SELECT
            p.intent_category,
            ROUND(AVG(CASE WHEN m.brand_mentioned THEN 1 ELSE 0 END)::numeric, 4)
                AS mention_rate,
            ROUND(AVG(CASE WHEN m.rank_position > 0
                       THEN 1.0 / m.rank_position ELSE 0 END)::numeric, 4)
                AS avg_rank_score,
            ROUND(AVG(CASE WHEN m.brand_mentioned AND m.sentiment = 'positive'
                       THEN 1 ELSE 0 END)::numeric, 4)
                AS positive_ratio,
            ROUND(AVG(m.recommendation_strength)::numeric, 4)
                AS rec_strength,
            COUNT(*)
                AS sample_size
        FROM brand_visibility_metrics m
        JOIN llm_responses r ON r.response_id = m.response_id
        JOIN prompts p ON p.prompt_id = r.prompt_id
        WHERE m.brand_name = %s
        GROUP BY p.intent_category
        ORDER BY mention_rate DESC
        """,
        (brand,),
    )


# ---------------------------------------------------------------------------
# 3. Sentiment distribution
# ---------------------------------------------------------------------------

def sentiment_distribution(
    brand: str = "HubSpot",
) -> List[Dict[str, Any]]:
    """
    Count and percentage of each sentiment class across brand-mentioning
    responses, broken down by LLM.

    Returns rows: {llm_name, sentiment, count, pct}
    """
    return fetch_all(
        """
        WITH totals AS (
            SELECT r.llm_name, COUNT(*) AS total
            FROM brand_visibility_metrics m
            JOIN llm_responses r ON r.response_id = m.response_id
            WHERE m.brand_name = %s AND m.brand_mentioned
            GROUP BY r.llm_name
        )
        SELECT
            r.llm_name,
            m.sentiment,
            COUNT(*)                                       AS count,
            ROUND(COUNT(*)::numeric / NULLIF(t.total, 0), 4) AS pct
        FROM brand_visibility_metrics m
        JOIN llm_responses r ON r.response_id = m.response_id
        JOIN totals t ON t.llm_name = r.llm_name
        WHERE m.brand_name = %s AND m.brand_mentioned
        GROUP BY r.llm_name, m.sentiment, t.total
        ORDER BY r.llm_name, m.sentiment
        """,
        (brand, brand),
    )


# ---------------------------------------------------------------------------
# 4. Competitor displacement rate
# ---------------------------------------------------------------------------

def competitor_displacement_rate(
    brand: str = "HubSpot",
) -> List[Dict[str, Any]]:
    """
    How often each competitor appears *instead of* the primary brand.

    "Displacement" = competitor mentioned AND primary brand NOT mentioned.

    Returns rows: {competitor, displacement_count, total_competitor_mentions,
                   displacement_rate}
    """
    return fetch_all(
        """
        WITH competitor_appearances AS (
            SELECT
                UNNEST(m.competitors_list) AS competitor,
                m.brand_mentioned
            FROM brand_visibility_metrics m
            WHERE m.brand_name = %s
        )
        SELECT
            competitor,
            SUM(CASE WHEN NOT brand_mentioned THEN 1 ELSE 0 END) AS displacement_count,
            COUNT(*)                                               AS total_competitor_mentions,
            ROUND(
                SUM(CASE WHEN NOT brand_mentioned THEN 1 ELSE 0 END)::numeric
                / NULLIF(COUNT(*), 0), 4
            )                                                       AS displacement_rate
        FROM competitor_appearances
        GROUP BY competitor
        ORDER BY displacement_rate DESC
        """,
        (brand,),
    )


# ---------------------------------------------------------------------------
# 5. Risk exposure index
# ---------------------------------------------------------------------------

def risk_exposure_index(
    brand: str = "HubSpot",
) -> List[Dict[str, Any]]:
    """
    Measures brand vulnerability by combining negative sentiment, criticism
    context, and low recommendation strength in risk/criticism prompts.

    Returns rows: {llm_name, negative_pct, criticism_pct,
                   avg_rec_strength, risk_index, sample_size}

    risk_index = (negative_pct × 0.4) + (criticism_pct × 0.4)
               + ((1 - avg_rec_strength) × 0.2)
    """
    return fetch_all(
        """
        SELECT
            r.llm_name,
            ROUND(AVG(CASE WHEN m.sentiment = 'negative' THEN 1 ELSE 0 END)::numeric, 4)
                AS negative_pct,
            ROUND(AVG(CASE WHEN m.context_type = 'criticism' THEN 1 ELSE 0 END)::numeric, 4)
                AS criticism_pct,
            ROUND(AVG(m.recommendation_strength)::numeric, 4)
                AS avg_rec_strength,
            ROUND((
                AVG(CASE WHEN m.sentiment = 'negative' THEN 1 ELSE 0 END) * 0.4
                + AVG(CASE WHEN m.context_type = 'criticism' THEN 1 ELSE 0 END) * 0.4
                + (1 - AVG(m.recommendation_strength)) * 0.2
            )::numeric, 4)
                AS risk_index,
            COUNT(*)
                AS sample_size
        FROM brand_visibility_metrics m
        JOIN llm_responses r ON r.response_id = m.response_id
        JOIN prompts p ON p.prompt_id = r.prompt_id
        WHERE m.brand_name = %s
          AND p.intent_category = 'risk_criticism'
        GROUP BY r.llm_name
        ORDER BY risk_index DESC
        """,
        (brand,),
    )


# ---------------------------------------------------------------------------
# 6. Aggregate AISOV leaderboard (cross-brand)
# ---------------------------------------------------------------------------

def aisov_leaderboard() -> List[Dict[str, Any]]:
    """
    Latest AISOV scores for all brands, ordered by score descending.
    """
    return fetch_all(
        """
        SELECT DISTINCT ON (brand_name)
            brand_name,
            llm_name,
            aisov_score,
            mention_rate,
            avg_rank_score,
            positive_sentiment_ratio,
            recommendation_strength_avg,
            sample_size,
            computed_at
        FROM visibility_scores
        WHERE llm_name IS NULL        -- overall (not per-LLM) scores
          AND intent_category IS NULL  -- overall (not per-intent) scores
        ORDER BY brand_name, computed_at DESC
        """
    )


# ---------------------------------------------------------------------------
# 7. Prompt cluster distribution
# ---------------------------------------------------------------------------

def cluster_distribution() -> List[Dict[str, Any]]:
    """
    Number of prompts per cluster label.
    """
    return fetch_all(
        """
        SELECT cluster_label, cluster_number, COUNT(*) AS prompt_count
        FROM prompt_clusters
        GROUP BY cluster_label, cluster_number
        ORDER BY prompt_count DESC
        """
    )


# ---------------------------------------------------------------------------
# 8. Time-series AISOV trend
# ---------------------------------------------------------------------------

def aisov_trend(
    brand: str = "HubSpot",
    limit: int = 30,
) -> List[Dict[str, Any]]:
    """
    Historical AISOV scores for a brand, most recent first.
    """
    return fetch_all(
        """
        SELECT period_start, period_end, aisov_score, sample_size, computed_at
        FROM visibility_scores
        WHERE brand_name = %s
          AND llm_name IS NULL
          AND intent_category IS NULL
        ORDER BY period_start DESC
        LIMIT %s
        """,
        (brand, limit),
    )
