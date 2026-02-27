"""
Unit tests for the AISOV scoring engine.
"""

from __future__ import annotations

import pytest

from analysis.visibility_scorer import AisovScore, compute_aisov
from config.settings import ScoringConfig


# Deterministic weights for testing
WEIGHTS = ScoringConfig(
    weight_mention_rate=0.30,
    weight_rank_score=0.25,
    weight_sentiment=0.25,
    weight_recommendation=0.20,
)


def _metric(mentioned=True, rank=None, sentiment="neutral", rec_strength=0.5):
    return {
        "brand_mentioned": mentioned,
        "rank_position": rank,
        "sentiment": sentiment,
        "recommendation_strength": rec_strength,
    }


class TestComputeAisov:
    def test_empty_metrics(self):
        score = compute_aisov([], weights=WEIGHTS)
        assert score.aisov == 0.0
        assert score.sample_size == 0

    def test_all_mentioned_positive_rank1(self):
        metrics = [_metric(True, 1, "positive", 1.0)] * 10
        score = compute_aisov(metrics, weights=WEIGHTS)
        # mention_rate=1.0, rank_score=1.0, positive_ratio=1.0, rec=1.0
        # AISOV = 0.30 + 0.25 + 0.25 + 0.20 = 1.00
        assert score.aisov == 1.0
        assert score.mention_rate == 1.0
        assert score.sample_size == 10

    def test_none_mentioned(self):
        metrics = [_metric(False, None, "neutral", 0.0)] * 5
        score = compute_aisov(metrics, weights=WEIGHTS)
        assert score.mention_rate == 0.0
        assert score.aisov == 0.0

    def test_mixed_metrics(self):
        metrics = [
            _metric(True, 1, "positive", 0.9),
            _metric(True, 3, "neutral", 0.5),
            _metric(False, None, "neutral", 0.0),
            _metric(True, 2, "negative", 0.3),
        ]
        score = compute_aisov(metrics, weights=WEIGHTS)
        assert score.sample_size == 4
        # mention_rate = 3/4 = 0.75
        assert score.mention_rate == 0.75
        # avg_rank = (1/1 + 1/3 + 1/2) / 3 ≈ 0.6111
        assert abs(score.avg_rank_score - 0.6111) < 0.01
        # positive_ratio = 1/3 (only 1 positive out of 3 mentioned)
        assert abs(score.positive_sentiment_ratio - 0.3333) < 0.01

    def test_returns_aisov_score_type(self):
        score = compute_aisov([_metric()], weights=WEIGHTS)
        assert isinstance(score, AisovScore)
        assert isinstance(score.as_dict(), dict)
