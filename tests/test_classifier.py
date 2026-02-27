"""
Unit tests for the classification module — parsing, validation, and fallback
behaviour.  These tests do NOT call any external LLM API.
"""

from __future__ import annotations

import json

import pytest

from data_pipeline.classifier import _extract_json, _validate_and_normalise


# ---------------------------------------------------------------------------
# JSON extraction
# ---------------------------------------------------------------------------

class TestExtractJson:
    def test_plain_json(self):
        raw = '{"brand_mentioned": true, "sentiment": "positive"}'
        result = _extract_json(raw)
        assert result["brand_mentioned"] is True

    def test_markdown_fenced(self):
        raw = '```json\n{"brand_mentioned": false}\n```'
        result = _extract_json(raw)
        assert result["brand_mentioned"] is False

    def test_surrounding_whitespace(self):
        raw = '  \n {"sentiment": "negative"} \n '
        result = _extract_json(raw)
        assert result["sentiment"] == "negative"

    def test_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            _extract_json("this is not json")


# ---------------------------------------------------------------------------
# Validation & normalisation
# ---------------------------------------------------------------------------

class TestValidateAndNormalise:
    def test_full_valid_input(self):
        data = {
            "brand_mentioned": True,
            "rank_position": 2,
            "sentiment": "positive",
            "context_type": "recommendation",
            "recommendation_strength": 0.85,
            "competitor_mentioned": True,
            "competitors_list": ["Salesforce", "Zoho"],
            "confidence": 0.92,
        }
        result = _validate_and_normalise(data)
        assert result["brand_mentioned"] is True
        assert result["rank_position"] == 2
        assert result["sentiment"] == "positive"
        assert result["context_type"] == "recommendation"
        assert result["recommendation_strength"] == 0.85
        assert result["competitor_mentioned"] is True
        assert result["competitors_list"] == ["Salesforce", "Zoho"]
        assert result["classification_confidence"] == 0.92

    def test_missing_fields_use_defaults(self):
        result = _validate_and_normalise({})
        assert result["brand_mentioned"] is False
        assert result["rank_position"] is None
        assert result["sentiment"] == "neutral"
        assert result["context_type"] == "neutral"
        assert result["recommendation_strength"] == 0.0
        assert result["competitor_mentioned"] is False
        assert result["competitors_list"] == []
        assert result["classification_confidence"] == 0.5

    def test_invalid_sentiment_defaults_neutral(self):
        result = _validate_and_normalise({"sentiment": "fantastic"})
        assert result["sentiment"] == "neutral"

    def test_invalid_context_defaults_neutral(self):
        result = _validate_and_normalise({"context_type": "praise"})
        assert result["context_type"] == "neutral"

    def test_recommendation_strength_clamped(self):
        result = _validate_and_normalise({"recommendation_strength": 1.5})
        assert result["recommendation_strength"] == 1.0

        result = _validate_and_normalise({"recommendation_strength": -0.3})
        assert result["recommendation_strength"] == 0.0
