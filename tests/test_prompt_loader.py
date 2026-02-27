"""
Tests for the prompt loader — validates the JSON dataset integrity.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "prompts.json"

VALID_CATEGORIES = {
    "generic_discovery",
    "comparison",
    "buying_intent",
    "alternatives",
    "segment_specific",
    "risk_criticism",
}


@pytest.fixture
def prompts():
    with open(DATA_PATH) as fh:
        return json.load(fh)


class TestPromptDataset:
    def test_has_120_prompts(self, prompts):
        assert len(prompts) == 120

    def test_all_have_required_fields(self, prompts):
        for p in prompts:
            assert "prompt_id" in p
            assert "prompt_text" in p
            assert "intent_category" in p

    def test_unique_ids(self, prompts):
        ids = [p["prompt_id"] for p in prompts]
        assert len(ids) == len(set(ids))

    def test_valid_categories(self, prompts):
        for p in prompts:
            assert p["intent_category"] in VALID_CATEGORIES, (
                f"Invalid category: {p['intent_category']} for prompt {p['prompt_id']}"
            )

    def test_category_distribution(self, prompts):
        counts = {}
        for p in prompts:
            counts[p["intent_category"]] = counts.get(p["intent_category"], 0) + 1
        # Each category should have exactly 20 prompts
        for cat in VALID_CATEGORIES:
            assert counts.get(cat, 0) == 20, f"Expected 20 for {cat}, got {counts.get(cat, 0)}"

    def test_no_empty_prompts(self, prompts):
        for p in prompts:
            assert len(p["prompt_text"].strip()) > 10
