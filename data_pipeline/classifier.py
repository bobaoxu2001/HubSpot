"""
LLM-powered classification workflow.

Takes a raw LLM response and sends it through a structured classification
prompt to extract brand-visibility signals.  Returns validated JSON that maps
directly to the ``brand_visibility_metrics`` table.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional

import openai

from config.settings import get_settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Classification prompt template
# ---------------------------------------------------------------------------

CLASSIFICATION_SYSTEM_PROMPT = """\
You are a brand-intelligence analyst.  You will receive two inputs:

1. PROMPT — the original question that was asked to an LLM.
2. RESPONSE — the raw text the LLM returned.

Your job is to extract structured brand-visibility signals for the primary
brand **{brand}** and its known competitors: {competitors}.

Return ONLY a valid JSON object (no markdown fences, no commentary) with
exactly these keys:

{{
  "brand_mentioned": <bool>,
  "rank_position": <int or null>,
  "sentiment": "<positive | neutral | negative>",
  "context_type": "<recommendation | comparison | criticism | neutral | alternative>",
  "recommendation_strength": <float 0.0–1.0>,
  "competitor_mentioned": <bool>,
  "competitors_list": [<string>, ...],
  "confidence": <float 0.0–1.0>
}}

### Field definitions

- **brand_mentioned**: true if "{brand}" (case-insensitive) appears in the
  response text.
- **rank_position**: ordinal position (1 = first) if the response presents a
  ranked list and {brand} is included; null otherwise.
- **sentiment**: overall sentiment *toward {brand}* in the response.  If
  {brand} is not mentioned, default to "neutral".
- **context_type**: how {brand} appears in the response (if at all).
  - "recommendation" — the LLM explicitly recommends {brand}.
  - "comparison" — the LLM compares {brand} to competitors.
  - "criticism" — the LLM highlights negatives about {brand}.
  - "alternative" — the LLM suggests {brand} as an alternative to something.
  - "neutral" — {brand} is mentioned without strong framing, or not at all.
- **recommendation_strength**: 0.0 (not recommended) to 1.0 (strongly
  recommended).  If {brand} is not mentioned, use 0.0.
- **competitor_mentioned**: true if any competitor name appears.
- **competitors_list**: list of competitor names found in the response
  (empty list if none).
- **confidence**: your self-assessed confidence that these extracted signals
  are accurate (0.0–1.0).

Be precise.  Do not hallucinate competitors that are not in the known list.
"""

CLASSIFICATION_USER_PROMPT = """\
PROMPT:
{prompt_text}

RESPONSE:
{response_text}
"""


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

_VALID_SENTIMENTS = {"positive", "neutral", "negative"}
_VALID_CONTEXTS = {"recommendation", "comparison", "criticism", "neutral", "alternative"}


def _extract_json(raw: str) -> Dict[str, Any]:
    """Extract the first JSON object from the model's output."""
    # Strip markdown code fences if present
    cleaned = re.sub(r"```(?:json)?\s*", "", raw)
    cleaned = re.sub(r"```", "", cleaned)
    cleaned = cleaned.strip()

    return json.loads(cleaned)


def _validate_and_normalise(data: Dict[str, Any]) -> Dict[str, Any]:
    """Coerce extracted JSON into the expected schema, filling safe defaults."""
    return {
        "brand_mentioned": bool(data.get("brand_mentioned", False)),
        "rank_position": int(data["rank_position"]) if data.get("rank_position") is not None else None,
        "sentiment": data.get("sentiment", "neutral") if data.get("sentiment") in _VALID_SENTIMENTS else "neutral",
        "context_type": data.get("context_type", "neutral") if data.get("context_type") in _VALID_CONTEXTS else "neutral",
        "recommendation_strength": max(0.0, min(1.0, float(data.get("recommendation_strength", 0.0)))),
        "competitor_mentioned": bool(data.get("competitor_mentioned", False)),
        "competitors_list": list(data.get("competitors_list", [])),
        "classification_confidence": max(0.0, min(1.0, float(data.get("confidence", 0.5)))),
    }


# ---------------------------------------------------------------------------
# Main classification function
# ---------------------------------------------------------------------------

def classify_response(
    prompt_text: str,
    response_text: str,
    brand: str | None = None,
    competitors: List[str] | None = None,
    model: str = "gpt-4o-mini",
) -> Dict[str, Any]:
    """
    Send a (prompt, response) pair through the classification workflow.

    Returns a normalised dict ready for ``response_store.save_metrics()``.

    Args:
        prompt_text: The original prompt that was sent to the LLM.
        response_text: The raw LLM response text.
        brand: Primary brand to analyse (defaults to settings).
        competitors: Competitor list (defaults to settings).
        model: OpenAI model used for classification.

    Returns:
        Dict with keys matching ``brand_visibility_metrics`` columns plus
        ``raw_classification`` containing the unprocessed JSON.
    """
    settings = get_settings()
    brand = brand or settings.brand.primary_brand
    competitors = competitors or settings.brand.competitors

    system_msg = CLASSIFICATION_SYSTEM_PROMPT.format(
        brand=brand,
        competitors=", ".join(competitors),
    )
    user_msg = CLASSIFICATION_USER_PROMPT.format(
        prompt_text=prompt_text,
        response_text=response_text,
    )

    client = openai.OpenAI(api_key=settings.llm.openai_api_key)

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_msg},
        ],
        temperature=0.0,
        max_tokens=512,
    )

    raw_output = response.choices[0].message.content or "{}"
    logger.debug("Classification raw output: %s", raw_output)

    raw_json = _extract_json(raw_output)
    result = _validate_and_normalise(raw_json)

    result["brand_name"] = brand
    result["classification_model"] = model
    result["raw_classification"] = raw_json

    return result


def classify_batch(
    items: List[Dict[str, Any]],
    brand: str | None = None,
    competitors: List[str] | None = None,
) -> List[Dict[str, Any]]:
    """
    Classify a batch of (prompt_text, response_text) pairs.

    Each item must have ``prompt_text`` and ``response_text`` keys.
    Returns a list of classification dicts in the same order.
    """
    results = []
    for item in items:
        try:
            result = classify_response(
                prompt_text=item["prompt_text"],
                response_text=item["response_text"],
                brand=brand,
                competitors=competitors,
            )
            results.append(result)
        except Exception:
            logger.exception("Classification failed for prompt: %.60s", item["prompt_text"])
            # Append a safe fallback so list indices stay aligned
            results.append({
                "brand_mentioned": False,
                "rank_position": None,
                "sentiment": "neutral",
                "context_type": "neutral",
                "recommendation_strength": 0.0,
                "competitor_mentioned": False,
                "competitors_list": [],
                "classification_confidence": 0.0,
                "brand_name": brand or get_settings().brand.primary_brand,
                "classification_model": "fallback",
                "raw_classification": {"error": "classification_failed"},
            })
    return results
