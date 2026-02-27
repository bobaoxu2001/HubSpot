"""
LLM client — unified interface for querying ChatGPT, Claude, and Perplexity.

Each provider is wrapped behind a common ``query()`` signature so the pipeline
can treat them interchangeably.  Responses include timing metadata to support
latency tracking.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Optional

import anthropic
import openai

from config.settings import LLMConfig, get_settings

logger = logging.getLogger(__name__)


@dataclass
class LLMResponse:
    """Normalised response envelope returned by every provider."""
    llm_name: str
    model_version: str
    response_text: str
    token_count: Optional[int]
    latency_ms: int


# ---------------------------------------------------------------------------
# Provider implementations
# ---------------------------------------------------------------------------

def _query_openai(prompt: str, cfg: LLMConfig) -> LLMResponse:
    """Query the OpenAI ChatCompletion API (GPT-4o by default)."""
    client = openai.OpenAI(api_key=cfg.openai_api_key)
    model = cfg.models["chatgpt"]

    t0 = time.perf_counter()
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are a helpful assistant that provides detailed, factual answers about software tools, CRM platforms, and marketing technology."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.3,
        max_tokens=2048,
    )
    latency = int((time.perf_counter() - t0) * 1000)

    msg = response.choices[0].message.content or ""
    tokens = response.usage.total_tokens if response.usage else None

    return LLMResponse(
        llm_name="chatgpt",
        model_version=model,
        response_text=msg,
        token_count=tokens,
        latency_ms=latency,
    )


def _query_anthropic(prompt: str, cfg: LLMConfig) -> LLMResponse:
    """Query the Anthropic Messages API (Claude by default)."""
    client = anthropic.Anthropic(api_key=cfg.anthropic_api_key)
    model = cfg.models["claude"]

    t0 = time.perf_counter()
    response = client.messages.create(
        model=model,
        max_tokens=2048,
        system="You are a helpful assistant that provides detailed, factual answers about software tools, CRM platforms, and marketing technology.",
        messages=[{"role": "user", "content": prompt}],
    )
    latency = int((time.perf_counter() - t0) * 1000)

    text = response.content[0].text if response.content else ""
    tokens = (response.usage.input_tokens + response.usage.output_tokens) if response.usage else None

    return LLMResponse(
        llm_name="claude",
        model_version=model,
        response_text=text,
        token_count=tokens,
        latency_ms=latency,
    )


def _query_perplexity(prompt: str, cfg: LLMConfig) -> LLMResponse:
    """Query Perplexity via its OpenAI-compatible chat endpoint."""
    client = openai.OpenAI(
        api_key=cfg.perplexity_api_key,
        base_url="https://api.perplexity.ai",
    )
    model = cfg.models["perplexity"]

    t0 = time.perf_counter()
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are a helpful assistant that provides detailed, factual answers about software tools, CRM platforms, and marketing technology."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.3,
        max_tokens=2048,
    )
    latency = int((time.perf_counter() - t0) * 1000)

    msg = response.choices[0].message.content or ""
    tokens = response.usage.total_tokens if response.usage else None

    return LLMResponse(
        llm_name="perplexity",
        model_version=model,
        response_text=msg,
        token_count=tokens,
        latency_ms=latency,
    )


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

_PROVIDERS = {
    "chatgpt": _query_openai,
    "claude": _query_anthropic,
    "perplexity": _query_perplexity,
}


def query_llm(prompt: str, llm_name: str, cfg: LLMConfig | None = None) -> LLMResponse:
    """
    Send a prompt to the specified LLM and return a normalised response.

    Args:
        prompt: The user-facing prompt text.
        llm_name: One of 'chatgpt', 'claude', 'perplexity'.
        cfg: Optional LLMConfig override (defaults to global settings).

    Returns:
        An ``LLMResponse`` dataclass.

    Raises:
        ValueError: If ``llm_name`` is not a known provider.
    """
    if cfg is None:
        cfg = get_settings().llm

    provider_fn = _PROVIDERS.get(llm_name)
    if provider_fn is None:
        raise ValueError(f"Unknown LLM provider: {llm_name!r}. Choose from {list(_PROVIDERS)}")

    logger.info("Querying %s with prompt (%.60s…)", llm_name, prompt)
    return provider_fn(prompt, cfg)


def query_all_llms(prompt: str, cfg: LLMConfig | None = None) -> list[LLMResponse]:
    """Query every configured LLM with the same prompt and return all responses."""
    if cfg is None:
        cfg = get_settings().llm

    results: list[LLMResponse] = []
    for name in _PROVIDERS:
        try:
            results.append(query_llm(prompt, name, cfg))
        except Exception:
            logger.exception("Failed to query %s", name)
    return results
