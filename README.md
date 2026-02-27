# AI Brand Visibility Intelligence Engine

> Measure, analyse, and optimise how your brand surfaces inside Large Language Models.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL 16](https://img.shields.io/badge/postgres-16-336791.svg)](https://www.postgresql.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

---

## The Problem

Large Language Models (ChatGPT, Claude, Perplexity) are becoming the new front door to software discovery. When a buyer asks *"What's the best CRM for my business?"*, the LLM's answer shapes their shortlist — often before they ever visit a website or read a review.

**Brands have zero visibility into how they are represented inside these models.**

This engine solves that.

---

## What It Does

The AI Brand Visibility Intelligence Engine is a production-grade analytics system that:

1. **Queries multiple LLMs** with a curated set of 120 prompts across 6 intent categories
2. **Classifies every response** for brand-visibility signals (mentions, sentiment, ranking, recommendation strength)
3. **Computes an AI Share of Voice (AISOV) score** — a composite metric that quantifies brand prominence
4. **Clusters prompts** by behavioural intent using embeddings and unsupervised learning
5. **Generates automated strategy reports** with competitive positioning, risk analysis, and actionable recommendations
6. **Serves an interactive dashboard** for real-time analytics exploration

### Brand Focus

| Role | Brands |
|------|--------|
| **Primary** | HubSpot |
| **Competitors** | Salesforce, Zoho, Pipedrive, Marketo, ActiveCampaign |

---

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   ChatGPT    │     │    Claude    │     │  Perplexity  │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │                    │                    │
       └────────────┬───────┘────────────────────┘
                    │
            ┌───────▼────────┐
            │  LLM Client    │  Unified query interface
            │  (llm_client)  │  with retry & rate-limiting
            └───────┬────────┘
                    │
            ┌───────▼────────┐
            │ Response Store  │  Append-only fact table
            └───────┬────────┘
                    │
            ┌───────▼────────┐
            │  Classifier     │  LLM-powered signal extraction
            │  (GPT-4o-mini)  │  → structured JSON
            └───────┬────────┘
                    │
         ┌──────────┼──────────┐
         │          │          │
   ┌─────▼────┐ ┌──▼───┐ ┌───▼──────┐
   │ AISOV    │ │Clust-│ │Analytics │
   │ Scorer   │ │ering │ │ Queries  │
   └─────┬────┘ └──┬───┘ └───┬──────┘
         │         │          │
         └─────────┼──────────┘
                   │
          ┌────────▼─────────┐
          │ Strategy Report  │
          │ Generator        │
          └────────┬─────────┘
                   │
          ┌────────▼─────────┐
          │ Streamlit        │
          │ Dashboard        │
          └──────────────────┘
```

---

## AISOV Scoring Formula

The **AI Share of Voice** score is a weighted composite of four normalised signals:

```
AISOV = (mention_rate       × 0.30)
      + (avg_rank_score     × 0.25)
      + (positive_sentiment × 0.25)
      + (recommendation_avg × 0.20)
```

| Component | Weight | Rationale |
|-----------|--------|-----------|
| **Mention Rate** | 30% | Foundational signal — if the brand isn't mentioned, nothing else matters. Highest weight reflects its gating role. |
| **Average Rank Score** | 25% | Positional prominence in ranked lists (`1/rank`). Being listed first signals stronger LLM endorsement. |
| **Positive Sentiment Ratio** | 25% | Qualitative tonality — a brand can be mentioned but criticised, which should depress the score. |
| **Recommendation Strength** | 20% | How explicitly the LLM recommends the brand (0–1 scale). Slightly lower weight due to correlation with sentiment. |

All components are normalised to `[0, 1]`, so **AISOV ∈ [0, 1]**.

---

## Project Structure

```
├── config/
│   └── settings.py             # Centralised configuration (env-driven)
├── data/
│   └── prompts.json            # 120-prompt catalogue (6 categories × 20)
├── data_pipeline/
│   ├── database.py             # Connection pool & query helpers
│   ├── prompt_loader.py        # Prompt ingestion & seeding
│   ├── llm_client.py           # Multi-LLM query dispatcher
│   ├── response_store.py       # Response & metrics persistence
│   ├── classifier.py           # LLM classification workflow
│   └── orchestrator.py         # End-to-end pipeline runner
├── warehouse/
│   ├── schema.sql              # PostgreSQL DDL (7 tables)
│   └── seed.py                 # Schema + prompt seeding script
├── analysis/
│   ├── visibility_scorer.py    # AISOV computation engine
│   ├── clustering.py           # Embedding + HDBSCAN/K-means clustering
│   └── queries.py              # Pre-built analytics SQL
├── report_generation/
│   └── strategy_report.py      # Template & LLM report generators
├── dashboard/
│   └── app.py                  # Streamlit interactive dashboard
├── tests/
│   ├── test_classifier.py      # Classifier unit tests
│   ├── test_visibility_scorer.py  # Scoring engine tests
│   └── test_prompt_loader.py   # Dataset integrity tests
├── main.py                     # CLI entry point
├── docker-compose.yml          # Local PostgreSQL setup
├── requirements.txt            # Python dependencies
├── .env.example                # Environment variable template
└── .gitignore
```

---

## Quick Start

### Prerequisites

- Python 3.11+
- Docker & Docker Compose (for PostgreSQL)
- API keys for OpenAI, Anthropic, and/or Perplexity

### 1. Clone & Install

```bash
git clone https://github.com/your-org/brand-visibility-engine.git
cd brand-visibility-engine

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your API keys and database credentials
```

### 3. Start Database

```bash
docker compose up -d
```

The schema is automatically applied on first start via the init script.

### 4. Seed Prompts

```bash
python main.py seed
```

### 5. Run the Pipeline

```bash
# Full pipeline: query all LLMs → classify → score
python main.py run

# Or run individual stages:
python main.py run --llms chatgpt,claude --limit 10   # Subset
python main.py classify                                 # Classify only
python main.py score                                    # Compute AISOV
python main.py cluster                                  # Cluster prompts
```

### 6. Generate Reports

```bash
# Deterministic template report
python main.py report

# LLM-synthesised narrative report
python main.py report --llm
```

### 7. Launch Dashboard

```bash
streamlit run dashboard/app.py
```

---

## Database Schema

Seven tables designed for analytical workloads:

| Table | Purpose |
|-------|---------|
| `prompts` | Canonical prompt catalogue with intent categories |
| `llm_responses` | Raw LLM outputs (append-only fact table) |
| `brand_visibility_metrics` | Classified brand signals per response |
| `prompt_clusters` | Embedding-derived behavioural clusters |
| `visibility_scores` | Aggregated AISOV scores by brand / LLM / intent |
| `strategy_reports` | Generated report artefacts |
| `pipeline_runs` | Execution audit log |

---

## Analytics Queries

Pre-built queries available in `analysis/queries.py`:

| Query | Description |
|-------|-------------|
| `mention_rate_by_llm()` | Brand mention percentage per LLM |
| `visibility_by_intent()` | AISOV components by prompt intent category |
| `sentiment_distribution()` | Positive/neutral/negative breakdown by LLM |
| `competitor_displacement_rate()` | How often competitors appear *instead of* the brand |
| `risk_exposure_index()` | Composite risk score from criticism/negative signals |
| `aisov_leaderboard()` | Cross-brand AISOV ranking |
| `cluster_distribution()` | Prompts per behavioural cluster |
| `aisov_trend()` | Historical AISOV scores over time |

---

## Prompt Categories

The 120-prompt dataset covers six intent categories that mirror real-world buyer behaviour:

| Category | Count | Example |
|----------|-------|---------|
| **Generic Discovery** | 20 | *"What is the best CRM software for small businesses?"* |
| **Comparison** | 20 | *"Compare HubSpot and Salesforce for mid-market companies."* |
| **Buying Intent** | 20 | *"Is HubSpot worth the investment for a 200-employee company?"* |
| **Alternatives** | 20 | *"What are the best alternatives to HubSpot?"* |
| **Segment-Specific** | 20 | *"Best CRM for e-commerce businesses?"* |
| **Risk / Criticism** | 20 | *"What are HubSpot's biggest weaknesses compared to competitors?"* |

---

## Testing

```bash
# Run all tests
pytest tests/ -v

# With coverage
pytest tests/ -v --cov=data_pipeline --cov=analysis --cov-report=term-missing
```

---

## Configuration

All configuration is driven by environment variables (see `.env.example`). Key settings:

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENAI_API_KEY` | — | Required for ChatGPT queries and classification |
| `ANTHROPIC_API_KEY` | — | Required for Claude queries |
| `PERPLEXITY_API_KEY` | — | Required for Perplexity queries |
| `W_MENTION` | 0.30 | AISOV weight: mention rate |
| `W_RANK` | 0.25 | AISOV weight: rank score |
| `W_SENTIMENT` | 0.25 | AISOV weight: positive sentiment |
| `W_RECOMMENDATION` | 0.20 | AISOV weight: recommendation strength |
| `CLUSTER_ALGO` | hdbscan | Clustering algorithm (hdbscan \| kmeans) |

---

## License

MIT

---

*Built for AI-era brand intelligence.*
