-- =============================================================================
-- AI Brand Visibility Intelligence Engine — PostgreSQL Schema
-- =============================================================================
-- Tracks how HubSpot (and competitors) surface across major LLMs.
-- Designed for analytical workloads: star-schema flavour with immutable fact
-- tables and slowly-changing dimension tables.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 0. Extensions
-- ---------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ---------------------------------------------------------------------------
-- 1. prompts — canonical prompt catalogue
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS prompts (
    prompt_id       SERIAL          PRIMARY KEY,
    prompt_text     TEXT            NOT NULL,
    intent_category VARCHAR(64)     NOT NULL
        CHECK (intent_category IN (
            'generic_discovery',
            'comparison',
            'buying_intent',
            'alternatives',
            'segment_specific',
            'risk_criticism'
        )),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    metadata        JSONB           DEFAULT '{}'::JSONB
);

CREATE INDEX idx_prompts_intent ON prompts (intent_category);

-- ---------------------------------------------------------------------------
-- 2. llm_responses — raw LLM outputs (append-only fact table)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS llm_responses (
    response_id     UUID            PRIMARY KEY DEFAULT uuid_generate_v4(),
    prompt_id       INT             NOT NULL REFERENCES prompts(prompt_id),
    llm_name        VARCHAR(64)     NOT NULL
        CHECK (llm_name IN ('chatgpt', 'claude', 'perplexity', 'gemini')),
    model_version   VARCHAR(128),
    response_text   TEXT            NOT NULL,
    token_count     INT,
    latency_ms      INT,
    run_id          UUID,
    timestamp       TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_responses_prompt   ON llm_responses (prompt_id);
CREATE INDEX idx_responses_llm      ON llm_responses (llm_name);
CREATE INDEX idx_responses_ts       ON llm_responses (timestamp);
CREATE INDEX idx_responses_run      ON llm_responses (run_id);

-- ---------------------------------------------------------------------------
-- 3. brand_visibility_metrics — classified signals per response
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS brand_visibility_metrics (
    metric_id                SERIAL          PRIMARY KEY,
    response_id              UUID            NOT NULL REFERENCES llm_responses(response_id),
    brand_name               VARCHAR(128)    NOT NULL DEFAULT 'HubSpot',
    brand_mentioned          BOOLEAN         NOT NULL,
    rank_position            INT             CHECK (rank_position >= 0),
    sentiment                VARCHAR(16)     NOT NULL
        CHECK (sentiment IN ('positive', 'neutral', 'negative')),
    context_type             VARCHAR(32)     NOT NULL
        CHECK (context_type IN (
            'recommendation', 'comparison', 'criticism', 'neutral', 'alternative'
        )),
    recommendation_strength  FLOAT           NOT NULL CHECK (recommendation_strength BETWEEN 0 AND 1),
    competitor_mentioned     BOOLEAN         NOT NULL DEFAULT FALSE,
    competitors_list         TEXT[],
    classification_model     VARCHAR(128),
    classification_confidence FLOAT          CHECK (classification_confidence BETWEEN 0 AND 1),
    raw_classification       JSONB,
    created_at               TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_metrics_response   ON brand_visibility_metrics (response_id);
CREATE INDEX idx_metrics_brand      ON brand_visibility_metrics (brand_mentioned);
CREATE INDEX idx_metrics_sentiment  ON brand_visibility_metrics (sentiment);

-- ---------------------------------------------------------------------------
-- 4. prompt_clusters — embedding-derived behavioural clusters
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS prompt_clusters (
    cluster_id      SERIAL          PRIMARY KEY,
    prompt_id       INT             NOT NULL REFERENCES prompts(prompt_id),
    cluster_label   VARCHAR(128)    NOT NULL,
    cluster_number  INT             NOT NULL,
    embedding       FLOAT[]         NOT NULL,
    algorithm       VARCHAR(32)     NOT NULL DEFAULT 'hdbscan',
    silhouette_score FLOAT,
    run_timestamp   TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_clusters_prompt    ON prompt_clusters (prompt_id);
CREATE INDEX idx_clusters_label     ON prompt_clusters (cluster_label);

-- ---------------------------------------------------------------------------
-- 5. visibility_scores — aggregated AISOV scores per brand / LLM / period
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS visibility_scores (
    score_id                    SERIAL          PRIMARY KEY,
    brand_name                  VARCHAR(128)    NOT NULL,
    llm_name                    VARCHAR(64),
    intent_category             VARCHAR(64),
    period_start                DATE            NOT NULL,
    period_end                  DATE            NOT NULL,
    mention_rate                FLOAT           NOT NULL,
    avg_rank_score              FLOAT           NOT NULL,
    positive_sentiment_ratio    FLOAT           NOT NULL,
    recommendation_strength_avg FLOAT           NOT NULL,
    aisov_score                 FLOAT           NOT NULL,
    sample_size                 INT             NOT NULL,
    computed_at                 TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_scores_brand   ON visibility_scores (brand_name);
CREATE INDEX idx_scores_llm     ON visibility_scores (llm_name);
CREATE INDEX idx_scores_period  ON visibility_scores (period_start, period_end);

-- ---------------------------------------------------------------------------
-- 6. strategy_reports — generated report artefacts
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS strategy_reports (
    report_id       UUID            PRIMARY KEY DEFAULT uuid_generate_v4(),
    report_type     VARCHAR(64)     NOT NULL DEFAULT 'full',
    report_content  TEXT            NOT NULL,
    metrics_snapshot JSONB          NOT NULL,
    generated_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- 7. pipeline_runs — execution audit log
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          UUID            PRIMARY KEY DEFAULT uuid_generate_v4(),
    started_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    status          VARCHAR(16)     NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'completed', 'failed')),
    prompts_count   INT,
    responses_count INT,
    error_message   TEXT,
    config_snapshot JSONB
);
