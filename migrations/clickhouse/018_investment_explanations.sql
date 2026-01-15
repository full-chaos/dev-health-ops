-- Migration: Add investment_explanations table for caching LLM-generated explanations

CREATE TABLE IF NOT EXISTS investment_explanations (
    cache_key String,
    explanation_json String,
    llm_provider String,
    llm_model Nullable(String),
    computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY cache_key;
