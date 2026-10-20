CREATE TABLE IF NOT EXISTS llm_token_usage (
    org_id String,
    provider LowCardinality(String),
    model String,
    source LowCardinality(String),
    input_tokens UInt64,
    output_tokens UInt64,
    calls UInt64,
    computed_at DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(computed_at)
ORDER BY (org_id, provider, model, source, computed_at);
