ALTER TABLE llm_token_usage
    ADD COLUMN IF NOT EXISTS use_case LowCardinality(String) DEFAULT 'legacy'
    AFTER source;
