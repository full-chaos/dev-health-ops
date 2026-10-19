ALTER TABLE llm_token_usage
    ADD COLUMN IF NOT EXISTS run_id String DEFAULT '' AFTER org_id;
