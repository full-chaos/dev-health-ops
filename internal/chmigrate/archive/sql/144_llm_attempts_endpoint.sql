-- Which endpoint an LLM categorization attempt was sent to.
--
--   endpoint_type   workers_ai or ai_gateway: the type of the clef endpoint, derived
--                   from the validated base_url. An empty string for a backend
--                   with no clef endpoint, and for a row written before this migration.
--   gateway_id      the AI Gateway id of the endpoint. An empty string when the
--                   endpoint has none (every Workers AI endpoint), and for a row
--                   written before this migration.
--
-- Additive, no data mutation, no backfill.
ALTER TABLE llm_categorization_attempts ADD COLUMN IF NOT EXISTS endpoint_type LowCardinality(String) DEFAULT '';
ALTER TABLE llm_categorization_attempts ADD COLUMN IF NOT EXISTS gateway_id String DEFAULT '';
