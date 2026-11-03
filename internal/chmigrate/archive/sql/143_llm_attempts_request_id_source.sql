-- Where the request_id of an LLM categorization attempt came from.
--
--   request_id_source   aig (the AI Gateway log id), ray (the Cloudflare ray id)
--                       or client (an id the client minted for the attempt).
--                       An empty string for a provider whose id is its own
--                       header, and for a row written before this migration.
--
-- Additive, no data mutation, no backfill.
ALTER TABLE llm_categorization_attempts ADD COLUMN IF NOT EXISTS request_id_source LowCardinality(String) DEFAULT '';
