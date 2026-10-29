-- Two facts of an LLM categorization attempt that the existing columns cannot
-- carry.
--
--   usage_reason        why input_tokens and output_tokens are zero on a call
--                       the provider answered: 'unavailable' when the answer
--                       reported no token usage, an empty string otherwise.
--   model_from_request  1 when the provider omitted the returned model and the
--                       row's model_returned was taken from the request, 0 when
--                       the provider named the model itself.
--
-- Additive, no data mutation, no backfill. A row written before this migration
-- reads usage_reason '' and model_from_request 0.
ALTER TABLE llm_categorization_attempts ADD COLUMN IF NOT EXISTS usage_reason LowCardinality(String) DEFAULT '';
ALTER TABLE llm_categorization_attempts ADD COLUMN IF NOT EXISTS model_from_request UInt8 DEFAULT 0;
