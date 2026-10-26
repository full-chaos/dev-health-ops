-- Rollback of 142_llm_attempts_usage_reason.sql. It is NOT part of the chain (the
-- migrator reads sql/ only); an operator runs it by hand after the writers are
-- back on a build that does not name usage_reason or model_from_request in its
-- INSERT.
--
-- Dropping the columns loses the two facts per row; no other column is touched.
-- The version row goes too: with it left in schema_migrations, a later
-- `dho migrate clickhouse upgrade` on a build that has the migration reads it
-- as applied and never applies it again.
ALTER TABLE llm_categorization_attempts DROP COLUMN IF EXISTS usage_reason;
ALTER TABLE llm_categorization_attempts DROP COLUMN IF EXISTS model_from_request;
ALTER TABLE schema_migrations DELETE WHERE version = '142_llm_attempts_usage_reason.sql' SETTINGS mutations_sync = 2;
