-- Rollback of 144_llm_attempts_endpoint.sql. It is NOT part of the chain (the
-- migrator reads sql/ only); an operator runs it by hand after the writers are
-- back on a build that does not name endpoint_type or gateway_id in its INSERT.
--
-- Dropping the columns loses the endpoint of each attempt; no other column is touched.
-- The version row goes too: with it left in schema_migrations, a later
-- `dho migrate clickhouse upgrade` on a build that has the migration reads it
-- as applied and never applies it again.
ALTER TABLE llm_categorization_attempts DROP COLUMN IF EXISTS endpoint_type;
ALTER TABLE llm_categorization_attempts DROP COLUMN IF EXISTS gateway_id;
ALTER TABLE schema_migrations DELETE WHERE version = '144_llm_attempts_endpoint.sql' SETTINGS mutations_sync = 2;
