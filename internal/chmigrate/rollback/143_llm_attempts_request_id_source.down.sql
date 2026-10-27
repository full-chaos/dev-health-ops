-- Rollback of 143_llm_attempts_request_id_source.sql. It is NOT part of the chain (the
-- migrator reads sql/ only); an operator runs it by hand after the writers are
-- back on a build that does not name request_id_source in its INSERT.
--
-- Dropping the column loses the source of each request id; no other column is touched.
-- The version row goes too: with it left in schema_migrations, a later
-- `dho migrate clickhouse upgrade` on a build that has the migration reads it
-- as applied and never applies it again.
ALTER TABLE llm_categorization_attempts DROP COLUMN IF EXISTS request_id_source;
ALTER TABLE schema_migrations DELETE WHERE version = '143_llm_attempts_request_id_source.sql' SETTINGS mutations_sync = 2;
