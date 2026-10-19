-- Rollback of 139_llm_token_usage_credential_source.sql. It is NOT part of the
-- chain (the migrator reads sql/ only); an operator runs it by hand after the
-- writers are back on a build that does not name credential_source in its INSERT.
--
-- Dropping the column loses which credential each row was spent with; no other
-- column is touched.
-- The version row goes too: with it left in schema_migrations, a later
-- `dho migrate clickhouse upgrade` on a build that has the migration reads it
-- as applied and never applies it again.
ALTER TABLE llm_token_usage DROP COLUMN IF EXISTS credential_source;
ALTER TABLE schema_migrations DELETE WHERE version = '139_llm_token_usage_credential_source.sql' SETTINGS mutations_sync = 2;
