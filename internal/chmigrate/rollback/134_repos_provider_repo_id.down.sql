-- Rollback of 134_repos_provider_repo_id.sql (CHAOS-3383). It is NOT part of
-- the chain (the migrator reads sql/ only); an operator runs it by hand after
-- the writers are back on a build that does not name provider_repo_id in its
-- INSERT or its stored-version read.
--
-- Dropping the column loses each repository's stored provider id; no other
-- column is touched. The next GitLab repository sync on the newer build writes
-- it again.
-- The version row goes too: with it left in schema_migrations, a later
-- `dho migrate clickhouse upgrade` on a build that has the migration reads it
-- as applied and never applies it again, so the schema would stay rolled back
-- under a build that expects it.
ALTER TABLE repos DROP COLUMN IF EXISTS provider_repo_id;
ALTER TABLE schema_migrations DELETE WHERE version = '134_repos_provider_repo_id.sql' SETTINGS mutations_sync = 2;
