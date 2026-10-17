-- Rollback of 138_repos_identity_path.sql (CHAOS-3383). It is NOT part of the
-- chain (the migrator reads sql/ only); an operator runs it by hand after the
-- writers are back on a build that does not name identity_path in its INSERT
-- or its stored-version read.
--
-- Dropping the column loses each repository's stored minting path; no other
-- column is touched. The next GitLab repository sync on the newer build writes
-- it again.
-- The version row goes too: with it left in schema_migrations, a later
-- `dho migrate clickhouse upgrade` on a build that has the migration reads it
-- as applied and never applies it again, so the schema would stay rolled back
-- under a build that expects it.
ALTER TABLE repos DROP COLUMN IF EXISTS identity_path;
ALTER TABLE schema_migrations DELETE WHERE version = '138_repos_identity_path.sql' SETTINGS mutations_sync = 2;
