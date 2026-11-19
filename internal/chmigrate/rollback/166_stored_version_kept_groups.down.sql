-- Rollback of 166_stored_version_kept_groups.sql. It is NOT part of the chain (the migrator reads sql/ only); an operator runs
-- it by hand, and only after the worker and stream-runner images are back on a build without 166 (a build with 166 refuses
-- to start on a schema that lacks it, and its writers read the two kept tables after each write).
--
-- The views go first, so no insert feeds a table that is about to go; then the two kept tables. The base tables keep every
-- row: a repair row a build with 166 wrote holds the FINAL row with a stated group, which is a correct value.
-- The version row goes last: with it left in schema_migrations, a later `dho migrate clickhouse upgrade` on a build that has
-- the migration reads it as applied and never applies it again.
DROP VIEW IF EXISTS git_pull_request_kept_mv;
DROP VIEW IF EXISTS deployment_kept_mv;
DROP TABLE IF EXISTS git_pull_request_kept;
DROP TABLE IF EXISTS deployment_kept;
ALTER TABLE schema_migrations DELETE WHERE version = '166_stored_version_kept_groups.sql' SETTINGS mutations_sync = 2;
