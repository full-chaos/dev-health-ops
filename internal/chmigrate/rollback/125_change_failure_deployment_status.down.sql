-- Rollback of 125_change_failure_deployment_status.sql (CHAOS-9621). It is NOT
-- part of the chain (the migrator reads sql/ only); an operator runs it by hand
-- after the writers are back on a build that does not write the two columns
-- (a writer of this release fails its insert once they are gone).
--
-- A helm rollback does not drop a column. The drop loses the status counts of
-- every row already written: failed_deployments_status and
-- deployments_status_known. A reader of the build before 125 never read them,
-- so it keeps its link-only numerator and its old unknown rule. The next
-- compute of a day writes the row without them. deployments_count keeps the
-- values written since 125 (a cancelled deployment is not counted); only its
-- comment returns to none, as before 125.
-- The version row goes too: with it left in schema_migrations, a later
-- `dho migrate clickhouse upgrade` on a build that has the migration reads it
-- as applied and never applies it again, so the schema would stay rolled back
-- under a build that expects it.
ALTER TABLE repo_change_failure_daily COMMENT COLUMN deployments_count '';
ALTER TABLE repo_change_failure_daily DROP COLUMN IF EXISTS deployments_status_known;
ALTER TABLE repo_change_failure_daily DROP COLUMN IF EXISTS failed_deployments_status;
ALTER TABLE schema_migrations DELETE WHERE version = '125_change_failure_deployment_status.sql' SETTINGS mutations_sync = 2;
