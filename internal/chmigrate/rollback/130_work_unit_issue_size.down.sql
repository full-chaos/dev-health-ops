-- Rollback of 130_work_unit_issue_size.sql (CHAOS-9807). It is NOT part of the
-- chain (the migrator reads sql/ only); an operator runs it by hand after the
-- materializer and the readers are back on a build that does not name the table.
--
-- Dropping the table loses the stored unit sizes; no other table is touched.
-- The version row goes too: with it left in schema_migrations, a later
-- `dho migrate clickhouse upgrade` on a build that has the migration reads it
-- as applied and never creates the table again. The next materialization on
-- that build writes the sizes again.
DROP TABLE IF EXISTS work_unit_issue_size;
ALTER TABLE schema_migrations DELETE WHERE version = '130_work_unit_issue_size.sql' SETTINGS mutations_sync = 2;
