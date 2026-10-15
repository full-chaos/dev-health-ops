-- Rollback of 130_work_unit_issue_size.sql (CHAOS-9619). It is NOT part of the
-- chain (the migrator reads sql/ only); an operator runs it by hand after the
-- materializer and the readers are back on a build that does not name the table.
--
-- Dropping the table loses the stored unit sizes; no other table is touched.
-- The next materialization on the newer build writes them again.
DROP TABLE IF EXISTS work_unit_issue_size;
