-- Rollback of 140_work_unit_investment_effort_basis.sql. It is NOT part of the
-- chain (the migrator reads sql/ only); an operator runs it by hand after the
-- writers are back on a build that does not name effort_basis in its INSERT.
--
-- Dropping the column loses which units had capped commit stats; no other
-- column is touched.
-- The version row goes too: with it left in schema_migrations, a later
-- `dho migrate clickhouse upgrade` on a build that has the migration reads it
-- as applied and never applies it again.
ALTER TABLE work_unit_investments DROP COLUMN IF EXISTS effort_basis;
ALTER TABLE schema_migrations DELETE WHERE version = '140_work_unit_investment_effort_basis.sql' SETTINGS mutations_sync = 2;
