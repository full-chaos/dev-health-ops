-- Rollback of 149_work_unit_investment_refs.sql. It is NOT part of the chain (the migrator reads sql/ only); an
-- operator runs it by hand. No writer names the two columns (ClickHouse fills them from their DEFAULT), so it needs
-- no writer rollback first; a reader that names them (CHAOS-10728) must be rolled back before it.
--
-- Dropping the columns loses nothing: each is a function of the row's structural_evidence_json, which stays.
-- mutations_sync = 2 waits for the parts to lose the columns, so an upgrade that adds them again starts clean.
-- The version row goes too: with it left in schema_migrations, a later
-- `dho migrate clickhouse upgrade` on a build that has the migration reads it
-- as applied and never applies it again.
ALTER TABLE work_unit_investments
    DROP COLUMN IF EXISTS issue_refs,
    DROP COLUMN IF EXISTS pr_refs
SETTINGS mutations_sync = 2;
ALTER TABLE schema_migrations DELETE WHERE version = '149_work_unit_investment_refs.sql' SETTINGS mutations_sync = 2;
