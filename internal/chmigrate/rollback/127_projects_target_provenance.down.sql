-- Rollback of 127_projects_target_provenance.sql (CHAOS-9613). It is NOT part of
-- the chain (the migrator reads sql/ only); an operator runs it by hand after the
-- writers are back on a build that does not name these columns in its INSERT.
--
-- Dropping the columns loses the stored provenance; target_date is not touched.
-- The next sync on the newer build writes the provenance again.
-- The version row goes too: with it left in schema_migrations, a later
-- `dho migrate clickhouse upgrade` on a build that has the migration reads it
-- as applied and never applies it again, so the schema would stay rolled back
-- under a build that expects it.
ALTER TABLE projects DROP COLUMN IF EXISTS target_fetched_at;
ALTER TABLE projects DROP COLUMN IF EXISTS target_other_open;
ALTER TABLE projects DROP COLUMN IF EXISTS target_ref_name;
ALTER TABLE projects DROP COLUMN IF EXISTS target_ref;
ALTER TABLE projects DROP COLUMN IF EXISTS target_source;
ALTER TABLE projects DROP COLUMN IF EXISTS target_kind;
ALTER TABLE projects DROP COLUMN IF EXISTS target_state;
ALTER TABLE schema_migrations DELETE WHERE version = '127_projects_target_provenance.sql' SETTINGS mutations_sync = 2;
