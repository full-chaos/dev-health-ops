-- Rollback of 127_projects_target_provenance.sql (CHAOS-9613). It is NOT part of
-- the chain (the migrator reads sql/ only); an operator runs it by hand after the
-- writers are back on a build that does not name these columns in its INSERT.
--
-- Dropping the columns loses the stored provenance; target_date is not touched.
-- The next sync on the newer build writes the provenance again.
ALTER TABLE projects DROP COLUMN IF EXISTS target_fetched_at;
ALTER TABLE projects DROP COLUMN IF EXISTS target_other_open;
ALTER TABLE projects DROP COLUMN IF EXISTS target_ref_name;
ALTER TABLE projects DROP COLUMN IF EXISTS target_ref;
ALTER TABLE projects DROP COLUMN IF EXISTS target_source;
ALTER TABLE projects DROP COLUMN IF EXISTS target_kind;
ALTER TABLE projects DROP COLUMN IF EXISTS target_state;
