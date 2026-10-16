-- Rollback of 137_teams_provider_identity.sql (CHAOS-9432). It is NOT part of
-- the chain (the migrator reads sql/ only); an operator runs it by hand after
-- the writers are back on a build that does not name these columns in its INSERT.
--
-- Dropping the columns loses each team's stored provider id, provider name and
-- gone time; no other column is touched. The next sync that lists a team on the
-- newer build writes the first two again.
-- The version row goes too: with it left in schema_migrations, a later
-- `dho migrate clickhouse upgrade` on a build that has the migration reads it
-- as applied and never applies it again, so the schema would stay rolled back
-- under a build that expects it.
ALTER TABLE teams DROP COLUMN IF EXISTS gone_at;
ALTER TABLE teams DROP COLUMN IF EXISTS provider_name;
ALTER TABLE teams DROP COLUMN IF EXISTS provider_team_id;
ALTER TABLE schema_migrations DELETE WHERE version = '137_teams_provider_identity.sql' SETTINGS mutations_sync = 2;
