-- Rollback of 126_deployments_pull_request_tier.sql (CHAOS-9610). It is NOT part of
-- the chain (the migrator reads sql/ only); an operator runs it by hand after the
-- writers are back on a build that does not name pull_request_tier in its INSERT.
--
-- Dropping the column loses the stored tiers; the pull request numbers are not
-- touched. The next sync on the newer build writes the tiers again.
-- The version row goes too: with it left in schema_migrations, a later
-- `dho migrate clickhouse upgrade` on a build that has the migration reads it
-- as applied and never applies it again, so the schema would stay rolled back
-- under a build that expects it.
ALTER TABLE deployments DROP COLUMN IF EXISTS pull_request_tier;
ALTER TABLE schema_migrations DELETE WHERE version = '126_deployments_pull_request_tier.sql' SETTINGS mutations_sync = 2;
