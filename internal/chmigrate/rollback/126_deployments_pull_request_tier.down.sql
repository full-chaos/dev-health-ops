-- Rollback of 126_deployments_pull_request_tier.sql (CHAOS-9610). It is NOT part of
-- the chain (the migrator reads sql/ only); an operator runs it by hand after the
-- writers are back on a build that does not name pull_request_tier in its INSERT.
--
-- Dropping the column loses the stored tiers; the pull request numbers are not
-- touched. The next sync on the newer build writes the tiers again.
ALTER TABLE deployments DROP COLUMN IF EXISTS pull_request_tier;
