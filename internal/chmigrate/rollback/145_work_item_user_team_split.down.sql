-- Rollback of 145_work_item_user_team_split.sql. It is NOT part of the chain (the
-- migrator reads sql/ only); an operator runs it by hand after the writers are
-- back on a build that does not name the team_split_* columns in its INSERT.
--
-- Dropping the columns loses the team split of each person's row; the landscape
-- of a build without them reads no split. No other column is touched.
-- The version row goes too: with it left in schema_migrations, a later
-- `dho migrate clickhouse upgrade` on a build that has the migration reads it
-- as applied and never applies it again.
ALTER TABLE work_item_user_metrics_daily DROP COLUMN IF EXISTS team_split_team_ids;
ALTER TABLE work_item_user_metrics_daily DROP COLUMN IF EXISTS team_split_is_primary;
ALTER TABLE work_item_user_metrics_daily DROP COLUMN IF EXISTS team_split_items_completed;
ALTER TABLE work_item_user_metrics_daily DROP COLUMN IF EXISTS team_split_wip_end_of_day;
ALTER TABLE schema_migrations DELETE WHERE version = '145_work_item_user_team_split.sql' SETTINGS mutations_sync = 2;
