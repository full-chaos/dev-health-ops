-- Rollback of 141_work_item_row_view.sql (CHAOS-8863). It is NOT part of the chain (the migrator reads sql/ only); an
-- operator runs it by hand after the writers are back on a build that does not write row_view.
--
-- ClickHouse cannot take a column out of the sorting key, so the column row_view STAYS in the three tables. The
-- rollback removes what makes a build that does not know the column read wrongly: the rows of the views 'org' and
-- 'scope' (a build that does not take the view would sum them with the legacy rows). The legacy rows ('') are the
-- rows the older build wrote and read; a day that the newer writer recomputed has its legacy key superseded by a row
-- of zeros, so that day reads as empty until it is recomputed by the older build. The version row goes too: with it
-- left in schema_migrations, a later `dho migrate clickhouse upgrade` on a build that has the migration reads it as
-- applied and never applies it again.
ALTER TABLE work_item_metrics_daily DELETE WHERE row_view != '' SETTINGS mutations_sync = 2;
ALTER TABLE work_item_state_durations_daily DELETE WHERE row_view != '' SETTINGS mutations_sync = 2;
ALTER TABLE work_item_user_metrics_daily DELETE WHERE row_view != '' SETTINGS mutations_sync = 2;
ALTER TABLE schema_migrations DELETE WHERE version = '141_work_item_row_view.sql' SETTINGS mutations_sync = 2;
