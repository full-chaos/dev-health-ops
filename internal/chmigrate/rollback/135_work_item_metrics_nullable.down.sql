-- Rollback of 135_work_item_metrics_nullable.sql (CHAOS-9920). It is NOT part of the chain (the migrator reads sql/
-- only); an operator runs it by hand after the writers are back on a build that writes 0 for "no data".
--
-- ClickHouse refuses to narrow a Nullable column without a DEFAULT expression. So each NULL is first written as 0
-- (the value the older build reads as "no data"), each column is narrowed with DEFAULT 0, and the default is then
-- dropped again (the original columns have none). The difference between a measured 0 and no data is lost for the
-- rows already written. The upgrade set every stored lead time of 0 to NULL (those 0 meant no lead time), so the
-- rollback gives those rows their 0 back: the data returns to what it was, the rows written after the upgrade included.
-- The version row goes too: with it left in schema_migrations, a later
-- `dho migrate clickhouse upgrade` on a build that has the migration reads it
-- as applied and never applies it again, so the schema would stay rolled back
-- under a build that expects it.
ALTER TABLE issue_type_metrics_daily UPDATE lead_p50_hours = 0 WHERE lead_p50_hours IS NULL SETTINGS mutations_sync = 2;
ALTER TABLE work_item_completion_daily UPDATE churn_loc = 0 WHERE churn_loc IS NULL SETTINGS mutations_sync = 2;
ALTER TABLE issue_type_metrics_daily MODIFY COLUMN lead_p50_hours Float64 DEFAULT 0;
ALTER TABLE work_item_completion_daily MODIFY COLUMN churn_loc UInt64 DEFAULT 0;
ALTER TABLE issue_type_metrics_daily MODIFY COLUMN lead_p50_hours Float64;
ALTER TABLE work_item_completion_daily MODIFY COLUMN churn_loc UInt64;
ALTER TABLE schema_migrations DELETE WHERE version = '135_work_item_metrics_nullable.sql' SETTINGS mutations_sync = 2;
