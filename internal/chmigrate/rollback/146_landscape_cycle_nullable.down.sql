-- Rollback of 146_landscape_cycle_nullable.sql (CHAOS-12562). It is NOT part of the chain (the migrator reads sql/
-- only); an operator runs it by hand after the writers are back on a build that writes 0 for "no cycle".
--
-- Each NULL is first written as 0 (the value the older build reads as no cycle), then the column is narrowed back to
-- Float64 DEFAULT 0, its original type and default. The difference between a measured 0 and no cycle is lost for the
-- rows already written.
-- The version row goes too: with it left in schema_migrations, a later
-- `dho migrate clickhouse upgrade` on a build that has the migration reads it
-- as applied and never applies it again, so the schema would stay rolled back
-- under a build that expects it.
ALTER TABLE ic_landscape_rolling_30d UPDATE cycle_p50_30d_hours = 0 WHERE cycle_p50_30d_hours IS NULL SETTINGS mutations_sync = 2;
ALTER TABLE ic_landscape_rolling_30d MODIFY COLUMN cycle_p50_30d_hours Float64 DEFAULT 0;
ALTER TABLE schema_migrations DELETE WHERE version = '146_landscape_cycle_nullable.sql' SETTINGS mutations_sync = 2;
