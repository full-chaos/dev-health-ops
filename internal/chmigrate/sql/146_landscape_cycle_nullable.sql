-- A person's 30-day landscape cycle can be "no cycle" (CHAOS-12562).
--
--   ic_landscape_rolling_30d.cycle_p50_30d_hours  Float64 DEFAULT 0 -> Nullable(Float64) DEFAULT NULL:
--   NULL when no day of the person's window holds a merged pull request.
--
-- A stored 0 is a measured cycle of 0 hours. The default is NULL, so the row of zeros that the stale-key rule writes
-- (its keys only) holds no cycle and is no measure (teamkeytables NullableMeasures).
--
-- A row of zeros stored before this migration holds the old default 0 in the cycle, and the liveness test reads a
-- Nullable measure as live when it is not NULL: such a row would read as a live point again. So those rows are set to
-- NULL. The test is exact: every other measure of a row of zeros is 0, and a computed point never holds 0 in x_norm
-- (its rank in its team is at least 0.5 / n). Computed points are NOT rewritten: a stored 30-day 0 can be a real 0 or
-- "no cycle", and a stored row cannot tell them apart; a recompute of the day (finalize-redrive) writes the new value.
-- Every statement can run again.
ALTER TABLE ic_landscape_rolling_30d MODIFY COLUMN cycle_p50_30d_hours Nullable(Float64) DEFAULT NULL;
ALTER TABLE ic_landscape_rolling_30d UPDATE cycle_p50_30d_hours = NULL
WHERE cycle_p50_30d_hours = 0 AND x_raw = 0 AND y_raw = 0 AND x_norm = 0 AND y_norm = 0
  AND churn_loc_30d = 0 AND delivery_units_30d = 0 AND wip_max_30d = 0
SETTINGS mutations_sync = 2;
