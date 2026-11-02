-- A person's 30-day landscape cycle can be "no cycle" (CHAOS-12562).
--
--   ic_landscape_rolling_30d.cycle_p50_30d_hours  Float64 DEFAULT 0 -> Nullable(Float64) DEFAULT NULL:
--   NULL when no day of the person's window holds a merged pull request.
--
-- A stored 0 is a measured cycle of 0 hours. The default is NULL, so the row of zeros that the stale-key rule writes
-- (its keys only) holds no cycle and is no measure (teamkeytables NullableMeasures). Stored rows are NOT rewritten: a
-- stored 30-day 0 can be a real 0 or "no cycle", and a stored row cannot tell them apart. A recompute of the day
-- (finalize-redrive) writes the new value. Re-running the statement changes nothing.
ALTER TABLE ic_landscape_rolling_30d MODIFY COLUMN cycle_p50_30d_hours Nullable(Float64) DEFAULT NULL;
