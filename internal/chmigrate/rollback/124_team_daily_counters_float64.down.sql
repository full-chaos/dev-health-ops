-- Rollback of 124_team_daily_counters_float64.sql (CHAOS-9601). It is NOT part of
-- the chain (the migrator reads sql/ only); an operator runs it by hand after the
-- writers are back on a build that writes whole counts.
--
-- A helm rollback does not narrow a column. Narrowing converts each Float64 with
-- the ClickHouse cast: a whole number is kept exactly; a fraction (a co-owning
-- team's share of a count) is truncated toward zero, so 0.4 of a commit reads 0
-- and 1.8 reads 1. The fractions are lost for the rows already written; the
-- next compute of each day writes whole counts again. A negative value would
-- wrap, and none is ever written (a share is in [0, 1] and a count is not
-- negative).
ALTER TABLE team_complexity_daily
    MODIFY COLUMN loc_total UInt64,
    MODIFY COLUMN cyclomatic_total UInt64,
    MODIFY COLUMN high_complexity_functions UInt64,
    MODIFY COLUMN very_high_complexity_functions UInt64;

ALTER TABLE team_metrics_daily
    MODIFY COLUMN commits_count UInt32,
    MODIFY COLUMN after_hours_commits_count UInt32,
    MODIFY COLUMN weekend_commits_count UInt32;

ALTER TABLE compounding_risk_daily
    MODIFY COLUMN merged_prs_30d Nullable(UInt32),
    MODIFY COLUMN reviewed_prs_30d Nullable(UInt32);
