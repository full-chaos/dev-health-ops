-- The blocked-work evidence list needs the duration for each individual work
-- item. work_item_state_durations_daily only contains the aggregate by status,
-- scope, and team, so it cannot identify which items contributed that time.
--
-- The native daily worker writes one snapshot for each processed item that
-- contributes state time, including duration_hours = 0. A later recompute must
-- be able to replace an earlier positive result when the item is no longer
-- blocked. The stable identity is (org_id, day, provider, work_item_id): scope
-- and team are snapshot fields, not identity, because an item can move.
--
-- Reader contract: group by that stable identity and take
-- argMax(tuple(work_scope_id, team_id, team_name, duration_hours), computed_at)
-- before filtering duration_hours > 0. Filtering stored positive rows first
-- would resurrect an old positive value after a zero snapshot.
CREATE TABLE IF NOT EXISTS work_item_blocked_durations_daily (
    day Date,
    provider String,
    work_scope_id String,
    team_id String,
    team_name String,
    work_item_id String,
    duration_hours Float64,
    computed_at DateTime64(3, 'UTC'),
    org_id String
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, day, provider, work_item_id);
