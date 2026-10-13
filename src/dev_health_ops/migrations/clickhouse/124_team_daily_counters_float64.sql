-- CHAOS-9601 (D5994, D5988): a team that co-owns a repository takes the
-- repository's additive counters at its own linked share, so a team row can
-- hold a fraction of a count. The counter columns of the two team-keyed daily
-- tables that stored whole numbers become Float64; nothing is rounded (D5991).
-- Existing whole numbers convert exactly. The columns are in no sorting key,
-- partition key or ver column.
ALTER TABLE team_complexity_daily
    MODIFY COLUMN loc_total Float64,
    MODIFY COLUMN cyclomatic_total Float64,
    MODIFY COLUMN high_complexity_functions Float64,
    MODIFY COLUMN very_high_complexity_functions Float64;

ALTER TABLE team_metrics_daily
    MODIFY COLUMN commits_count Float64,
    MODIFY COLUMN after_hours_commits_count Float64,
    MODIFY COLUMN weekend_commits_count Float64;
