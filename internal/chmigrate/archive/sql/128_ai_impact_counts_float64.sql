-- CHAOS-9604 (D5994, D5988, D5998): a team that co-owns a repository takes the
-- repository's pull-request counts at its own linked share, so an ai_impact row
-- can hold a fraction of a count. The twelve count columns become Float64; the
-- ratios, averages and the leverage components stay the repository's own.
-- Nothing is rounded (D5991). Existing whole numbers convert exactly. The
-- columns are in no sorting key, partition key or version column.
ALTER TABLE ai_impact_metrics_daily
    MODIFY COLUMN prs_total Float64,
    MODIFY COLUMN prs_merged Float64,
    MODIFY COLUMN ai_assisted_prs Float64,
    MODIFY COLUMN agent_created_prs Float64,
    MODIFY COLUMN human_prs Float64,
    MODIFY COLUMN unknown_prs Float64,
    MODIFY COLUMN agent_created_pr_count Float64,
    MODIFY COLUMN rework_prs Float64,
    MODIFY COLUMN followup_commits_count Float64,
    MODIFY COLUMN revert_prs Float64,
    MODIFY COLUMN incidents_count Float64,
    MODIFY COLUMN test_gap_prs Float64;
