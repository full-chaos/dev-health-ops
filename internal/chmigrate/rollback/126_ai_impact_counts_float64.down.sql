-- Rollback of 126_ai_impact_counts_float64.sql (CHAOS-9604). It is NOT part of
-- the chain (the migrator reads sql/ only); an operator runs it by hand after
-- the writers are back on a build that writes whole counts.
--
-- A helm rollback does not narrow a column. Narrowing converts each Float64 with
-- the ClickHouse cast: a whole number is kept exactly; a fraction (a co-owning
-- team's share of a count) is truncated toward zero, so 0.4 of a pull request
-- reads 0 and 1.8 reads 1. The fractions are lost for the rows already written;
-- the next compute of each day writes whole counts again. A negative value would
-- wrap, and none is ever written.
ALTER TABLE ai_impact_metrics_daily
    MODIFY COLUMN prs_total UInt32,
    MODIFY COLUMN prs_merged UInt32,
    MODIFY COLUMN ai_assisted_prs UInt32,
    MODIFY COLUMN agent_created_prs UInt32,
    MODIFY COLUMN human_prs UInt32,
    MODIFY COLUMN unknown_prs UInt32,
    MODIFY COLUMN agent_created_pr_count UInt32,
    MODIFY COLUMN rework_prs UInt32,
    MODIFY COLUMN followup_commits_count UInt32,
    MODIFY COLUMN revert_prs UInt32,
    MODIFY COLUMN incidents_count UInt32,
    MODIFY COLUMN test_gap_prs UInt32;
