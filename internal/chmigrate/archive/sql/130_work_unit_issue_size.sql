-- CHAOS-9619 (D5999: Investment effort is the size of the issues a work unit
-- worked on; a unit with no size counts 1; lines of code are never effort).
--
-- One row per work unit and materialization run: the sum of the positive
-- story_points of the unit's issues (work_items.story_points, the one column
-- every provider writes its size to: Jira story points, GitLab weight, Linear
-- estimate, GitHub Projects number field), how many of its issues carry a size,
-- and how many issues it has. issue_size 0 = the unit carries no size; a reader
-- then weighs it 1 (the work-unit count).
--
-- A table of its own, not a column of work_unit_investments: the materializer
-- does not rewrite the investment row of an unchanged unit, so a column there
-- would keep the size of the first categorization forever. Like
-- work_unit_repo_effort, this row is written for every unit of every run, so a
-- size set later in the provider reaches the readers at the next run.
--
-- Readers take the newest row per (org_id, work_unit_id) with argMax over
-- computed_at; a unit with no row is unsized.
CREATE TABLE IF NOT EXISTS work_unit_issue_size (
    org_id String,
    work_unit_id String,
    issue_size Float64,
    sized_issue_count UInt32,
    issue_count UInt32,
    categorization_run_id String,
    computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(computed_at)
ORDER BY (org_id, work_unit_id)
SETTINGS index_granularity = 8192;
