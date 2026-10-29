-- work_item_completion_daily: the items completed on one day, by repository and team (CHAOS-9264).
--
-- investment_metrics_daily held the same counts but split each (repository, team, day) by the rows of the
-- retired rule classifier (investment_area, project_stream). That split is gone: the analytics measures
-- (count, throughput, cycle time by team, repository or work type) never needed it, and a rule-based area next to
-- the LLM categorization gave two answers to one question. This table has NO area or stream column; the
-- cycle p50 is one value for the whole (repository, team, day).
--
-- Plain MergeTree, append only, newest computed_at per key (like the table it replaces): when a recompute no longer
-- produces a key, the family writes a row of zeros for it, and a reader takes the newest row of each key.
-- The old table gets no new rows and no reader after this change; it is dropped later, on its own word.
CREATE TABLE IF NOT EXISTS work_item_completion_daily
(
    org_id String,
    repo_id Nullable(UUID),
    day Date,
    team_id LowCardinality(String),
    delivery_units UInt32,
    work_items_completed UInt32,
    prs_merged UInt32,
    churn_loc UInt64,
    cycle_p50_hours Float64,
    computed_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, day, team_id)
SETTINGS allow_nullable_key = 1;
