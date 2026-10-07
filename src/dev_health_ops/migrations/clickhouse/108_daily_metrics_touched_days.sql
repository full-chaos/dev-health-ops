-- A work-items sync unit writes raw rows that belong to days other than the
-- days of its window: an item closed in week 2 and commented in week 12 is
-- fetched by the week-12 unit. The derived daily tables of week 2 are then
-- stale until a daily metrics run computes that day again. This table is the
-- record of which (organization, day, repository) the stored raw rows touched
-- and which of them a daily run was started for since.
--
-- Two event kinds for each (org_id, day, repo_id):
--   'touched'    appended by the post-sync fan-out. `at` is the ClickHouse
--                clock at the append, never a provider or worker clock.
--   'dispatched' appended by the post-sync fan-out after the Postgres
--                transaction that started the daily run of that day committed.
--                `at` is the ClickHouse clock read by the query that took the
--                day, so a 'touched' event appended after that read stays
--                newer.
-- repo_id is the nil UUID for the work items that have no repository.
--
-- Reader contract: group by (org_id, day, repo_id). The key is pending when
-- maxIf(at, kind = 'touched') > maxIf(at, kind = 'dispatched'). A key with no
-- 'dispatched' event is pending. The engine keeps the newest event for each
-- kind; merges are eventual, so a reader always aggregates and never reads the
-- rows as they are.
--
-- Every failure errs toward one more recompute: a 'dispatched' append that
-- fails leaves the day pending, and the next fan-out starts a run for it
-- again. A daily run is a full recompute from stored rows, so a second run
-- writes the same newest rows.
CREATE TABLE IF NOT EXISTS daily_metrics_touched_days (
    org_id String,
    day Date,
    repo_id UUID,
    kind LowCardinality(String),
    at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(at)
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, day, repo_id, kind);
