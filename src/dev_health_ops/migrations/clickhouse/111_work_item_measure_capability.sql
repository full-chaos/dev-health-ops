-- Whether a work-item provider tracks a measure at all (CHAOS-8895).
-- work_item_metrics_daily stores bug_completed_ratio and story_points_completed
-- as non-null Float64, so a stored 0 and "this provider does not track the
-- measure" read the same. This table keeps that fact apart, so a reader can
-- answer not-applicable instead of 0.
--
-- The daily metrics job writes it once for each run (finalize family
-- work_item_measure_capability), from every work item of the organization in
-- a 90-day window that ends on the run's target day:
--   measure story_points_completed: tracked = 1 when an item of the provider
--     in the window has story points, else 0.
--   measure bug_completed_ratio: tracked = 1 when an item of the provider in
--     the window has the type 'bug', else 0.
-- A provider with no item in the window gets NO row: a missing row is unknown,
-- never 0 and never "not tracked". evidence_count is the number of items that
-- carry the measure and item_count the number of items of the provider in the
-- window; window_start and window_end are its first and last day.
--
-- window_end is in the sorting key so that a run for an old day (a backfill)
-- adds the answer of its own window and never replaces the answer of a newer
-- window. Reader contract: take the row with the latest window_end at or
-- before the end of the question, and argMax on computed_at for that key.
-- Merges are eventual, so a reader always deduplicates and never reads the
-- rows as they are.
CREATE TABLE IF NOT EXISTS work_item_measure_capability (
    org_id String,
    provider LowCardinality(String),
    measure LowCardinality(String),
    tracked UInt8,
    evidence_count UInt32,
    item_count UInt32,
    window_start Date,
    window_end Date,
    computed_at DateTime('UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (org_id, provider, measure, window_end);
