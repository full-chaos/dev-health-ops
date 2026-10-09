-- Change-failure rate is incident-based (CHAOS-8981).
--
-- repo_metrics_daily.change_failure_rate holds reverted pull requests / merged
-- pull requests, with the denominator forced to 1 when nothing merged, so a
-- repository with no incident data at all read 0%. Change failure rate is now
-- the share of deployments linked to an incident. Revert rate is a metric of
-- its own, revert_rate.
--
-- The inputs of the incident-based rate live in repo_change_failure_daily, one
-- row for each repository and day that has a deployment or an incident tied to
-- the repository. A repository and day without one has no row: a missing row is
-- "no deployments and no incident evidence", never a measured 0. Readers sum the
-- counts over the view's window and subject (a team = its owned repositories)
-- and apply one rule (internal/jobs/metrics/changefailure.Rate):
--   no deployments              -> NULL, not applicable
--   no incident tied in window  -> NULL, unknown
--   else                        -> (failed_deployments_native +
--                                   failed_deployments_heuristic) / deployments_count
-- A failed deployment is a deployment with at least one deployment-incident
-- link (work_graph_deployment_incident_edges). Links are counted by tier:
-- a deployment with a native link counts as native, else as heuristic, so a
-- reader can name the lowest tier that contributes. An incident ties to a
-- repository directly (a service-to-repository mapping row) or, only when it
-- has no direct row, through the repository of its linked deployment.
--
-- Merges are eventual: a reader keeps the newest computed_at per
-- (org_id, repo_id, day) before it sums. computed_at keeps milliseconds so two
-- runs of one day in the same second do not tie.
CREATE TABLE IF NOT EXISTS repo_change_failure_daily (
    org_id String,
    repo_id UUID,
    day Date,
    deployments_count UInt32,
    failed_deployments_native UInt32,
    failed_deployments_heuristic UInt32,
    incidents_direct UInt32,
    incidents_via_deployment UInt32,
    computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, repo_id, day);

-- This migration only ADDS: old pods and a rollback keep reading the schema
-- they know, with the values they know (expand step. The contract step is
-- CHAOS-9017).
--
-- DEPRECATED (CHAOS-9017 drops it): repo_metrics_daily.change_failure_rate.
-- It stays Float64, not Nullable, and keeps its old meaning: reverted / merged
-- pull requests with the denominator forced to 1. The writer keeps writing
-- that value there. No reader of this release reads it, for change failure
-- rate or for revert rate.
--
-- change_failure_rate_incident is the incident-based one-day value for the
-- readers that read one repo_metrics_daily row: NULL when the day is not
-- applicable or unknown (see above).
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS change_failure_rate_incident Nullable(Float64);
-- revert_rate is reverted / merged pull requests. It is NULL (unknown) on every
-- row: no writer detects a reverted pull request yet, so nothing was measured.
-- The deprecated column is NOT copied into it. The reverted count behind that
-- column was always 0 (the pull request title it needs was never loaded), and
-- a copy would store an unmeasured 0% as a rate.
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS revert_rate Nullable(Float64);

-- The deployment-status ratio (failed deployment runs / deployments) is
-- deployment failure rate, not change failure rate. The DORA writer now stores
-- it in dora_metrics_daily under metric_name = 'deployment_failure_rate'. This
-- migration does not touch that table.
--
-- DEPRECATED (CHAOS-9017 renames or deletes them): the dora_metrics_daily rows
-- with metric_name = 'change_failure_rate', written before this change or by an
-- older pod. They stay as they are, so an older reader and a rollback see what
-- they saw before. No reader of this release selects them by name.
