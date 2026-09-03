-- Migration 085: run-tracking tables for the work_item_attribution daily
-- backstop kind (CHAOS-3092 PR-B).
--
-- WHY A WATERMARK TABLE
--   The daily backstop only re-derives work_item_team_attributions for items
--   whose scope (repo/project/org) had an OWNERSHIP change since the last
--   backstop run -- unlike the sync-time deriver (which re-derives on every
--   item sync) and unlike the retired Python daily sweep (which re-derived
--   every work item loaded that day, unconditionally). Detecting "changed
--   since" needs a persisted watermark per scope; there is nothing to diff
--   against without one.
--
-- SHAPE mirrors work_unit_membership_runs / work_unit_membership_scoped_runs
-- (migrations 047, 059, CHAOS-2433): one org-wide marker table, one scoped
-- marker table. A scoped (repo- or project-level) run never supersedes the
-- org-wide marker, and an org-wide run's watermark covers every scope.
--
-- NOTE: the migration runner strips line comments and splits on semicolon
-- before executing each statement, so each statement must end with a
-- semicolon and comments must not contain semicolons.
CREATE TABLE IF NOT EXISTS work_item_attribution_backstop_runs (
    org_id       String,
    run_id       String,
    completed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(completed_at)
ORDER BY (org_id, run_id);

-- scope_kind is 'repo' or 'project' (the two ownership-table triggers this
-- backstop scopes on -- see the ruling in CHAOS-3092 PR-B). An
-- identities/teams change is org-wide by design (team = ownership; admin
-- membership has no single-repo/project scope to key on), so it is recorded
-- via work_item_attribution_backstop_runs, never this table.
CREATE TABLE IF NOT EXISTS work_item_attribution_backstop_scoped_runs (
    org_id       String,
    scope_kind   LowCardinality(String),
    scope_id     String,
    run_id       String,
    completed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(completed_at)
ORDER BY (org_id, scope_kind, scope_id, run_id);
