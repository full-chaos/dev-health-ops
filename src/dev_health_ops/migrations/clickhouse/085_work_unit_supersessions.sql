-- CHAOS-4441 plan.md section 5a, option A (ruled by team-lead 2026-09-01):
-- an explicit supersession record for the investment.materialize dedup
-- obligation.
--
-- THE PROBLEM THIS TABLE CLOSES
--
-- Nothing in work_unit_investments/work_unit_repo_effort marks a row dead.
-- A superseded work_unit_id today is retired ONLY by falling out of the
-- next complete work_unit_membership_runs marker, and readers honour that
-- only while investment_membership_scope.py's scope gate is ON
-- (scope_enabled = 1, "scoped" mode). Under "unscoped_fallback" -- which is
-- the state EVERY materialize run enters for the duration between writing
-- new investment rows and the next membership projection completing, and
-- INDEFINITELY if that projection fails or lags (CHAOS-4312) -- the
-- superseded id's investment and repo-effort rows stay readable ALONGSIDE
-- the reunited unit's, double-counting the same effort.
--
-- variant C (CHAOS-4758-adjacent grouping change) supersedes 479 stored
-- ids while minting 635 replacements on org 70d529e0 alone -- that whole
-- population resurrects in every fallback window without this table.
--
-- WHY A SIDECAR, NOT A TOMBSTONE ROW OR A RUN MARKER
--
-- A tombstone row in work_unit_investments would fight the ReplacingMergeTree
-- version column (computed_at): the tombstone must always win argMax, and
-- any later legitimate write for that id would silently resurrect it. A
-- run marker mirroring work_unit_membership_runs was explicitly considered
-- and rejected in backfill.py:52-64 -- investment rows are written per-
-- component batch with no globally consistent categorization_run_id, so
-- "the latest complete investments run" is not well-defined today. A
-- sidecar table is additive (no existing RMT version-column semantics to
-- reason about), trivially auditable ("what retired this id and when"),
-- and readers apply it as a plain left-anti-join.
--
-- BINDING CONDITION: a reader must honour this table INDEPENDENTLY of
-- investment_membership_scope.py's scope_enabled gate. Folding it into
-- that gate's own OR-condition would make the sidecar inherit the exact
-- bypass window it exists to close.
CREATE TABLE IF NOT EXISTS work_unit_supersessions (
    org_id                  String,
    superseded_work_unit_id String,
    superseded_by_run_id    String,
    superseded_at           DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(superseded_at)
ORDER BY (org_id, superseded_work_unit_id);
