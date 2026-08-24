-- Migration 077 (CHAOS-4194, shape locked with CHAOS-4193): append-only
-- work_item -> project reassignment events.
--
-- Why a new table and not columns on work_items. work_items is a
-- ReplacingMergeTree keyed on the work item, so a project reassignment
-- OVERWRITES project_id and the previous value is gone once ClickHouse
-- compacts. There is no FINAL-versioned history to recover it from later.
-- That is not a gap this table backfills -- the lost values are lost -- it is
-- the reason every future reassignment gets its own durable row.
--
-- Engine and read convention mirror work_item_transitions exactly:
-- ReplacingMergeTree(last_synced), read through SELECT ... FINAL.
--
-- event_id is in the sorting key because ClickHouse has no unique constraint.
-- Dedupe on re-sync is the ENGINE's job here, not the sink's: a provider that
-- replays the same reassignment collapses to one row because the key matches,
-- and last_synced picks the newer observation. Drop event_id from the key and
-- two genuinely distinct reassignments that share a timestamp silently merge
-- into one. The sink treats the value as opaque -- CHAOS-4193 owns the
-- derivation formula, this table owns only its position in the key.
--
-- ORDER BY omits repo_id even though the column is carried. jira and linear
-- have no repo scope and write uuid.Nil, so a repo_id in the key would buy no
-- selectivity for half the providers while splitting each work item's history
-- across two prefixes if a provider's repo attribution ever changed.
--
-- Nullability follows CHAOS-4194's provisional defaults, not the earlier
-- CHAOS-4193 draft: every column is non-nullable except actor. An
-- unattributed reassignment (provider automation, a bulk move) is a normal
-- event and there is no honest identity to write, whereas from_project_id and
-- from_project_key on a FIRST assignment have an honest empty value -- exactly
-- how work_item_transitions already handles from_status. If Context Fabric's
-- 4193 producer scope pass reverses this, the change is an ALTER on this
-- table, not a reshape of the key.
--
-- No *_raw variants. work_item_transitions carries from_status_raw/to_status_raw
-- because status is NORMALIZED into a closed enum and the provider's own word
-- would otherwise be unrecoverable. Project identity is not normalized -- the
-- sink passes the provider's id and key through verbatim -- so a raw variant
-- would be a byte-for-byte duplicate column. Recorded here rather than left
-- silent, per the lock's raw-variant open item.
CREATE TABLE IF NOT EXISTS work_item_project_transitions (
    org_id String,
    source_id Nullable(UUID),
    repo_id UUID,
    work_item_id String,
    provider LowCardinality(String),
    from_project_id String,
    to_project_id String,
    from_project_key String,
    to_project_key String,
    actor Nullable(String),
    occurred_at DateTime64(3),
    last_synced DateTime64(3),
    event_id String
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (org_id, work_item_id, occurred_at, event_id);

-- Presence projection (CHAOS-4194 deliverable 2). The current-value
-- work_item -> project edge, derived from the event stream, with a fallback to
-- the pre-CDC column for work items that have no transition history yet.
--
-- This is a VIEW and not a backfill INSERT on purpose. The alternative --
-- synthesising one transition row per existing work_items.project_id -- would
-- put events into CHAOS-4193's history table that no provider ever emitted,
-- with a fabricated event_id and an occurred_at that is really just "whenever
-- we happened to sync". CHAOS-4193 derives validity intervals from that
-- stream, so those rows would become invented ValidFrom boundaries presented as
-- observed fact. The fallback arm answers the same presence question without
-- writing anything, and the `source` column keeps the two provenances
-- distinguishable instead of blending them: 'transition' rows are observed
-- events, 'work_item_column' rows are the current value with no history behind
-- them. A consumer that needs history can filter to the former and get an
-- honest empty answer rather than a plausible fabricated one.
--
-- argMax over the (occurred_at, event_id) tuple, not over occurred_at alone:
-- two reassignments can share a timestamp (bulk moves land in the same second
-- routinely), and argMax with a non-unique ordering column returns an
-- arbitrary one of the tied rows. event_id is already the tiebreaker in the
-- sorting key, so reusing it here makes the projection deterministic and makes
-- it agree with the table's own order.
--
-- The four argMax calls are INDEPENDENT aggregates, so nothing structurally
-- forces them to describe the same row -- they agree only because
-- (occurred_at, event_id) is unique within each (org_id, work_item_id) group.
-- That uniqueness comes from FINAL plus the sorting key, which is why the FINAL
-- below is load-bearing and not merely conventional. Drop it and a duplicate
-- part could hand project_id from one row and project_key from another, which
-- reads as a real edge and joins to nothing.
--
-- The `project_id != ''` filter on the transition arm is what lets a future
-- unassignment event (to_project_id empty) retire an edge instead of
-- projecting an empty-string project. Nothing emits one today -- the schema
-- refuses it -- but the projection is written so that admitting one later is a
-- producer change, not a rewrite here.
CREATE OR REPLACE VIEW work_item_project_presence AS
WITH latest_transition AS (
    SELECT
        org_id,
        work_item_id,
        argMax(repo_id, (occurred_at, event_id)) AS repo_id,
        argMax(provider, (occurred_at, event_id)) AS provider,
        argMax(to_project_id, (occurred_at, event_id)) AS project_id,
        argMax(to_project_key, (occurred_at, event_id)) AS project_key,
        max(occurred_at) AS observed_at
    FROM work_item_project_transitions FINAL
    GROUP BY org_id, work_item_id
)
SELECT
    org_id,
    work_item_id,
    repo_id,
    provider,
    project_id,
    project_key,
    observed_at,
    'transition' AS source
FROM latest_transition
WHERE project_id != ''
UNION ALL
SELECT
    w.org_id AS org_id,
    w.work_item_id AS work_item_id,
    w.repo_id AS repo_id,
    w.provider AS provider,
    w.project_id AS project_id,
    w.project_key AS project_key,
    w.updated_at AS observed_at,
    'work_item_column' AS source
FROM work_items AS w FINAL
WHERE w.project_id != ''
    AND (w.org_id, w.work_item_id) NOT IN (
        SELECT org_id, work_item_id FROM latest_transition
    );
