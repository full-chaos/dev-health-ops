-- Migration 077 (CHAOS-4194, final shape ruled by Context Fabric 2026-08-24):
-- append-only project MEMBERSHIP reassignment events, for work items AND pull
-- requests.
--
-- Why a new table and not columns on work_items. work_items is a
-- ReplacingMergeTree keyed on the work item, so a project reassignment
-- OVERWRITES project_id and the previous value is gone once ClickHouse
-- compacts. There is no FINAL-versioned history to recover it from later.
-- That is not a gap this table backfills -- the lost values are lost -- it is
-- the reason every future reassignment gets its own durable row. Pull requests
-- had no project column to overwrite in the first place: git_pull_requests
-- carries no project fields at all, so a PR's board membership was never
-- anywhere in the graph.
--
-- Why one table for both subjects rather than two. GitHub Projects V2 puts
-- issues and pull requests on the SAME board through the same items
-- connection, so the membership fact is one fact with two subject shapes.
-- subject_kind carries the shape, and splitting it into two tables would fork
-- the presence projection and the CHAOS-4193 validity-interval derivation for
-- no semantic gain.
--
-- Engine and read convention mirror work_item_transitions exactly:
-- ReplacingMergeTree(last_synced), read through SELECT ... FINAL.
--
-- ORDER BY (org_id, subject_kind, repo_id, subject_id, occurred_at, event_id)
-- mirrors the Context Fabric identity registry's natural keys
-- (acr/internal/contextfabric/identity/registry.go:50-56): a work_item is
-- identified by (repo_id, work_item_id) and a pull request by (repo_id,
-- number), so repo_id is a key member for BOTH subject kinds rather than a
-- carried-along attribute. subject_kind precedes it because the two subjects
-- share a repo_id while their subject_id spaces are unrelated -- interleaving
-- two disjoint histories under one prefix buys nothing and costs selectivity.
--
-- subject_id per kind:
--   work_item     -> the canonical work_items.work_item_id, and repo_id is
--                    that row's repo_id (uuid.Nil only where the provider is
--                    unscoped, i.e. jira and linear).
--   pull_request  -> the PR number as a decimal string, scoped by the repo
--                    uuid. There is no work_item_id for a PR and inventing one
--                    would key the row to a `work_items` row that does not
--                    exist.
--
-- event_id is in the sorting key because ClickHouse has no unique constraint.
-- Dedupe on re-sync is the ENGINE's job here, not the sink's: a provider that
-- replays the same reassignment collapses to one row because the key matches,
-- and last_synced picks the newer observation. Drop event_id from the key and
-- two genuinely distinct reassignments that share a timestamp silently merge
-- into one. It is content-determined -- native provider id when the provider
-- exposes one, else hash(org_id, subject_kind, repo_id, subject_id,
-- from_project_id, to_project_id, occurred_at) -- so a re-sync of one event
-- produces one value and two different events cannot share one. The sink
-- refuses a same-event_id/different-content pair within a batch rather than
-- letting FINAL choose arbitrarily between them.
--
-- Nullability: source_id is the ONLY Nullable column, matching
-- work_item_transitions.source_id (065_external_ingest_source_id.sql:13).
-- Every other shared column mirrors work_item_transitions' own bare-String
-- convention, actor included: an unattributed reassignment writes "" rather
-- than NULL, exactly as external_clickhouse.go already does for
-- work_item_transitions. from_project_id/from_project_key are "" on a first
-- assignment for the same reason from_status is.
--
-- from_project_id is the project LEFT and to_project_id the project JOINED, so
-- one row carries both sides of a move (Context Fabric, 2026-08-24): an add is
-- ("", P), a removal is (P, ""), and a move is (P, Q). Keeping both sides in a
-- single row is what lets the presence view retire P and create Q from one
-- observed event, rather than making a consumer pair up two rows and guess
-- whether a missing partner means "not yet synced" or "never happened".
--
-- A removal MUST name the board it left. ("", "") is refused by the sink,
-- because presence is keyed per (subject, project): a row naming neither side
-- could not retire or create any membership, and would sit in the history
-- looking like a removal that silently did nothing. Under the earlier
-- one-project-per-subject keying that shape meant "removed from everything",
-- but it has no meaning now, so it is a contradiction rather than a sentinel.
--
-- An id with NO key is entirely normal and stays accepted: GitHub Projects V2
-- boards have a number and a title and no key concept at all. The mirror --- a
-- key with no id --- is refused, since (provider, id) is what resolves to a
-- `projects` row.
--
-- VOCABULARY CONSTRAINT: these rows carry provider PROJECT entities only --
-- (provider, project_id) must resolve to a row in `projects`. There is a THIRD
-- meaning of "project" in this schema that must never reach here: the
-- external-ingest path writes the REPOSITORY full name into
-- work_items.project_id for github and gitlab (external_clickhouse.go:556-574).
-- The sink refuses such a value on the way in, and the presence view's arm
-- filters it out on the way back (see the arm below). gitlab is not registered
-- for this kind at all -- GitLab's own "project" concept IS this schema's
-- repo_id, so a gitlab row would violate the constraint by construction and
-- the registry refusal is the fail-closed guard.
--
-- No *_raw variants. work_item_transitions carries from_status_raw/to_status_raw
-- because status is NORMALIZED into a closed enum and the provider's own word
-- would otherwise be unrecoverable. Project identity is not normalized -- the
-- sink passes the provider's id and key through verbatim -- so a raw variant
-- would be a byte-for-byte duplicate column. Recorded here rather than left
-- silent, per the lock's raw-variant open item.
CREATE TABLE IF NOT EXISTS project_membership_transitions (
    org_id String,
    source_id Nullable(UUID),
    repo_id UUID,
    subject_kind LowCardinality(String),
    subject_id String,
    provider LowCardinality(String),
    from_project_id String,
    to_project_id String,
    from_project_key String,
    to_project_key String,
    actor String,
    occurred_at DateTime64(3),
    last_synced DateTime64(3),
    event_id String
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (org_id, subject_kind, repo_id, subject_id, occurred_at, event_id);

-- Presence projection (CHAOS-4194 deliverable 2), keyed PER (subject, project)
-- as Context Fabric ruled on 2026-08-24.
--
-- A subject can hold several memberships at once. GitHub Projects V2 puts one
-- pull request on as many boards as someone adds it to, and the producer emits
-- one row per board. An earlier version of this view argMax'd a single project
-- per subject, which answered "the most recently joined board" and silently
-- dropped the rest. Worse, it did not degrade gracefully: removing the subject
-- from the LATEST board made the transition arm yield nothing at all, so a PR
-- still on board A disappeared from presence entirely because it had left
-- board B. One correct answer became none.
--
-- The row shape carries both sides of a move: from_project_id is the project
-- LEFT and to_project_id the project JOINED, so an add is ("", P), a removal is
-- (P, ""), and a move is (P, Q) in a single row. Each row therefore TOUCHES up
-- to two memberships, and each membership is decided independently:
--
--   (subject, P) is ACTIVE iff the latest row by (occurred_at, event_id)
--   among the rows touching P has to_project_id = P.
--
-- Read that as "the last thing that happened to this membership was joining
-- it". A move (P, Q) is the last word on BOTH memberships at once -- it retires
-- P and creates Q -- which is exactly why both sides live in one row instead of
-- two rows a consumer would have to pair up.
--
-- ("", "") cannot occur: the sink refuses it, because a row naming neither side
-- could not retire or create any membership and would sit in the history
-- looking like a removal that did nothing. That refusal is what lets the
-- arrayFilter below assume every row contributes at least one touch.
--
-- The FINAL inside the CTE is load-bearing, not conventional: argMax over
-- (occurred_at, event_id) is deterministic only because that pair is unique
-- within a group, which is what FINAL plus the sorting key provide. Without it
-- a duplicate part could hand back the project id from one row and the project
-- key from another -- an edge that reads as real and joins to nothing. The id
-- and key are carried as a TUPLE through the arrayJoin for the same reason:
-- from_project_key belongs to from_project_id and to_project_key to
-- to_project_id, and unpivoting them separately would let a row's key drift
-- onto the other row's id.
--
-- arrayDistinct guards the degenerate (P, P) row -- a provider re-asserting a
-- membership it already had. Without it that row would contribute the same
-- touch twice, which changes no verdict but makes the intermediate result
-- misleading to anyone reading it.
--
-- The COLUMN arm is deliberately NOT keyed per project. work_items.project_id
-- holds one current value and carries no history, so it can only ever describe
-- a single membership, and it is excluded exactly when the subject has ANY
-- transition row, not when it has a row for that particular project. A subject
-- with observed history is answered by that history, whole -- mixing the
-- history-less column value into a per-project answer would invent a membership
-- nobody observed.
--
-- Single-project subjects degenerate to the previous shape: one touched
-- project, one active row. jira and linear work items, which belong to one
-- project at a time, are unaffected by this keying.
CREATE OR REPLACE VIEW project_membership_presence AS
WITH touched AS (
    SELECT
        org_id,
        subject_kind,
        repo_id,
        subject_id,
        provider,
        occurred_at,
        event_id,
        to_project_id,
        arrayJoin(arrayDistinct(arrayFilter(
            pair -> pair.1 != '',
            [(to_project_id, to_project_key), (from_project_id, from_project_key)]
        ))) AS touch
    FROM project_membership_transitions FINAL
),
latest_membership AS (
    SELECT
        org_id,
        subject_kind,
        repo_id,
        subject_id,
        touch.1 AS project_id,
        argMax(touch.2, (occurred_at, event_id)) AS project_key,
        argMax(provider, (occurred_at, event_id)) AS provider,
        argMax(to_project_id, (occurred_at, event_id)) AS latest_to_project_id,
        max(occurred_at) AS observed_at
    FROM touched
    GROUP BY org_id, subject_kind, repo_id, subject_id, project_id
),
subjects_with_history AS (
    SELECT DISTINCT org_id, subject_kind, repo_id, subject_id
    FROM project_membership_transitions FINAL
)
SELECT
    org_id,
    subject_kind,
    repo_id,
    subject_id,
    provider,
    project_id,
    project_key,
    observed_at,
    'transition' AS source
FROM latest_membership
WHERE latest_to_project_id = project_id
UNION ALL
SELECT
    w.org_id AS org_id,
    'work_item' AS subject_kind,
    w.repo_id AS repo_id,
    w.work_item_id AS subject_id,
    w.provider AS provider,
    w.project_id AS project_id,
    w.project_key AS project_key,
    w.updated_at AS observed_at,
    'work_item_column' AS source
FROM work_items AS w FINAL
WHERE w.project_id != ''
    AND w.provider != 'gitlab'
    AND (w.provider != 'github' OR startsWith(w.project_id, 'ghprojv2:'))
    AND (w.org_id, 'work_item', w.repo_id, w.work_item_id) NOT IN (
        SELECT org_id, subject_kind, repo_id, subject_id FROM subjects_with_history
    );
