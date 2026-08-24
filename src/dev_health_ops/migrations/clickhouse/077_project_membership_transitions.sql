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
-- subject_kind carries the shape; splitting it into two tables would fork the
-- presence projection and the CHAOS-4193 validity-interval derivation for no
-- semantic gain.
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
-- to_project_id = '' AND to_project_key = '' is the UNASSIGNMENT sentinel:
-- the subject was removed from every project. It is the single value exempt
-- from the resolve-to-`projects` constraint below, and the presence view's
-- transition arm yields NO row for it -- deliberately NOT falling through to
-- the column arm, which would resurrect the stale current value as if the
-- removal had never been observed.
--
-- The view tests that sentinel with `to_project_id = ''` ALONE, not the
-- conjunction, and that is exact rather than sloppy: the sink refuses a
-- destination key with no project id, so an empty id already implies an empty
-- key by the time a row exists. Writing the conjunction anyway would add a
-- clause whose removal changes no behaviour -- a coverage claim nothing backs.
-- An id with NO key is the opposite case and is entirely normal: GitHub
-- Projects V2 boards have a number and a title and no key concept at all.
--
-- VOCABULARY CONSTRAINT: these rows carry provider PROJECT entities only --
-- (provider, project_id) must resolve to a row in `projects`. There is a THIRD
-- meaning of "project" in this schema that must never reach here: the
-- external-ingest path writes the REPOSITORY full name into
-- work_items.project_id for github and gitlab (external_clickhouse.go:556-574).
-- The sink refuses such a value on the way in; the presence view's column arm
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

-- Presence projection (CHAOS-4194 deliverable 2). The current-value
-- subject -> project edge, derived from the event stream, with a fallback to
-- the pre-CDC work_items column for work items that have no transition history
-- yet.
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
-- The three argMax calls are INDEPENDENT aggregates, so nothing structurally
-- forces them to describe the same row -- they agree only because
-- (occurred_at, event_id) is unique within each group. That uniqueness comes
-- from FINAL plus the sorting key, which is why the FINAL below is load-bearing
-- and not merely conventional. Drop it and a duplicate part could hand
-- project_id from one row and project_key from another, which reads as a real
-- edge and joins to nothing.
--
-- The unassignment filter is `project_id != ''` (see the sentinel note above
-- for why that is exact), and the column arm's anti-join runs against the
-- UNFILTERED latest_transition.
-- Both halves are load-bearing: a subject whose latest observed event removed
-- it from every project yields no presence row at all, and specifically does
-- NOT reappear through the column arm carrying the stale work_items value.
-- Anti-joining against the filtered set instead would do exactly that.
--
-- The column arm's provider predicate is the vocabulary fence. gitlab is
-- excluded outright: GitLab's "project" IS a repository in this schema, so
-- work_items.project_id is never a project entity there. github is admitted
-- only for the `ghprojv2:` prefix the Projects V2 route mints
-- (providersync/github_work_items_projects_v2.go), because the external-ingest
-- path writes the repository full name into the same column
-- (external_clickhouse.go:556-574) and a repo full name resolves to no
-- `projects` row. jira and linear have no repo-as-project ambiguity -- their
-- column already holds the provider's own project key/id -- so no prefix
-- applies to them.
--
-- The column arm emits subject_kind = 'work_item' only. There is no pull-request
-- current-value column anywhere to fall back to: git_pull_requests carries no
-- project fields, which is the defect this ticket exists to fix. A PR therefore
-- appears here exactly when a transition observed it, and never otherwise.
CREATE OR REPLACE VIEW project_membership_presence AS
WITH latest_transition AS (
    SELECT
        org_id,
        subject_kind,
        repo_id,
        subject_id,
        argMax(provider, (occurred_at, event_id)) AS provider,
        argMax(to_project_id, (occurred_at, event_id)) AS project_id,
        argMax(to_project_key, (occurred_at, event_id)) AS project_key,
        max(occurred_at) AS observed_at
    FROM project_membership_transitions FINAL
    GROUP BY org_id, subject_kind, repo_id, subject_id
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
FROM latest_transition
WHERE project_id != ''
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
        SELECT org_id, subject_kind, repo_id, subject_id FROM latest_transition
    );
