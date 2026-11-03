-- project_membership_presence exposes only event time: observed_at is
-- max(occurred_at) on the transition arm and work_items.updated_at on the
-- column arm, both PROVIDER times. A consumer that reads the view
-- incrementally needs an ingest time, and the existing last_synced columns are
-- no help: every writer stamps project_membership_transitions.last_synced and
-- work_items.last_synced on the CLIENT with its own normalizedAt, and
-- last_synced is the ReplacingMergeTree version of both tables. Changing its
-- meaning would change which row wins on a replay or backfill, so it is left
-- exactly as it is.
--
-- ingested_at is a NEW column on both source tables, DEFAULT now64(3). No
-- writer sends a value: every INSERT into either table names its columns and
-- omits ingested_at, so the server stamps the row when it processes the
-- insert. It is not a version and not part of any sorting key.
--
-- Reader contract (also the column COMMENT, and the same text as
-- team_project_ownership.last_synced, migration 099): the view column
-- last_synced = server insert time (DEFAULT now64(3), stamped when the server processes the insert). It is not commit order across concurrent inserts. A reader must re-read a window of at least 300 seconds behind its cursor (last_synced > cursor - 300s) and dedup by key with FINAL.
--
-- The view column is named last_synced (ruled D3541/D3602) and is
-- max(ingested_at) of the rows that touch the membership on the transition arm
-- and work_items.ingested_at on the work_item_column arm. It is NOT the source
-- tables' last_synced, which stays client-stamped and is not exposed by the
-- view. Because the view keeps only ACTIVE memberships, a membership that is
-- retired simply stops appearing: an incremental reader sees additions and
-- re-assertions, not removals.
--
-- Legacy rows: ADD COLUMN ... DEFAULT now64(3) alone would make every row that
-- existed before this migration read the DEFAULT at QUERY time, a different
-- current timestamp on each read. MATERIALIZE COLUMN writes the DEFAULT into
-- the existing parts once, so a legacy row keeps ONE stable value: the time of
-- this migration, the earliest ingest time known for it (it is not the time
-- the row was really ingested, which was not recorded). A failed and repeated
-- migration can only move a legacy row's ingested_at forward, never lose it.
-- MATERIALIZE rewrites the column in every existing part and runs
-- synchronously (mutations_sync = 2). work_items is the large table, so run it
-- in a quiet window.
ALTER TABLE project_membership_transitions ADD COLUMN IF NOT EXISTS ingested_at DateTime64(3, 'UTC') DEFAULT now64(3) COMMENT 'ingested_at = server insert time (DEFAULT now64(3), stamped when the server processes the insert). It is not commit order across concurrent inserts. A reader must re-read a window of at least 300 seconds behind its cursor (ingested_at > cursor - 300s) and dedup by key with FINAL. Rows that existed before migration 100 carry the migration time.';

ALTER TABLE project_membership_transitions MATERIALIZE COLUMN ingested_at SETTINGS mutations_sync = 2;

ALTER TABLE work_items ADD COLUMN IF NOT EXISTS ingested_at DateTime64(3, 'UTC') DEFAULT now64(3) COMMENT 'ingested_at = server insert time (DEFAULT now64(3), stamped when the server processes the insert). It is not commit order across concurrent inserts. A reader must re-read a window of at least 300 seconds behind its cursor (ingested_at > cursor - 300s) and dedup by key with FINAL. Rows that existed before migration 100 carry the migration time.';

ALTER TABLE work_items MATERIALIZE COLUMN ingested_at SETTINGS mutations_sync = 2;

-- The view below is migration 077's definition plus the last_synced column.
-- Its long rationale (per-(subject, project) keying, FINAL, the (P, P) rule,
-- the column arm) lives in 077 and is unchanged.
CREATE OR REPLACE VIEW project_membership_presence AS
WITH touched AS (
    SELECT
        org_id,
        subject_kind,
        repo_id,
        subject_id,
        provider,
        occurred_at,
        ingested_at,
        event_id,
        to_project_id,
        arrayJoin(arrayFilter(
            pair -> pair.1 != '',
            if(from_project_id = to_project_id,
               [(to_project_id, to_project_key)],
               [(to_project_id, to_project_key), (from_project_id, from_project_key)])
        )) AS touch
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
        max(occurred_at) AS observed_at,
        max(ingested_at) AS max_ingested_at
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
    max_ingested_at AS last_synced,
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
    w.ingested_at AS last_synced,
    'work_item_column' AS source
FROM work_items AS w FINAL
WHERE w.project_id != ''
    AND w.provider != 'gitlab'
    AND (w.provider != 'github' OR startsWith(w.project_id, 'ghprojv2:'))
    AND (w.org_id, 'work_item', w.repo_id, w.work_item_id) NOT IN (
        SELECT org_id, subject_kind, repo_id, subject_id FROM subjects_with_history
    );
