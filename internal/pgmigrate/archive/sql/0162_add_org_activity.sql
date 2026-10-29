-- Revision 0162 (down_revision 0161), written by hand.
-- org_activity projects, per organization, the two facts the login organization choice takes from the daily metrics
-- tables in ClickHouse: whether any metric row exists (has_data) and the newest computed_at over them (computed_at).
-- The metrics finalize job upserts the row; the auth plane reads it and never reads ClickHouse. A missing row means the
-- activity is unknown. computed_at is the freshness of the fact, refreshed_at the time of the last upsert.
-- Expand-only: a new table, no change to an existing one.

CREATE TABLE org_activity (
    org_id       uuid        PRIMARY KEY REFERENCES organizations (id) ON DELETE CASCADE,
    has_data     boolean     NOT NULL,
    computed_at  timestamptz,
    refreshed_at timestamptz NOT NULL
);
