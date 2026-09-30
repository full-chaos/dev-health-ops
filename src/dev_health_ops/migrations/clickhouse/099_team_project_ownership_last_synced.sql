-- team_project_ownership carries only updated_at, the ReplacingMergeTree
-- version. Providers stamp it with their own time (and the Atlassian writer
-- used one provider stamp for a whole write), so it is not monotonic across
-- syncs: a row whose provider time is old can land after a consumer's cursor
-- has already passed that time, and a cursor keyed on updated_at never sees it.
--
-- last_synced is the UTC time the ClickHouse server processed the INSERT. No
-- writer sends a value: every writer names its columns and omits last_synced,
-- so the server stamps the row when the insert runs. A client-side stamp would
-- be taken before the lease check and the batch send, and a delayed write would
-- then land behind a consumer cursor that had already moved past its stamp.
-- updated_at keeps its meaning and its role as the version column, and
-- last_synced is not part of the sorting key.
--
-- ADD COLUMN ... DEFAULT now64(3) alone would make every row that existed
-- before this migration read the DEFAULT at QUERY time, so each read would
-- return a different, current timestamp for old rows. MATERIALIZE COLUMN
-- writes the DEFAULT into the existing parts once, so those rows keep one
-- stable value: the time of this migration, the earliest ingest time that is
-- known for them. The column is created with IF NOT EXISTS and the
-- materialization runs synchronously, so a failed and repeated migration
-- can only move a legacy row's last_synced forward, never lose the row.
ALTER TABLE team_project_ownership ADD COLUMN IF NOT EXISTS last_synced DateTime64(3, 'UTC') DEFAULT now64(3);

ALTER TABLE team_project_ownership MATERIALIZE COLUMN last_synced SETTINGS mutations_sync = 2;
