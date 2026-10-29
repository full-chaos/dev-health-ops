-- 0035 restore fence state and the timeline probe. ADDITIVE ONLY. See 0001's header.
--
-- A point-in-time restore (or any promote) starts a new PostgreSQL timeline. The
-- restored database knows nothing of the writes after the restore target: its
-- aggregate revision counters are behind what consumers saw, and credentials
-- revoked after the target are live again. auth-migrate restore-fence apply
-- fixes both and records that it did, per timeline, in restore_fence_state. The
-- runtime's readiness check (internal/auth/restorefence) is ready only while a
-- row exists for the CURRENT timeline, so a restored database takes no traffic
-- until the fence has run. This migration writes NO row: a row written here would
-- travel inside a backup and mark a restored database ready. A database that was
-- never restored becomes ready when an operator runs `auth-migrate restore-fence
-- init` on it (one audited row, refused in recovery and when a row exists).
--
-- The timeline is read, not stored by a function: the runtime role calls the WAL
-- built-ins itself (pg_walfile_name(pg_current_wal_insert_lsn()), EXECUTE for
-- PUBLIC) and parses the first eight hex characters. There is no SECURITY
-- DEFINER function in the auth schema, and a server in recovery raises an error
-- that reads as "not fenced".
CREATE TABLE restore_fence_state (
    timeline_id           bigint      NOT NULL CHECK (timeline_id >= 1),
    manifest_sha256       text        NOT NULL CHECK (manifest_sha256 ~ '^([0-9a-f]{64}|initial|blind_margin|inputs)$'),
    -- source says what the fence was applied from.
    source                text        NOT NULL CHECK (source IN ('init', 'manifest', 'blind_margin', 'inputs')),
    applied_at            timestamptz NOT NULL DEFAULT now(),
    floors_raised         bigint      NOT NULL DEFAULT 0 CHECK (floors_raised >= 0),
    revocations_reapplied bigint      NOT NULL DEFAULT 0 CHECK (revocations_reapplied >= 0),
    -- operator_actions lists what an operator must still do (ids only): force
    -- a password reset, re-queue a provider revocation.
    operator_actions      jsonb       NOT NULL DEFAULT '[]'::jsonb,
    PRIMARY KEY (timeline_id, manifest_sha256)
);

-- Only the fence's audit.Commit (apply, init) writes the state table (0006's guard).
CREATE TRIGGER restore_fence_state_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON restore_fence_state
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER restore_fence_state_truncate_guard
    BEFORE TRUNCATE ON restore_fence_state
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();
