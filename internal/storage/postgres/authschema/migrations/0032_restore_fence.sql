-- 0032 restore fence state and the timeline probe. ADDITIVE ONLY. See 0001's header.
--
-- A point-in-time restore (or any promote) starts a new PostgreSQL timeline. The
-- restored database knows nothing of the writes after the restore target: its
-- aggregate revision counters are behind what consumers saw, and credentials
-- revoked after the target are live again. auth-migrate restore-fence apply
-- fixes both and records that it did, per timeline, in restore_fence_state. The
-- runtime's readiness check (internal/auth/restorefence) is ready only while a
-- row exists for the CURRENT timeline, so a restored database takes no traffic
-- until the fence has run. This migration stamps the timeline it runs on, so a
-- database that was never restored is ready.
--
-- restore_fence_timeline() is the one way the runtime learns the timeline:
-- SECURITY DEFINER, owned by the migration role, no arguments, returns the
-- timeline id and nothing else (NULL while the server is in recovery, which
-- reads as "not fenced"). search_path is pinned to pg_catalog so the definer's
-- rights cannot be redirected by a caller's search_path. EXECUTE is revoked from
-- PUBLIC here; the runtime role gets it from the grant manifest (grants.go).
CREATE FUNCTION restore_fence_timeline() RETURNS bigint
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
    IF pg_is_in_recovery() THEN
        RETURN NULL;
    END IF;
    RETURN ('x' || substr(pg_walfile_name(pg_current_wal_insert_lsn()), 1, 8))::bit(32)::bigint;
END;
$$;
REVOKE ALL ON FUNCTION restore_fence_timeline() FROM PUBLIC;

CREATE TABLE restore_fence_state (
    timeline_id           bigint      NOT NULL CHECK (timeline_id >= 1),
    manifest_sha256       text        NOT NULL CHECK (manifest_sha256 ~ '^([0-9a-f]{64}|initial|blind_margin|inputs)$'),
    -- source says what the fence was applied from.
    source                text        NOT NULL CHECK (source IN ('migration', 'manifest', 'blind_margin', 'inputs')),
    applied_at            timestamptz NOT NULL DEFAULT now(),
    floors_raised         bigint      NOT NULL DEFAULT 0 CHECK (floors_raised >= 0),
    revocations_reapplied bigint      NOT NULL DEFAULT 0 CHECK (revocations_reapplied >= 0),
    PRIMARY KEY (timeline_id, manifest_sha256)
);

-- The timeline this migration runs on is fenced by definition.
INSERT INTO restore_fence_state (timeline_id, manifest_sha256, source)
SELECT restore_fence_timeline(), 'initial', 'migration'
WHERE restore_fence_timeline() IS NOT NULL;

-- Only the fence's audit.Commit writes the state table (0006's guard), after
-- the stamp above so the stamp itself is not refused.
CREATE TRIGGER restore_fence_state_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON restore_fence_state
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER restore_fence_state_truncate_guard
    BEFORE TRUNCATE ON restore_fence_state
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();
