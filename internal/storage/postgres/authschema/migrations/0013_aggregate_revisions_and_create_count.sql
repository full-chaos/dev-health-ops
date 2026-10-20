-- 0013 per-aggregate revision counter and the create guard for
-- provisioning-event.v1. ADDITIVE ONLY. See 0001's header.
--
-- audit.Commit bumps the row of each aggregate it writes an event for, in the
-- mutation's own transaction, and writes the returned value as
-- payload.revision. The consumer applies only last+1, so the revision must be
-- contiguous per aggregate. A counter row, not max(revision) over the outbox:
-- outbox and audit rows are reaped (audit.Reap, 0009) and an unlink deletes
-- the external identity row, so neither can hold the high-water mark. A row
-- lock, not a sequence: a sequence value survives a rollback, which would
-- leave a gap the consumer reads as "resynchronise". No foreign key: the
-- aggregate may be deleted and its revision must survive it. The runtime role
-- has no DELETE (a deleted row would restart an aggregate at revision 1).
CREATE TABLE aggregate_revisions (
    aggregate_type text        NOT NULL CHECK (aggregate_type IN ('principal', 'user', 'external_identity',
        'organization', 'membership', 'platform_role_assignment', 'service_account', 'workload', 'resource_grant')),
    aggregate_id   text        NOT NULL CHECK (aggregate_id ~ '^[a-z]{3}_[A-Za-z0-9_-]{1,64}$'),
    revision       bigint      NOT NULL CHECK (revision BETWEEN 1 AND 9007199254740991),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (aggregate_type, aggregate_id)
);

-- Only audit.Commit writes it (0006 guard). Commit reads its changed-row
-- count before it bumps a revision, so these guarded writes never make a
-- no-op mutation look like a change.
CREATE TRIGGER aggregate_revisions_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON aggregate_revisions
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER aggregate_revisions_truncate_guard
    BEFORE TRUNCATE ON aggregate_revisions
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();

-- Create guard: a NEW principal or user row written inside audit.Commit is a
-- state change, though 0006 leaves both tables unguarded for insert. AFTER
-- INSERT ROW fires only for a row actually inserted, so an
-- INSERT ... ON CONFLICT DO NOTHING that inserts nothing counts nothing.
-- Outside an audit.Commit transaction it counts nothing and refuses nothing:
-- the tables stay unguarded.
CREATE FUNCTION audit_count_create() RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
DECLARE
    counter text;
    changed bigint;
BEGIN
    IF current_setting('dev_health.audit_txid', true) IS DISTINCT FROM pg_current_xact_id()::text THEN
        RETURN NULL;
    END IF;
    counter := current_setting('dev_health.audit_changed', true);
    IF counter LIKE pg_current_xact_id()::text || ':%' THEN
        changed := split_part(counter, ':', 2)::bigint + 1;
    ELSE
        changed := 1;
    END IF;
    PERFORM set_config('dev_health.audit_changed', pg_current_xact_id()::text || ':' || changed::text, true);
    RETURN NULL;
END;
$$;
CREATE TRIGGER principals_create_count
    AFTER INSERT ON principals
    FOR EACH ROW EXECUTE FUNCTION audit_count_create();
CREATE TRIGGER users_create_count
    AFTER INSERT ON users
    FOR EACH ROW EXECUTE FUNCTION audit_count_create();
