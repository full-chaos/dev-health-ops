-- 0006 database-enforced "all three or none" for security mutations (G-53).
-- ADDITIVE ONLY. See 0001's header.
--
-- internal/audit.Commit sets dev_health.audit_txid to pg_current_xact_id() as
-- the first statement of its transaction. The trigger below refuses any write
-- to a guarded table unless that setting equals the CURRENT transaction's id.
-- A write on another pooled connection, or SQL assembled at run time outside
-- internal/audit, runs in a different transaction (or none that set the
-- setting) and is refused by PostgreSQL itself, not by a source scan.
--
-- The setting is transaction-local (set_config(..., true)), so it cannot leak
-- to a later transaction on a pooled connection.

CREATE FUNCTION audit_commit_guard() RETURNS trigger
    LANGUAGE plpgsql
AS $$
DECLARE
    counter text;
    changed bigint;
BEGIN
    IF current_setting('dev_health.audit_txid', true) IS DISTINCT FROM pg_current_xact_id()::text THEN
        RAISE EXCEPTION 'write to % refused: security tables are written only through audit.Commit',
            TG_TABLE_NAME
            USING ERRCODE = '42501';
    END IF;
    -- Count the state rows this transaction writes, so audit.Commit learns
    -- from the database, not from the callback, whether any state changed. The
    -- audit and outbox rows are the companions, not the state. The counter is
    -- stored as "<txid>:<n>" so a value left over from another transaction or a
    -- session-level set_config is never read as this transaction's count.
    IF TG_TABLE_NAME NOT IN ('auth_outbox_events', 'security_audit_events') THEN
        counter := current_setting('dev_health.audit_changed', true);
        IF counter LIKE pg_current_xact_id()::text || ':%' THEN
            changed := split_part(counter, ':', 2)::bigint + 1;
        ELSE
            changed := 1;
        END IF;
        PERFORM set_config('dev_health.audit_changed', pg_current_xact_id()::text || ':' || changed::text, true);
    END IF;
    IF TG_LEVEL = 'STATEMENT' THEN
        RETURN NULL;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

-- GUARDED tables. The set is every table a security event mutates plus the
-- audit and outbox tables themselves. TestEveryAuthTableIsClassified makes a
-- new, unclassified table fail CI.
CREATE TRIGGER auth_outbox_events_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON auth_outbox_events
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER security_audit_events_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON security_audit_events
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER resource_grants_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON resource_grants
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER memberships_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON memberships
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER platform_role_assignments_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON platform_role_assignments
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER policy_revisions_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON policy_revisions
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER entitlement_snapshots_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON entitlement_snapshots
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER credential_registry_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON credential_registry
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER service_accounts_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON service_accounts
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER workloads_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON workloads
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER external_identities_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON external_identities
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER organizations_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON organizations
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();

-- TRUNCATE skips row triggers, so each guarded table also gets a statement
-- trigger (same check). TRUNCATE ... CASCADE reaches the guarded tables it
-- cascades into, and each of their triggers fires.
CREATE TRIGGER auth_outbox_events_truncate_guard
    BEFORE TRUNCATE ON auth_outbox_events
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER security_audit_events_truncate_guard
    BEFORE TRUNCATE ON security_audit_events
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER resource_grants_truncate_guard
    BEFORE TRUNCATE ON resource_grants
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER memberships_truncate_guard
    BEFORE TRUNCATE ON memberships
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER platform_role_assignments_truncate_guard
    BEFORE TRUNCATE ON platform_role_assignments
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER policy_revisions_truncate_guard
    BEFORE TRUNCATE ON policy_revisions
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER entitlement_snapshots_truncate_guard
    BEFORE TRUNCATE ON entitlement_snapshots
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER credential_registry_truncate_guard
    BEFORE TRUNCATE ON credential_registry
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER service_accounts_truncate_guard
    BEFORE TRUNCATE ON service_accounts
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER workloads_truncate_guard
    BEFORE TRUNCATE ON workloads
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER external_identities_truncate_guard
    BEFORE TRUNCATE ON external_identities
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER organizations_truncate_guard
    BEFORE TRUNCATE ON organizations
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();

-- SECURITY COLUMNS of the otherwise unguarded tables. Rows of these tables are
-- written at a high rate (logins, refresh rotation, last-seen stamps), so the
-- table is not guarded, but the columns below change who or what may act:
-- revoking or disabling, expiry, credential material, identity. An UPDATE that
-- names one of them is guarded; an UPDATE of any other column is not.
-- TestEveryUnguardedColumnIsClassified makes a new column fail CI until it is
-- classified.
CREATE TRIGGER principals_security_guard
    BEFORE UPDATE OF disabled_at ON principals
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER users_security_guard
    BEFORE UPDATE OF email, email_lower, email_verified_at, password_hash, password_algorithm, password_updated_at ON users
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER sessions_security_guard
    BEFORE UPDATE OF token_hash, expires_at, revoked_at, revoked_reason ON sessions
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER refresh_credentials_security_guard
    BEFORE UPDATE OF token_hash, expires_at, revoked_at, revoked_reason ON refresh_credentials
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
