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
BEGIN
    IF current_setting('dev_health.audit_txid', true) IS DISTINCT FROM pg_current_xact_id()::text THEN
        RAISE EXCEPTION 'write to % refused: security tables are written only through audit.Commit',
            TG_TABLE_NAME
            USING ERRCODE = '42501';
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
