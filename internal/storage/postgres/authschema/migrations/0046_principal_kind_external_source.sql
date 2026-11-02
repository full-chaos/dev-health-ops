-- 0046 principals.kind admits external_source.
-- NOT ADDITIVE in one place, on purpose (additiveExemptions): it REDEFINES the
-- kind CHECK and only WIDENS it (user, service_account and workload stay;
-- external_source is the one new value, the principal.v1 type of the same name).
-- The new list can refuse no stored row, so there is no pre-flight. A unit test
-- holds the list equal to the principal.v1 types that name a stored principal.
--
-- An external_source principal stands for one External Push source of one
-- organization (CHAOS-10896). It is never a person and never signs in: the three
-- triggers below refuse it a session, a membership and a platform-owned
-- credential_registry row, in the database, so no writer has to remember. Its
-- domain-owned (external_push) registry rows are the point of the kind and stay
-- allowed. The delegation target check (kind = 'user') refuses it already.
--
-- Locks in writer order, as 0041 does: auth.principals first. The CHECK swap
-- needs ACCESS EXCLUSIVE there; CREATE TRIGGER needs SHARE ROW EXCLUSIVE on each
-- table after it. apply.go's lock_timeout bounds every wait, and auth-migrate
-- does not retry a deadlock.
LOCK TABLE principals IN SHARE ROW EXCLUSIVE MODE;

-- NOT VALID, then VALIDATE in a statement of its own, so the table scan of the
-- validation does not run under the ACCESS EXCLUSIVE lock of the ALTER. Both run
-- in the migration's one transaction.
ALTER TABLE principals
    DROP CONSTRAINT principals_kind_check,
    ADD CONSTRAINT principals_kind_check
    CHECK (kind IN ('user', 'service_account', 'workload', 'external_source')) NOT VALID;

ALTER TABLE principals VALIDATE CONSTRAINT principals_kind_check;

CREATE FUNCTION principal_not_external_source() RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM principals WHERE id = NEW.principal_id AND kind = 'external_source') THEN
        RAISE EXCEPTION 'principal % is an external_source: it holds no % row', NEW.principal_id, TG_TABLE_NAME
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER sessions_not_external_source
    BEFORE INSERT OR UPDATE OF principal_id ON sessions
    FOR EACH ROW EXECUTE FUNCTION principal_not_external_source();

CREATE TRIGGER memberships_not_external_source
    BEFORE INSERT OR UPDATE OF principal_id ON memberships
    FOR EACH ROW EXECUTE FUNCTION principal_not_external_source();

CREATE TRIGGER credential_registry_not_external_source
    BEFORE INSERT OR UPDATE OF principal_id ON credential_registry
    FOR EACH ROW WHEN (NEW.domain_owner = 'platform') EXECUTE FUNCTION principal_not_external_source();
