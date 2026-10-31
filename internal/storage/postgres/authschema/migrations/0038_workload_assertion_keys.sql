-- 0038 workload assertion keys: the public key metadata of a workload that
-- signs request-bound assertions (the web issuer of the ACR web assertion).
-- ADDITIVE ONLY. See 0001's header.
--
-- One row is one signing key of one workload in this deployment: the issuer
-- string the verifier pins, the kid the signer puts in the token header, its
-- status, when it was put in service and, while it is retiring, when its
-- verification stops. It holds no key: there is no key column, no JWK member
-- and no digest, so the leak is unrepresentable. The private key stays in a
-- file mounted into the signer; the public key stays in the verifier's JWKS.
--
-- Why a table and not a contract file: the issuer and the kid differ per
-- environment and a rotation is an operational event that must not need a
-- release. Only audit.Commit writes the table (the 0006 guard); the runtime
-- role has no privilege on it (the migration role reads it for the operator
-- list verb).
--
-- status: active (signs and verifies), retiring (verifies only, until
-- retire_after), retired (verifies nothing; terminal).
--   * At most one active kid per workload (partial unique index).
--   * retire_after is present exactly while the key is retiring and is after
--     rotated_at.
--   * A retired key never changes again (trigger): a rotation or rollback
--     cannot reactivate a retired kid, even from a writer inside audit.Commit.
--     Nothing reads the table at runtime today, so the restore fence lists it
--     as not access-bearing; the tripwire there demands a section once a
--     reader exists.
--   * Identity columns (workload, issuer, kid, rotated_at) are immutable.
-- The 24 h bound on an overlap is the verb's rule (it reads the active key's
-- rotated_at), not a constraint: a constraint cannot see another row.

CREATE TABLE workload_assertion_keys (
    id                    uuid        PRIMARY KEY,
    workload_principal_id uuid        NOT NULL REFERENCES workloads (principal_id) ON DELETE RESTRICT,
    issuer                text        NOT NULL CHECK (char_length(issuer) BETWEEN 9 AND 512 AND issuer ~ '^https?://[!-~]+$'),
    kid                   text        NOT NULL CHECK (octet_length(kid) BETWEEN 1 AND 256 AND kid ~ '^[!-~]+$'),
    status                text        NOT NULL CHECK (status IN ('active', 'retiring', 'retired')),
    rotated_at            timestamptz NOT NULL,
    retire_after          timestamptz,
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT workload_assertion_keys_retire_after_shape
        CHECK ((status = 'retiring') = (retire_after IS NOT NULL) AND (retire_after IS NULL OR retire_after > rotated_at))
);

CREATE UNIQUE INDEX workload_assertion_keys_identity_key
    ON workload_assertion_keys (workload_principal_id, issuer, kid);
CREATE UNIQUE INDEX workload_assertion_keys_one_active_key
    ON workload_assertion_keys (workload_principal_id) WHERE status = 'active';

CREATE FUNCTION workload_assertion_key_guard() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    IF NEW.id IS DISTINCT FROM OLD.id
       OR NEW.workload_principal_id IS DISTINCT FROM OLD.workload_principal_id
       OR NEW.issuer IS DISTINCT FROM OLD.issuer
       OR NEW.kid IS DISTINCT FROM OLD.kid
       OR NEW.rotated_at IS DISTINCT FROM OLD.rotated_at
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'workload_assertion_keys: an identity column is immutable'
            USING ERRCODE = '23514';
    END IF;
    IF OLD.status = 'retired' AND NEW.status IS DISTINCT FROM OLD.status THEN
        RAISE EXCEPTION 'workload_assertion_keys: a retired key is terminal'
            USING ERRCODE = '23514';
    END IF;
    IF OLD.status = 'retiring' AND NEW.status = 'active' THEN
        RAISE EXCEPTION 'workload_assertion_keys: a retiring key does not become active again'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER workload_assertion_keys_state_guard
    BEFORE UPDATE ON workload_assertion_keys
    FOR EACH ROW EXECUTE FUNCTION workload_assertion_key_guard();
CREATE TRIGGER workload_assertion_keys_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON workload_assertion_keys
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER workload_assertion_keys_truncate_guard
    BEFORE TRUNCATE ON workload_assertion_keys
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();
