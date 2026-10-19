-- 0010 session revision, refresh lineage, issuer-scoped external identities,
-- link credentials and lockout (Wave 2 A2, CHAOS-10156). ADDITIVE except one
-- named index swap, which the additive-diff test exempts by version.
--
-- Everything here stores hashes only. A new hash column is a 32-byte digest
-- (SHA-256 or HMAC-SHA-256): a CHECK on the length refuses an address or a
-- token written by mistake, though it cannot tell a 32-byte secret from a digest. A dump of these tables replays nothing.

-- sessions: revision replaces the legacy token_version (an access token carries
-- the revision it was minted at; bumping it invalidates older tokens). auth_time
-- and amr are what the token claims need to say about how the session began.
ALTER TABLE sessions
    ADD COLUMN revision  integer     NOT NULL DEFAULT 1 CHECK (revision >= 1),
    ADD COLUMN auth_time timestamptz,
    ADD COLUMN amr       text[]      NOT NULL DEFAULT '{}';

-- refresh_credentials: a family is the unit of reuse detection (a replayed
-- superseded credential revokes the whole family). Existing rows each default
-- to their own family. parent_id records rotation order; ON DELETE SET NULL so
-- the expiry sweep can delete a parent without failing or taking live children.
-- legacy_jti_hash holds the digest of a go-api refresh JWT's jti, accepted once
-- and exchanged for an opaque credential.
ALTER TABLE refresh_credentials
    ADD COLUMN family_id        uuid  NOT NULL DEFAULT gen_random_uuid(),
    ADD COLUMN parent_id        uuid  REFERENCES refresh_credentials (id) ON DELETE SET NULL,
    ADD COLUMN legacy_jti_hash  bytea CHECK (legacy_jti_hash IS NULL OR octet_length(legacy_jti_hash) = 32);
CREATE INDEX refresh_credentials_family_idx ON refresh_credentials (family_id);
CREATE INDEX refresh_credentials_parent_idx ON refresh_credentials (parent_id) WHERE parent_id IS NOT NULL;
CREATE UNIQUE INDEX refresh_credentials_legacy_jti_hash_key
    ON refresh_credentials (legacy_jti_hash) WHERE legacy_jti_hash IS NOT NULL;

-- external_identities: the same provider subject under two issuers is two
-- identities. The old (provider, subject) unique index would keep issuer inert,
-- so it is replaced here (the one non-additive step in this migration).
ALTER TABLE external_identities ADD COLUMN issuer text NOT NULL DEFAULT '';
DROP INDEX external_identities_provider_subject_key;
CREATE UNIQUE INDEX external_identities_issuer_provider_subject_key
    ON external_identities (issuer, provider, subject);

-- link_credentials: single-use tokens sent out of band (email verification,
-- password reset). Hash only. A sweep deletes rows expired or consumed more
-- than 7 days ago.
CREATE TABLE link_credentials (
    id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    principal_id uuid        NOT NULL REFERENCES principals (id) ON DELETE CASCADE,
    purpose      text        NOT NULL CHECK (purpose IN ('email_verification', 'password_reset')),
    token_hash   bytea       NOT NULL CHECK (octet_length(token_hash) = 32),
    expires_at   timestamptz NOT NULL,
    consumed_at  timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX link_credentials_token_hash_key ON link_credentials (token_hash);
CREATE INDEX link_credentials_principal_idx ON link_credentials (principal_id, purpose);
CREATE INDEX link_credentials_expires_idx ON link_credentials (expires_at);

-- login_attempts: lockout state per login identifier. The identifier is stored
-- as a hash, never the raw email, and no client address is kept. The row is
-- deleted on a successful login; a sweep deletes unlocked rows idle for 24 h.
CREATE TABLE login_attempts (
    id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    identifier_hash  bytea       NOT NULL CHECK (octet_length(identifier_hash) = 32),
    attempt_count    integer     NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    first_attempt_at timestamptz,
    locked_until     timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX login_attempts_identifier_hash_key ON login_attempts (identifier_hash);
CREATE INDEX login_attempts_updated_idx ON login_attempts (updated_at) WHERE locked_until IS NULL;

-- Commit guard (0006): the new SECURITY columns of the unguarded tables get
-- UPDATE OF triggers, so changing them needs an audit.Commit transaction. New
-- trigger names keep this step additive. login_attempts is unguarded and all
-- lifecycle (rate limiting, not authority); link_credentials is unguarded for
-- insert and sweep but every column that can retarget or consume a token is a
-- security column.
CREATE TRIGGER sessions_security_guard_0010
    BEFORE UPDATE OF revision, auth_time, amr ON sessions
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER refresh_credentials_security_guard_0010
    BEFORE UPDATE OF family_id, legacy_jti_hash ON refresh_credentials
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
-- parent_id: re-pointing a credential's parent rewrites rotation history, so a
-- move to a non-null parent is guarded. Setting it to NULL is not: that is the
-- ON DELETE SET NULL action of the expiry sweep, which runs outside a commit
-- transaction by design (reuse detection reads family_id, not parent_id).
CREATE TRIGGER refresh_credentials_parent_guard
    BEFORE UPDATE OF parent_id ON refresh_credentials
    FOR EACH ROW WHEN (NEW.parent_id IS NOT NULL)
    EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER link_credentials_security_guard
    BEFORE UPDATE OF principal_id, purpose, token_hash, expires_at, consumed_at ON link_credentials
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();

-- consumed_at is write-once. The commit guard above checks that a write happens
-- inside an audit transaction, not which transition it makes, so without this a
-- second guarded UPDATE could re-consume (replay) a used token or clear
-- consumed_at. Only changes to consumed_at are refused: revoked_at and the
-- ON DELETE SET NULL actions on a consumed row stay legal. A consume must be
-- written as UPDATE ... WHERE consumed_at IS NULL and checked for one affected
-- row; a replay then matches no row and never reaches this trigger.
CREATE FUNCTION consumed_at_write_once() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '%.consumed_at of % is already set and cannot change', TG_TABLE_NAME, OLD.id
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$;
CREATE TRIGGER link_credentials_consumed_at_write_once
    BEFORE UPDATE OF consumed_at ON link_credentials
    FOR EACH ROW WHEN (OLD.consumed_at IS NOT NULL AND NEW.consumed_at IS DISTINCT FROM OLD.consumed_at)
    EXECUTE FUNCTION consumed_at_write_once();
CREATE TRIGGER refresh_credentials_consumed_at_write_once
    BEFORE UPDATE OF consumed_at ON refresh_credentials
    FOR EACH ROW WHEN (OLD.consumed_at IS NOT NULL AND NEW.consumed_at IS DISTINCT FROM OLD.consumed_at)
    EXECUTE FUNCTION consumed_at_write_once();

-- A live lockout cannot be erased. The runtime role must update these rows
-- (count up, set the lock, clear it after expiry, delete on a successful
-- login, sweep idle rows), but while locked_until is in the future the row may
-- not be retargeted, lose its lock or count, or be deleted. Login is refused
-- while a lock holds, so no legitimate path needs any of those. An early admin
-- unlock therefore runs as the owner/migration role.
CREATE FUNCTION login_attempts_live_lock() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'login_attempts row % is locked until % and cannot be deleted', OLD.id, OLD.locked_until
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.identifier_hash IS DISTINCT FROM OLD.identifier_hash THEN
        RAISE EXCEPTION 'login_attempts row % is locked and its identifier cannot change', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.locked_until IS NULL OR NEW.locked_until < OLD.locked_until OR NEW.attempt_count < OLD.attempt_count THEN
        RAISE EXCEPTION 'login_attempts row % is locked until % and cannot be relaxed', OLD.id, OLD.locked_until
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER login_attempts_live_lock_update
    BEFORE UPDATE ON login_attempts
    FOR EACH ROW WHEN (OLD.locked_until > now())
    EXECUTE FUNCTION login_attempts_live_lock();
CREATE TRIGGER login_attempts_live_lock_delete
    BEFORE DELETE ON login_attempts
    FOR EACH ROW WHEN (OLD.locked_until > now())
    EXECUTE FUNCTION login_attempts_live_lock();

-- identifier_hash never changes on any row: a retarget moves one identifier's
-- failure history onto another.
CREATE FUNCTION login_attempts_identifier_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'login_attempts row % identifier cannot change', OLD.id
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$;
CREATE TRIGGER login_attempts_identifier_immutable
    BEFORE UPDATE OF identifier_hash ON login_attempts
    FOR EACH ROW WHEN (NEW.identifier_hash IS DISTINCT FROM OLD.identifier_hash)
    EXECUTE FUNCTION login_attempts_identifier_immutable();

-- A child credential belongs to its parent's family, so a family-wide revoke
-- reaches it. On insert a NULL family_id is filled from the parent; a supplied
-- family_id must match the parent's or the insert is refused (caller bugs
-- surface, nothing is rewritten silently). Moving a credential to another
-- family, or to a parent of another family, is refused.
CREATE FUNCTION refresh_credentials_family_follows_parent() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    parent_family uuid;
BEGIN
    SELECT family_id INTO parent_family FROM refresh_credentials WHERE id = NEW.parent_id;
    IF TG_OP = 'INSERT' AND NEW.family_id IS NULL THEN
        NEW.family_id := parent_family;
    ELSIF NEW.family_id IS DISTINCT FROM parent_family THEN
        RAISE EXCEPTION 'refresh credential family must match the family of its parent %', NEW.parent_id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER refresh_credentials_family_follows_parent
    BEFORE INSERT OR UPDATE OF family_id, parent_id ON refresh_credentials
    FOR EACH ROW WHEN (NEW.parent_id IS NOT NULL)
    EXECUTE FUNCTION refresh_credentials_family_follows_parent();
