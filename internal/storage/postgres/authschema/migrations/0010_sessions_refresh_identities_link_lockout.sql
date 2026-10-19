-- 0010 session revision, refresh lineage, issuer-scoped external identities,
-- link credentials and lockout (Wave 2 A2, CHAOS-10156). ADDITIVE except one
-- named index swap, which the additive-diff test exempts by version.
--
-- Everything here stores hashes only. A dump of these tables replays nothing.

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
    ADD COLUMN legacy_jti_hash  bytea;
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
    token_hash   bytea       NOT NULL,
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
    identifier_hash  bytea       NOT NULL,
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
CREATE TRIGGER link_credentials_security_guard
    BEFORE UPDATE OF principal_id, purpose, token_hash, expires_at, consumed_at ON link_credentials
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
