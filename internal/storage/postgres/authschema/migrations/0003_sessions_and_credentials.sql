-- 0003 sessions, refresh credentials and signing-key metadata (TRD §9).
-- ADDITIVE ONLY. See 0001's header.
--
-- Every column in this file that relates to a secret holds a HASH or a
-- REFERENCE. Sessions and refresh credentials are the two places where a
-- careless schema would store a bearer value verbatim, so the rule is stated
-- again here: this service must be unable to replay its own users' credentials
-- from a database dump.

CREATE TABLE sessions (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    principal_id    uuid        NOT NULL REFERENCES principals (id) ON DELETE CASCADE,
    organization_id uuid        REFERENCES organizations (id) ON DELETE CASCADE,
    -- token_hash is a one-way digest of the session token. The token itself
    -- is returned to the client once and never stored: a dump of this table
    -- cannot be replayed as a session.
    token_hash      bytea       NOT NULL,
    -- ACP-ADR-04 §3 transfers sessions FIRST, because they are the
    -- shortest-lived state and therefore the cheapest rollback. These columns
    -- are shaped for that transfer.
    issued_at       timestamptz NOT NULL DEFAULT now(),
    expires_at      timestamptz NOT NULL,
    -- revoked_at makes explicit revocation distinguishable from expiry. A
    -- logout and a timeout are different security events and an audit trail
    -- that cannot tell them apart is much less useful.
    revoked_at      timestamptz,
    revoked_reason  text,
    -- Client attributes are for operator diagnosis and anomaly review. They
    -- are descriptive, never authorization inputs.
    client_ip       inet,
    user_agent      text,
    last_seen_at    timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX sessions_token_hash_key ON sessions (token_hash);
CREATE INDEX sessions_principal_idx ON sessions (principal_id);
-- Partial index over live sessions only: the overwhelmingly common lookup is
-- "is this session currently valid", and an index carrying every expired row
-- ever created answers it more slowly every day.
CREATE INDEX sessions_live_idx
    ON sessions (principal_id, expires_at) WHERE revoked_at IS NULL;

-- refresh_credentials are the rotating half of a session. Each row is one link
-- in a rotation chain: issuing a new one supersedes its predecessor, and
-- reuse of a superseded credential is the canonical replay signal.
CREATE TABLE refresh_credentials (
    id             uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id     uuid        NOT NULL REFERENCES sessions (id) ON DELETE CASCADE,
    principal_id   uuid        NOT NULL REFERENCES principals (id) ON DELETE CASCADE,
    -- Hash only, for the same reason as sessions.token_hash.
    token_hash     bytea       NOT NULL,
    -- superseded_by makes the rotation chain explicit rather than implied by
    -- timestamps. Detecting reuse means finding a presented credential whose
    -- superseded_by is already set, which is a lookup, not a race-prone
    -- comparison of issue times.
    superseded_by  uuid        REFERENCES refresh_credentials (id) ON DELETE SET NULL,
    issued_at      timestamptz NOT NULL DEFAULT now(),
    expires_at     timestamptz NOT NULL,
    consumed_at    timestamptz,
    revoked_at     timestamptz,
    revoked_reason text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX refresh_credentials_token_hash_key ON refresh_credentials (token_hash);
CREATE INDEX refresh_credentials_session_idx ON refresh_credentials (session_id);
CREATE INDEX refresh_credentials_live_idx
    ON refresh_credentials (principal_id, expires_at)
    WHERE consumed_at IS NULL AND revoked_at IS NULL;

-- signing_keys holds PUBLIC key metadata and a REFERENCE to where the private
-- half lives. ACP-ADR-02 §2: production custody is an external KMS or secret
-- store, and application tables carry public metadata and a reference only.
-- There is deliberately no column here that could hold private material.
CREATE TABLE signing_keys (
    -- kid is the JWKS key id and the natural primary key: it is what a token
    -- header carries and what a verifier looks up.
    kid            text        PRIMARY KEY,
    -- ACP-ADR-01 §4 fixes Ed25519/EdDSA for every asymmetric platform token.
    -- The column is not constrained to it, because a compatibility issuer
    -- during a future migration may legitimately publish another algorithm,
    -- and a CHECK here would have to be migrated to allow it.
    algorithm      text        NOT NULL,
    -- public_key_jwk is publishable by definition -- it is what /jwks serves.
    public_key_jwk jsonb       NOT NULL,
    -- custody_ref names the private half in the KMS or secret store. It is a
    -- LOCATOR, not material: reading this table must not yield a signing key.
    custody_ref    text        NOT NULL,
    custody_kind   text        NOT NULL CHECK (custody_kind IN ('kms', 'file')),
    -- ACP-ADR-02 §5: every key gets a kid and a JWKS entry from day one so
    -- G-18 overlap and G-19 bounded refresh are representable BEFORE they are
    -- needed. not_before/not_after are how an overlap window is expressed.
    status         text        NOT NULL DEFAULT 'active'
                               CHECK (status IN ('pending', 'active', 'retiring', 'revoked')),
    not_before     timestamptz NOT NULL DEFAULT now(),
    not_after      timestamptz,
    -- A revoked key is never re-enabled by a rollback (ACP-ADR-02 §6), so
    -- revocation is recorded separately from status expiry.
    revoked_at     timestamptz,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX signing_keys_active_idx ON signing_keys (status, not_before)
    WHERE revoked_at IS NULL;

-- credential_registry is the inventory of non-session credentials: API keys,
-- bootstrap credentials, workload exchange registrations. It stores a hash and
-- a lifecycle, never the credential.
CREATE TABLE credential_registry (
    id             uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    principal_id   uuid        NOT NULL REFERENCES principals (id) ON DELETE CASCADE,
    -- credential_class matches the platform's existing credential-class
    -- vocabulary (contracts/auth/v1/credential-classes.json) so this registry
    -- and the inventory contract describe the same things.
    credential_class text      NOT NULL,
    -- A displayable prefix lets an operator recognise a credential in a list
    -- without the credential being recoverable. It is not secret and not
    -- sufficient to authenticate.
    display_prefix text        NOT NULL DEFAULT '',
    secret_hash    bytea       NOT NULL,
    hash_algorithm text        NOT NULL,
    last_used_at   timestamptz,
    expires_at     timestamptz,
    revoked_at     timestamptz,
    revoked_reason text,
    created_by     uuid        REFERENCES principals (id) ON DELETE SET NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX credential_registry_secret_hash_key ON credential_registry (secret_hash);
CREATE INDEX credential_registry_principal_idx ON credential_registry (principal_id);
CREATE INDEX credential_registry_live_idx
    ON credential_registry (credential_class, expires_at) WHERE revoked_at IS NULL;
