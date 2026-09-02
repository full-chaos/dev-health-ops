-- 0001 principals and identities (TRD §9; CHAOS-4882, Wave 1).
--
-- ADDITIVE ONLY. This migration creates new objects in the auth-owned schema
-- and touches nothing outside it. No existing row moves and no existing writer
-- changes: nothing in production reads or writes these tables yet, because the
-- service that will is built dormant (CHAOS-4881).
--
-- NO PLAINTEXT CREDENTIAL COLUMN appears anywhere in this lineage. Where a
-- secret is unavoidable, the column holds a HASH (verifier-side, one-way) or a
-- REFERENCE to material held by a KMS or secret store (ACP-ADR-02 §2). The
-- naming convention is enforced by a test, not by review.

-- principals is the single subject type every authorization decision is made
-- about: a human user, a service account, or a workload. Giving them one table
-- and one id space is what lets a grant, a session or an audit row name "who"
-- without a polymorphic column pair, and what makes "revoke everything for
-- this subject" a single predicate.
CREATE TABLE principals (
    id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    -- kind is closed at the database level rather than in application code:
    -- an unrecognised subject kind must be impossible to store, because every
    -- authorization path downstream switches on it.
    kind         text        NOT NULL CHECK (kind IN ('user', 'service_account', 'workload')),
    -- display_name is operator-facing only and never an identity.
    display_name text        NOT NULL DEFAULT '',
    -- disabled_at is the revocation switch. A NULL means active; a timestamp
    -- means every session, token and grant for this principal is void. It is
    -- nullable-with-meaning rather than a boolean so the audit trail keeps
    -- WHEN, which a boolean discards.
    disabled_at  timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX principals_kind_idx ON principals (kind);
CREATE INDEX principals_active_idx ON principals (id) WHERE disabled_at IS NULL;

-- users is the human-specific half of a principal. The split keeps
-- person-only attributes off service accounts and workloads instead of
-- carrying columns that are meaningless for two of the three kinds.
CREATE TABLE users (
    principal_id  uuid        PRIMARY KEY REFERENCES principals (id) ON DELETE RESTRICT,
    -- email is an identifier here, not a contact field: it is what an
    -- upstream IdP asserts and what a login flow matches on.
    email         text        NOT NULL,
    email_lower   text        NOT NULL,
    email_verified_at timestamptz,
    -- password_hash holds a verifier, never a password. It is nullable
    -- because a user authenticated solely through an external IdP has no
    -- local credential at all, and storing a placeholder would make "has a
    -- password" unanswerable.
    password_hash text,
    -- password_algorithm records which KDF produced password_hash, so a
    -- rehash-on-login migration can tell old verifiers from new ones without
    -- parsing the hash. ACP-ADR-01 defers the KDF choice to the Wave 1
    -- addendum, so this deliberately does not constrain the value yet.
    password_algorithm text,
    password_updated_at timestamptz,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT users_password_hash_needs_algorithm
        CHECK ((password_hash IS NULL) = (password_algorithm IS NULL))
);

-- Case-insensitive uniqueness is enforced on a stored lowercase column rather
-- than a functional index, so the normalisation rule is visible in the row and
-- identical for every reader, including ones that do not go through the Go
-- code. lower() in an index would put the rule somewhere only the planner sees.
CREATE UNIQUE INDEX users_email_lower_key ON users (email_lower);

-- external_identities links a principal to an identity asserted by an upstream
-- provider. One principal may hold several: the same person through two IdPs,
-- or a migrated account keeping its old subject.
CREATE TABLE external_identities (
    id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    principal_id uuid        NOT NULL REFERENCES principals (id) ON DELETE CASCADE,
    -- provider and subject together are what the IdP actually asserts.
    provider     text        NOT NULL,
    subject      text        NOT NULL,
    -- last_authenticated_at supports "this link is stale" without needing the
    -- session history, which expires.
    last_authenticated_at timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- One provider subject maps to exactly one principal. Without this a second
-- link could silently split one upstream identity across two principals, and
-- every authorization answer would then depend on which link a login happened
-- to match.
CREATE UNIQUE INDEX external_identities_provider_subject_key
    ON external_identities (provider, subject);
CREATE INDEX external_identities_principal_idx ON external_identities (principal_id);
