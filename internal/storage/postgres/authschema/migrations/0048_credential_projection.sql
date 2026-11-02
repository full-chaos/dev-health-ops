-- 0048 projection of external credentials (CHAOS-10896, CHAOS-11196).
-- NOT ADDITIVE in one place, on purpose (additiveExemptions): it REDEFINES the
-- credential_registry.revoked_reason CHECK and only WIDENS it by one value,
-- domain_revoked (a revoked event of a domain that names no reason; the other
-- two values stay). The new list can refuse no stored row, so there is no
-- pre-flight. A test holds the list equal to vocabulary/reason-codes.json.
--
-- The projection is keyed by the DOMAIN and the domain's own reference, so the
-- External Push consumer and the acr endpoint share one set of tables and one
-- rule set: domain_owner is the credential_registry's value (external_push, acr).
--
--   push_sources                 one external_source principal per (organization, source), and one per organization
--   credential_projection        one row per credential: the last applied state
--   credential_projection_digest one row per (credential, revision) decided: the digest of
--                                the event body, so a same-revision delivery with another
--                                body is a conflict, and the applied set the reader reads
--   projection_cursor            the reader's low-water mark per stream
--   projection_retry             events the reader parked (a gap, a parent not seen yet)
--
-- Every table is a security table written only inside audit.Commit (the 0006
-- guard). No secret is stored: a body is the outbox payload, which holds none
-- by contract. Locks in writer order: the tables the new ones reference first.
LOCK TABLE principals, organizations, credential_registry IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE credential_registry
    DROP CONSTRAINT credential_registry_revoked_reason_code,
    ADD CONSTRAINT credential_registry_revoked_reason_code
    CHECK (revoked_reason IS NULL OR revoked_reason IN ('compromised', 'domain_revoked', 'owner_request')) NOT VALID;

ALTER TABLE credential_registry VALIDATE CONSTRAINT credential_registry_revoked_reason_code;

-- One external_source principal per (organization, source) and ONE per organization for the organization-bound
-- credentials (answers-10896-4; an organization-bound event has no source). A null source_id is the organization's
-- principal, so each key is a partial UNIQUE: a plain unique key would treat two null sources as different.
CREATE TABLE push_sources (
    principal_id    uuid        PRIMARY KEY REFERENCES principals (id) ON DELETE RESTRICT,
    organization_id uuid        NOT NULL REFERENCES organizations (id) ON DELETE RESTRICT,
    source_id       text        CHECK (source_id IS NULL OR source_id ~ '^[A-Za-z0-9_-]{1,64}$'),
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX push_sources_source_key ON push_sources (organization_id, source_id) WHERE source_id IS NOT NULL;
CREATE UNIQUE INDEX push_sources_organization_key ON push_sources (organization_id) WHERE source_id IS NULL;

CREATE TABLE credential_projection (
    domain_owner              text        NOT NULL CHECK (domain_owner IN ('external_push', 'acr')),
    external_reference        text        NOT NULL CHECK (external_reference ~ '^[A-Za-z0-9_-]{1,128}$'),
    organization_id           uuid        NOT NULL REFERENCES organizations (id) ON DELETE RESTRICT,
    binding                   text        NOT NULL CHECK (binding IN ('source', 'organization')),
    source_id                 text,
    revision                  bigint      NOT NULL CHECK (revision >= 1),
    max_seen_revision         bigint      NOT NULL CHECK (max_seen_revision >= revision),
    status                    text        NOT NULL CHECK (status IN ('active', 'revoked', 'expired')),
    fingerprint               text        NOT NULL CHECK (fingerprint ~ '^[0-9a-f]{16}$'),
    scopes                    text[]      NOT NULL,
    rotation_parent_reference text,
    expires_at                text,
    terminal_at               timestamptz,
    principal_id              uuid        NOT NULL REFERENCES principals (id) ON DELETE RESTRICT,
    registry_id               uuid        NOT NULL REFERENCES credential_registry (id) ON DELETE RESTRICT,
    acr_class                 text,
    owner_subject             text,
    updated_at                timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (domain_owner, external_reference),
    CONSTRAINT credential_projection_binding_source CHECK ((binding = 'source') = (source_id IS NOT NULL)),
    CONSTRAINT credential_projection_terminal_shape
        CHECK ((status = 'active') = (terminal_at IS NULL))
);

CREATE TABLE credential_projection_digest (
    domain_owner       text        NOT NULL CHECK (domain_owner IN ('external_push', 'acr')),
    external_reference text        NOT NULL CHECK (external_reference ~ '^[A-Za-z0-9_-]{1,128}$'),
    revision           bigint      NOT NULL CHECK (revision >= 1),
    digest             bytea       NOT NULL CHECK (octet_length(digest) = 32),
    outcome            text        NOT NULL CHECK (outcome ~ '^[a-z_]{1,32}$'),
    decided_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (domain_owner, external_reference, revision)
);

CREATE TABLE projection_cursor (
    stream          text        PRIMARY KEY CHECK (stream ~ '^[a-z_]{1,32}$'),
    low_water       bigint      NOT NULL DEFAULT 0 CHECK (low_water >= 0),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE projection_retry (
    stream       text        NOT NULL CHECK (stream ~ '^[a-z_]{1,32}$'),
    outbox_id    bigint      NOT NULL,
    aggregate_id text        NOT NULL,
    revision     bigint      NOT NULL CHECK (revision >= 1),
    body         jsonb       NOT NULL,
    attempts     integer     NOT NULL DEFAULT 0,
    next_at      timestamptz NOT NULL DEFAULT now(),
    last_outcome text        NOT NULL CHECK (last_outcome ~ '^[a-z_]{1,32}$'),
    created_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (stream, outbox_id)
);

CREATE INDEX projection_retry_aggregate_idx ON projection_retry (stream, aggregate_id, revision);
CREATE INDEX projection_retry_due_idx ON projection_retry (stream, next_at);

-- The reader's read: the lowest id above the low-water mark of one aggregate
-- type with no digest row. This partial index holds only those rows, in id
-- order, so the read is a range scan from the mark; the mark moves over rows
-- older than the horizon, which bounds the range.
CREATE INDEX auth_outbox_events_push_idx
    ON auth_outbox_events (id) WHERE aggregate_type = 'external_push_credential';

CREATE TRIGGER push_sources_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON push_sources
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER push_sources_truncate_guard
    BEFORE TRUNCATE ON push_sources
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER credential_projection_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON credential_projection
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER credential_projection_truncate_guard
    BEFORE TRUNCATE ON credential_projection
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER credential_projection_digest_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON credential_projection_digest
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER credential_projection_digest_truncate_guard
    BEFORE TRUNCATE ON credential_projection_digest
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER projection_cursor_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON projection_cursor
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER projection_cursor_truncate_guard
    BEFORE TRUNCATE ON projection_cursor
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER projection_retry_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON projection_retry
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER projection_retry_truncate_guard
    BEFORE TRUNCATE ON projection_retry
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();
