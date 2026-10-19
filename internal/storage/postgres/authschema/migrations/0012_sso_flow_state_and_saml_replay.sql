-- 0012 SSO flow state and SAML assertion replay (CHAOS-10229; serves the OIDC
-- and SAML sign-in flows). ADDITIVE ONLY. See 0001's header.
--
-- Hashes only: a flow's state value and a SAML assertion id are stored as
-- 32-byte digests, never the values. A CHECK on the width refuses a plaintext
-- value written by mistake, though it cannot tell a 32-byte secret from a
-- digest.

-- sso_flow_states: one row per in-flight sign-in (OIDC/OAuth authorization
-- request, or SAML AuthnRequest id). Consuming a flow is single-use and must
-- work even when the callback is then rejected, so the table is unguarded (a
-- consume cannot require an audit transaction); consumed_at is write-once and
-- every other column is immutable. A sweep deletes rows expired more than 1 h
-- ago. provider_id names an sso_providers row in the public schema, so there is
-- no foreign key.
CREATE TABLE sso_flow_states (
    id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    protocol     text        NOT NULL CHECK (protocol IN ('oidc', 'oauth', 'saml')),
    provider_id  uuid        NOT NULL,
    flow_hash    bytea       NOT NULL CHECK (octet_length(flow_hash) = 32),
    binding_hash bytea       CHECK (binding_hash IS NULL OR octet_length(binding_hash) = 32),
    expires_at   timestamptz NOT NULL,
    consumed_at  timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX sso_flow_states_flow_hash_key ON sso_flow_states (flow_hash);
CREATE INDEX sso_flow_states_expires_idx ON sso_flow_states (expires_at);

CREATE TRIGGER sso_flow_states_consumed_at_write_once
    BEFORE UPDATE OF consumed_at ON sso_flow_states
    FOR EACH ROW WHEN (OLD.consumed_at IS NOT NULL AND NEW.consumed_at IS DISTINCT FROM OLD.consumed_at)
    EXECUTE FUNCTION consumed_at_write_once();

-- The runtime role holds table-level UPDATE here (the manifest models no column
-- grants), so the one column it may change is enforced by this trigger.
CREATE FUNCTION sso_flow_states_immutable_columns() RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    RAISE EXCEPTION 'sso_flow_states % is immutable except consumed_at', OLD.id
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$;
CREATE TRIGGER sso_flow_states_immutable_columns
    BEFORE UPDATE ON sso_flow_states
    FOR EACH ROW WHEN (
        NEW.id IS DISTINCT FROM OLD.id
        OR NEW.protocol IS DISTINCT FROM OLD.protocol
        OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
        OR NEW.flow_hash IS DISTINCT FROM OLD.flow_hash
        OR NEW.binding_hash IS DISTINCT FROM OLD.binding_hash
        OR NEW.expires_at IS DISTINCT FROM OLD.expires_at
        OR NEW.created_at IS DISTINCT FROM OLD.created_at)
    EXECUTE FUNCTION sso_flow_states_immutable_columns();

-- saml_assertion_uses: the replay ledger. The primary key is the replay
-- check: a second use of the same assertion from the same issuer fails to
-- insert. Insert-only for the runtime (no UPDATE); a sweep deletes rows
-- expired more than 1 h ago.
CREATE TABLE saml_assertion_uses (
    issuer            text        NOT NULL CHECK (issuer <> ''),
    assertion_id_hash bytea       NOT NULL CHECK (octet_length(assertion_id_hash) = 32),
    expires_at        timestamptz NOT NULL,
    created_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (issuer, assertion_id_hash)
);
CREATE INDEX saml_assertion_uses_expires_idx ON saml_assertion_uses (expires_at);
