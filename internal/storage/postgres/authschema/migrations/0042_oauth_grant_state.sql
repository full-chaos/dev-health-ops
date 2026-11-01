-- 0042 OAuth authorization server state: grants, authorization requests and the
-- link from a browser client's session to the web session (ACP-ADR-13,
-- CHAOS-12398). ADDITIVE ONLY. See 0001's header.
--
-- Every grant has its own auth.sessions row. A web sid is never a grant's sid:
-- a refresh credential on the web sid would set the web session's expiry
-- (effectiveExpiry takes the newest credential of the session), and revoking a
-- client would sign the user out of web.
--
-- sessions.web_session_id links a browser client's session to the web session
-- it was consented under. Nothing is written when the web session ends: the
-- session store's effective revoke and effective expiry read the parent, so a
-- web logout, an organization switch, web idle expiry, revoke-all and a password
-- change all end the child on those paths' own writes. An MCP client's session
-- has no link, so none of the web-only ends touch it.
--
-- oauth_grants records what the user consented to. It is under the audit commit
-- guard: a grant exists only with its audit row. A revoked grant keeps its row,
-- so the runtime role has no DELETE on it.
--
-- oauth_authorization_requests holds one authorization transaction and its
-- single-use code. It is written by an unauthenticated GET, so it is not under
-- the guard (as sso_flow_states); the rows a decision writes are written inside
-- audit.Commit by the same path. Only hashes of the code and of the anti-CSRF
-- token are stored: a dump cannot replay either. state is the client's own
-- value and is echoed to it: it is not a credential here, and is never logged.
--
-- Locks in writer order: auth.principals, auth.organizations, then auth.sessions,
-- as 0041. The foreign keys to the first two need SHARE ROW EXCLUSIVE there; taken
-- after the ALTER, that could wait behind a writer that waits for sessions, and
-- auth-migrate does not retry a deadlock.
LOCK TABLE principals IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE organizations IN SHARE ROW EXCLUSIVE MODE;

-- A CHECK sees one row, so the parent's shape is the trigger's. The shape here:
-- a link is not to itself, carries no delegation, and needs an organization.
ALTER TABLE sessions
    ADD COLUMN web_session_id uuid REFERENCES sessions (id) ON DELETE CASCADE,
    ADD CONSTRAINT sessions_web_link_shape CHECK (
        web_session_id IS NULL
        OR (web_session_id <> id AND actor_principal_id IS NULL AND organization_id IS NOT NULL));

CREATE INDEX sessions_web_session_idx ON sessions (web_session_id)
    WHERE web_session_id IS NOT NULL;

CREATE FUNCTION sessions_web_link_immutable() RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    RAISE EXCEPTION 'the web session link of session % cannot change', OLD.id
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$;
CREATE TRIGGER sessions_web_link_immutable
    BEFORE UPDATE OF web_session_id ON sessions
    FOR EACH ROW WHEN (OLD.web_session_id IS DISTINCT FROM NEW.web_session_id)
    EXECUTE FUNCTION sessions_web_link_immutable();

-- Depth one: the parent is a plain web session of the same principal and
-- organization. It has no link and no actor, so the effective expressions read
-- exactly one parent, and a delegated session never has a child.
CREATE FUNCTION sessions_web_link_parent() RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
DECLARE
    parent sessions%ROWTYPE;
BEGIN
    SELECT * INTO parent FROM sessions WHERE id = NEW.web_session_id;
    IF NOT FOUND OR parent.web_session_id IS NOT NULL OR parent.actor_principal_id IS NOT NULL
        OR parent.principal_id <> NEW.principal_id
        OR parent.organization_id IS DISTINCT FROM NEW.organization_id THEN
        RAISE EXCEPTION 'session % cannot link to that web session', NEW.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER sessions_web_link_parent
    BEFORE INSERT ON sessions
    FOR EACH ROW WHEN (NEW.web_session_id IS NOT NULL)
    EXECUTE FUNCTION sessions_web_link_parent();

CREATE TABLE oauth_grants (
    id                 uuid        PRIMARY KEY,
    principal_id       uuid        NOT NULL REFERENCES principals (id) ON DELETE CASCADE,
    organization_id    uuid        NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
    client_id          text        NOT NULL CHECK (char_length(client_id) BETWEEN 1 AND 512),
    client_class       text        NOT NULL CHECK (client_class IN ('browser', 'mcp')),
    purpose            text        NOT NULL DEFAULT 'oauth' CHECK (purpose = 'oauth'),
    resource           text        NOT NULL CHECK (char_length(resource) BETWEEN 1 AND 2048),
    audience           text        NOT NULL CHECK (audience IN ('acr-api', 'acr-mcp')),
    scope              text        NOT NULL CHECK (char_length(scope) BETWEEN 1 AND 256),
    session_id         uuid        NOT NULL UNIQUE REFERENCES sessions (id) ON DELETE CASCADE,
    binding            text        NOT NULL CHECK (binding IN ('web_session', 'client_session')),
    consented_at       timestamptz NOT NULL,
    revoked_at         timestamptz,
    created_at         timestamptz NOT NULL DEFAULT now(),
    -- An MCP client can never ask for the browser audience (ADR-13 decision 7).
    CONSTRAINT oauth_grants_class_binding CHECK (
        (client_class = 'browser' AND binding = 'web_session' AND audience = 'acr-api')
        OR (client_class = 'mcp' AND binding = 'client_session' AND audience = 'acr-mcp'))
);
CREATE INDEX oauth_grants_principal_idx ON oauth_grants (principal_id) WHERE revoked_at IS NULL;

CREATE TRIGGER oauth_grants_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON oauth_grants
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER oauth_grants_truncate_guard
    BEFORE TRUNCATE ON oauth_grants
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();

CREATE TABLE oauth_authorization_requests (
    id                    uuid        PRIMARY KEY,
    client_id             text        NOT NULL CHECK (char_length(client_id) BETWEEN 1 AND 512),
    client_class          text        NOT NULL CHECK (client_class IN ('browser', 'mcp')),
    redirect_uri          text        NOT NULL CHECK (char_length(redirect_uri) BETWEEN 1 AND 2048),
    resource              text        NOT NULL CHECK (char_length(resource) BETWEEN 1 AND 2048),
    audience              text        NOT NULL CHECK (audience IN ('acr-api', 'acr-mcp')),
    scope                 text        NOT NULL CHECK (char_length(scope) BETWEEN 1 AND 256),
    state                 text        NOT NULL CHECK (char_length(state) BETWEEN 1 AND 1024),
    code_challenge        text        NOT NULL CHECK (code_challenge ~ '^[A-Za-z0-9_-]{43}$'),
    code_challenge_method text        NOT NULL CHECK (code_challenge_method = 'S256'),
    -- Bound by describe, once, together.
    web_session_id        uuid        REFERENCES sessions (id) ON DELETE CASCADE,
    principal_id          uuid        REFERENCES principals (id) ON DELETE CASCADE,
    organization_id       uuid        REFERENCES organizations (id) ON DELETE CASCADE,
    csrf_hash             bytea       CHECK (csrf_hash IS NULL OR octet_length(csrf_hash) = 32),
    -- Written once, by the decision. An approval writes the grant and the code.
    decision              text        CHECK (decision IN ('approved', 'denied')),
    grant_id              uuid        REFERENCES oauth_grants (id) ON DELETE CASCADE,
    code_hash             bytea       CHECK (code_hash IS NULL OR octet_length(code_hash) = 32),
    code_expires_at       timestamptz,
    consumed_at           timestamptz,
    expires_at            timestamptz NOT NULL,
    created_at            timestamptz NOT NULL DEFAULT now(),
    -- organization_id stays NULL when an org-less web session binds: the refusal
    -- of such a session is approve's, for both client classes.
    CONSTRAINT oauth_requests_bound_together CHECK (
        (web_session_id IS NULL) = (principal_id IS NULL)
        AND (web_session_id IS NULL) = (csrf_hash IS NULL)
        AND (web_session_id IS NOT NULL OR organization_id IS NULL)),
    CONSTRAINT oauth_requests_class_audience CHECK (
        (client_class = 'browser' AND audience = 'acr-api') OR (client_class = 'mcp' AND audience = 'acr-mcp')),
    CONSTRAINT oauth_requests_decision_bound CHECK (decision IS NULL OR web_session_id IS NOT NULL),
    CONSTRAINT oauth_requests_approval_shape CHECK (
        (decision IS NOT DISTINCT FROM 'approved') = (grant_id IS NOT NULL)
        AND (grant_id IS NULL) = (code_hash IS NULL)
        AND (grant_id IS NULL) = (code_expires_at IS NULL)),
    CONSTRAINT oauth_requests_consumed_needs_code CHECK (consumed_at IS NULL OR code_hash IS NOT NULL)
);
CREATE UNIQUE INDEX oauth_requests_code_hash_key ON oauth_authorization_requests (code_hash)
    WHERE code_hash IS NOT NULL;
-- The cleanup's scan: expired rows, oldest first.
CREATE INDEX oauth_requests_expires_idx ON oauth_authorization_requests (expires_at);

-- The binding, the decision and the consume are each written once.
CREATE FUNCTION oauth_requests_write_once() RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    IF (OLD.web_session_id IS NOT NULL AND (NEW.web_session_id IS DISTINCT FROM OLD.web_session_id
            OR NEW.principal_id IS DISTINCT FROM OLD.principal_id
            OR NEW.organization_id IS DISTINCT FROM OLD.organization_id
            OR NEW.csrf_hash IS DISTINCT FROM OLD.csrf_hash))
        OR (OLD.decision IS NOT NULL AND (NEW.decision IS DISTINCT FROM OLD.decision
            OR NEW.grant_id IS DISTINCT FROM OLD.grant_id
            OR NEW.code_hash IS DISTINCT FROM OLD.code_hash
            OR NEW.code_expires_at IS DISTINCT FROM OLD.code_expires_at))
        OR (OLD.consumed_at IS NOT NULL AND NEW.consumed_at IS DISTINCT FROM OLD.consumed_at) THEN
        RAISE EXCEPTION 'authorization request % was already written', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER oauth_requests_write_once
    BEFORE UPDATE ON oauth_authorization_requests
    FOR EACH ROW EXECUTE FUNCTION oauth_requests_write_once();

-- A denial's only write is its decision, so the decision is that audit.Commit's
-- state change (0013's counter; outside a Commit it counts nothing).
CREATE TRIGGER oauth_requests_decision_count
    AFTER UPDATE OF decision ON oauth_authorization_requests
    FOR EACH ROW WHEN (OLD.decision IS DISTINCT FROM NEW.decision)
    EXECUTE FUNCTION audit_count_create();
