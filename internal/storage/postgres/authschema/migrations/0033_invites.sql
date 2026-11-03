-- 0033 invites: the Invite aggregate's table (CHAOS-10981). Additive, except
-- one CHECK that only WIDENS: aggregate_revisions.aggregate_type admits
-- 'invite' (the 0013 list plus that one value, so no stored row can violate it).
--
-- An invite row holds the SHA-256 hex digest of its token, never the token:
-- a read of the table does not yield a usable link. The token is single
-- purpose (org_invite_token) and consumed once: status leaves 'pending' for
-- 'accepted' or 'revoked' and never returns (no DELETE in the runtime
-- manifest; a revoked invite keeps its row). An expired invite is a pending
-- row past expires_at; no writer stamps it.
--
-- email_normalized is the invite's address in its login-key form. Unlike
-- invite.v1 on the wire, the row needs it: an accept compares it with the
-- account's address.
CREATE TABLE invites (
    id                    uuid        PRIMARY KEY,
    organization_id       uuid        NOT NULL REFERENCES organizations (id) ON DELETE RESTRICT,
    email_normalized      text        NOT NULL CHECK (email_normalized <> '' AND length(email_normalized) <= 254),
    role                  text        NOT NULL CHECK (role IN ('viewer', 'member', 'admin', 'owner')),
    token_hash            text        NOT NULL UNIQUE CHECK (token_hash ~ '^[0-9a-f]{64}$'),
    invited_by            uuid        REFERENCES principals (id) ON DELETE SET NULL,
    status                text        NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'accepted', 'revoked')),
    expires_at            timestamptz NOT NULL,
    accepted_at           timestamptz,
    accepted_principal_id uuid        REFERENCES principals (id) ON DELETE SET NULL,
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT invites_accepted_has_principal CHECK (status <> 'accepted' OR (accepted_at IS NOT NULL AND accepted_principal_id IS NOT NULL))
);

CREATE INDEX invites_organization_email_status ON invites (organization_id, email_normalized, status);

-- A security table: written only through audit.Commit (the 0006 guard).
CREATE TRIGGER invites_commit_guard
    BEFORE INSERT OR UPDATE OR DELETE ON invites
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
CREATE TRIGGER invites_truncate_guard
    BEFORE TRUNCATE ON invites
    FOR EACH STATEMENT EXECUTE FUNCTION audit_commit_guard();

ALTER TABLE aggregate_revisions
    DROP CONSTRAINT aggregate_revisions_aggregate_type_check,
    ADD CONSTRAINT aggregate_revisions_aggregate_type_check
    CHECK (aggregate_type IN ('principal', 'user', 'external_identity',
        'organization', 'membership', 'platform_role_assignment', 'service_account', 'workload', 'resource_grant', 'invite'));
