-- 0002 organizations, membership and non-human principals (TRD §9).
-- ADDITIVE ONLY. See 0001's header for the lineage-wide rules.

CREATE TABLE organizations (
    id         uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    -- slug is the stable external handle; name is the mutable display label.
    -- Keeping them apart means renaming an organization never invalidates a
    -- URL, an audit row, or anything else that captured the handle.
    slug       text        NOT NULL,
    name       text        NOT NULL,
    disabled_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX organizations_slug_key ON organizations (slug);

-- memberships is the principal-to-organization edge. It carries no role: a
-- role is a platform_role_assignment or a resource_grant, and collapsing
-- "belongs to" with "may do" into one row is what makes membership tables
-- impossible to reason about later.
CREATE TABLE memberships (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id uuid        NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
    principal_id    uuid        NOT NULL REFERENCES principals (id) ON DELETE CASCADE,
    -- status is closed at the database level. An invited member is not yet a
    -- member for authorization purposes, and a suspended one has stopped
    -- being one without the edge being deleted.
    status          text        NOT NULL DEFAULT 'active'
                                CHECK (status IN ('invited', 'active', 'suspended')),
    invited_by      uuid        REFERENCES principals (id) ON DELETE SET NULL,
    joined_at       timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX memberships_org_principal_key
    ON memberships (organization_id, principal_id);
CREATE INDEX memberships_principal_idx ON memberships (principal_id);
CREATE INDEX memberships_active_idx
    ON memberships (organization_id, principal_id) WHERE status = 'active';

-- platform_role_assignments are grants that exist ABOVE any organization --
-- platform administration, support access, billing operations. They are kept
-- separate from resource_grants precisely because they are the dangerous ones:
-- a single table makes "who holds platform power" a query with a WHERE clause
-- someone can forget.
CREATE TABLE platform_role_assignments (
    id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    principal_id uuid        NOT NULL REFERENCES principals (id) ON DELETE CASCADE,
    role_key     text        NOT NULL,
    granted_by   uuid        REFERENCES principals (id) ON DELETE SET NULL,
    -- expires_at makes a time-bounded elevation representable. NULL is
    -- permanent, which should be rare and is visible as such.
    expires_at   timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX platform_role_assignments_principal_role_key
    ON platform_role_assignments (principal_id, role_key);

-- service_accounts is the non-human, non-workload principal: an integration or
-- an automation acting on an organization's behalf.
CREATE TABLE service_accounts (
    principal_id    uuid        PRIMARY KEY REFERENCES principals (id) ON DELETE RESTRICT,
    organization_id uuid        NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
    name            text        NOT NULL,
    -- created_by keeps provenance for an account that, by construction, no
    -- person logs into.
    created_by      uuid        REFERENCES principals (id) ON DELETE SET NULL,
    last_used_at    timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX service_accounts_org_name_key ON service_accounts (organization_id, name);

-- workloads is the deployed-software principal, whose identity is federated
-- rather than stored: ACP-ADR-06 fixes Kubernetes workload identity as a live
-- TokenReview call, never a local JWT decode. This table therefore records the
-- REGISTRATION a TokenReview result is matched against -- issuer, namespace,
-- service account -- and holds no credential of any kind.
CREATE TABLE workloads (
    principal_id     uuid        PRIMARY KEY REFERENCES principals (id) ON DELETE RESTRICT,
    organization_id  uuid        REFERENCES organizations (id) ON DELETE CASCADE,
    name             text        NOT NULL,
    -- trust_domain, issuer, namespace and service_account_name are the exact
    -- fields ACP-ADR-06 requires be validated against TokenReview's own
    -- response rather than trusted from a token body.
    trust_domain     text        NOT NULL,
    issuer           text        NOT NULL,
    namespace        text        NOT NULL,
    service_account_name text    NOT NULL,
    -- audience is what the workload's token must be minted for; an exact
    -- audience per workload is ACP-ADR-06 §3's "no universal internal
    -- credential" rule expressed as data.
    audience         text        NOT NULL,
    disabled_at      timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

-- The federation tuple must be unique: two workload registrations matching the
-- same TokenReview result would make the resulting principal ambiguous.
CREATE UNIQUE INDEX workloads_federation_key
    ON workloads (trust_domain, issuer, namespace, service_account_name);
CREATE INDEX workloads_organization_idx ON workloads (organization_id);
