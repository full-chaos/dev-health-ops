-- 0004 the authorization model (TRD §9; ACP-ADR-05 for revisions).
-- ADDITIVE ONLY. See 0001's header.

-- actions is the closed vocabulary of things a principal can attempt. It is a
-- TABLE rather than a Go enum on purpose: ACP-ADR-05 makes policy a versioned,
-- inspectable artifact, and an action set that lives only in compiled code
-- cannot be diffed against a policy revision or audited by an operator.
CREATE TABLE actions (
    -- Named action_key rather than a bare "key": every foreign key that
    -- references it is already called action_key, and in an auth schema a
    -- column called "key" reads as signing-key material at a glance.
    action_key  text        PRIMARY KEY,
    description text        NOT NULL DEFAULT '',
    -- resource_kind is what the action applies to; a grant is only meaningful
    -- against a resource of a matching kind.
    resource_kind text      NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX actions_resource_kind_idx ON actions (resource_kind);

-- roles are named bundles of actions. scope separates platform-level roles
-- from organization-level ones so a query for platform power cannot
-- accidentally include tenant roles.
CREATE TABLE roles (
    -- role_key, for the same reason as actions.action_key above.
    role_key    text        PRIMARY KEY,
    name        text        NOT NULL,
    description text        NOT NULL DEFAULT '',
    scope       text        NOT NULL CHECK (scope IN ('platform', 'organization')),
    -- built_in roles are defined by the platform and must not be edited by a
    -- tenant. Marking them in the row means the rule is enforceable in one
    -- place rather than inferred from the key's spelling.
    built_in    boolean     NOT NULL DEFAULT false,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- role_actions is the many-to-many between roles and actions. ON DELETE
-- RESTRICT on the action side: removing an action still referenced by a role
-- would silently shrink that role's meaning, which is exactly the kind of
-- change that should require an explicit migration.
CREATE TABLE role_actions (
    role_key   text        NOT NULL REFERENCES roles (role_key) ON DELETE CASCADE,
    action_key text        NOT NULL REFERENCES actions (action_key) ON DELETE RESTRICT,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (role_key, action_key)
);

CREATE INDEX role_actions_action_idx ON role_actions (action_key);

-- resource_grants binds a principal to a role over a specific resource. The
-- resource is identified by kind plus id rather than by a foreign key, because
-- the resources being granted over live in the OPS schema and beyond -- and
-- ACP-ADR-04 forbids this schema reaching across that boundary. The cost is
-- that referential integrity for the resource half is the application's job;
-- the benefit is that the ownership boundary is real.
CREATE TABLE resource_grants (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    principal_id    uuid        NOT NULL REFERENCES principals (id) ON DELETE CASCADE,
    organization_id uuid        NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
    role_key        text        NOT NULL REFERENCES roles (role_key) ON DELETE RESTRICT,
    resource_kind   text        NOT NULL,
    -- resource_id is text, not uuid: not every grantable resource in the
    -- platform is keyed by uuid, and coercing them would make this table lie
    -- about the ones that are not.
    resource_id     text        NOT NULL,
    granted_by      uuid        REFERENCES principals (id) ON DELETE SET NULL,
    expires_at      timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX resource_grants_unique_key
    ON resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id);
CREATE INDEX resource_grants_lookup_idx
    ON resource_grants (principal_id, organization_id, resource_kind);

-- policy_revisions is ACP-ADR-05's versioning surface: each row is an
-- immutable snapshot of the policy that was in force, with the digest that
-- identifies it. A decision recorded in the audit log names a revision, so
-- "why was this allowed" is answerable after the policy has since changed --
-- which it is not if only the current policy exists.
CREATE TABLE policy_revisions (
    id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    revision     bigint      NOT NULL,
    -- digest identifies the content. Two deployments computing the same
    -- digest hold the same policy, which is what makes rollout verifiable.
    digest       bytea       NOT NULL,
    document     jsonb       NOT NULL,
    created_by   uuid        REFERENCES principals (id) ON DELETE SET NULL,
    activated_at timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX policy_revisions_revision_key ON policy_revisions (revision);
CREATE UNIQUE INDEX policy_revisions_digest_key ON policy_revisions (digest);

-- entitlement_snapshots is ACP-ADR-07's contract surface: the entitlement
-- state an organization held at a point in time, kept as a snapshot rather
-- than derived on demand so a billing or licensing decision can be replayed
-- exactly as it was made.
CREATE TABLE entitlement_snapshots (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id uuid        NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
    -- sequence orders snapshots within an organization without depending on
    -- timestamp resolution, which two snapshots in the same millisecond would
    -- make ambiguous.
    sequence        bigint      NOT NULL,
    document        jsonb       NOT NULL,
    digest          bytea       NOT NULL,
    effective_from  timestamptz NOT NULL DEFAULT now(),
    effective_to    timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX entitlement_snapshots_org_sequence_key
    ON entitlement_snapshots (organization_id, sequence);
CREATE INDEX entitlement_snapshots_effective_idx
    ON entitlement_snapshots (organization_id, effective_from DESC);
