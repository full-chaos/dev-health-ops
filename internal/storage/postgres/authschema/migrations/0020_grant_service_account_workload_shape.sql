-- 0020 grants, service accounts and workloads: closed shape, no cascade.
-- NOT ADDITIVE in five places, on purpose (additiveExemptions): the foreign keys
-- resource_grants_principal_id_fkey, resource_grants_organization_id_fkey,
-- service_accounts_organization_id_fkey and workloads_organization_id_fkey change
-- from ON DELETE CASCADE to ON DELETE RESTRICT, and resource_grants_unique_key is
-- recreated as a partial unique index (same name, same columns, WHERE revoked_at
-- IS NULL). Nothing is dropped from stored data. New CHECKs on existing columns
-- are NOT VALID: enforced for every new or changed row, validated by the leaf that
-- fills the table (W4 4.19 service accounts, 4.20 workloads, 4.21 grants).
--
-- resource_grants:
--   * resource_kind is closed to 'repository' (no kind without a producer).
--   * a repository resource_id is the canonical lowercase owner/repo key or the
--     explicit organization-wide '*'. The pattern is the one of
--     authorization_request.v1 (repository key); a unit test holds them equal.
--   * revoked_at / revoked_by mark a revoked grant. A revoked row stays as history
--     and frees its tuple: uniqueness holds among rows that are not revoked, so a
--     re-grant is a new row and the earlier granted_by / revoked_by stay verbatim.
--   * principal and organization foreign keys are RESTRICT: deactivate first,
--     delete never.
-- service_accounts: status (active, disabled) with disabled_at, a named owner
--   (owner_principal_id; required for every new or changed row), RESTRICT.
-- workloads: allowed_actions is the list of actions the workload may be granted,
--   empty by default and never a wildcard; a unique name per organization and a
--   unique name among platform workloads (organization_id NULL), so no shared
--   universal principal can be registered twice; RESTRICT.

CREATE FUNCTION allowed_actions_are_valid(actions text[]) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
SET search_path = pg_catalog, pg_temp AS $$
    SELECT cardinality(actions) <= 64
       AND NOT EXISTS (SELECT 1 FROM unnest(actions) AS a WHERE a !~ '^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$')
       AND (SELECT count(DISTINCT a) FROM unnest(actions) AS a) = cardinality(actions)
$$;

ALTER TABLE resource_grants
    ADD COLUMN revoked_at timestamptz,
    ADD COLUMN revoked_by uuid REFERENCES principals (id) ON DELETE RESTRICT;

ALTER TABLE resource_grants
    ADD CONSTRAINT resource_grants_kind_closed
    CHECK (resource_kind IN ('repository')) NOT VALID,
    ADD CONSTRAINT resource_grants_repository_key_canonical
    CHECK (resource_kind <> 'repository' OR resource_id ~ '^([a-z0-9][a-z0-9._-]{0,99}/[a-z0-9][a-z0-9._-]{0,99}|\*)$') NOT VALID,
    ADD CONSTRAINT resource_grants_revoked_by_needs_revoked_at
    CHECK (revoked_by IS NULL OR revoked_at IS NOT NULL);

DROP INDEX resource_grants_unique_key;
CREATE UNIQUE INDEX resource_grants_unique_key
    ON resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id)
    WHERE revoked_at IS NULL;

ALTER TABLE resource_grants
    DROP CONSTRAINT resource_grants_principal_id_fkey,
    ADD CONSTRAINT resource_grants_principal_id_fkey
    FOREIGN KEY (principal_id) REFERENCES principals (id) ON DELETE RESTRICT,
    DROP CONSTRAINT resource_grants_organization_id_fkey,
    ADD CONSTRAINT resource_grants_organization_id_fkey
    FOREIGN KEY (organization_id) REFERENCES organizations (id) ON DELETE RESTRICT;

ALTER TABLE service_accounts
    ADD COLUMN status text NOT NULL DEFAULT 'active',
    ADD COLUMN disabled_at timestamptz,
    ADD COLUMN owner_principal_id uuid REFERENCES principals (id) ON DELETE RESTRICT;

ALTER TABLE service_accounts
    ADD CONSTRAINT service_accounts_status_closed
    CHECK (status IN ('active', 'disabled')),
    ADD CONSTRAINT service_accounts_disabled_at_matches_status
    CHECK ((status = 'disabled') = (disabled_at IS NOT NULL)),
    ADD CONSTRAINT service_accounts_named_owner
    CHECK (owner_principal_id IS NOT NULL) NOT VALID,
    DROP CONSTRAINT service_accounts_organization_id_fkey,
    ADD CONSTRAINT service_accounts_organization_id_fkey
    FOREIGN KEY (organization_id) REFERENCES organizations (id) ON DELETE RESTRICT;

ALTER TABLE workloads
    ADD COLUMN allowed_actions text[] NOT NULL DEFAULT '{}',
    ADD CONSTRAINT workloads_allowed_actions_valid
    CHECK (allowed_actions_are_valid(allowed_actions)),
    DROP CONSTRAINT workloads_organization_id_fkey,
    ADD CONSTRAINT workloads_organization_id_fkey
    FOREIGN KEY (organization_id) REFERENCES organizations (id) ON DELETE RESTRICT;

CREATE UNIQUE INDEX workloads_organization_name_key
    ON workloads (organization_id, name) WHERE organization_id IS NOT NULL;
CREATE UNIQUE INDEX workloads_platform_name_key
    ON workloads (name) WHERE organization_id IS NULL;
