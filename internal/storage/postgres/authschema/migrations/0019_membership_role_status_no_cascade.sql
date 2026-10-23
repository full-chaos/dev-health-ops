-- 0019 memberships: role, status 'removed', no cascade from organizations or
-- principals. NOT ADDITIVE in three places, on purpose (additiveExemptions):
-- the status CHECK is redefined (widened by 'removed', nothing removed), and the
-- two cascading foreign keys become RESTRICT. Nothing is dropped from stored data.
--
-- role: the organization role of the membership, one of the organization roles
-- of the closed catalog (contracts/auth/v1/vocabulary/roles.v1.json; a unit test
-- holds this list equal to the catalog). A platform role is not a value here:
-- platform authority is a platform_role_assignment, never a membership role.
-- The column has no default and is nullable, so no row gains a role by itself.
-- memberships_active_has_role requires a role on an active row. It is NOT VALID:
-- it is enforced for every new and changed row, and the leaf that backfills the
-- memberships of the public tables (W4 4.6) validates it once every row has a role.
--
-- status: 'removed' keeps the edge and denies the member; removal is a status
-- change, not a delete, so the revision history of the membership survives.
--
-- foreign keys: deleting an organization or a principal that still has a
-- membership is refused (RESTRICT) instead of silently deleting the memberships.
-- invited_by keeps ON DELETE SET NULL.
ALTER TABLE memberships ADD COLUMN role text;

ALTER TABLE memberships
    ADD CONSTRAINT memberships_role_closed
    CHECK (role IN ('viewer', 'member', 'admin', 'owner'));

ALTER TABLE memberships
    DROP CONSTRAINT memberships_status_check,
    ADD CONSTRAINT memberships_status_check
    CHECK (status IN ('invited', 'active', 'suspended', 'removed'));

ALTER TABLE memberships
    ADD CONSTRAINT memberships_active_has_role
    CHECK (status <> 'active' OR role IS NOT NULL) NOT VALID;

ALTER TABLE memberships
    DROP CONSTRAINT memberships_organization_id_fkey,
    ADD CONSTRAINT memberships_organization_id_fkey
    FOREIGN KEY (organization_id) REFERENCES organizations (id) ON DELETE RESTRICT;

ALTER TABLE memberships
    DROP CONSTRAINT memberships_principal_id_fkey,
    ADD CONSTRAINT memberships_principal_id_fkey
    FOREIGN KEY (principal_id) REFERENCES principals (id) ON DELETE RESTRICT;
