-- 0034 platform_role_assignments: closed role, no cascade.
-- NOT ADDITIVE in two places, on purpose (additiveExemptions): the foreign keys
-- platform_role_assignments_principal_id_fkey (ON DELETE CASCADE) and
-- platform_role_assignments_granted_by_fkey (ON DELETE SET NULL) become
-- ON DELETE RESTRICT. Nothing is dropped from stored data.
--
-- role_key: one of the platform roles of the closed catalog
-- (contracts/auth/v1/vocabulary/roles.v1.json, scope platform; a unit test
-- holds this list equal to the catalog). An organization role is not a value
-- here: organization authority is a membership role, never a platform role.
-- The CHECK is validated now: the pre-flight below refuses a table that holds
-- another value.
--
-- principal_id: deleting a principal that holds a platform role is refused
-- instead of silently deleting the role. Revoke first, delete never.
-- granted_by: deleting the granter is refused too, so a granted row can never
-- turn into one with no granter (the users backfill revokes only rows with no
-- granter).

-- Pre-flight: name the rows the role CHECK below would refuse, by id only.
-- Nothing is changed.
DO $$
DECLARE
    refused text;
BEGIN
    SELECT string_agg(id::text, ', ' ORDER BY id)
      INTO refused
      FROM platform_role_assignments
     WHERE role_key NOT IN ('platform_admin');
    IF refused IS NOT NULL THEN
        RAISE EXCEPTION 'migration 0034: platform role assignments with a role outside the platform roles: %', refused
            USING ERRCODE = '23514';
    END IF;
END
$$;

ALTER TABLE platform_role_assignments
    ADD CONSTRAINT platform_role_assignments_role_closed
    CHECK (role_key IN ('platform_admin'));

ALTER TABLE platform_role_assignments
    DROP CONSTRAINT platform_role_assignments_principal_id_fkey,
    ADD CONSTRAINT platform_role_assignments_principal_id_fkey
    FOREIGN KEY (principal_id) REFERENCES principals (id) ON DELETE RESTRICT,
    DROP CONSTRAINT platform_role_assignments_granted_by_fkey,
    ADD CONSTRAINT platform_role_assignments_granted_by_fkey
    FOREIGN KEY (granted_by) REFERENCES principals (id) ON DELETE RESTRICT;
