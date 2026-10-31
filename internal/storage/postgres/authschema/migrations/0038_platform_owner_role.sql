-- 0038 platform_role_assignments.role_key admits platform_owner.
-- NOT ADDITIVE in one place, on purpose (additiveExemptions): it REDEFINES the
-- role CHECK and only WIDENS it (platform_admin stays; platform_owner is the one
-- new value, the second platform role of contracts/auth/v1/vocabulary/roles.v1.json).
--
-- platform_owner is a marker role: it holds exactly the platform_admin actions
-- and is granted and revoked only by auth-migrate (platform-owner-grant,
-- platform-owner-revoke), never through the API. The new list can refuse no
-- stored row, so there is no pre-flight. A unit test holds the list equal to the
-- catalog's platform roles.
--
-- The CHECK is added NOT VALID and validated in a statement of its own, so the
-- table scan of the validation does not run under the ACCESS EXCLUSIVE lock of
-- the ALTER. Both run in the migration's one transaction.
ALTER TABLE platform_role_assignments
    DROP CONSTRAINT platform_role_assignments_role_closed,
    ADD CONSTRAINT platform_role_assignments_role_closed
    CHECK (role_key IN ('platform_admin', 'platform_owner')) NOT VALID;

ALTER TABLE platform_role_assignments
    VALIDATE CONSTRAINT platform_role_assignments_role_closed;
