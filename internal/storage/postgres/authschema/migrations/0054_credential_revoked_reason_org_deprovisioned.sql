-- 0054 credential_registry.revoked_reason admits org_deprovisioned (CHAOS-12941,
-- part of CHAOS-3278). An organization deprovision revokes the organization's
-- service-account keys in its own commit and now names itself as the reason,
-- apart from the operator's owner_request. The list is the 0049 list plus that
-- one value, so it can refuse no stored row and needs no pre-flight; the three
-- earlier values stay. A test holds the list equal to vocabulary/reason-codes.json.
--
-- The constraint is dropped and added again NOT VALID, then validated, as 0049 did.
LOCK TABLE credential_registry IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE credential_registry
    DROP CONSTRAINT credential_registry_revoked_reason_code,
    ADD CONSTRAINT credential_registry_revoked_reason_code
    CHECK (revoked_reason IS NULL OR revoked_reason IN ('compromised', 'domain_revoked', 'org_deprovisioned', 'owner_request')) NOT VALID;

ALTER TABLE credential_registry VALIDATE CONSTRAINT credential_registry_revoked_reason_code;
