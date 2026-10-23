-- credential_registry: a domain-owned row (acr, external_push) stores its
-- public fingerprint and no verifier of any secret.
--
-- ACP-ADR-08/09: the domain system keeps its own validator and its own hashed
-- lookup. The registry projects metadata only, so a projected row must not be
-- able to hold a hash that could be mistaken for, or used as, a second
-- verifier. secret_hash and hash_algorithm stay NOT NULL (the additive guard
-- forbids relaxing them), so a domain-owned row uses the one fingerprint-only
-- form:
--     hash_algorithm = 'fingerprint'
--     secret_hash    = decode(fingerprint, 'hex')   (the 8 public bytes)
-- Those bytes are the non-secret fingerprint itself, never a digest of a
-- secret. A platform row keeps a real hash and may not use the 'fingerprint'
-- algorithm, so the two forms cannot be confused.
--
-- The pre-check counts violating rows and refuses with the constraint name and
-- the count only (never row data), before the CHECK is added.
DO $$
DECLARE
    violating bigint;
BEGIN
    SELECT count(*) INTO violating FROM credential_registry
    WHERE NOT (
        CASE WHEN domain_owner = 'platform'
             THEN hash_algorithm <> 'fingerprint'
             ELSE hash_algorithm = 'fingerprint' AND fingerprint IS NOT NULL
                  AND secret_hash = decode(fingerprint, 'hex')
        END);
    IF violating > 0 THEN
        RAISE EXCEPTION 'migration refused: credential_registry_domain_owned_fingerprint_only has % violating row(s); fix or remove them, then migrate again', violating
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
END
$$;

ALTER TABLE credential_registry
    ADD CONSTRAINT credential_registry_domain_owned_fingerprint_only
    CHECK (
        CASE WHEN domain_owner = 'platform'
             THEN hash_algorithm <> 'fingerprint'
             ELSE hash_algorithm = 'fingerprint' AND fingerprint IS NOT NULL
                  AND secret_hash = decode(fingerprint, 'hex')
        END
    ) NOT VALID;

ALTER TABLE credential_registry VALIDATE CONSTRAINT credential_registry_domain_owned_fingerprint_only;
