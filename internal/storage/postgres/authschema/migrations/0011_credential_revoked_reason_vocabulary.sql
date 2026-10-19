-- 0011 credential_registry.revoked_reason becomes a closed vocabulary
-- (CHAOS-10194). NOT ADDITIVE in one place, on purpose: it TIGHTENS one CHECK.
--
-- 0008 admitted any lowercase code, ^[a-z][a-z0-9_]{2,63}$, so a secret-shaped
-- lowercase value was storable and could reach the wire. The closed set is the
-- one contracts/auth/v1/vocabulary/reason-codes.json carries for the
-- credential-entry and credential-list wire forms:
--
--   compromised, owner_request
--
-- A new value arrives with the producer that emits it and its own migration. A
-- test compares this list with the vocabulary file in both directions.
--
-- The pre-flight below refuses the migration, with a count and no values, when
-- a stored row would violate the new CHECK. It never rewrites or deletes: a
-- row holding an unlisted code is something an operator has to look at, and
-- the auth schema carries no production authority yet (CHAOS-4881).
--
-- sessions.revoked_reason and refresh_credentials.revoked_reason stay free
-- text: they are not on the credential wire and have no vocabulary.
-- The lock comes first. The ALTER below needs ACCESS EXCLUSIVE anyway; taking it
-- before the count means the count and the ALTER see the same rows. Without it a
-- writer could commit a legacy-valid code between the two, and the migration
-- would fail on the generic constraint violation instead of the named count.
LOCK TABLE credential_registry IN ACCESS EXCLUSIVE MODE;

DO $$
DECLARE
    offending bigint;
BEGIN
    SELECT count(*) INTO offending
      FROM credential_registry
     WHERE revoked_reason IS NOT NULL
       AND revoked_reason NOT IN ('compromised', 'owner_request');
    IF offending > 0 THEN
        RAISE EXCEPTION '% credential_registry row(s) hold a revoked_reason outside the closed vocabulary (compromised, owner_request); resolve them before applying 0011', offending
            USING ERRCODE = '23514';
    END IF;
END
$$;

ALTER TABLE credential_registry
    DROP CONSTRAINT credential_registry_revoked_reason_code,
    ADD CONSTRAINT credential_registry_revoked_reason_code
    CHECK (revoked_reason IS NULL OR revoked_reason IN ('compromised', 'owner_request'));
