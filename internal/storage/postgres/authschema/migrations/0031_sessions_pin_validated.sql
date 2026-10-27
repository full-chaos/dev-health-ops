-- 0031 validate the session pin check. ADDITIVE ONLY. See 0001's header.
--
-- 0029 added sessions_pin_present NOT VALID: new and changed rows must carry a
-- pin or be revoked. This validates it for every stored row. It refuses, with
-- a count and no row data, while an unrevoked session has no pin; end those
-- sessions first (auth-migrate sessions revoke-unpinned -apply), then migrate
-- again. A refusal fails the migration job and records nothing.
--
-- VALIDATE CONSTRAINT takes SHARE UPDATE EXCLUSIVE: reads and writes continue.
-- There is no down step: the lineage is forward-only.
DO $$
DECLARE
    violating bigint;
BEGIN
    SELECT count(*) INTO violating FROM sessions WHERE legacy_token_version IS NULL AND revoked_at IS NULL;
    IF violating > 0 THEN
        RAISE EXCEPTION 'migration 0031 refused: % unrevoked session(s) without a pin; run auth-migrate sessions revoke-unpinned -apply, then migrate again', violating
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
END
$$;

ALTER TABLE sessions VALIDATE CONSTRAINT sessions_pin_present;
