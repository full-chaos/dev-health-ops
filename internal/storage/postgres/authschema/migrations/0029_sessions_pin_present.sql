-- 0029 every new session row carries its token_version pin. ADDITIVE ONLY. See
-- 0001's header.
--
-- sessions.legacy_token_version is written once, at insert (0016). A row from
-- before 0016 has no pin. The constraint below is NOT VALID: it refuses every
-- new or changed row that is live and has no pin, and it does not look at the
-- rows already stored. A later migration validates it once those rows are
-- revoked (auth-migrate sessions revoke-unpinned).
ALTER TABLE sessions
    ADD CONSTRAINT sessions_pin_present
    CHECK (legacy_token_version IS NOT NULL OR revoked_at IS NOT NULL) NOT VALID;
