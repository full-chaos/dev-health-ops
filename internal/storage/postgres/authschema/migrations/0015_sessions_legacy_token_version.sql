-- 0015 the go-api token_version a session was created at. ADDITIVE ONLY. See
-- 0001's header.
--
-- An access token auth-service mints carries tv, which go-api's readers check
-- against public.users.token_version; a go-api password reset bumps that
-- version. legacy_token_version is the version the session began at. A refresh
-- refuses a session whose pin differs from the user's current version, so a
-- reset ends the auth-service sessions as it ends go-api's own refresh tokens.
-- A session row from before this migration has no pin and is refused.
--
-- It is written once, at insert. The trigger refuses any change.
ALTER TABLE sessions ADD COLUMN legacy_token_version bigint;

CREATE FUNCTION sessions_legacy_token_version_write_once() RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    RAISE EXCEPTION 'sessions.legacy_token_version of % cannot change', OLD.id
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$;
CREATE TRIGGER sessions_legacy_token_version_write_once
    BEFORE UPDATE OF legacy_token_version ON sessions
    FOR EACH ROW WHEN (OLD.legacy_token_version IS DISTINCT FROM NEW.legacy_token_version)
    EXECUTE FUNCTION sessions_legacy_token_version_write_once();
