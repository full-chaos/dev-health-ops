-- 0018 the go-api token version and last sign-in of a user. ADDITIVE ONLY. See
-- 0001's header.
--
-- legacy_token_version is the user's current go-api token_version. An access
-- token carries tv, and the per-request readers compare it with the user's
-- current version; a password reset bumps it. 0 is the version of a user that
-- never had one bumped.
--
-- last_login_at is the user's latest sign-in, NULL when there is none.
ALTER TABLE users
    ADD COLUMN legacy_token_version bigint NOT NULL DEFAULT 0,
    ADD COLUMN last_login_at timestamptz;

-- legacy_token_version changes who may act (a bump ends every token minted
-- before it), so like users' other security columns it is written only inside
-- audit.Commit. last_login_at is a high-rate lifecycle column and is not
-- guarded.
CREATE TRIGGER users_token_version_guard
    BEFORE UPDATE OF legacy_token_version ON users
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
