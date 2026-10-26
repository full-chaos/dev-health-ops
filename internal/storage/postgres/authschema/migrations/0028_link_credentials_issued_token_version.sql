-- 0028 the credential state a link was issued against. ADDITIVE ONLY. See
-- 0001's header.
--
-- issued_token_version is public.users.token_version at the moment the link was
-- issued. Every password write, on go-api's side or auth-service's, raises that
-- counter in the same statement, so a reset link is redeemable only while it
-- still equals the user's current value. It is a number, not material: no copy
-- or digest of the password hash is kept. NULL = issued before this migration;
-- such a row never redeems. No backfill, no index.
ALTER TABLE link_credentials ADD COLUMN issued_token_version bigint;

-- issued_token_version decides whether a link redeems, so it is a security
-- column: written once by the issuer's INSERT, and any later UPDATE of it runs
-- only inside an audit transaction, as the other security columns of the row.
CREATE TRIGGER link_credentials_issued_token_version_guard
    BEFORE UPDATE OF issued_token_version ON link_credentials
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();
