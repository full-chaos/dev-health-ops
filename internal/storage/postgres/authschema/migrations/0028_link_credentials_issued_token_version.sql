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
-- column: any UPDATE of it runs only inside an audit transaction, as the other
-- security columns of the row.
CREATE TRIGGER link_credentials_issued_token_version_guard
    BEFORE UPDATE OF issued_token_version ON link_credentials
    FOR EACH ROW EXECUTE FUNCTION audit_commit_guard();

-- It is also write-once: the issuer's INSERT stores it and no UPDATE changes
-- it, not even inside an audit transaction (the guard above checks where a
-- write happens, not which value it writes). A row without a value keeps none,
-- so it never redeems.
CREATE FUNCTION issued_token_version_write_once() RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    RAISE EXCEPTION '%.issued_token_version of % is written once and cannot change', TG_TABLE_NAME, OLD.id
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$;
CREATE TRIGGER link_credentials_issued_token_version_write_once
    BEFORE UPDATE OF issued_token_version ON link_credentials
    FOR EACH ROW WHEN (NEW.issued_token_version IS DISTINCT FROM OLD.issued_token_version)
    EXECUTE FUNCTION issued_token_version_write_once();
