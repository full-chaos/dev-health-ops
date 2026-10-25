-- Revision 0156 downgrade (0156 -> 0155), the reverse of ../0156_add_internal_service_credential_audits_principal_id.sql.
-- The migrator runs this file and the alembic_version update in one transaction.
-- Audit rows are never changed: when a row holds a principal_id, the downgrade stops with the count and leaves the
-- schema as it was.

-- The table is locked first, so no audit row can commit between the count and the drop of the column.
LOCK TABLE internal_service_credential_audits IN ACCESS EXCLUSIVE MODE;

DO $$
DECLARE
    pending bigint;
BEGIN
    SELECT count(*) INTO pending FROM internal_service_credential_audits WHERE principal_id IS NOT NULL;
    IF pending > 0 THEN
        RAISE EXCEPTION 'cannot downgrade 0156: % internal_service_credential_audits rows hold a principal_id; audit rows are never changed', pending
            USING ERRCODE = 'check_violation';
    END IF;
END
$$;

ALTER TABLE internal_service_credential_audits DROP CONSTRAINT ck_internal_service_credential_audits_principal_id_length;
ALTER TABLE internal_service_credential_audits DROP CONSTRAINT ck_internal_service_credential_audits_one_actor;
ALTER TABLE internal_service_credential_audits DROP COLUMN principal_id;
