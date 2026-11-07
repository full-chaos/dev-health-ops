-- Revision 0164 downgrade (0164 -> 0163), the reverse of ../0164_admit_user_access_token_routing_audits.sql.
-- The migrator runs this file and the alembic_version update in one transaction.
-- go_api_routing_audits is append-only: a user_access_token row is never deleted to let the downgrade pass, so the
-- downgrade refuses while one exists.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM go_api_routing_audits WHERE credential_class = 'user_access_token') THEN
        RAISE EXCEPTION 'go_api_routing_audits holds user_access_token rows: revision 0164 cannot be reverted';
    END IF;
END
$$;
ALTER TABLE go_api_routing_audits
    DROP CONSTRAINT ck_go_api_routing_audits_credential_class,
    ADD CONSTRAINT ck_go_api_routing_audits_credential_class
        CHECK (credential_class = ANY (ARRAY['effective_principal_envelope'::text, 'operator_direct'::text])),
    DROP CONSTRAINT ck_go_api_routing_audits_principal_pairing,
    ADD CONSTRAINT ck_go_api_routing_audits_principal_pairing
        CHECK ((credential_class = 'effective_principal_envelope'::text) = (principal_id IS NOT NULL));
