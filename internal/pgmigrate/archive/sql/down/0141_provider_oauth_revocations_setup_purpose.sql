-- Alembic revision 0141 downgrade (0141 -> 0140), the reverse of ../0141_provider_oauth_revocations_setup_purpose.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

ALTER TABLE provider_oauth_revocations DROP CONSTRAINT ck_provider_oauth_revocations_purpose;

ALTER TABLE provider_oauth_revocations ADD CONSTRAINT ck_provider_oauth_revocations_purpose CHECK (purpose IN ('replacement', 'disconnect'));
