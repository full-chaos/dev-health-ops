-- Alembic revision 0141 (down_revision 0140), rendered with `alembic upgrade 0140:0141 --sql`.
-- The migrator runs this file and the alembic_version update in one transaction.

ALTER TABLE provider_oauth_revocations DROP CONSTRAINT ck_provider_oauth_revocations_purpose;

ALTER TABLE provider_oauth_revocations ADD CONSTRAINT ck_provider_oauth_revocations_purpose CHECK (purpose IN ('replacement', 'disconnect', 'setup'));
