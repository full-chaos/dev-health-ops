-- Revision 0155 downgrade (0155 -> 0154), the reverse of ../0155_add_users_sso_provider_scope.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

DROP INDEX ix_users_sso_identity;
ALTER TABLE users DROP COLUMN sso_provider_id;
