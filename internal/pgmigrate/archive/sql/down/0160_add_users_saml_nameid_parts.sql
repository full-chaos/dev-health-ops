-- Revision 0160 downgrade (0160 -> 0159), the reverse of ../0160_add_users_saml_nameid_parts.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

ALTER TABLE users DROP COLUMN saml_sp_name_qualifier;
ALTER TABLE users DROP COLUMN saml_nameid_format;
