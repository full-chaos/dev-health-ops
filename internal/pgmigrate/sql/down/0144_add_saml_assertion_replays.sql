-- Alembic revision 0144 downgrade (0144 -> 0143), the reverse of ../0144_add_saml_assertion_replays.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

DROP INDEX ix_saml_assertion_replays_expires_at;

DROP TABLE saml_assertion_replays;
