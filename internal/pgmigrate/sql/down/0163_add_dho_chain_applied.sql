-- Revision 0163 downgrade (0163 -> 0162), the reverse of ../0163_add_dho_chain_applied.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

DROP TABLE dho_chain_applied;
