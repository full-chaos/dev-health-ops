-- Alembic revision 0146 downgrade (0146 -> 0145), the reverse of ../0146_add_go_api_class_decision.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

DROP TABLE go_api_class_decision;
