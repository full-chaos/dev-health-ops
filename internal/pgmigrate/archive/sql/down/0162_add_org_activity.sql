-- Revision 0162 downgrade (0162 -> 0161), the reverse of ../0162_add_org_activity.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

DROP TABLE org_activity;
