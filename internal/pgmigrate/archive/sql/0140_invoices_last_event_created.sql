-- Alembic revision 0140 (down_revision 0139), rendered with `alembic upgrade 0139:0140 --sql`.
-- The migrator runs this file and the alembic_version update in one transaction.

ALTER TABLE invoices ADD COLUMN last_event_created BIGINT;
