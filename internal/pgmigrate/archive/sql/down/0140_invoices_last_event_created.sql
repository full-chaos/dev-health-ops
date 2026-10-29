-- Alembic revision 0140 downgrade (0140 -> 0139), the reverse of ../0140_invoices_last_event_created.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

ALTER TABLE invoices DROP COLUMN last_event_created;
