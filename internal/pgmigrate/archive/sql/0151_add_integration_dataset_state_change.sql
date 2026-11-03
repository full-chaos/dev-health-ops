-- Alembic revision 0151 (down_revision 0150), rendered with `alembic upgrade 0150:0151 --sql`.
-- The migrator runs this file and the alembic_version update in one transaction.

ALTER TABLE integration_datasets ADD COLUMN state_changed_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE integration_datasets ADD COLUMN state_changed_by TEXT;
