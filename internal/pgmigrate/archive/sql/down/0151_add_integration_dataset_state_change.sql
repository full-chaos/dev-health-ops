-- Alembic revision 0151 downgrade (0151 -> 0150), the reverse of ../0151_add_integration_dataset_state_change.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

ALTER TABLE integration_datasets DROP COLUMN state_changed_by;

ALTER TABLE integration_datasets DROP COLUMN state_changed_at;
