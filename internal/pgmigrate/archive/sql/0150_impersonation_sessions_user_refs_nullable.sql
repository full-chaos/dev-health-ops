-- Alembic revision 0150 (down_revision 0149), rendered with `alembic upgrade 0149:0150 --sql`.
-- The migrator runs this file and the alembic_version update in one transaction.

ALTER TABLE impersonation_sessions ALTER COLUMN admin_user_id DROP NOT NULL;

ALTER TABLE impersonation_sessions ALTER COLUMN target_user_id DROP NOT NULL;
