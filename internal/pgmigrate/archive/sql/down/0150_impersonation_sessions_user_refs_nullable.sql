-- Alembic revision 0150 downgrade (0150 -> 0149), the reverse of ../0150_impersonation_sessions_user_refs_nullable.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

ALTER TABLE impersonation_sessions ALTER COLUMN target_user_id SET NOT NULL;

ALTER TABLE impersonation_sessions ALTER COLUMN admin_user_id SET NOT NULL;
