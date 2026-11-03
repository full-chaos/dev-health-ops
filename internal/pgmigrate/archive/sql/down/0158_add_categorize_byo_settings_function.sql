-- Revision 0158 downgrade (0158 -> 0157), the reverse of ../0158_add_categorize_byo_settings_function.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

DROP FUNCTION public.categorize_byo_settings(text);
