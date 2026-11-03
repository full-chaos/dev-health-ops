-- Revision 0156 downgrade (0156 -> 0155), the reverse of ../0156_add_categorize_byo_opted_in_function.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

DROP FUNCTION public.categorize_byo_opted_in(text);
