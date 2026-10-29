-- Revision 0159 downgrade (0159 -> 0158), the reverse of ../0159_add_sync_run_units_failure_streak_index.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

DROP INDEX IF EXISTS public.ix_sync_run_units_failure_streak;
