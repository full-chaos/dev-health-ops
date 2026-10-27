-- Revision 0161 downgrade (0161 -> 0160), the reverse of ../0161_add_sync_run_post_dispatches_recorded_at.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

DROP INDEX IF EXISTS public.ix_sync_run_units_open_run;
DROP INDEX IF EXISTS public.ix_sync_run_post_dispatches_recorded;
ALTER TABLE public.sync_run_post_dispatches DROP COLUMN IF EXISTS recorded_at;
