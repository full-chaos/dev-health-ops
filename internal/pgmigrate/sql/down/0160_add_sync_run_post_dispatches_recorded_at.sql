-- Revision 0160 downgrade (0160 -> 0159), the reverse of ../0160_add_sync_run_post_dispatches_recorded_at.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

DROP INDEX IF EXISTS public.ix_sync_run_units_open_run;
DROP INDEX IF EXISTS public.ix_sync_run_post_dispatches_recorded;
ALTER TABLE public.sync_run_post_dispatches DROP COLUMN IF EXISTS recorded_at;
