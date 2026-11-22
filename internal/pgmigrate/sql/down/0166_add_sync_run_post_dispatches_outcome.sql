-- Revision 0166 downgrade (0166 -> 0165), the reverse of ../0166_add_sync_run_post_dispatches_outcome.sql.
-- The migrator runs this file and the alembic_version update in one transaction. Dropping a column drops its CHECKs and
-- its column grants with it.

ALTER TABLE public.sync_run_post_dispatches
    DROP CONSTRAINT IF EXISTS ck_sync_run_post_dispatches_post_sync_outcome_pair,
    DROP CONSTRAINT IF EXISTS ck_sync_run_post_dispatches_post_sync_outcome,
    DROP COLUMN IF EXISTS post_sync_committed_at,
    DROP COLUMN IF EXISTS post_sync_outcome;
