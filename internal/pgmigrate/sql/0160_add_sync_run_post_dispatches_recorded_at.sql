-- Revision 0160 (down_revision 0159), written by hand.
-- recorded_at is the database clock at the moment a post_sync ledger row is inserted. The materializer rebuilds a
-- missing post_sync outbox row only for ledger rows recorded inside its window (internal/syncreconciler, postSyncRebuildWindow),
-- so its candidate scan is bounded by recent runs, not by all history. dispatched_at cannot serve: the finalizer writes
-- the run's earlier completed_at there when one is already set.
-- Every row written before this revision reads -infinity, which is outside every window, so it is never rebuilt. Both
-- ALTERs change catalog metadata only (a constant default, then a volatile default that applies to later inserts): no
-- table rewrite. The migrator runs this file in one transaction, so the ledger index below is a plain build (a SHARE lock on
-- that one small table: inserts wait until it commits). No earlier revision sets lock_timeout or statement_timeout.
-- ix_sync_run_units_open_run (open units only) serves the materializer's dispatch and finalize candidate reads. On a large
-- sync_run_units a plain build here would hold a SHARE lock that stops unit writes, so `dho migrate postgres build-index
-- open-units` (the chart's migrations.hook.openUnitsIndex Job) builds it CONCURRENTLY first and the statement below then
-- finds it and does nothing; on a small or fresh table it builds here at once. Its definition is OpenUnitsIndexBody
-- (internal/pgmigrate/indexbuild.go); TestOpenUnitsIndexDefinitionMatchesRevision0160 keeps the two equal.

ALTER TABLE public.sync_run_post_dispatches ADD COLUMN recorded_at timestamp with time zone NOT NULL DEFAULT '-infinity';
ALTER TABLE public.sync_run_post_dispatches ALTER COLUMN recorded_at SET DEFAULT now();

CREATE INDEX IF NOT EXISTS ix_sync_run_post_dispatches_recorded ON public.sync_run_post_dispatches (recorded_at, sync_run_id) WHERE kind = 'post_sync';

CREATE INDEX IF NOT EXISTS ix_sync_run_units_open_run ON public.sync_run_units (sync_run_id)
WHERE status NOT IN ('success', 'failed');
