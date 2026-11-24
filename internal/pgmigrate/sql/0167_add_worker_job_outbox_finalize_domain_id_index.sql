-- Revision 0167 (down_revision 0166), written by hand.
-- A partial expression index over the metrics.daily_finalize outbox rows, keyed by the run id of the envelope's domain
-- link. The reconciler's stale-finalize survey probes the outbox once per run in its base with exactly this expression
-- and predicate; without the index each probe walks the whole outbox. On a large outbox the build-index hook Job
-- (dho migrate postgres build-index finalize-outbox) builds it CONCURRENTLY before this walk, so this statement finds
-- it and does nothing; on a fresh or small database the walk builds it here.

-- A failed CONCURRENTLY build can leave an INVALID index of this name. IF NOT EXISTS would skip it and the walk would
-- record 0167 over an index the planner cannot use, so an invalid one is dropped first. A valid one is kept.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_index AS i
        WHERE i.indexrelid = to_regclass('public.ix_worker_job_outbox_finalize_domain_id') AND NOT i.indisvalid
    ) THEN
        DROP INDEX public.ix_worker_job_outbox_finalize_domain_id;
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS ix_worker_job_outbox_finalize_domain_id ON public.worker_job_outbox ((args #>> '{domain,id}'))
WHERE job_kind = 'metrics.daily_finalize';
