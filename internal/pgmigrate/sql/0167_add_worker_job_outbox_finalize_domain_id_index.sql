-- Revision 0167 (down_revision 0166), written by hand.
-- A partial expression index over the metrics.daily_finalize outbox rows, keyed by the run id of the envelope's domain
-- link. The reconciler's stale-finalize survey probes the outbox once per run in its base with exactly this expression
-- and predicate; without the index each probe walks the whole outbox. On a large outbox the build-index hook Job
-- (dho migrate postgres build-index finalize-outbox) builds it CONCURRENTLY before this walk, so this statement finds
-- it and does nothing; on a fresh or small database the walk builds it here.

-- One block, so the walk:
-- * refuses while build-index finalize-outbox runs (its lock, "dho_iz"): an index that build is still making valid reads
--   as invalid here, and dropping it would throw away a build about to succeed;
-- * drops an INVALID index of this name (a failed CONCURRENTLY build): IF NOT EXISTS would skip it and record 0167 over
--   an index the planner cannot use;
-- * creates the index only when none exists, so over a valid prebuilt index it takes no lock on worker_job_outbox.
DO $$
BEGIN
    IF NOT pg_catalog.pg_try_advisory_xact_lock(110399707900282) THEN
        RAISE EXCEPTION 'build-index finalize-outbox is running (its build lock is held); upgrade again after it ends';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_index AS i
        WHERE i.indexrelid = to_regclass('public.ix_worker_job_outbox_finalize_domain_id') AND NOT i.indisvalid
    ) THEN
        DROP INDEX public.ix_worker_job_outbox_finalize_domain_id;
    END IF;
    IF to_regclass('public.ix_worker_job_outbox_finalize_domain_id') IS NULL THEN
        CREATE INDEX ix_worker_job_outbox_finalize_domain_id ON public.worker_job_outbox ((args #>> '{domain,id}'))
        WHERE job_kind = 'metrics.daily_finalize';
    END IF;
END
$$;
