-- Revision 0167 downgrade (0167 -> 0166), the reverse of ../0167_seed_sync_bucket_sweep_route.sql.
-- A build at 0166 has no sync.bucket_sweep kind and no bucket sweep state.
DROP TABLE public.sync_dispatch_bucket_reservations;
DROP TABLE public.sync_dispatch_bucket_signals;
DROP INDEX public.ix_sync_run_units_run_bucket_waiting;
DROP INDEX public.ix_sync_runs_org_class_active_order;
DELETE FROM public.worker_job_routes WHERE job_kind = 'sync.bucket_sweep';
