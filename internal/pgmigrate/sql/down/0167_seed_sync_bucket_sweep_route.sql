-- Revision 0167 downgrade (0167 -> 0166), the reverse of ../0167_seed_sync_bucket_sweep_route.sql.
-- A build at 0166 has no sync.bucket_sweep kind, so its route row is removed.
DELETE FROM public.worker_job_routes WHERE job_kind = 'sync.bucket_sweep';
