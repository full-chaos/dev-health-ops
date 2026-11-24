-- Revision 0167 downgrade (0167 -> 0166), the reverse of ../0167_add_worker_job_outbox_finalize_domain_id_index.sql.

DROP INDEX IF EXISTS public.ix_worker_job_outbox_finalize_domain_id;
