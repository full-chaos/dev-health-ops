-- Revision 0153 downgrade (0153 -> 0152), the reverse of ../0153_add_worker_job_outbox_job_kind_index.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

DROP INDEX ix_worker_job_outbox_job_kind;
