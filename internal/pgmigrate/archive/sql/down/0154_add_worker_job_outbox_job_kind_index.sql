-- Revision 0154 downgrade (0154 -> 0153), the reverse of ../0154_add_worker_job_outbox_job_kind_index.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

DROP INDEX ix_worker_job_outbox_job_kind;
