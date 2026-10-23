-- Revision 0153 (down_revision 0152), written by hand.
-- The migrator runs this file and the alembic_version update in one transaction, so the index cannot be built
-- CONCURRENTLY: the build holds a SHARE lock on worker_job_outbox (writes wait) until the run commits.

CREATE INDEX ix_worker_job_outbox_job_kind ON worker_job_outbox (job_kind, id);
