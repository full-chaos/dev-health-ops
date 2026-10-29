-- Revision 0154 (down_revision 0153), written by hand.
-- The migrator runs this file and the alembic_version update in one transaction, so the index cannot be built
-- CONCURRENTLY: the build holds a SHARE lock on worker_job_outbox (writes wait) until the run commits.
-- The index is partial on the retired workgraph kinds (retiredWorkgraphKinds in internal/joboutbox/strand_repair.go):
-- only the retired-kind survey may use it. A full job_kind index lets the planner enter the outbox by kind for the
-- strand-repair shape queries, which read every delivered row of that kind and grow without bound.

CREATE INDEX ix_worker_job_outbox_job_kind ON worker_job_outbox (job_kind, id)
WHERE job_kind IN ('investment.dispatch', 'investment.chunk', 'investment.finalize');
