-- Planted test-only revision 0153: never part of the real sql dir.
CREATE INDEX ix_planted_0153 ON worker_job_outbox (job_kind, id);
