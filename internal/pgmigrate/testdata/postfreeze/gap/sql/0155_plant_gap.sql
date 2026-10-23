-- Planted test-only revision 0155: never part of the real sql dir.
CREATE INDEX ix_planted_0155 ON worker_job_outbox (job_kind, id);
