-- Planted test-only revision 0154: never part of the real sql dir.
CREATE INDEX ix_planted_0154 ON worker_job_outbox (job_kind, id);
