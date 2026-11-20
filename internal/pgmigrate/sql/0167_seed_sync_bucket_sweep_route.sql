-- Revision 0167 (down_revision 0166), written by hand.
-- Seeds the worker_job_routes row of the new jobs-registry kind sync.bucket_sweep (one River sweep per sync dispatch
-- bucket, CHAOS-14250). The kind is native from birth: River route, generation 1, not paused. Every registry kind
-- needs its row, because the relay resolves the route of every descriptor on each step (jobroute.Controller) and a
-- kind without one makes that step unavailable. No other row and no schema object is touched.
INSERT INTO public.worker_job_routes (job_kind, transport, paused, generation, updated_at)
VALUES ('sync.bucket_sweep', 'river', false, 1, now())
ON CONFLICT (job_kind) DO NOTHING;
