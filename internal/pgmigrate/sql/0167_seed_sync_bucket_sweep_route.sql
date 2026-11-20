-- Revision 0167 (down_revision 0166), written by hand.
-- Seeds the worker_job_routes row of the new jobs-registry kind sync.bucket_sweep (one River sweep per sync dispatch
-- bucket, CHAOS-14250). The kind is native from birth: River route, generation 1, not paused. Every registry kind
-- needs its row, because the relay resolves the route of every descriptor on each step (jobroute.Controller) and a
-- kind without one makes that step unavailable. It also creates the bucket sweep's two indexes and two tables (below).
INSERT INTO public.worker_job_routes (job_kind, transport, paused, generation, updated_at)
VALUES ('sync.bucket_sweep', 'river', false, 1, now())
ON CONFLICT (job_kind) DO NOTHING;
-- The bucket sweep's state (CHAOS-14250 r1 classes 1-7, acp plan3):
-- Head reads of one class of a bucket walk the org's open runs in age order (no queue is materialized).
CREATE INDEX ix_sync_runs_org_class_active_order ON public.sync_runs USING btree
    (org_id, ((((triggered_by = 'backfill'::text) OR ((mode)::text = 'backfill'::text)))::integer), created_at, id)
    WHERE ((status)::text <> ALL ((ARRAY['success'::character varying, 'partial_failed'::character varying, 'failed'::character varying])::text[]));
-- One probe per run: a planned or due retrying unit in a bucket.
CREATE INDEX ix_sync_run_units_run_bucket_waiting ON public.sync_run_units USING btree
    (sync_run_id, provider, cost_class, status, available_at)
    WHERE ((status)::text = ANY ((ARRAY['planned'::character varying, 'retrying'::character varying])::text[]));
-- The per-bucket generation: every wake source bumps it; the sweep acknowledges the value it started from.
CREATE TABLE public.sync_dispatch_bucket_signals (
    org_id text NOT NULL,
    provider character varying(64) NOT NULL,
    cost_class character varying(32) NOT NULL,
    generation bigint DEFAULT 0 NOT NULL,
    acked_generation bigint DEFAULT 0 NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT sync_dispatch_bucket_signals_pkey PRIMARY KEY (org_id, provider, cost_class)
);
-- The sweep's reservations: written in the sweep's bucket-locked transaction, counted by the dispatch guard per class,
-- removed by the reserved run's own pass or past the liveness bound.
CREATE TABLE public.sync_dispatch_bucket_reservations (
    org_id text NOT NULL,
    provider character varying(64) NOT NULL,
    cost_class character varying(32) NOT NULL,
    sync_run_id uuid NOT NULL,
    backfill boolean NOT NULL,
    reserved_at timestamp with time zone NOT NULL,
    CONSTRAINT sync_dispatch_bucket_reservations_pkey PRIMARY KEY (org_id, provider, cost_class, sync_run_id),
    CONSTRAINT fk_sync_dispatch_bucket_reservations_run FOREIGN KEY (sync_run_id) REFERENCES public.sync_runs(id) ON DELETE CASCADE
);
