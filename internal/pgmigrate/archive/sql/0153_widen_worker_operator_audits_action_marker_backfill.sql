-- Revision 0153 (down_revision 0152), written by hand. Go-only: no Alembic script exists.
-- The migrator runs this file and the alembic_version update in one transaction.
-- Widens ck_worker_operator_audits_action by one value, metrics.daily_marker_backfill, the Action of the
-- `dho workers metrics daily-marker-backfill` audit row. Every existing row already satisfies the wider check.
-- The table stays ACCESS EXCLUSIVE-locked while the new constraint validates (a scan of a small audit table).

ALTER TABLE worker_operator_audits DROP CONSTRAINT ck_worker_operator_audits_action;
ALTER TABLE worker_operator_audits ADD CONSTRAINT ck_worker_operator_audits_action CHECK ((action)::text = ANY ((ARRAY[
    'jobs.cancel'::character varying,
    'jobs.retry'::character varying,
    'queues.pause'::character varying,
    'queues.resume'::character varying,
    'workers.drain'::character varying,
    'workers.undrain'::character varying,
    'job_routes.apply_checked_in'::character varying,
    'job_routes.rollback'::character varying,
    'routes.pause'::character varying,
    'routes.drain'::character varying,
    'routes.resume'::character varying,
    'providersync.retire_linear_pseudo_projects'::character varying,
    'providersync.retire_stale_linear_project_ownership'::character varying,
    'sync_dispatch_outbox.close_terminal_backlog'::character varying,
    'workgraph.manual_trigger'::character varying,
    'investment.manual_trigger'::character varying,
    'ledger.repair'::character varying,
    'metrics.daily_start'::character varying,
    'metrics.daily_redrive'::character varying,
    'metrics.daily_finalize'::character varying,
    'metrics.finalize_redrive'::character varying,
    'metrics.partition_recompute'::character varying,
    'metrics.remaining_start'::character varying,
    'metrics.remaining_trigger_backstop'::character varying,
    'metrics.remaining_redrive'::character varying,
    'external_recompute.replay'::character varying,
    'metrics.daily_marker_backfill'::character varying
])::text[]));
