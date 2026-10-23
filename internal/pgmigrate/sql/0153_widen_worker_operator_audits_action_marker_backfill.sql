-- Revision 0153 (down_revision 0152), written by hand. Go-only: no Alembic script exists.
-- The migrator runs this file and the alembic_version update in one transaction.
-- Widens ck_worker_operator_audits_action by one value, metrics.daily_marker_backfill, the Action of the
-- `dho workers metrics daily-marker-backfill` audit row. Every existing row already satisfies the wider check.
-- The table stays ACCESS EXCLUSIVE-locked while the new constraint validates (a scan of a small audit table).

ALTER TABLE worker_operator_audits DROP CONSTRAINT ck_worker_operator_audits_action;
ALTER TABLE worker_operator_audits ADD CONSTRAINT ck_worker_operator_audits_action CHECK (action IN (
    'jobs.cancel',
    'jobs.retry',
    'queues.pause',
    'queues.resume',
    'workers.drain',
    'workers.undrain',
    'job_routes.apply_checked_in',
    'job_routes.rollback',
    'routes.pause',
    'routes.drain',
    'routes.resume',
    'providersync.retire_linear_pseudo_projects',
    'providersync.retire_stale_linear_project_ownership',
    'sync_dispatch_outbox.close_terminal_backlog',
    'workgraph.manual_trigger',
    'investment.manual_trigger',
    'ledger.repair',
    'metrics.daily_start',
    'metrics.daily_redrive',
    'metrics.daily_finalize',
    'metrics.finalize_redrive',
    'metrics.partition_recompute',
    'metrics.remaining_start',
    'metrics.remaining_trigger_backstop',
    'metrics.remaining_redrive',
    'external_recompute.replay',
    'metrics.daily_marker_backfill'
));
