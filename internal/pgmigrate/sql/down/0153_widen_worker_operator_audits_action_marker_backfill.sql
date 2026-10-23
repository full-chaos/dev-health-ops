-- Revision 0153 downgrade (0153 -> 0152), the reverse of ../0153_widen_worker_operator_audits_action_marker_backfill.sql.
-- The migrator runs this file and the alembic_version update in one transaction.
-- Audit rows are never deleted: when a row carries the action this revision added, the downgrade stops with the
-- count and leaves the schema as it was.

-- The table is locked first, so no audit row can commit between the count and the swap of the check.
LOCK TABLE worker_operator_audits IN ACCESS EXCLUSIVE MODE;

DO $$
DECLARE
    pending bigint;
BEGIN
    SELECT count(*) INTO pending FROM worker_operator_audits WHERE action = 'metrics.daily_marker_backfill';
    IF pending > 0 THEN
        RAISE EXCEPTION 'cannot downgrade 0153: % worker_operator_audits rows hold action metrics.daily_marker_backfill; audit rows are never deleted', pending
            USING ERRCODE = 'check_violation';
    END IF;
END
$$;

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
    'external_recompute.replay'
));
