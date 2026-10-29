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
    'external_recompute.replay'::character varying
])::text[]));
