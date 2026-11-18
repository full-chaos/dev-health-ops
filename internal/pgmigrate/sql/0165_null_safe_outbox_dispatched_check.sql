-- Revision 0165 (down_revision 0164), written by hand.
-- ck_sync_dispatch_outbox_dispatched_route_coherence compared last_error with = and <>, so a dispatched row with
-- last_error NULL and no transport made the CHECK NULL, and a CHECK passes on NULL. The CHECK is redefined with
-- IS [NOT] DISTINCT FROM. The new CHECK refuses only rows the old one admitted through that NULL; while one exists the
-- revision refuses, naming the count. No row is changed.
DO $$
DECLARE
    held bigint;
BEGIN
    SELECT count(*) INTO held FROM sync_dispatch_outbox
    WHERE status = 'dispatched' AND last_error IS NULL
      AND (dispatched_transport IS NULL OR dispatched_route_generation IS NULL);
    IF held > 0 THEN
        RAISE EXCEPTION 'sync_dispatch_outbox holds % dispatched row(s) with no last_error and no transport: revision 0165 cannot be applied', held;
    END IF;
END
$$;
ALTER TABLE sync_dispatch_outbox
    DROP CONSTRAINT ck_sync_dispatch_outbox_dispatched_route_coherence,
    ADD CONSTRAINT ck_sync_dispatch_outbox_dispatched_route_coherence CHECK (
        (status = 'dispatched' AND (
            (last_error IS NOT DISTINCT FROM 'feature_disabled'
                AND dispatched_transport IS NULL AND dispatched_route_generation IS NULL AND transport_job_id IS NULL)
            OR (last_error IS DISTINCT FROM 'feature_disabled'
                AND dispatched_transport IS NOT NULL AND dispatched_route_generation IS NOT NULL)))
        OR (status <> 'dispatched'
            AND dispatched_transport IS NULL AND dispatched_route_generation IS NULL AND transport_job_id IS NULL)
    );
