-- Revision 0165 downgrade (0165 -> 0164), the reverse of ../0165_null_safe_outbox_dispatched_check.sql.
-- The migrator runs this file and the alembic_version update in one transaction. The 0164 CHECK admits every row the
-- 0165 CHECK admits, so the downgrade never refuses and changes no row.
ALTER TABLE sync_dispatch_outbox
    DROP CONSTRAINT ck_sync_dispatch_outbox_dispatched_route_coherence,
    ADD CONSTRAINT ck_sync_dispatch_outbox_dispatched_route_coherence CHECK (((((status)::text = 'dispatched'::text) AND (((last_error = 'feature_disabled'::text) AND (dispatched_transport IS NULL) AND (dispatched_route_generation IS NULL) AND (transport_job_id IS NULL)) OR (((last_error IS NULL) OR (last_error <> 'feature_disabled'::text)) AND (dispatched_transport IS NOT NULL) AND (dispatched_route_generation IS NOT NULL)))) OR (((status)::text <> 'dispatched'::text) AND (dispatched_transport IS NULL) AND (dispatched_route_generation IS NULL) AND (transport_job_id IS NULL))));
