-- Alembic revision 0142 downgrade (0142 -> 0141), the reverse of ../0142_add_webhook_sync_requests.sql.
-- The migrator runs this file and the alembic_version update in one transaction.

DROP INDEX ix_webhook_sync_requests_minted;

DROP INDEX ix_webhook_sync_requests_pending;

DROP TABLE webhook_sync_requests;
