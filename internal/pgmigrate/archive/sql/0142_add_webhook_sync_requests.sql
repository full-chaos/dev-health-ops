-- Alembic revision 0142 (down_revision 0141), rendered with `alembic upgrade 0141:0142 --sql`.
-- The migrator runs this file and the alembic_version update in one transaction.

CREATE TABLE webhook_sync_requests (
    delivery_id UUID NOT NULL, 
    org_id TEXT NOT NULL, 
    sync_config_id UUID NOT NULL, 
    mode TEXT NOT NULL, 
    source_ids TEXT[], 
    scheduled_for TIMESTAMP WITH TIME ZONE NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL, 
    attempts INTEGER DEFAULT 0 NOT NULL, 
    next_attempt_at TIMESTAMP WITH TIME ZONE, 
    last_error TEXT, 
    refused_at TIMESTAMP WITH TIME ZONE, 
    refused_reason TEXT, 
    minted_at TIMESTAMP WITH TIME ZONE, 
    occurrence_id TEXT, 
    CONSTRAINT pk_webhook_sync_requests PRIMARY KEY (delivery_id)
);

CREATE INDEX ix_webhook_sync_requests_pending ON webhook_sync_requests (created_at) WHERE refused_at IS NULL AND minted_at IS NULL;

CREATE INDEX ix_webhook_sync_requests_minted ON webhook_sync_requests (minted_at) WHERE minted_at IS NOT NULL;
