-- Alembic revision 0144 (down_revision 0143), rendered with `alembic upgrade 0143:0144 --sql`.
-- The migrator runs this file and the alembic_version update in one transaction.

CREATE TABLE saml_assertion_replays (
    provider_id UUID NOT NULL,
    assertion_id TEXT NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_saml_assertion_replays PRIMARY KEY (provider_id, assertion_id)
);

CREATE INDEX ix_saml_assertion_replays_expires_at ON saml_assertion_replays (expires_at);
