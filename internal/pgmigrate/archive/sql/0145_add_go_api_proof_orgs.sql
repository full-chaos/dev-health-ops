-- Alembic revision 0145 (down_revision 0144), rendered with `alembic upgrade 0144:0145 --sql`.
-- The migrator runs this file and the alembic_version update in one transaction.

CREATE TABLE go_api_proof_orgs (
    org_id TEXT NOT NULL,
    added_by TEXT NOT NULL,
    reason TEXT NOT NULL,
    added_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (org_id),
    CONSTRAINT ck_go_api_proof_orgs_added_by_bounded CHECK (char_length(added_by) BETWEEN 1 AND 128),
    CONSTRAINT ck_go_api_proof_orgs_reason_bounded CHECK (char_length(reason) BETWEEN 1 AND 2000)
);

CREATE TABLE go_api_proof_org_audits (
    id BIGSERIAL NOT NULL,
    org_id TEXT NOT NULL,
    action TEXT NOT NULL,
    credential_class TEXT NOT NULL,
    recorded_by TEXT NOT NULL,
    reason TEXT NOT NULL,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT ck_go_api_proof_org_audits_action CHECK (action IN ('add', 'remove')),
    CONSTRAINT ck_go_api_proof_org_audits_credential_class CHECK (credential_class IN ('operator_direct')),
    CONSTRAINT ck_go_api_proof_org_audits_recorded_by_bounded CHECK (char_length(recorded_by) BETWEEN 1 AND 128),
    CONSTRAINT ck_go_api_proof_org_audits_reason_bounded CHECK (char_length(reason) BETWEEN 1 AND 2000)
);

CREATE INDEX ix_go_api_proof_org_audits_org_recorded ON go_api_proof_org_audits (org_id, recorded_at DESC);
