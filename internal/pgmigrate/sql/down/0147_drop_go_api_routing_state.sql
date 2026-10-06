-- Alembic revision 0147 downgrade (0147 -> 0146), the reverse of ../0147_drop_go_api_routing_state.sql.
-- The migrator runs this file and the alembic_version update in one transaction.
-- The table comes back EMPTY: the rows are not restored (see the revision docstring).

CREATE TABLE go_api_routing_state (
    schema_digest TEXT NOT NULL,
    document_digest TEXT NOT NULL,
    selected_operation TEXT NOT NULL,
    current_candidate_build TEXT NOT NULL,
    owner TEXT NOT NULL,
    mode TEXT DEFAULT 'python' NOT NULL,
    eligible_orgs JSON,
    rollout_percentage INTEGER DEFAULT '0' NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    review_evidence TEXT,
    recorded_by TEXT,
    CONSTRAINT pk_go_api_routing_state PRIMARY KEY (schema_digest, document_digest, selected_operation),
    CONSTRAINT fk_go_api_routing_state_candidate_build FOREIGN KEY(schema_digest, document_digest, selected_operation, current_candidate_build) REFERENCES go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build),
    CONSTRAINT ck_go_api_routing_state_owner CHECK (owner IN ('python', 'go')),
    CONSTRAINT ck_go_api_routing_state_mode CHECK (mode IN ('python', 'shadow', 'canary', 'primary', 'disabled')),
    CONSTRAINT ck_go_api_routing_state_rollout_percentage CHECK (rollout_percentage >= 0 AND rollout_percentage <= 100)
);
