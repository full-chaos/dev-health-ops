-- Alembic revision 0146 (down_revision 0145), rendered with `alembic upgrade 0145:0146 --sql`.
-- The migrator runs this file and the alembic_version update in one transaction.

CREATE TABLE go_api_class_decision (
    operation TEXT NOT NULL,
    mode TEXT NOT NULL,
    current_candidate_build TEXT NOT NULL,
    schema_digest TEXT NOT NULL,
    review_evidence TEXT,
    recorded_by TEXT,
    decided_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (operation),
    CONSTRAINT ck_go_api_class_decision_mode CHECK (mode IN ('python', 'shadow', 'canary', 'primary', 'disabled')),
    CONSTRAINT ck_go_api_class_decision_operation CHECK (left(operation, 4) = 'mcp:')
);

INSERT INTO go_api_class_decision
            (operation, mode, current_candidate_build, schema_digest, review_evidence, recorded_by, decided_at)
        SELECT DISTINCT ON (selected_operation)
               selected_operation, mode, current_candidate_build, schema_digest,
               review_evidence, recorded_by, updated_at
          FROM go_api_routing_state
         WHERE left(selected_operation, 4) = 'mcp:'
         ORDER BY selected_operation,
                  updated_at DESC,
                  (mode IN ('canary', 'primary')) ASC,
                  schema_digest DESC;
