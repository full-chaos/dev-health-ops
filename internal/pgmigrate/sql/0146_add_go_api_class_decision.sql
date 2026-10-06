-- Alembic revision 0146 (down_revision 0145). The CREATE TABLE is rendered with `alembic upgrade 0145:0146 --sql`; the backfill reads data and is written by hand.
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

-- Backfill (CHAOS-8735, D4819): each class operation gets exactly the row the RUNNING (old) image serves: the row at
-- that image's schema digest under the class document digest. That is the primary key of go_api_routing_state, so
-- there is at most one row per operation, and no timestamp or tie-break takes part. The old image's digest is passed
-- in as the setting dho.class_decision_live_schema_digest (the migrator sets it from the env
-- DHO_CLASS_DECISION_LIVE_SCHEMA_DIGEST, read by the roll from the old image's GET /registry). An operation with no
-- row at that digest gets no decision: dark, as the old image answers. With class rows present, a missing or
-- malformed setting, or a digest that holds no class row, fails the walk; it rolls back whole and nothing changes.
DO $$
DECLARE
    live_digest text := coalesce(current_setting('dho.class_decision_live_schema_digest', true), '');
    class_rows bigint;
    live_rows bigint;
BEGIN
    SELECT count(*) INTO class_rows FROM go_api_routing_state WHERE left(selected_operation, 4) = 'mcp:';
    IF class_rows = 0 THEN
        RETURN;
    END IF;
    IF live_digest !~ '^sha256:[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'go_api_class_decision backfill: % MCP class rows exist and dho.class_decision_live_schema_digest is empty or not sha256:<64 hex>; set DHO_CLASS_DECISION_LIVE_SCHEMA_DIGEST to the schema_digest of the running query-api (GET /registry)', class_rows;
    END IF;
    SELECT count(*) INTO live_rows FROM go_api_routing_state
     WHERE left(selected_operation, 4) = 'mcp:'
       AND schema_digest = live_digest
       AND document_digest = '9c509c3594856bed7f4896345d687c0fca3a1e298b65b440ae519938bac7ed92';
    IF live_rows = 0 THEN
        RAISE EXCEPTION 'go_api_class_decision backfill: % MCP class rows exist and none is at schema digest % under the class document digest', class_rows, live_digest;
    END IF;
END
$$;

INSERT INTO go_api_class_decision
            (operation, mode, current_candidate_build, schema_digest, review_evidence, recorded_by, decided_at)
        SELECT selected_operation, mode, current_candidate_build, schema_digest,
               review_evidence, recorded_by, updated_at
          FROM go_api_routing_state
         WHERE left(selected_operation, 4) = 'mcp:'
           AND schema_digest = current_setting('dho.class_decision_live_schema_digest', true)
           AND document_digest = '9c509c3594856bed7f4896345d687c0fca3a1e298b65b440ae519938bac7ed92';
