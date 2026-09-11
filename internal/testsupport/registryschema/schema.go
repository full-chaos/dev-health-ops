// Package registryschema creates the Go-API registry Postgres fixture
// schema that internal/goapiproof's integration tests and
// cmd/go-api-routing's end-to-end verb tests share.
//
// ONE copy, for the reason providersyncschema exists: the alternative was
// a second hand-kept DDL in the cmd package, drifting from this one with
// nothing to notice. The DDL below is pinned to the alembic migrations by
// tests/test_0128_go_api_proof_run_measurement_provenance_migration.py::
// test_registry_ddl_mirror_covers_every_migrated_column.
package registryschema

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DDL mirrors alembic 0114 (the three tables), 0127 (the
// review_evidence/recorded_by provenance columns), 0128 (the
// measurement-route / baseline-defect provenance columns) and 0129
// (build_binding).
//
// The constraints are not decoration and are NOT trimmed to "what the
// test needs": the 4-column composite FK from go_api_proof_run to
// go_api_candidate_build, and the stage/terminal_state CHECKs, are the
// database-level reason a receipt cannot be reattributed to a build that
// never registered or carry a verdict outside the signed vocabulary. A
// test against a relaxed schema would pass while the real one rejected
// every write.
//
// The drift check lives in PYTHON rather than here, deliberately: reading
// the alembic files FROM a Go test made them inputs to the Go workflow,
// and go.yml's path filters do not cover src/dev_health_ops/alembic/
// versions -- so a PR changing only a migration would have satisfied
// go-quality vacuously (caught by tests/tooling/test_go_workflow_path_
// filters.py). Enforcing it from the Python side keeps the guard and
// costs no cross-language trigger, because Python's own workflow already
// runs on those files.
const DDL = `
CREATE TABLE go_api_candidate_build (
	schema_digest TEXT NOT NULL,
	document_digest TEXT NOT NULL,
	selected_operation TEXT NOT NULL,
	candidate_build TEXT NOT NULL,
	registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	CONSTRAINT pk_go_api_candidate_build
		PRIMARY KEY (schema_digest, document_digest, selected_operation, candidate_build)
);

CREATE TABLE go_api_routing_state (
	schema_digest TEXT NOT NULL,
	document_digest TEXT NOT NULL,
	selected_operation TEXT NOT NULL,
	current_candidate_build TEXT NOT NULL,
	owner TEXT NOT NULL,
	mode TEXT NOT NULL DEFAULT 'python',
	eligible_orgs JSON,
	rollout_percentage INTEGER NOT NULL DEFAULT 0,
	review_evidence TEXT,
	recorded_by TEXT,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	CONSTRAINT pk_go_api_routing_state
		PRIMARY KEY (schema_digest, document_digest, selected_operation),
	CONSTRAINT fk_go_api_routing_state_candidate_build
		FOREIGN KEY (schema_digest, document_digest, selected_operation, current_candidate_build)
		REFERENCES go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build),
	CONSTRAINT ck_go_api_routing_state_owner CHECK (owner IN ('python', 'go')),
	CONSTRAINT ck_go_api_routing_state_mode
		CHECK (mode IN ('python', 'shadow', 'canary', 'primary', 'disabled')),
	CONSTRAINT ck_go_api_routing_state_rollout_percentage
		CHECK (rollout_percentage >= 0 AND rollout_percentage <= 100)
);

CREATE TABLE go_api_proof_run (
	id UUID NOT NULL PRIMARY KEY,
	schema_digest TEXT NOT NULL,
	document_digest TEXT NOT NULL,
	selected_operation TEXT NOT NULL,
	candidate_build TEXT NOT NULL,
	request_identity TEXT NOT NULL,
	stage TEXT NOT NULL,
	terminal_state TEXT NOT NULL,
	baseline_response_ref TEXT,
	candidate_response_ref TEXT,
	side_effect_digest TEXT,
	data_watermark TEXT,
	org_id TEXT,
	review_evidence TEXT,
	recorded_by TEXT,
	measurement_route TEXT,
	baseline_defect TEXT[],
	differences_outside_baseline_defect INTEGER NOT NULL DEFAULT 0,
	build_binding TEXT,
	observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	CONSTRAINT fk_go_api_proof_run_candidate_build
		FOREIGN KEY (schema_digest, document_digest, selected_operation, candidate_build)
		REFERENCES go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build),
	CONSTRAINT ck_go_api_proof_run_stage
		CHECK (stage IN ('dual_run', 'deployed_executed', 'shadow', 'canary')),
	CONSTRAINT ck_go_api_proof_run_terminal_state
		CHECK (terminal_state IN ('match', 'mismatch', 'auth_rejected', 'validation_rejected',
			'dependency_failed', 'timeout', 'cancelled', 'resource_exhausted',
			'fallback', 'unsupported', 'proof_failed')),
	CONSTRAINT ck_go_api_proof_run_build_binding
		CHECK (build_binding IS NULL OR build_binding IN ('per_request', 'absent')),
	CONSTRAINT ck_go_api_proof_run_shadow_requires_watermark
		CHECK (stage <> 'shadow' OR data_watermark IS NOT NULL),
	CONSTRAINT ck_go_api_proof_run_measurement_route
		CHECK (measurement_route IS NULL OR measurement_route IN ('edge', 'proof'))
);
`

// Create builds the three registry tables in an empty database.
func Create(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, DDL); err != nil {
		return fmt.Errorf("registryschema: create registry schema: %w", err)
	}
	return nil
}
