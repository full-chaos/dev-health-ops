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

// DDL mirrors alembic 0114 (the three go_api_* GraphQL tables), 0127 (the
// review_evidence/recorded_by provenance columns), 0128 (the
// measurement-route / baseline-defect provenance columns), 0129
// (build_binding), 0134 (go_api_rest_proof_run, the REST sibling
// ledger go-api-rest-prove writes), and 0135 (go_api_rest_proof_run's
// baseline_defect_declared column: which tickets a request DECLARED
// that run, alongside baseline_defect's existing matched-only record).
//
// The constraints are not decoration and are NOT trimmed to "what the
// test needs": the 4-column composite FK from go_api_proof_run to
// go_api_candidate_build, and the stage/terminal_state CHECKs, are the
// database-level reason a receipt cannot be reattributed to a build that
// never registered or carry a verdict outside the signed vocabulary. A
// test against a relaxed schema would pass while the real one rejected
// every write.
//
// go_api_rest_proof_run's stage/terminal_state/measurement_route/
// build_binding CHECKs are byte-identical in vocabulary to go_api_proof_run's
// own -- deliberately, so internal/goapiproof.EnablementProofClause (one
// predicate, parameterised only by a SQL alias) judges a row from EITHER
// table without a second copy of the admission rule. It carries no FK to
// go_api_candidate_build: that registry exists for go_api_routing_state to
// reference an immutable build by, and nothing on the REST side plays that
// role -- see alembic 0134's own module doc comment.
//
// The drift check for the four GraphQL-table migrations (0114/0127/0128/
// 0129) lives in PYTHON rather than here, deliberately: reading the
// alembic files FROM a Go test made them inputs to the Go workflow, and
// go.yml's path filters do not cover src/dev_health_ops/alembic/versions
// -- so a PR changing only a migration would have satisfied go-quality
// vacuously (caught by tests/tooling/test_go_workflow_path_filters.py).
// Enforcing it from the Python side keeps the guard and costs no
// cross-language trigger, because Python's own workflow already runs on
// those files. That check's own column-pattern scan names its four source
// migrations explicitly (it does not enumerate the versions directory), so
// it does not -- and is not meant to -- cover 0134; this table's own Go
// integration test (internal/goapiproof's restreceipt_integration_test.go)
// is what proves this mirror against the real migrated schema instead.
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
		CHECK (stage IN ('dual_run', 'deployed_executed', 'shadow', 'canary', 'write_executed')),
	CONSTRAINT ck_go_api_proof_run_terminal_state
		CHECK (terminal_state IN ('match', 'mismatch', 'auth_rejected', 'validation_rejected',
			'dependency_failed', 'timeout', 'cancelled', 'resource_exhausted',
			'fallback', 'unsupported', 'proof_failed')),
	CONSTRAINT ck_go_api_proof_run_build_binding
		CHECK (build_binding IS NULL OR build_binding IN ('per_request', 'absent')),
	CONSTRAINT ck_go_api_proof_run_shadow_requires_watermark
		CHECK (stage <> 'shadow' OR data_watermark IS NOT NULL),
	CONSTRAINT ck_go_api_proof_run_measurement_route
		CHECK (measurement_route IS NULL OR measurement_route IN ('edge', 'proof')),
	CONSTRAINT ck_go_api_proof_run_write_executed_shape
		CHECK (stage <> 'write_executed' OR
			(side_effect_digest IS NOT NULL AND measurement_route IS NOT NULL))
);

CREATE TABLE go_api_rest_proof_run (
	id UUID NOT NULL PRIMARY KEY,
	method TEXT NOT NULL,
	path TEXT NOT NULL,
	candidate_build TEXT NOT NULL,
	request_identity TEXT NOT NULL,
	stage TEXT NOT NULL,
	terminal_state TEXT NOT NULL,
	baseline_response_ref TEXT,
	candidate_response_ref TEXT,
	org_id TEXT,
	review_evidence TEXT,
	recorded_by TEXT,
	measurement_route TEXT,
	baseline_defect TEXT[],
	baseline_defect_declared TEXT[],
	differences_outside_baseline_defect INTEGER NOT NULL DEFAULT 0,
	build_binding TEXT,
	observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	CONSTRAINT ck_go_api_rest_proof_run_stage
		CHECK (stage IN ('dual_run', 'deployed_executed', 'shadow', 'canary')),
	CONSTRAINT ck_go_api_rest_proof_run_terminal_state
		CHECK (terminal_state IN ('match', 'mismatch', 'auth_rejected', 'validation_rejected',
			'dependency_failed', 'timeout', 'cancelled', 'resource_exhausted',
			'fallback', 'unsupported', 'proof_failed')),
	CONSTRAINT ck_go_api_rest_proof_run_measurement_route
		CHECK (measurement_route IS NULL OR measurement_route IN ('edge', 'proof')),
	CONSTRAINT ck_go_api_rest_proof_run_build_binding
		CHECK (build_binding IS NULL OR build_binding IN ('per_request', 'absent'))
);
`

// Create builds the four registry tables (three GraphQL, one REST) in an
// empty database.
func Create(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, DDL); err != nil {
		return fmt.Errorf("registryschema: create registry schema: %w", err)
	}
	return nil
}
