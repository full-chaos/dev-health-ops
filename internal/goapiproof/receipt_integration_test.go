//go:build integration

package goapiproof

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// registryDDL mirrors alembic 0114 (the three tables), 0127 (the
// review_evidence/recorded_by provenance columns) and 0128 (the
// measurement-route / baseline-defect provenance columns).
//
// The constraints are not decoration and are NOT trimmed to "what the
// test needs": the 4-column composite FK from go_api_proof_run to
// go_api_candidate_build, and the stage/terminal_state CHECKs, are the
// database-level reason a receipt cannot be reattributed to a build that
// never registered or carry a verdict outside the signed vocabulary. A
// test against a relaxed schema would pass while the real one rejected
// every write. TestRegistryDDLCoversEveryMigratedColumn below fails if
// this DDL falls behind the migrations it mirrors.
const registryDDL = `
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
	CONSTRAINT ck_go_api_proof_run_shadow_requires_watermark
		CHECK (stage <> 'shadow' OR data_watermark IS NOT NULL),
	CONSTRAINT ck_go_api_proof_run_measurement_route
		CHECK (measurement_route IS NULL OR measurement_route IN ('edge', 'proof'))
);
`

const (
	testSchemaDigest   = "sha256:29d509cd414cd957a7bcd73a1c0e78a07f17dd8a8794893233954aaa87241b88"
	testDocumentDigest = "06ca28a0517a34c0f5a6cc25b193da7b5682bea5192ae93e5a79edc7e7742208"
	testCandidateBuild = "b18e56fa79cfe20ce0f75df148144b832d92be36"
)

func startRegistryPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate Postgres: %v", err)
		}
	})

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx, registryDDL); err != nil {
		t.Fatalf("create registry schema: %v", err)
	}
	return pool
}

// The whole point of the receipt: what prove WRITES must be what
// `enable`'s preflight READS. Both halves are exercised here so a
// regression in either fails, rather than each half agreeing with itself.
func TestWrittenReceiptSatisfiesTheEnablementPredicate(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	receipt := Receipt{
		SchemaDigest:      testSchemaDigest,
		DocumentDigest:    testDocumentDigest,
		SelectedOperation: "featureFlags",
		CandidateBuild:    testCandidateBuild,
		RequestIdentity:   "identity-1",
		Stage:             EnablementProofStage,
		TerminalState:     EnablementProofTerminalState,
		MeasurementRoute:  RouteEdge,
		OrgID:             "70d529e0",
		RecordedBy:        "lane-5425-prove",
		ReviewEvidence:    "CHAOS-5425 integration test",
		ObservedAt:        time.Now().UTC(),
	}
	if _, err := Write(ctx, pool, receipt); err != nil {
		t.Fatalf("Write: %v", err)
	}

	found, err := OperationsWithEnablementProof(ctx, pool, testSchemaDigest, testCandidateBuild,
		map[string]string{"featureFlags": testDocumentDigest})
	if err != nil {
		t.Fatalf("OperationsWithEnablementProof: %v", err)
	}
	if !found["featureFlags"] {
		t.Fatal("a deployed_executed/match receipt must satisfy the enablement predicate")
	}
}

// Every one of the four key columns must be load-bearing. A proof
// recorded against a different build, document, schema or operation must
// NOT authorize this one -- plan §8.3: "never carried forward across any
// of the four changing".
func TestEnablementPredicateRejectsEveryWrongKeyColumn(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	base := Receipt{
		SchemaDigest:      testSchemaDigest,
		DocumentDigest:    testDocumentDigest,
		SelectedOperation: "featureFlags",
		CandidateBuild:    testCandidateBuild,
		RequestIdentity:   "identity-1",
		Stage:             EnablementProofStage,
		TerminalState:     EnablementProofTerminalState,
		MeasurementRoute:  RouteEdge,
		ObservedAt:        time.Now().UTC(),
	}

	for name, mutate := range map[string]func(Receipt) Receipt{
		"different candidate build": func(r Receipt) Receipt { r.CandidateBuild = "0000000000000000000000000000000000000000"; return r },
		"different schema digest":   func(r Receipt) Receipt { r.SchemaDigest = "sha256:67b87d38e4"; return r },
		"different document digest": func(r Receipt) Receipt { r.DocumentDigest = "deadbeef"; return r },
		"different operation":       func(r Receipt) Receipt { r.SelectedOperation = "hotspots"; return r },
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Write(ctx, pool, mutate(base)); err != nil {
				t.Fatalf("Write: %v", err)
			}
			found, err := OperationsWithEnablementProof(ctx, pool, testSchemaDigest, testCandidateBuild,
				map[string]string{"featureFlags": testDocumentDigest})
			if err != nil {
				t.Fatalf("OperationsWithEnablementProof: %v", err)
			}
			if found["featureFlags"] {
				t.Fatalf("a receipt with a %s must not authorize this operation", name)
			}
		})
	}
}

// A mismatch receipt is real, recorded evidence -- and it authorizes
// NOTHING. This is exactly the shadow operations' shape: served by Go,
// divergence recorded, not promoted.
func TestMismatchReceiptIsRecordedButAuthorizesNothing(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	receipt := Receipt{
		SchemaDigest:      testSchemaDigest,
		DocumentDigest:    testDocumentDigest,
		SelectedOperation: "featureFlags",
		CandidateBuild:    testCandidateBuild,
		RequestIdentity:   "identity-1",
		Stage:             EnablementProofStage,
		TerminalState:     TerminalStateMismatch,
		MeasurementRoute:  RouteProof,
		ObservedAt:        time.Now().UTC(),
	}
	if _, err := Write(ctx, pool, receipt); err != nil {
		t.Fatalf("Write: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM go_api_proof_run WHERE terminal_state = 'mismatch'`,
	).Scan(&count); err != nil {
		t.Fatalf("count receipts: %v", err)
	}
	if count != 1 {
		t.Fatalf("the mismatch must be RECORDED, got %d rows", count)
	}

	found, err := OperationsWithEnablementProof(ctx, pool, testSchemaDigest, testCandidateBuild,
		map[string]string{"featureFlags": testDocumentDigest})
	if err != nil {
		t.Fatalf("OperationsWithEnablementProof: %v", err)
	}
	if found["featureFlags"] {
		t.Fatal("a mismatch must never authorize an enablement")
	}
}

// Receipts are append-only: a second run adds a row, never overwrites
// one. "It matched on Monday and mismatched on Tuesday" must stay
// readable.
func TestReceiptsAreAppendOnly(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	base := Receipt{
		SchemaDigest:      testSchemaDigest,
		DocumentDigest:    testDocumentDigest,
		SelectedOperation: "featureFlags",
		CandidateBuild:    testCandidateBuild,
		RequestIdentity:   "identity-1",
		Stage:             EnablementProofStage,
		TerminalState:     EnablementProofTerminalState,
		MeasurementRoute:  RouteEdge,
		ObservedAt:        time.Now().UTC(),
	}
	if _, err := Write(ctx, pool, base); err != nil {
		t.Fatalf("Write: %v", err)
	}
	second := base
	second.TerminalState = TerminalStateMismatch
	if _, err := Write(ctx, pool, second); err != nil {
		t.Fatalf("Write: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM go_api_proof_run`).Scan(&count); err != nil {
		t.Fatalf("count receipts: %v", err)
	}
	if count != 2 {
		t.Fatalf("both verdicts must survive, got %d rows", count)
	}
}

// The candidate-build registration is idempotent -- registering the same
// build twice is a no-op, never an error, and never an UPDATE.
func TestCandidateBuildRegistrationIsIdempotent(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	receipt := Receipt{
		SchemaDigest:      testSchemaDigest,
		DocumentDigest:    testDocumentDigest,
		SelectedOperation: "featureFlags",
		CandidateBuild:    testCandidateBuild,
		RequestIdentity:   "identity-1",
		Stage:             EnablementProofStage,
		TerminalState:     EnablementProofTerminalState,
		MeasurementRoute:  RouteEdge,
		ObservedAt:        time.Now().UTC(),
	}
	for i := 0; i < 3; i++ {
		if _, err := Write(ctx, pool, receipt); err != nil {
			t.Fatalf("Write #%d: %v", i, err)
		}
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM go_api_candidate_build`).Scan(&count); err != nil {
		t.Fatalf("count builds: %v", err)
	}
	if count != 1 {
		t.Fatalf("the candidate build must be registered exactly once, got %d rows", count)
	}
}

// The DB is the backstop, and it must actually bite: a stage outside the
// signed vocabulary is rejected by the CHECK even if the Go-side
// validation were removed.
func TestDatabaseRejectsAnOutOfVocabularyStage(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	if _, err := pool.Exec(ctx,
		`INSERT INTO go_api_candidate_build
		   (schema_digest, document_digest, selected_operation, candidate_build)
		 VALUES ($1,$2,$3,$4)`,
		testSchemaDigest, testDocumentDigest, "featureFlags", testCandidateBuild,
	); err != nil {
		t.Fatalf("register build: %v", err)
	}

	_, err := pool.Exec(ctx,
		`INSERT INTO go_api_proof_run
		   (id, schema_digest, document_digest, selected_operation, candidate_build,
		    request_identity, stage, terminal_state)
		 VALUES (gen_random_uuid(),$1,$2,$3,$4,'id','not_a_stage','match')`,
		testSchemaDigest, testDocumentDigest, "featureFlags", testCandidateBuild)
	if err == nil {
		t.Fatal("the stage CHECK constraint must reject an unknown stage")
	}
	if !strings.Contains(err.Error(), "ck_go_api_proof_run_stage") {
		t.Fatalf("expected the stage CHECK to fire, got %v", err)
	}
}

// A receipt naming a build that was never registered must be rejected by
// the composite FK, not silently accepted.
func TestDatabaseRejectsAReceiptForAnUnregisteredBuild(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	_, err := pool.Exec(ctx,
		`INSERT INTO go_api_proof_run
		   (id, schema_digest, document_digest, selected_operation, candidate_build,
		    request_identity, stage, terminal_state)
		 VALUES (gen_random_uuid(),$1,$2,$3,$4,'id','deployed_executed','match')`,
		testSchemaDigest, testDocumentDigest, "featureFlags", "never-registered")
	if err == nil {
		t.Fatal("the composite FK must reject a receipt for an unregistered build")
	}
	if !strings.Contains(err.Error(), "fk_go_api_proof_run_candidate_build") {
		t.Fatalf("expected the composite FK to fire, got %v", err)
	}
}

// registryDDL is a hand-kept mirror of two alembic migrations, so it can
// fall behind them. This reads the migrations and fails if any column
// they add is missing here -- the same "an exclusion must actually match
// something" discipline applied to a schema mirror.
func TestRegistryDDLCoversEveryMigratedColumn(t *testing.T) {
	root := repoRoot(t)
	columnPattern := regexp.MustCompile(`sa\.Column\(\s*"([a-z_]+)"`)
	// Non-vacuity guard: if the pattern stops matching (a migration is
	// renamed, or alembic's spelling changes), every assertion below
	// silently passes over an empty match set and this test becomes a
	// green no-op -- the exact vacuous-pass class that let a loader
	// regression test short-circuit before it reached its call site.
	checked := 0

	for _, migration := range []string{
		"0114_add_go_api_operation_registry.py",
		"0127_add_go_api_routing_provenance.py",
		"0128_add_go_api_proof_run_measurement_provenance.py",
	} {
		path := filepath.Join(root, "src", "dev_health_ops", "alembic", "versions", migration)
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", migration, err)
		}
		for _, match := range columnPattern.FindAllStringSubmatch(string(source), -1) {
			column := match[1]
			checked++
			if !strings.Contains(registryDDL, "\t"+column+" ") {
				t.Errorf("%s adds column %q, which registryDDL does not declare -- update the mirror", migration, column)
			}
		}
	}

	// 28 columns match across the two migrations today. The floor is set
	// well below that so an ordinary schema edit does not fail here,
	// while a collapse to zero -- the pattern silently stopping matching,
	// which would make every assertion above a green no-op -- still does.
	if checked < 20 {
		t.Fatalf("only %d migrated columns were checked -- the column pattern has stopped matching and this test is now vacuous", checked)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find the repository root from the test working directory")
		}
		dir = parent
	}
}

// The measurement route must round-trip: a proof-route receipt has to be
// distinguishable from a served-traffic one by reading the row, not by
// remembering how the run was invoked.
func TestMeasurementRouteRoundTrips(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	receipt := Receipt{
		SchemaDigest:      testSchemaDigest,
		DocumentDigest:    testDocumentDigest,
		SelectedOperation: "flowMatrix",
		CandidateBuild:    testCandidateBuild,
		RequestIdentity:   "identity-1",
		Stage:             EnablementProofStage,
		TerminalState:     TerminalStateMismatch,
		MeasurementRoute:  RouteProof,
		BaselineDefects:   []string{"CHAOS-5448", "CHAOS-5450"},
		ObservedAt:        time.Now().UTC(),
	}
	if _, err := Write(ctx, pool, receipt); err != nil {
		t.Fatalf("Write: %v", err)
	}

	var route string
	var defects []string
	var outside int
	if err := pool.QueryRow(ctx,
		`SELECT measurement_route, baseline_defect, differences_outside_baseline_defect
		   FROM go_api_proof_run WHERE selected_operation = 'flowMatrix'`,
	).Scan(&route, &defects, &outside); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if route != RouteProof {
		t.Fatalf("route must round-trip, got %q", route)
	}
	if len(defects) != 2 {
		t.Fatalf("the covering tickets must round-trip, got %v", defects)
	}
	// The explicit zero IS the claim: every difference was a known Python
	// defect. A NULL here would leave a reader guessing.
	if outside != 0 {
		t.Fatalf("differences_outside_baseline_defect must be an explicit 0, got %d", outside)
	}
}

// The database is the backstop for the route vocabulary too, exactly as
// it is for stage and terminal_state.
func TestDatabaseRejectsAnUnknownMeasurementRoute(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	if _, err := pool.Exec(ctx,
		`INSERT INTO go_api_candidate_build
		   (schema_digest, document_digest, selected_operation, candidate_build)
		 VALUES ($1,$2,$3,$4)`,
		testSchemaDigest, testDocumentDigest, "featureFlags", testCandidateBuild,
	); err != nil {
		t.Fatalf("register build: %v", err)
	}

	_, err := pool.Exec(ctx,
		`INSERT INTO go_api_proof_run
		   (id, schema_digest, document_digest, selected_operation, candidate_build,
		    request_identity, stage, terminal_state, measurement_route)
		 VALUES (gen_random_uuid(),$1,$2,$3,$4,'id','deployed_executed','match','sideways')`,
		testSchemaDigest, testDocumentDigest, "featureFlags", testCandidateBuild)
	if err == nil {
		t.Fatal("the measurement-route CHECK must reject an unknown route")
	}
	if !strings.Contains(err.Error(), "ck_go_api_proof_run_measurement_route") {
		t.Fatalf("expected the route CHECK to fire, got %v", err)
	}
}

// A baseline-defect annotation must never make a mismatch enablable --
// the whole separation this column exists to preserve.
func TestBaselineDefectAnnotationStillAuthorizesNothing(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	if _, err := Write(ctx, pool, Receipt{
		SchemaDigest:      testSchemaDigest,
		DocumentDigest:    testDocumentDigest,
		SelectedOperation: "featureFlags",
		CandidateBuild:    testCandidateBuild,
		RequestIdentity:   "identity-1",
		Stage:             EnablementProofStage,
		TerminalState:     TerminalStateMismatch,
		MeasurementRoute:  RouteEdge,
		BaselineDefects:   []string{"CHAOS-5448"},
		ObservedAt:        time.Now().UTC(),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	found, err := OperationsWithEnablementProof(ctx, pool, testSchemaDigest, testCandidateBuild,
		map[string]string{"featureFlags": testDocumentDigest})
	if err != nil {
		t.Fatalf("OperationsWithEnablementProof: %v", err)
	}
	if found["featureFlags"] {
		t.Fatal("a mismatch annotated with a known Python defect must still authorize nothing")
	}
}
