//go:build integration

package goapiproof

import (
	"context"
	"encoding/json"
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
// this DDL falls behind the migrations it mirrors. That drift check lives
// in Python (tests/test_0128_...::test_registry_ddl_mirror_covers_every_
// migrated_column) rather than here, deliberately: reading the alembic
// files FROM this test made them inputs to the Go workflow, and go.yml's
// path filters do not cover src/dev_health_ops/alembic/versions -- so a PR
// changing only a migration would have satisfied go-quality vacuously
// (caught by tests/tooling/test_go_workflow_path_filters.py). Enforcing it
// from the Python side keeps the guard and costs no cross-language
// trigger, because Python's own workflow already runs on those files.
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

// Both statements must land in ONE transaction. Rolling back the caller's
// transaction must leave NEITHER the receipt nor the candidate-build row --
// if the build insert autocommitted separately, a failed receipt would
// leave an orphan behind.
func TestWriteParticipatesInTheCallersTransaction(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := Write(ctx, tx, Receipt{
		SchemaDigest:      testSchemaDigest,
		DocumentDigest:    testDocumentDigest,
		SelectedOperation: "featureFlags",
		CandidateBuild:    testCandidateBuild,
		RequestIdentity:   "identity-1",
		Stage:             EnablementProofStage,
		TerminalState:     EnablementProofTerminalState,
		MeasurementRoute:  RouteEdge,
		ObservedAt:        time.Now().UTC(),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	for _, table := range []string{"go_api_proof_run", "go_api_candidate_build"} {
		var count int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("%s kept %d row(s) after a rollback -- the two statements are not atomic", table, count)
		}
	}
}

// WriteAtomic on a pool commits both rows.
func TestWriteAtomicCommitsBothRows(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	if _, err := WriteAtomic(ctx, pool, Receipt{
		SchemaDigest:      testSchemaDigest,
		DocumentDigest:    testDocumentDigest,
		SelectedOperation: "featureFlags",
		CandidateBuild:    testCandidateBuild,
		RequestIdentity:   "identity-1",
		Stage:             EnablementProofStage,
		TerminalState:     EnablementProofTerminalState,
		MeasurementRoute:  RouteEdge,
		ObservedAt:        time.Now().UTC(),
	}); err != nil {
		t.Fatalf("WriteAtomic: %v", err)
	}

	for _, table := range []string{"go_api_proof_run", "go_api_candidate_build"} {
		var count int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 1 {
			t.Fatalf("%s has %d row(s), want 1", table, count)
		}
	}
}

// Round 2's F3, both directions, against a real database.
//
// Receipts used to commit as each operation finished, before the run-level
// build-stability check ran -- so a build that moved mid-run left
// already-committed `match` receipts behind, eligible for enablement,
// describing a build that was not serving for all of the run. Nothing rolled
// them back.
//
// The run is now buffered and nothing is written until stability holds. When
// it does not: ZERO match receipts, and one `proof_failed` row per measured
// operation so the run is visible rather than silently empty.
func TestNoMatchReceiptSurvivesABuildThatMovedMidRun(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	// One operation was measured and matched before the build moved.
	outcomes := []Outcome{{
		Operation:      "featureFlags",
		DocumentDigest: testDocumentDigest,
		Route:          RouteEdge,
		Executed:       true,
		Admitted:       true,
		TerminalState:  TerminalStateMatch,
	}}
	runner := &Runner{
		Registry: RegistryView{SchemaDigest: testSchemaDigest, BuildIdentity: testCandidateBuild},
		Config:   Config{OrgID: "70d529e0", Window: DefaultWindow(), RecordedBy: "test", ReviewEvidence: "why"},
	}

	receipts, err := runner.RefusalReceipts(outcomes, time.Now().UTC(),
		"the serving build moved DURING the run (build-before -> build-after)")
	if err != nil {
		t.Fatalf("RefusalReceipts: %v", err)
	}
	written, err := WriteReceipts(ctx, pool, receipts)
	if err != nil {
		t.Fatalf("WriteReceipts: %v", err)
	}
	if len(written) != 1 || !written["featureFlags"] {
		t.Fatalf("the run must stay visible: wrote %v, want featureFlags", written)
	}

	// ZERO rows may satisfy the enablement predicate.
	found, err := OperationsWithEnablementProof(ctx, pool, testSchemaDigest, testCandidateBuild,
		map[string]string{"featureFlags": testDocumentDigest})
	if err != nil {
		t.Fatalf("OperationsWithEnablementProof: %v", err)
	}
	if len(found) != 0 {
		t.Fatalf("a run whose build moved must authorize nothing, got %v", found)
	}

	var state, evidence string
	if err := pool.QueryRow(ctx,
		`SELECT terminal_state, review_evidence FROM go_api_proof_run WHERE selected_operation = 'featureFlags'`,
	).Scan(&state, &evidence); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if state != "proof_failed" {
		t.Fatalf("the recorded state must say the PROOF failed, got %q", state)
	}
	// review_evidence is a JSON provenance object, not prose (r1 P2), so
	// this reads the fields rather than grepping a sentence -- which is
	// the whole point of the change: a machine-written fact should be
	// readable by a machine.
	var provenance ReceiptProvenance
	if err := json.Unmarshal([]byte(evidence), &provenance); err != nil {
		t.Fatalf("review_evidence is not a JSON object: %q (%v)", evidence, err)
	}
	if !strings.Contains(provenance.Refusal, "moved DURING the run") {
		t.Fatalf("the row must name the cause, got %q", provenance.Refusal)
	}
	if provenance.Measured != 1 || provenance.Attempted != 1 {
		t.Fatalf("the row must carry the counts, got measured=%d attempted=%d", provenance.Measured, provenance.Attempted)
	}
}

// The control: with a stable build the same outcome DOES produce a match
// receipt that satisfies the predicate. Without this, the test above would
// pass on an instrument that never writes anything at all.
func TestAStableBuildStillProducesAnEnablingReceipt(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	outcomes := []Outcome{{
		Operation:      "featureFlags",
		DocumentDigest: testDocumentDigest,
		Route:          RouteEdge,
		Executed:       true,
		Admitted:       true,
		TerminalState:  TerminalStateMatch,
	}}
	runner := &Runner{
		Registry: RegistryView{SchemaDigest: testSchemaDigest, BuildIdentity: testCandidateBuild},
		Config:   Config{OrgID: "70d529e0", Window: DefaultWindow(), RecordedBy: "test", ReviewEvidence: "why"},
	}

	receipts, err := runner.ReceiptsFor(outcomes, time.Now().UTC())
	if err != nil {
		t.Fatalf("ReceiptsFor: %v", err)
	}
	if _, err := WriteReceipts(ctx, pool, receipts); err != nil {
		t.Fatalf("WriteReceipts: %v", err)
	}

	found, err := OperationsWithEnablementProof(ctx, pool, testSchemaDigest, testCandidateBuild,
		map[string]string{"featureFlags": testDocumentDigest})
	if err != nil {
		t.Fatalf("OperationsWithEnablementProof: %v", err)
	}
	if !found["featureFlags"] {
		t.Fatal("a stable run must still be able to authorize an enablement")
	}
}

// A refusal receipt is written only for operations that were MEASURED. A
// run that refused everything writes nothing, because there is no
// measurement whose result is being withheld.
func TestRefusalReceiptsCoverOnlyMeasuredOperations(t *testing.T) {
	runner := &Runner{
		Registry: RegistryView{SchemaDigest: testSchemaDigest, BuildIdentity: testCandidateBuild},
		Config:   Config{OrgID: "70d529e0", Window: DefaultWindow()},
	}
	receipts, err := runner.RefusalReceipts([]Outcome{
		{Operation: "featureFlags", DocumentDigest: testDocumentDigest, Route: RouteEdge, Executed: false, RefusalReason: RefusalPlaneUnidentified},
	}, time.Now().UTC(), "build moved")
	if err != nil {
		t.Fatalf("RefusalReceipts: %v", err)
	}
	if len(receipts) != 0 {
		t.Fatalf("an unmeasured operation has no result to withhold, got %d receipt(s)", len(receipts))
	}
}
