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
	"github.com/full-chaos/dev-health-ops/internal/testsupport/registryschema"
)

// registryDDL now lives in internal/testsupport/registryschema, shared
// with cmd/go-api-routing's end-to-end verb tests (those tests
// have to drive the REAL verbs, and a second hand-kept copy of this DDL
// would drift from this one with nothing to notice). The alias keeps this
// file's existing references and its one name for the thing.
const registryDDL = registryschema.DDL

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
		// per_request, not absent. The rule now requires
		// a BOUND measurement on both arms, so a receipt written with
		// `absent` is refused for its BINDING -- which would make every
		// assertion below hold for a reason that has nothing to do with
		// what the assertion says. The unbound case is the second half
		// of this test, asserted as a refusal rather than smuggled in
		// as the base.
		BuildBinding:   EdgeBuildPresent,
		OrgID:          "70d529e0",
		RecordedBy:     "lane-5425-prove",
		ReviewEvidence: "CHAOS-5425 integration test",
		ObservedAt:     time.Now().UTC(),
	}
	if _, err := Write(ctx, pool, receipt); err != nil {
		t.Fatalf("Write: %v", err)
	}

	found, err := OperationsWithEnablementProof(ctx, pool, testSchemaDigest, testCandidateBuild,
		TargetModeCanary, map[string]string{"featureFlags": testDocumentDigest})
	if err != nil {
		t.Fatalf("OperationsWithEnablementProof: %v", err)
	}
	if !found["featureFlags"] {
		t.Fatal("a deployed_executed/match receipt must satisfy the enablement predicate")
	}

	// This test proved the receipt was READABLE by the predicate
	// and never that what landed in the row was what Write was handed.
	// Mutating Write's parameter to nullIfEmpty("") persisted NULL and
	// this test still passed, because the predicate does not look at
	// build_binding. Nothing else would have noticed either: no
	// enablement reader consumes the column yet, so a regression writing
	// the wrong binding stays invisible while enablement goes on reading
	// green -- which is precisely the "audit data quietly wrong" case the
	// column exists to prevent.
	var storedBinding *string
	if err := pool.QueryRow(ctx,
		`SELECT build_binding FROM go_api_proof_run
		  WHERE schema_digest = $1 AND candidate_build = $2 AND selected_operation = $3`,
		testSchemaDigest, testCandidateBuild, "featureFlags",
	).Scan(&storedBinding); err != nil {
		t.Fatalf("read back build_binding: %v", err)
	}
	if storedBinding == nil {
		t.Fatal("build_binding was written as NULL: the run KNEW the binding and the row does not say so, and no reader will ever flag it")
	}
	if *storedBinding != EdgeBuildPresent {
		t.Fatalf("build_binding read back as %q, want %q -- the row must carry the binding the run established, not another one", *storedBinding, EdgeBuildPresent)
	}

	// The other half of the same seam. An UNBOUND
	// measurement is a real, recorded observation of a response nobody
	// can attribute to a replica -- so it is written, and it authorizes
	// nothing. Asserting only the admitting direction is how the hole
	// survived: `primary enable` returned rc=0 on exactly this row.
	unbound := receipt
	unbound.SelectedOperation = "hotspots"
	unbound.BuildBinding = EdgeBuildAbsent
	if _, err := Write(ctx, pool, unbound); err != nil {
		t.Fatalf("Write the unbound receipt: %v", err)
	}
	for _, mode := range []string{TargetModeCanary, TargetModePrimary} {
		admitted, err := OperationsWithEnablementProof(ctx, pool, testSchemaDigest, testCandidateBuild,
			mode, map[string]string{"hotspots": testDocumentDigest})
		if err != nil {
			t.Fatalf("OperationsWithEnablementProof(%s): %v", mode, err)
		}
		if admitted["hotspots"] {
			t.Fatalf("an UNBOUND deployed_executed/match receipt authorized %s: the response was never attributed to a serving build, so nothing here says WHICH replica was measured", mode)
		}
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
		// Admissible in every respect EXCEPT the column each subtest
		// changes. With `absent` here (as this base once had) every
		// subtest would pass on the BINDING, and the
		// key columns it claims to pin would be doing no work at all.
		BuildBinding: EdgeBuildPresent,
		ObservedAt:   time.Now().UTC(),
	}

	// The control: the UNMUTATED base IS admitted. Without it, "the base
	// is admissible" is an assumption, and this whole table can go
	// vacuous again the next time the rule gains a requirement -- which
	// is exactly what happened when the binding requirement was added
	// to a base written with `absent`.
	//
	// It is written under its OWN operation, and asked about under that
	// operation. Writing the base at `featureFlags` would leave an
	// admitting row in this shared pool, and every subtest below asks
	// about `featureFlags` -- the control would then make all four of
	// them fail for its reason instead of passing for theirs. (Measured:
	// it did.) The table is append-only by design, so there is nothing
	// to undo afterwards; the fix is to not collide in the first place.
	t.Run("the unmutated base is admitted", func(t *testing.T) {
		control := base
		control.SelectedOperation = "controlOperation"
		if _, err := Write(ctx, pool, control); err != nil {
			t.Fatalf("Write: %v", err)
		}
		found, err := OperationsWithEnablementProof(ctx, pool, testSchemaDigest, testCandidateBuild,
			TargetModeCanary, map[string]string{"controlOperation": testDocumentDigest})
		if err != nil {
			t.Fatalf("OperationsWithEnablementProof: %v", err)
		}
		if !found["controlOperation"] {
			t.Fatal("the base receipt is not admitted, so every refusal below proves nothing about the key column it names")
		}
	})

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
				TargetModeCanary, map[string]string{"featureFlags": testDocumentDigest})
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
		// Bound, so the refusal below is about the UNCITED MISMATCH this
		// test is named for and not about the binding.
		BuildBinding: EdgeBuildPresent,
		ObservedAt:   time.Now().UTC(),
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
		TargetModeCanary, map[string]string{"featureFlags": testDocumentDigest})
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
		BuildBinding:      EdgeBuildAbsent,
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
		BuildBinding:      EdgeBuildAbsent,
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
	// Rollback is deferred as well as asserted below, because a t.Fatalf
	// between here and the explicit Rollback calls runtime.Goexit and
	// skips it -- and then t.Cleanup's pool.Close blocks forever waiting
	// for the connection this transaction still holds. That turned a
	// two-second Write failure into a 25-minute test-binary timeout whose
	// panic named this test and not the three that had actually failed.
	// The second Rollback is a no-op on an already-finished transaction.
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := Write(ctx, tx, Receipt{
		SchemaDigest:      testSchemaDigest,
		DocumentDigest:    testDocumentDigest,
		SelectedOperation: "featureFlags",
		CandidateBuild:    testCandidateBuild,
		RequestIdentity:   "identity-1",
		Stage:             EnablementProofStage,
		TerminalState:     EnablementProofTerminalState,
		MeasurementRoute:  RouteEdge,
		BuildBinding:      EdgeBuildAbsent,
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
		BuildBinding:      EdgeBuildAbsent,
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

// Exercises both directions, against a real database.
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

	// A REAL measurement, driven through Run against a fake edge. Testing
	// found the previous version of this fixture hand-building an Outcome
	// and setting the unexported admitted bit directly -- which the seal
	// then made impossible to complete correctly, because it also had to
	// know about the sealed verdict, and it did not. A fixture that has to
	// be kept in step with an invariant is a fixture that will fall out of
	// step with it.
	runner := measuredRunner(t)

	receipts, err := runner.RefusalReceipts(time.Now().UTC(),
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
		TargetModeCanary, map[string]string{"featureFlags": testDocumentDigest})
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
	// review_evidence is a JSON provenance object, not prose, so
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

	runner := measuredRunner(t)

	receipts, err := runner.ReceiptsFor(time.Now().UTC())
	if err != nil {
		t.Fatalf("ReceiptsFor: %v", err)
	}
	if _, err := WriteReceipts(ctx, pool, receipts); err != nil {
		t.Fatalf("WriteReceipts: %v", err)
	}

	found, err := OperationsWithEnablementProof(ctx, pool, testSchemaDigest, testCandidateBuild,
		TargetModeCanary, map[string]string{"featureFlags": testDocumentDigest})
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
	// A run whose only operation was REFUSED: the fake edge answers with
	// no plane header, so admission refuses and nothing is measured.
	edge := &fakeEdge{goBody: `{"data":{"featureFlags":[]}}`, pythonBody: `{"data":{"featureFlags":[]}}`, goPlane: "python"}
	runner := newRunner(t, edge, "canary")
	runner.Registry.SchemaDigest = testSchemaDigest
	runner.Registry.BuildIdentity = testCandidateBuild
	runner.Registry.DocumentDigest = map[string]string{"featureFlags": testDocumentDigest}
	runner.Routing = map[string]RoutingRow{"featureFlags": {Mode: "canary", CandidateBuild: testCandidateBuild}}
	// Run reports that it measured nothing, which is the precondition.
	if _, _, err := runner.Run(context.Background()); err == nil {
		t.Fatal("precondition: this run must measure nothing")
	}

	receipts, err := runner.RefusalReceipts(time.Now().UTC(), "build moved")
	if err != nil {
		t.Fatalf("RefusalReceipts: %v", err)
	}
	if len(receipts) != 0 {
		t.Fatalf("an unmeasured operation has no result to withhold, got %d receipt(s)", len(receipts))
	}
}

// measuredRunner drives a REAL run against a fake edge and returns the
// runner holding its sealed records.
//
// The receipt constructors read only what a run sealed, so a fixture
// cannot hand them an outcome any more -- which is the point. It also
// means these tests exercise the same path production does, rather than a
// hand-assembled approximation of it that has to be re-approximated every
// time the invariant tightens.
func measuredRunner(t *testing.T) *Runner {
	t.Helper()
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	edge := &fakeEdge{goBody: body, pythonBody: body}
	runner := newRunner(t, edge, "canary")
	// The registry identity these tests write against.
	runner.Registry.SchemaDigest = testSchemaDigest
	runner.Registry.BuildIdentity = testCandidateBuild
	runner.Registry.DocumentDigest = map[string]string{"featureFlags": testDocumentDigest}
	runner.Routing = map[string]RoutingRow{"featureFlags": {Mode: "canary", CandidateBuild: testCandidateBuild}}
	runner.Config.RecordedBy = "test"
	runner.Config.ReviewEvidence = "why"
	// A bound measurement, so the run terminates `match` rather than
	// being downgraded for an absent build binding.
	edge.goBuild = testCandidateBuild

	if _, _, err := runner.Run(context.Background()); err != nil {
		t.Fatalf("measuredRunner: Run: %v", err)
	}
	return runner
}
