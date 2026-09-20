//go:build integration

package goapiproof

import (
	"context"
	"testing"
	"time"
)

// TestWriteRESTThenReadBackThroughTheMigratedSchema drives WriteREST/
// WriteRESTAtomic against a real PostgreSQL built from registryschema.DDL
// -- the same mirror of alembic 0134 internal/migrationmatrix's own
// TestReadRESTProofAppliesEnablementProofClause exercises from the reader
// side. This test is the writer side: what lands in
// go_api_rest_proof_run must be exactly what WriteREST was handed, not
// merely "some row exists" -- the same discipline
// TestWrittenReceiptSatisfiesTheEnablementPredicate applies to the
// GraphQL Write.
func TestWriteRESTThenReadBackThroughTheMigratedSchema(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	observedAt := time.Now().UTC().Truncate(time.Microsecond)
	receipt := RESTReceipt{
		Method:               "GET",
		Path:                 "/api/v1/quadrant",
		CandidateBuild:       testCandidateBuild,
		RequestIdentity:      "rest-identity-1",
		Stage:                EnablementProofStage,
		TerminalState:        EnablementProofTerminalState,
		BaselineResponseRef:  "artifact://baseline/1",
		CandidateResponseRef: "artifact://candidate/1",
		OrgID:                "70d529e0",
		ReviewEvidence:       "goapiproof integration test fixture",
		RecordedBy:           "goapiproof-integration-test",
		ObservedAt:           observedAt,
		MeasurementRoute:     RouteProof,
		BaselineDefects:      nil,
		BuildBinding:         EdgeBuildPresent,
	}

	id, err := WriteRESTAtomic(ctx, pool, receipt)
	if err != nil {
		t.Fatalf("WriteRESTAtomic: %v", err)
	}

	// Read back every column WriteREST is responsible for, not just a
	// sentinel one -- the same "the row must carry what the run
	// established" discipline receipt_integration_test.go's own
	// build_binding assertion states.
	var (
		method, path, candidateBuild, requestIdentity, stage, terminalState string
		measurementRoute, buildBinding                                      *string
		differencesOutside                                                  int
	)
	if err := pool.QueryRow(ctx,
		`SELECT method, path, candidate_build, request_identity, stage, terminal_state,
		        measurement_route, build_binding, differences_outside_baseline_defect
		   FROM go_api_rest_proof_run WHERE id = $1`,
		id,
	).Scan(&method, &path, &candidateBuild, &requestIdentity, &stage, &terminalState,
		&measurementRoute, &buildBinding, &differencesOutside); err != nil {
		t.Fatalf("read back the written row: %v", err)
	}

	if method != receipt.Method || path != receipt.Path {
		t.Fatalf("read back method/path = %q/%q, want %q/%q", method, path, receipt.Method, receipt.Path)
	}
	if candidateBuild != receipt.CandidateBuild {
		t.Fatalf("read back candidate_build = %q, want %q", candidateBuild, receipt.CandidateBuild)
	}
	if requestIdentity != receipt.RequestIdentity || stage != receipt.Stage || terminalState != receipt.TerminalState {
		t.Fatalf("read back request_identity/stage/terminal_state = %q/%q/%q, want %q/%q/%q",
			requestIdentity, stage, terminalState, receipt.RequestIdentity, receipt.Stage, receipt.TerminalState)
	}
	if measurementRoute == nil || *measurementRoute != RouteProof {
		t.Fatalf("read back measurement_route = %v, want %q", measurementRoute, RouteProof)
	}
	if buildBinding == nil || *buildBinding != EdgeBuildPresent {
		t.Fatalf("read back build_binding = %v, want %q -- an unbound-looking row would silently fail every enablement reader that checks this column", buildBinding, EdgeBuildPresent)
	}
	if differencesOutside != 0 {
		t.Fatalf("read back differences_outside_baseline_defect = %d, want 0", differencesOutside)
	}

	// Same predicate ReadRESTProof (internal/migrationmatrix) composes --
	// reused here unchanged, not restated, so this test cannot drift from
	// what the real reader actually runs.
	clause, err := EnablementProofClause("p", TargetModeCanary)
	if err != nil {
		t.Fatalf("EnablementProofClause: %v", err)
	}
	var admitted bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM go_api_rest_proof_run AS p WHERE p.id = $1 AND `+clause+`)`,
		id,
	).Scan(&admitted); err != nil {
		t.Fatalf("judge the written row against EnablementProofClause: %v", err)
	}
	if !admitted {
		t.Fatal("a clean deployed_executed/match REST receipt with a bound build must satisfy EnablementProofClause")
	}
}

// TestWriteREST_RefusesAnUnknownVocabularyAgainstTheRealCheckConstraints
// proves the Go-side vocabulary guard (validateRESTVocabulary) is not the
// ONLY thing standing between a programming error and a written row: the
// database's own CHECK constraints (alembic 0134) refuse it too, even if
// a caller somehow bypassed the Go-side check.
func TestWriteREST_RefusesAnUnknownVocabularyAgainstTheRealCheckConstraints(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	if _, err := pool.Exec(ctx,
		`INSERT INTO go_api_rest_proof_run
		   (id, method, path, candidate_build, request_identity, stage, terminal_state, observed_at)
		 VALUES (gen_random_uuid(), 'GET', '/api/v1/quadrant', $1, 'req', 'deployed_executed', 'not-a-real-terminal-state', now())`,
		testCandidateBuild,
	); err == nil {
		t.Fatal("the database accepted an out-of-vocabulary terminal_state -- ck_go_api_rest_proof_run_terminal_state did not fire")
	}
}

// TestWriteREST_DeclaredDefectsRoundTripsThroughTheMigratedSchema proves
// alembic 0135's baseline_defect_declared column against a real,
// migrated Postgres: nil writes SQL NULL, and a non-nil (even empty)
// slice writes a real array -- read back exactly, not approximated by
// either side collapsing the distinction.
func TestWriteREST_DeclaredDefectsRoundTripsThroughTheMigratedSchema(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	known := validRESTReceipt()
	known.RequestIdentity = "rest-identity-declared-known"
	known.DeclaredDefects = []string{"ABC-123", "DEF-456"}
	knownID, err := WriteRESTAtomic(ctx, pool, known)
	if err != nil {
		t.Fatalf("WriteRESTAtomic (known): %v", err)
	}

	unknown := validRESTReceipt()
	unknown.RequestIdentity = "rest-identity-declared-unknown"
	unknown.DeclaredDefects = nil
	unknownID, err := WriteRESTAtomic(ctx, pool, unknown)
	if err != nil {
		t.Fatalf("WriteRESTAtomic (unknown): %v", err)
	}

	var declared *[]string
	if err := pool.QueryRow(ctx,
		`SELECT baseline_defect_declared FROM go_api_rest_proof_run WHERE id = $1`, knownID,
	).Scan(&declared); err != nil {
		t.Fatalf("read back the known row: %v", err)
	}
	if declared == nil {
		t.Fatal("baseline_defect_declared read back NULL, want the written array")
	}
	if len(*declared) != 2 || (*declared)[0] != "ABC-123" || (*declared)[1] != "DEF-456" {
		t.Fatalf("baseline_defect_declared = %v, want [ABC-123 DEF-456]", *declared)
	}

	declared = nil
	if err := pool.QueryRow(ctx,
		`SELECT baseline_defect_declared FROM go_api_rest_proof_run WHERE id = $1`, unknownID,
	).Scan(&declared); err != nil {
		t.Fatalf("read back the unknown row: %v", err)
	}
	if declared != nil {
		t.Fatalf("baseline_defect_declared = %v, want NULL (nil)", *declared)
	}
}

// TestWriteREST_BaselineTimeoutReceiptSatisfiesTheEnablementPredicate writes the
// receipt shape a request admitted under its BaselineTimeoutDeclared carries --
// a cited mismatch with nothing outside the citation, no baseline body, the
// declaration's ticket as the one baseline_defect entry -- through the real
// writer and judges the stored row with the predicate the enablement readers
// run. The same row with one difference outside the citation is not admitted.
func TestWriteREST_BaselineTimeoutReceiptSatisfiesTheEnablementPredicate(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)
	clause, err := EnablementProofClause("p", TargetModeCanary)
	if err != nil {
		t.Fatalf("EnablementProofClause: %v", err)
	}
	for _, cell := range []struct {
		name    string
		outside int
		want    bool
	}{
		{"nothing outside the citation", 0, true},
		{"one difference outside the citation", 1, false},
	} {
		t.Run(cell.name, func(t *testing.T) {
			id, err := WriteRESTAtomic(ctx, pool, RESTReceipt{
				Method: "POST", Path: "/api/v1/home", CandidateBuild: testCandidateBuild,
				RequestIdentity: "rest-identity-timeout-" + cell.name, Stage: EnablementProofStage,
				TerminalState: EnablementCitedMismatchState, CandidateResponseRef: "artifact://candidate/2",
				OrgID: "70d529e0", ReviewEvidence: `{"baseline_timed_out_after":"180.0s"}`, RecordedBy: "goapiproof-integration-test",
				ObservedAt: time.Now().UTC().Truncate(time.Microsecond), MeasurementRoute: RouteProof,
				BaselineDefects: []string{"ABC-123"}, DeclaredDefects: []string{"ABC-123"},
				DifferencesOutsideBaselineDefect: cell.outside, BuildBinding: EdgeBuildPresent,
			})
			if err != nil {
				t.Fatalf("WriteRESTAtomic: %v", err)
			}
			var admitted bool
			if err := pool.QueryRow(ctx,
				`SELECT EXISTS (SELECT 1 FROM go_api_rest_proof_run AS p WHERE p.id = $1 AND `+clause+`)`, id,
			).Scan(&admitted); err != nil {
				t.Fatalf("judge the row: %v", err)
			}
			if admitted != cell.want {
				t.Fatalf("EnablementProofClause admitted = %v, want %v", admitted, cell.want)
			}
		})
	}
}
