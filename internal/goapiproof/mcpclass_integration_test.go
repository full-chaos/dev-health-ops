//go:build integration

package goapiproof

// CHAOS-7214: the class receipt and the document receipt never admit each
// other, in BOTH directions, against a real Postgres. Each case writes a real
// receipt through Write and reads it back through the enablement reader.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
)

// admittedAs reports whether a receipt written for (operation, documentDigest, route)
// is admitted when the reader is asked about that operation as the given kind.
func admittedAs(t *testing.T, operation, documentDigest, route, askedDigest, kind string) bool {
	t.Helper()
	ctx := context.Background()
	pool := startRegistryPostgres(t)
	if _, err := Write(ctx, pool, Receipt{
		SchemaDigest: testSchemaDigest, DocumentDigest: documentDigest, SelectedOperation: operation,
		CandidateBuild: testCandidateBuild, RequestIdentity: "identity-1", Stage: EnablementProofStage,
		TerminalState: EnablementProofTerminalState, MeasurementRoute: route, BuildBinding: EdgeBuildPresent,
		OrgID: "70d529e0", RecordedBy: "test", ReviewEvidence: "class receipt directions", ObservedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	found, err := OperationsWithEnablementProofByKind(ctx, pool, testSchemaDigest, testCandidateBuild, TargetModeCanary,
		map[string]string{operation: askedDigest}, map[string]string{operation: kind})
	if err != nil {
		t.Fatalf("OperationsWithEnablementProofByKind: %v", err)
	}
	return found[operation]
}

// A class-kind operation must itself have the class shape: the class digest and a
// proof-route receipt do not make a document operation a class operation.
func TestClassKindIsAdmittedOnlyForAClassShapedOperation(t *testing.T) {
	if admittedAs(t, "hotspots", mcpclass.DocumentDigest(), RouteProof, mcpclass.DocumentDigest(), OperationKindMCPClass) {
		t.Fatal("an operation without the class prefix was admitted under the class kind")
	}
}

// A write receipt is no more admissible for a class-shaped operation than a query receipt.
func TestWriteReceiptForAClassShapedOperationAdmitsNothing(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)
	op, digest := mcpclass.Operation("hotspots"), mcpclass.DocumentDigest()
	if _, err := Write(ctx, pool, Receipt{
		SchemaDigest: testSchemaDigest, DocumentDigest: digest, SelectedOperation: op, CandidateBuild: testCandidateBuild,
		RequestIdentity: "identity-w", Stage: EnablementWriteProofStage, TerminalState: EnablementProofTerminalState,
		MeasurementRoute: RouteProof, BuildBinding: EdgeBuildPresent, SideEffectDigest: "sha256:" + strings.Repeat("e", 64),
		OrgID: "70d529e0", RecordedBy: "test", ReviewEvidence: "write receipt", ObservedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	for _, kind := range []string{OperationKindMutation, OperationKindMCPClass, OperationKindQuery} {
		found, err := OperationsWithEnablementProofByKind(ctx, pool, testSchemaDigest, testCandidateBuild, TargetModeCanary,
			map[string]string{op: digest}, map[string]string{op: kind})
		if err != nil {
			t.Fatal(err)
		}
		if found[op] {
			t.Fatalf("a write_executed receipt for a class-shaped operation was admitted as kind %q", kind)
		}
	}
}

func TestClassReceiptIsAdmittedAsAClassReceipt(t *testing.T) {
	op, digest := mcpclass.Operation("hotspots"), mcpclass.DocumentDigest()
	if !admittedAs(t, op, digest, RouteProof, digest, OperationKindMCPClass) {
		t.Fatal("a proof-route receipt for the class digest was not admitted for its class operation")
	}
}

// Direction 1: a class operation admits only a proof-route receipt for the class digest.
func TestClassOperationRefusesAReceiptThatIsNotAProofRouteClassReceipt(t *testing.T) {
	op, digest := mcpclass.Operation("hotspots"), mcpclass.DocumentDigest()
	if admittedAs(t, op, digest, RouteEdge, digest, OperationKindMCPClass) {
		t.Fatal("a class operation was admitted by an edge-route receipt")
	}
	if admittedAs(t, op, testDocumentDigest, RouteProof, testDocumentDigest, OperationKindMCPClass) {
		t.Fatal("a class operation was admitted by a receipt for a document digest that is not the class digest")
	}
}

// Direction 2: a document operation never admits a class-shaped receipt.
func TestDocumentOperationNeverAdmitsAClassShapedReceipt(t *testing.T) {
	op, digest := mcpclass.Operation("hotspots"), mcpclass.DocumentDigest()
	for _, kind := range []string{OperationKindQuery, OperationKindMutation} {
		if admittedAs(t, op, digest, RouteProof, digest, kind) {
			t.Fatalf("a %s-kind operation was admitted by a class receipt", kind)
		}
	}
}

// A class operation with no kind at all (or the wrong kind for its shape) is admitted by nothing.
func TestClassOperationWithoutTheClassKindIsAdmittedByNothing(t *testing.T) {
	op, digest := mcpclass.Operation("hotspots"), mcpclass.DocumentDigest()
	if admittedAs(t, op, digest, RouteProof, digest, "") {
		t.Fatal("a class operation with an unknown kind was admitted")
	}
}

// DocumentOperationsReceiptBacked is `enable`'s reader for a query operation: an
// operation holding an admissible receipt for exactly this build is backed; one with
// no receipt, a receipt for another build, or an inadmissible one (unbound) is not.
func TestDocumentOperationsReceiptBackedReadsTheEnablementRule(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)
	write := func(op, digest, build, binding string) {
		t.Helper()
		if _, err := Write(ctx, pool, Receipt{
			SchemaDigest: testSchemaDigest, DocumentDigest: digest, SelectedOperation: op, CandidateBuild: build,
			RequestIdentity: "identity-" + op, Stage: EnablementProofStage, TerminalState: EnablementProofTerminalState,
			MeasurementRoute: RouteEdge, BuildBinding: binding, OrgID: "70d529e0", RecordedBy: "test",
			ReviewEvidence: "doc receipt", ObservedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("Write %s: %v", op, err)
		}
	}
	d := func(c string) string { return strings.Repeat(c, 64) }
	write("backed", d("a"), testCandidateBuild, EdgeBuildPresent)
	write("otherbuild", d("b"), "ffffffffffffffffffffffffffffffffffffffff", EdgeBuildPresent)
	write("unbound", d("c"), testCandidateBuild, EdgeBuildAbsent)
	got, err := DocumentOperationsReceiptBacked(ctx, pool, testSchemaDigest, testCandidateBuild,
		map[string]string{"backed": d("a"), "otherbuild": d("b"), "unbound": d("c"), "none": d("e")})
	if err != nil {
		t.Fatal(err)
	}
	if !got["backed"] || got["otherbuild"] || got["unbound"] || got["none"] {
		t.Fatalf("backed = %v, want only the operation with an admissible receipt for this build", got)
	}
}

// status shows what a class root's proof rests on: the reference, the counted shapes and
// every excluded shape by name, read from the ADMISSIBLE receipt of the build the live row
// names (a receipt for another build or an unbound one shows nothing).
func TestMCPClassStatusShowsWhatTheClassProofRestsOn(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)
	op, digest := mcpclass.Operation("hotspots"), mcpclass.DocumentDigest()
	if _, err := pool.Exec(ctx, `INSERT INTO go_api_class_decision
		(operation, mode, current_candidate_build, schema_digest, review_evidence, recorded_by)
		VALUES ($1,'canary',$2,$3,'e','t')`, op, testCandidateBuild, testSchemaDigest); err != nil {
		t.Fatal(err)
	}
	evidence, _ := json.Marshal(ReceiptProvenance{
		MeasurementRoute: RouteProof, EdgeBuildBinding: EdgeBuildPresent, EdgeMode: EdgeModeDocRoute,
		MCPClass: &MCPClassProvenance{Root: "hotspots", Reference: "go_document_route", Executed: 5, Matched: 4,
			Excluded: []string{"featureFlagTimeseries=doc_operation_not_receipt_backed"}},
	})
	other, _ := json.Marshal(ReceiptProvenance{MCPClass: &MCPClassProvenance{Root: "hotspots", Reference: "NEWER_INADMISSIBLE", Executed: 99, Matched: 99}})
	write := func(binding, route, ev string) {
		t.Helper()
		time.Sleep(5 * time.Millisecond)
		if _, err := Write(ctx, pool, Receipt{
			SchemaDigest: testSchemaDigest, DocumentDigest: digest, SelectedOperation: op, CandidateBuild: testCandidateBuild,
			RequestIdentity: "identity-" + binding + route + ev[:5], Stage: EnablementProofStage, TerminalState: EnablementProofTerminalState,
			MeasurementRoute: route, BuildBinding: binding, OrgID: "70d529e0", RecordedBy: "test",
			ReviewEvidence: ev, ObservedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	rootRow := func() MCPClassRootStatus {
		t.Helper()
		rows, err := MCPClassStatusRows(ctx, pool, testSchemaDigest, map[string]bool{"hotspots": true})
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if row.Root == "hotspots" {
				return row
			}
		}
		t.Fatal("no hotspots row")
		return MCPClassRootStatus{}
	}
	write(EdgeBuildAbsent, RouteProof, string(evidence)) // inadmissible: shows nothing
	if r := rootRow(); r.Proven || r.ProofReference != "" || len(r.ProofExcluded) != 0 {
		t.Fatalf("an unbound receipt shows proof: %+v", r)
	}
	write(EdgeBuildPresent, RouteProof, string(evidence))
	// NEWER receipts that are not admissible class receipts must not replace what is shown:
	// one unbound, one on the edge route.
	write(EdgeBuildAbsent, RouteProof, string(other))
	write(EdgeBuildPresent, RouteEdge, string(other))
	r := rootRow()
	if !r.Proven || r.ProofReference != "go_document_route" || r.ProofExecuted != 5 || r.ProofMatched != 4 ||
		len(r.ProofExcluded) != 1 || r.ProofExcluded[0] != "featureFlagTimeseries=doc_operation_not_receipt_backed" {
		t.Fatalf("status = %+v, want the reference, the counts and the excluded shape named", r)
	}
}

// CHAOS-7499: a class receipt for a stochastic root is the CITED mismatch; `enable`'s reader admits it only with a named class citation and
// nothing outside it, and still refuses a plain mismatch (no citation), a blank citation, and a difference outside the citation.
func TestClassReceiptOfAStochasticRootIsAdmittedOnlyAsTheCitedMismatch(t *testing.T) {
	op, digest := mcpclass.Operation("capacityForecast"), mcpclass.DocumentDigest()
	for name, tc := range map[string]struct {
		terminal string
		defects  []string
		outside  int
		want     bool
	}{
		"cited mismatch, nothing outside":     {TerminalStateMismatch, []string{testStochasticCitation}, 0, true},
		"plain mismatch, no citation":         {TerminalStateMismatch, nil, 0, false},
		"a difference outside the citation":   {TerminalStateMismatch, []string{testStochasticCitation}, 1, false},
		"a stochastic root never reads match": {"proof_failed", []string{testStochasticCitation}, 0, false},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			pool := startRegistryPostgres(t)
			if _, err := Write(ctx, pool, Receipt{
				SchemaDigest: testSchemaDigest, DocumentDigest: digest, SelectedOperation: op,
				CandidateBuild: testCandidateBuild, RequestIdentity: "identity-1", Stage: EnablementProofStage,
				TerminalState: tc.terminal, BaselineDefects: tc.defects, DifferencesOutsideBaselineDefect: tc.outside,
				MeasurementRoute: RouteProof, BuildBinding: EdgeBuildPresent,
				OrgID: "70d529e0", RecordedBy: "test", ReviewEvidence: "stochastic class receipt", ObservedAt: time.Now().UTC(),
			}); err != nil {
				t.Fatalf("Write: %v", err)
			}
			found, err := OperationsWithEnablementProofByKind(ctx, pool, testSchemaDigest, testCandidateBuild, TargetModeCanary,
				map[string]string{op: digest}, map[string]string{op: OperationKindMCPClass})
			if err != nil {
				t.Fatal(err)
			}
			if found[op] != tc.want {
				t.Fatalf("admitted=%v, want %v", found[op], tc.want)
			}
		})
	}
}

// A blank class citation never reaches the table: the writer refuses it, so `enable` can never read a stochastic receipt that cites nothing.
func TestClassReceiptWithABlankCitationIsRefusedAtTheWriter(t *testing.T) {
	_, err := Write(context.Background(), startRegistryPostgres(t), Receipt{
		SchemaDigest: testSchemaDigest, DocumentDigest: mcpclass.DocumentDigest(), SelectedOperation: mcpclass.Operation("capacityForecast"),
		CandidateBuild: testCandidateBuild, RequestIdentity: "identity-1", Stage: EnablementProofStage, TerminalState: TerminalStateMismatch,
		BaselineDefects: []string{"  "}, MeasurementRoute: RouteProof, BuildBinding: EdgeBuildPresent,
		OrgID: "70d529e0", RecordedBy: "test", ReviewEvidence: "blank citation", ObservedAt: time.Now().UTC(),
	})
	if err == nil || !strings.Contains(err.Error(), "empty citation") {
		t.Fatalf("a blank citation was not refused: %v", err)
	}
}
