//go:build integration

package goapiproof

// CHAOS-7214: the class receipt and the document receipt never admit each
// other, in BOTH directions, against a real Postgres. Each case writes a real
// receipt through Write and reads it back through the enablement reader.

import (
	"context"
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
