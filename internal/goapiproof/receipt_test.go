package goapiproof

import (
	"context"
	"strings"
	"testing"
	"time"
)

// wellFormedReceipt is everything Write's pre-database guards accept, so
// each test below can break exactly ONE field and attribute the refusal.
func wellFormedReceipt() Receipt {
	return Receipt{
		SchemaDigest:      "sha256:29d509cd",
		DocumentDigest:    "06ca28a0",
		SelectedOperation: "featureFlags",
		CandidateBuild:    "b18e56fa79cfe20ce0f75df148144b832d92be36",
		RequestIdentity:   "sha256:deadbeef",
		Stage:             "deployed_executed",
		TerminalState:     "match",
		OrgID:             "70d529e0",
		RecordedBy:        "go-api-prove",
		MeasurementRoute:  RouteEdge,
		ObservedAt:        time.Unix(1757000000, 0).UTC(),
	}
}

// r9 F10a: the empty-candidate-build guard had no killer. Without it the
// INSERT reaches PostgreSQL and the composite FK refuses it there -- with a
// constraint name instead of a sentence, which is the entire reason the
// guard is stated at the call site.
func TestWriteRefusesAReceiptWithNoCandidateBuild(t *testing.T) {
	receipt := wellFormedReceipt()
	receipt.CandidateBuild = ""

	// db is nil on purpose: both guards must fire BEFORE any statement is
	// sent, so a nil Querier proves nothing was executed.
	_, err := Write(context.Background(), nil, receipt)
	if err == nil {
		t.Fatal("a receipt naming no candidate build was accepted: `proven` is keyed on the build, so such a row can never be matched by the enablement predicate")
	}
	if !strings.Contains(err.Error(), "empty candidate build") {
		t.Fatalf("the refusal must name the empty build, got: %v", err)
	}
}

// r9 F10b: the measurement-route guard had no killer either. An empty
// route is the value a caller that never set the field would send, and
// such a row cannot be told apart from served traffic later -- the whole
// reason the column exists (R50).
func TestWriteRefusesAReceiptWithNoMeasurementRoute(t *testing.T) {
	for _, route := range []string{"", "served", "Edge"} {
		receipt := wellFormedReceipt()
		receipt.MeasurementRoute = route

		_, err := Write(context.Background(), nil, receipt)
		if err == nil {
			t.Fatalf("measurement route %q was accepted: a receipt that does not SAY which route produced it cannot be told apart from served traffic", route)
		}
		if !strings.Contains(err.Error(), "measurement route") {
			t.Fatalf("route %q: the refusal must name the route, got: %v", route, err)
		}
	}

	// Control: the two legal routes must still pass the guard and reach
	// the database (which is nil here, so a panic proves they got past).
	for _, route := range []string{RouteEdge, RouteProof} {
		receipt := wellFormedReceipt()
		receipt.MeasurementRoute = route
		func() {
			defer func() { _ = recover() }()
			_, err := Write(context.Background(), nil, receipt)
			if err != nil && strings.Contains(err.Error(), "measurement route") {
				t.Fatalf("route %q is legal but the guard refused it: %v", route, err)
			}
		}()
	}
}
