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
		// Required since CHAOS-5484: Write refuses a receipt that does
		// not say whether the build was bound per response.
		BuildBinding: EdgeBuildPresent,
		ObservedAt:   time.Unix(1757000000, 0).UTC(),
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

// CHAOS-5484 added a third pre-database guard beside the two above, and it
// arrived without a unit killer -- only the integration suite touched it.
// Same argument as the measurement-route guard one line down from it: a
// receipt that does not say how well it knew which build served it cannot
// be told apart later from one that knew exactly, and `proven` is keyed on
// candidate_build.
func TestWriteRefusesAReceiptWithNoBuildBinding(t *testing.T) {
	for _, binding := range []string{"", "run_level", "per-request", "strong"} {
		receipt := wellFormedReceipt()
		receipt.BuildBinding = binding

		// db is nil on purpose: the guard must fire BEFORE any statement
		// is sent, so a nil Querier proves nothing was executed.
		_, err := Write(context.Background(), nil, receipt)
		if err == nil {
			t.Fatalf("build binding %q was accepted", binding)
		}
		if !strings.Contains(err.Error(), "build binding") {
			t.Fatalf("binding %q: the refusal must name the binding, got: %v", binding, err)
		}
	}

	// "run_level" is first in that list on purpose: it is what an earlier
	// draft of CHAOS-5484 called the weak case, so it is the value a
	// future writer is most likely to reinvent. It was dropped because
	// every row that exists already carries run-level evidence -- R70
	// makes VerifyCandidateBuild a hard refusal -- so the value would
	// distinguish nothing. The DB CHECK says the same thing; this says it
	// before the round trip.

	// Control: both legal bindings pass the guard and reach the database
	// (nil here, so getting past shows in the error, not a refusal).
	for _, binding := range []string{EdgeBuildPresent, EdgeBuildAbsent} {
		receipt := wellFormedReceipt()
		receipt.BuildBinding = binding
		func() {
			defer func() { _ = recover() }()
			_, err := Write(context.Background(), nil, receipt)
			if err != nil && strings.Contains(err.Error(), "build binding") {
				t.Fatalf("binding %q is legal but the guard refused it: %v", binding, err)
			}
		}()
	}
}
