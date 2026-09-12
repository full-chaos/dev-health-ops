package workgraph

import (
	"encoding/json"
	"errors"
	"testing"
)

// TestBoundMaterializeStartLeavesASuppliedStartAlone pins the branch that must
// never fire. Every operator backfill supplies its own from_date, and a bound
// that "helpfully" narrowed one would silently turn a requested 90-day
// rematerialize into a 30-day one -- the widening-by-default failure this
// repository already refuses for the recompute caps, in the narrowing
// direction.
func TestBoundMaterializeStartLeavesASuppliedStartAlone(t *testing.T) {
	scope := []byte(`{"force":false,"from_date":"2026-01-01","to_date":"2026-09-12"}`)
	bounded, fromDate, windowDays, err := boundMaterializeStart(scope)
	if err != nil {
		t.Fatalf("bound = %v, want nil", err)
	}
	if fromDate != "" || windowDays != 0 {
		t.Fatalf("reported a bound (%q, %d) for a scope that supplied its own start", fromDate, windowDays)
	}
	if string(bounded) != string(scope) {
		t.Fatalf("bounded = %s, want the scope unchanged", bounded)
	}
}

// TestBoundMaterializeStartComputesFromTheScopesOwnToDate proves the filled-in
// start names the SAME window the executor's default would have produced. The
// executor advances a supplied to_date by one day to make the window
// end-exclusive over whole days, so the offset here is +1 - 30: a plain -30
// would name a window one day short of the default and quietly stop
// recomputing the oldest day of every start-less request.
func TestBoundMaterializeStartComputesFromTheScopesOwnToDate(t *testing.T) {
	bounded, fromDate, windowDays, err := boundMaterializeStart([]byte(`{"to_date":"2026-09-12"}`))
	if err != nil {
		t.Fatalf("bound = %v, want nil", err)
	}
	// 2026-09-12 + 1 day - 30 days = 2026-08-14.
	if fromDate != "2026-08-14" {
		t.Fatalf("bounded from_date = %q, want 2026-08-14", fromDate)
	}
	if windowDays != 0 {
		t.Fatalf("window_days = %d, want 0: an anchored scope gets an explicit start, not a length", windowDays)
	}
	var fields map[string]any
	if err := json.Unmarshal(bounded, &fields); err != nil {
		t.Fatalf("bounded scope is not an object: %v", err)
	}
	if fields["from_date"] != "2026-08-14" {
		t.Fatalf("bounded scope from_date = %v, want 2026-08-14", fields["from_date"])
	}
	if fields["to_date"] != "2026-09-12" {
		t.Fatalf("bounded scope dropped to_date: %s", bounded)
	}
}

// TestBoundMaterializeStartIsDeterministic is the retry invariant, and it is
// the reason this function takes no clock. Producers derive request ids
// deterministically, so a lease reclaim re-runs the same write with the same
// id; writeRowTx refuses a conflicting row whose stored scope differs from the
// incoming one. A bound that moved with the wall clock would therefore make a
// retry that crossed midnight permanently unconfirmable.
func TestBoundMaterializeStartIsDeterministic(t *testing.T) {
	for _, scope := range [][]byte{
		[]byte(`{"to_date":"2026-09-12"}`),
		[]byte(`{"force":false}`),
		[]byte(`{"window_days":365}`),
	} {
		first, _, _, err := boundMaterializeStart(scope)
		if err != nil {
			t.Fatalf("bound %s = %v", scope, err)
		}
		second, _, _, err := boundMaterializeStart(scope)
		if err != nil {
			t.Fatalf("re-bound %s = %v", scope, err)
		}
		if string(first) != string(second) {
			t.Fatalf("bound of %s is not deterministic: %s vs %s", scope, first, second)
		}
	}
}

// TestBoundMaterializeStartPinsAWindowWithNoAnchor covers the scope that names
// neither a start nor anything to compute one from -- the shape 1094 of the
// backlog's requests had. No value written at enqueue time can pin WHEN such a
// window ends, so what gets pinned is its LENGTH.
func TestBoundMaterializeStartPinsAWindowWithNoAnchor(t *testing.T) {
	bounded, fromDate, windowDays, err := boundMaterializeStart([]byte(`{"force":false}`))
	if err != nil {
		t.Fatalf("bound = %v, want nil", err)
	}
	if fromDate != "" {
		t.Fatalf("from_date = %q, want empty: there is no anchor to compute one from", fromDate)
	}
	if windowDays != MaxMaterializeLookbackDays {
		t.Fatalf("window_days = %d, want %d", windowDays, MaxMaterializeLookbackDays)
	}
	var fields map[string]any
	if err := json.Unmarshal(bounded, &fields); err != nil {
		t.Fatalf("bounded scope is not an object: %v", err)
	}
	if fields["window_days"] != float64(MaxMaterializeLookbackDays) {
		t.Fatalf("bounded scope window_days = %v, want %d", fields["window_days"], MaxMaterializeLookbackDays)
	}
}

// TestBoundMaterializeStartClampsAWiderWindow is the only branch that narrows
// something a producer asked for. A start-less scope naming a 365-day window
// is an unbounded-start request wearing a number, and it is exactly the kind
// of request the ticket forbids enqueueing.
func TestBoundMaterializeStartClampsAWiderWindow(t *testing.T) {
	_, _, windowDays, err := boundMaterializeStart([]byte(`{"window_days":365}`))
	if err != nil {
		t.Fatalf("bound = %v, want nil", err)
	}
	if windowDays != MaxMaterializeLookbackDays {
		t.Fatalf("window_days = %d, want it clamped to %d", windowDays, MaxMaterializeLookbackDays)
	}
}

// TestBoundMaterializeStartLeavesANarrowerWindowAlone is the other half of the
// clamp: the bound is a CEILING, not a setting. A producer asking for less
// work than the maximum is not asking for something unbounded.
func TestBoundMaterializeStartLeavesANarrowerWindowAlone(t *testing.T) {
	scope := []byte(`{"window_days":7}`)
	bounded, fromDate, windowDays, err := boundMaterializeStart(scope)
	if err != nil {
		t.Fatalf("bound = %v, want nil", err)
	}
	if fromDate != "" || windowDays != 0 {
		t.Fatalf("reported a bound (%q, %d) for a window already inside it", fromDate, windowDays)
	}
	if string(bounded) != string(scope) {
		t.Fatalf("bounded = %s, want the scope unchanged", bounded)
	}
}

// TestBoundMaterializeStartRefusesANonObjectScope moves a rejection the
// executor already makes from execution time to enqueue time, where it costs
// one producer error instead of a job that can never succeed.
func TestBoundMaterializeStartRefusesANonObjectScope(t *testing.T) {
	for _, scope := range [][]byte{[]byte(`null`), []byte(`[]`), []byte(`"from_date"`), []byte(`not json`)} {
		if _, _, _, err := boundMaterializeStart(scope); !errors.Is(err, ErrInvalidState) {
			t.Fatalf("bound %s = %v, want ErrInvalidState", scope, err)
		}
	}
}
