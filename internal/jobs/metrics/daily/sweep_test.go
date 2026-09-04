package daily

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

type recordingSweepObserver struct {
	counts map[string]int
}

func newRecordingSweepObserver() *recordingSweepObserver {
	return &recordingSweepObserver{counts: map[string]int{}}
}

func (observer *recordingSweepObserver) ObserveDailyMetricsExecutionSweep(outcome string, count int) error {
	observer.counts[outcome] += count
	return nil
}

type stubSweeper struct {
	result SweepResult
	err    error
	calls  int
}

func (sweeper *stubSweeper) SweepDeadClaims(context.Context, string, int) (SweepResult, error) {
	sweeper.calls++
	return sweeper.result, sweeper.err
}

// A sweep failure must never reach the dispatch job, and must never be silent to
// metrics. Those two properties are in tension -- the obvious way to make a
// failure loud is to return it -- so both halves are asserted together.
func TestSweepFailureIsInvisibleToTheJobButNotToMetrics(t *testing.T) {
	observer := newRecordingSweepObserver()
	sweeper := &stubSweeper{err: errors.New("bridge unreachable")}
	handler := &Dispatcher{}
	handler.SetExecutionSweeper(sweeper)
	handler.SetExecutionSweepObserver(observer)

	// Returns nothing: there is no error path back into Work by construction.
	handler.sweepDeadClaimExecutions(context.Background(), "run-1")

	if sweeper.calls != 1 {
		t.Fatalf("sweeper calls = %d, want 1", sweeper.calls)
	}
	if observer.counts["failed"] != 1 {
		t.Fatalf(
			"failed count = %d, want 1 -- a fail-open path with no failure counter "+
				"is indistinguishable from one that is working",
			observer.counts["failed"],
		)
	}
	if observer.counts["swept"] != 0 {
		t.Fatalf("swept count = %d on a failed sweep, want 0", observer.counts["swept"])
	}
}

func TestSweepRecordsBothOutcomesSeparately(t *testing.T) {
	observer := newRecordingSweepObserver()
	handler := &Dispatcher{}
	handler.SetExecutionSweeper(&stubSweeper{result: SweepResult{
		Swept:              2,
		SkippedClaimActive: 3,
		SweptIDs:           []string{"a", "b"},
	}})
	handler.SetExecutionSweepObserver(observer)

	handler.sweepDeadClaimExecutions(context.Background(), "run-1")

	if observer.counts["swept"] != 2 {
		t.Fatalf("swept = %d, want 2", observer.counts["swept"])
	}
	// Distinct from swept: a live claim is real in-flight work that was
	// correctly refused, not a failure and not a no-op.
	if observer.counts["skipped_claim_active"] != 3 {
		t.Fatalf("skipped_claim_active = %d, want 3", observer.counts["skipped_claim_active"])
	}
	if observer.counts["failed"] != 0 {
		t.Fatalf("failed = %d on a successful sweep, want 0", observer.counts["failed"])
	}
}

// Idempotent under retry. Placed before the publish loop, this runs again on
// every attempt of a retrying dispatch, so repeating it must be harmless.
func TestSweepIsSafeToRepeatAcrossRetryAttempts(t *testing.T) {
	observer := newRecordingSweepObserver()
	sweeper := &stubSweeper{result: SweepResult{Swept: 1, SweptIDs: []string{"a"}}}
	handler := &Dispatcher{}
	handler.SetExecutionSweeper(sweeper)
	handler.SetExecutionSweepObserver(observer)

	for attempt := 0; attempt < 3; attempt++ {
		handler.sweepDeadClaimExecutions(context.Background(), "run-1")
	}

	if sweeper.calls != 3 {
		t.Fatalf("sweeper calls = %d across 3 attempts, want 3", sweeper.calls)
	}
	// Nothing accumulates beyond the counter; the endpoint's own
	// `state = 'executing'` predicate is what makes a second pass find fewer
	// rows rather than re-sweeping the same ones.
	if observer.counts["failed"] != 0 {
		t.Fatalf("failed = %d across repeats, want 0", observer.counts["failed"])
	}
}

// Optional by construction: a Dispatcher with no sweeper must behave exactly as
// it did before this change, so the feature can roll out without touching
// NewDispatcher or any existing caller.
func TestSweepIsANoOpWhenUnconfigured(t *testing.T) {
	handler := &Dispatcher{}
	handler.sweepDeadClaimExecutions(context.Background(), "run-1")

	observer := newRecordingSweepObserver()
	handler.SetExecutionSweepObserver(observer)
	handler.sweepDeadClaimExecutions(context.Background(), "run-1")
	if len(observer.counts) != 0 {
		t.Fatalf("counts = %v with no sweeper attached, want none", observer.counts)
	}

	var nilHandler *Dispatcher
	nilHandler.sweepDeadClaimExecutions(context.Background(), "run-1")
	nilHandler.SetExecutionSweeper(&stubSweeper{})
}

func TestNewHTTPExecutionSweeperRefusesAnEmptyToken(t *testing.T) {
	_, err := NewHTTPExecutionSweeper(
		defaultSweepTestClient(),
		HTTPCompatibilityConfig{Endpoint: "https://api.internal", AllowInsecureInternal: false},
		"   ",
	)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("error = %v, want ErrUnavailable for a blank token", err)
	}
}

func defaultSweepTestClient() *http.Client { return nil }
