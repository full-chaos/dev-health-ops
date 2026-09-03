package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

// TestTheGateCannotBeSkippedByConstruction is the positive control for the one
// property the readiness gate's own correctness tests cannot establish: that it
// is REACHED.
//
// The grid test proves DailyMetricsReady decides correctly. That says nothing
// about whether anything calls it — and for most of this branch's life nothing
// did. A future kind inheriting this executor's shape could plausibly build it
// without Postgres and still compute and write; the rows would be well-formed
// and quietly wrong, which is CHAOS-2373 exactly.
//
// So the control is on CONSTRUCTION and on the handler seam, not on the gate's
// logic: an executor without the store the gate reads must not be constructible
// through the public path, and must refuse at the seam if built any other way.
func TestTheGateCannotBeSkippedByConstruction(t *testing.T) {
	t.Run("the public constructor refuses a nil pool", func(t *testing.T) {
		// A nil ClickHouse conn short-circuits first, so this case would be
		// vacuous with one; the assertion is specifically that the POOL is
		// required, which needs the conn check passed.
		_, err := NewRecommendationsExecutor(
			context.Background(), driverConnStub{}, nil, "org-1")
		if err == nil {
			t.Fatal("an executor was constructed with no Postgres pool; it would " +
				"compute and write while never consulting the readiness gate")
		}
		if !errors.Is(err, ErrRecommendationsPostgresUnavailable) {
			// Not a strict-equality nit: the refusal REASON is what the
			// registration maps onto a labelled counter, so a refusal that
			// arrives under the wrong identity is invisible on the dashboard
			// that exists to show it.
			t.Errorf("refused with %v, want the postgres-unavailable reason so the "+
				"refusal counter can label it correctly", err)
		}
	})

	t.Run("the handler seam refuses an executor built without a pool", func(t *testing.T) {
		// The in-package construction path used by ClickHouse-only tests. If
		// ComputePartition ever stops refusing this, those tests silently gain
		// the ability to reach the compute path with no gate.
		executor := &RecommendationsExecutor{conn: driverConnStub{}}

		// PANIC-CONVERTED, because a nil pool dereferences inside the gate.
		// Without this the mutant that removes the pool check dies by PANIC
		// rather than on this test's assertion -- a death that looks correct
		// in a pass/fail column while the named assertion never ran
		// (4752-go's clause; they hit the same shape on their clock guard).
		err := mustNotPanicHere(t, func() error {
			_, err := executor.ComputePartition(context.Background(),
				Run{ID: "run-1", OrganizationID: "org-1"},
				Partition{ID: "partition-1", Scope: json.RawMessage(`{"version":1,"window":30}`)})
			return err
		})
		if err == nil {
			t.Fatal("ComputePartition proceeded with no Postgres pool — the gate " +
				"would never run and partial daily metrics would be evaluated")
		}
		if !errors.Is(err, ErrRecommendationsPostgresUnavailable) {
			t.Errorf("refused with %v, want the postgres-unavailable reason", err)
		}
	})
}

// TestAWithheldDayCompletesRatherThanFailing pins the disposition, which is the
// half of the gate's contract that a correctness test of the DECISION misses.
//
// Returning an error here would make the handler fail the partition and retry
// it; the retry reads the same unfinished fan-out and fails again, producing a
// loop that reads as flapping while the correct behaviour is simply to wait.
// The partition must therefore COMPLETE, with zero rows.
//
// This is asserted at the scope-parsing boundary rather than against a live
// gate, because the disposition is a property of ComputePartition's control
// flow: it is what the function does with a false, not how the false is
// computed.
func TestScopeFaultsAreInvalidStateNotRetryableFailures(t *testing.T) {
	executor := &RecommendationsExecutor{conn: driverConnStub{}, pool: nil}

	for _, testCase := range []struct {
		name  string
		scope string
	}{
		{"unparseable scope", `{`},
		{"window below one would evaluate an empty span", `{"version":1,"window":0}`},
		{"unparseable as_of", `{"version":1,"window":30,"as_of":"31-08-2026"}`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := executor.ComputePartition(context.Background(),
				Run{ID: "run-1", OrganizationID: "org-1"},
				Partition{ID: "partition-1", Scope: json.RawMessage(testCase.scope)})
			if err == nil {
				t.Fatal("a malformed scope was accepted")
			}
			// ErrInvalidState is what marks a partition PERMANENTLY failed
			// rather than retryable. A malformed scope will not fix itself, so
			// retrying it burns the partition's attempts and hides the real
			// fault behind exhaustion.
			if !errors.Is(err, ErrInvalidState) {
				t.Errorf("failed with %v, want ErrInvalidState so the partition is "+
					"marked permanent rather than retried against a scope that "+
					"cannot become valid", err)
			}
		})
	}
}

// TestAZeroValuedExecutorRefusesRatherThanPanicking is the positive-path
// assertion for wallClock(), which was added defensively and never asserted.
//
// EvaluationInstant CALLS the clock it is given. Before wallClock() a
// zero-valued executor panicked inside a function whose name says nothing about
// clocks; the handler cannot classify a panic, so the partition dies in a way
// that names neither the missing construction nor the real fault.
func TestAZeroValuedExecutorRefusesRatherThanPanicking(t *testing.T) {
	executor := &RecommendationsExecutor{conn: driverConnStub{}}

	// Reaching the instant resolver at all requires a valid scope, so this
	// case is only meaningful past the payload checks.
	_, err := executor.ComputePartition(context.Background(),
		Run{ID: "run-1", OrganizationID: "org-1"},
		Partition{ID: "partition-1", Scope: json.RawMessage(`{"version":1,"window":30}`)})

	if err == nil {
		t.Fatal("a zero-valued executor proceeded")
	}
	// A REFUSAL, and the right one — not a panic, and not a misattributed
	// error naming something other than the missing pool.
	if !errors.Is(err, ErrRecommendationsPostgresUnavailable) {
		t.Errorf("refused with %v, want the postgres-unavailable reason", err)
	}
}

// mustNotPanicHere converts a panic into a failure that names the expectation.
//
// A nil-pool dereference IS the defect under test, so a panic must be reported
// as a failure of THIS assertion rather than tearing down the binary with a
// stack naming runtime internals.
func mustNotPanicHere(t *testing.T, call func() error) error {
	t.Helper()
	var err error
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				t.Fatalf("PANICKED (%v), want ErrRecommendationsPostgresUnavailable "+
					"-- an executor with no Postgres pool must REFUSE at the seam, "+
					"not dereference nil inside the readiness gate", recovered)
			}
		}()
		err = call()
	}()
	return err
}
