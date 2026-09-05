package daily

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// CHAOS-4290, #2241 r1 Findings 1 and 3.
//
// The policy under test is that rowsWritten -- not merely err -- decides
// whether the compatibility bridge is allowed to recompute a family. The old
// code ignored rowsWritten entirely, so any error handed the family back to the
// bridge even when the native executor had already written rows.

// skipRecordingCompatibility records what the bridge was told to skip and
// whether it would have written. `wrote` is the assertion that matters: the
// bridge writing over a partial native result is the actual defect, and a test
// that only inspected the skip list would pass on a bridge that ignored it.
type skipRecordingCompatibility struct {
	fakeCompatibility
	sawSkip []string
	wrote   bool
}

func (bridge *skipRecordingCompatibility) Finalize(_ context.Context, _ Run, skip []string) error {
	bridge.sawSkip = append([]string(nil), skip...)
	for _, name := range skip {
		if name == "ic_finalize" {
			return nil
		}
	}
	bridge.wrote = true
	return nil
}

func finalizeHandlerWithFamily(
	t *testing.T, family NativeFinalizeFamilyExecutor,
) (*FinalizeHandler, *skipRecordingCompatibility, *recordingNativeFamilyObserver) {
	t.Helper()
	bridge := &skipRecordingCompatibility{}
	handler, err := NewFinalizeHandler(finalizeStoreWithClaim(), bridge)
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingNativeFamilyObserver{}
	handler.SetNativeFinalizeFamilyObserver(observer)
	if err := handler.SetNativeFinalizeFamilies(
		map[string]NativeFinalizeFamilyExecutor{"ic_finalize": family},
	); err != nil {
		t.Fatal(err)
	}
	return handler, bridge, observer
}

// THE FINDING ITSELF. An executor that fails after writing rows must keep the
// bridge out, because a ClickHouse insert can commit and the call still fail.
func TestAFailingFamilyThatWroteRowsKeepsTheBridgeOut(t *testing.T) {
	family := &stubFinalizeFamily{rows: 3, err: errors.New("failed after a durable insert")}
	handler, bridge, observer := finalizeHandlerWithFamily(t, family)

	if err := handler.Work(context.Background(), finalizeExecutionFor(testRunID)); err != nil {
		t.Fatalf("Work = %v, want success -- one family's failure must not fail the finalize", err)
	}
	if bridge.wrote {
		t.Fatal("the bridge recomputed a family that may already have written rows -- " +
			"two writers on an append-only table, the later one winning silently")
	}
	if len(bridge.sawSkip) != 1 || bridge.sawSkip[0] != "ic_finalize" {
		t.Fatalf("skip=%v, want [ic_finalize]", bridge.sawSkip)
	}
	assertObserved(t, observer, jobruntime.DailyMetricsNativeFamilyOutcomeUncertain, 3)
}

// The other half of the same predicate, and the reason this is not simply
// "never fail open": a family that provably wrote nothing is safe to hand back.
func TestAFailingFamilyThatWroteNothingStillFailsOpen(t *testing.T) {
	family := &stubFinalizeFamily{rows: 0, err: errors.New("failed before writing anything")}
	handler, bridge, observer := finalizeHandlerWithFamily(t, family)

	if err := handler.Work(context.Background(), finalizeExecutionFor(testRunID)); err != nil {
		t.Fatalf("Work = %v, want success", err)
	}
	if !bridge.wrote {
		t.Fatal("the bridge did NOT recompute a family that wrote nothing -- the family " +
			"would simply be missing, which fail-open exists to prevent")
	}
	if len(bridge.sawSkip) != 0 {
		t.Fatalf("skip=%v, want empty", bridge.sawSkip)
	}
	assertObserved(t, observer, jobruntime.DailyMetricsNativeFamilyOutcomeRefused, 0)
}

func TestASucceedingFamilyIsReportedComputedWithItsRowCount(t *testing.T) {
	family := &stubFinalizeFamily{rows: 7}
	handler, bridge, observer := finalizeHandlerWithFamily(t, family)

	if err := handler.Work(context.Background(), finalizeExecutionFor(testRunID)); err != nil {
		t.Fatalf("Work = %v, want success", err)
	}
	if bridge.wrote {
		t.Fatal("the bridge recomputed a family that succeeded natively")
	}
	assertObserved(t, observer, jobruntime.DailyMetricsNativeFamilyOutcomeComputed, 7)
}

// Finding 3. A cooperative executor -- one that honours its ctx, as the
// interface contract requires -- must let the compute return promptly once the
// lease context is cancelled, rather than running the remaining families
// against a run another worker may already own.
func TestACancelledContextStopsRemainingFamilies(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"alpha", "zeta"}

	bridge := &skipRecordingCompatibility{}
	handler, err := NewFinalizeHandler(finalizeStoreWithClaim(), bridge)
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingNativeFamilyObserver{}
	handler.SetNativeFinalizeFamilyObserver(observer)

	ctx, cancel := context.WithCancel(context.Background())
	// "alpha" sorts first, cancels while running, and cooperates by returning.
	// "zeta" must then never be called at all.
	alpha := &cancellingFinalizeFamily{cancel: cancel}
	zeta := &stubFinalizeFamily{rows: 99}
	if err := handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{
		"alpha": alpha, "zeta": zeta,
	}); err != nil {
		t.Fatal(err)
	}

	done := make(chan []string, 1)
	go func() { done <- handler.computeNativeFinalizeFamilies(ctx, Run{}) }()

	select {
	case skip := <-done:
		if zeta.calls != 0 {
			t.Fatalf("zeta ran %d time(s) after the lease context was cancelled -- "+
				"another worker may already be computing it", zeta.calls)
		}
		if len(skip) != 1 || skip[0] != "alpha" {
			t.Fatalf("skip=%v, want [alpha] -- the family that completed before cancellation", skip)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("computeNativeFinalizeFamilies did not return after cancellation")
	}

	// zeta never ran, so it provably wrote nothing and is REFUSED: the bridge
	// on whichever worker owns the run next is expected to cover it.
	var sawZetaRefused bool
	for _, call := range observer.calls {
		if call.family == "zeta" && call.outcome == jobruntime.DailyMetricsNativeFamilyOutcomeRefused {
			sawZetaRefused = true
		}
	}
	if !sawZetaRefused {
		t.Fatalf("zeta was not reported refused; observations=%v -- a family that silently "+
			"never ran is exactly what this telemetry exists to surface", observer.calls)
	}
}

// cancellingFinalizeFamily cancels the lease context from INSIDE the compute,
// which is what losing a lease mid-family looks like, then cooperates by
// returning. A non-cooperative executor cannot be simulated usefully here:
// Go cannot interrupt one, which is precisely why the interface documents
// non-cooperation as a contract violation rather than defending against it.
type cancellingFinalizeFamily struct {
	cancel context.CancelFunc
	calls  int
}

func (family *cancellingFinalizeFamily) ComputeFinalizeFamily(context.Context, Run) (int, error) {
	family.calls++
	family.cancel()
	return 1, nil
}

func assertObserved(
	t *testing.T, observer *recordingNativeFamilyObserver,
	outcome jobruntime.DailyMetricsNativeFamilyOutcome, rows int,
) {
	t.Helper()
	for _, call := range observer.calls {
		if call.family == "ic_finalize" && call.outcome == outcome && call.rowsWritten == rows {
			return
		}
	}
	t.Fatalf("no observation of ic_finalize as %q with %d row(s); got %v", outcome, rows, observer.calls)
}
