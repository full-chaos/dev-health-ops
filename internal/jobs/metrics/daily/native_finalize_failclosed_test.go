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
// whether a partial native write still fails the attempt. The old code
// ignored rowsWritten entirely, so any error was treated identically whether
// or not the native executor had already written rows.
//
// CHAOS-3092 PR-A' removed the compatibility bridge these tests used to also
// assert stayed OUT after a failure (there is no bridge left to call at
// all), so every test below narrows to the single behaviour that still
// applies without it: does the attempt fail, and is the right outcome
// reported.

func finalizeHandlerWithFamily(
	t *testing.T, family NativeFinalizeFamilyExecutor,
) (*FinalizeHandler, *recordingNativeFamilyObserver) {
	t.Helper()
	handler, err := NewFinalizeHandler(finalizeStoreWithClaim())
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
	return handler, observer
}

// THE FINDING ITSELF. An executor that fails after writing rows must still
// fail the attempt, because a ClickHouse insert can commit and the call
// still fail -- a redrive is what repairs the run, not a second writer.
func TestAPartialNativeWriteFailsTheAttempt(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}

	family := &stubFinalizeFamily{rows: 3, err: errors.New("failed after a durable insert")}
	handler, observer := finalizeHandlerWithFamily(t, family)

	err := handler.Work(context.Background(), finalizeExecutionFor(testRunID))
	if err == nil {
		t.Fatal("Work succeeded after a partial native write -- the run would be " +
			"recorded complete over incomplete output, with nothing to repair it")
	}
	if !errors.Is(err, ErrNativeFinalizeFamilyFailed) {
		t.Fatalf("err = %v, want it to wrap ErrNativeFinalizeFamilyFailed", err)
	}
	assertObserved(t, observer, jobruntime.DailyMetricsNativeFamilyOutcomePartialWrite, 3)
}

// There is NO fail-open half any more (#2241 r2 Findings 1 and 2). A family
// that wrote nothing on THIS attempt may still have written on a previous
// one, so the attempt fails and River redrives it regardless.
func TestAFailingFamilyThatWroteNothingAlsoFailsTheAttempt(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}

	family := &stubFinalizeFamily{rows: 0, err: errors.New("failed before writing anything")}
	handler, observer := finalizeHandlerWithFamily(t, family)

	err := handler.Work(context.Background(), finalizeExecutionFor(testRunID))
	if err == nil {
		t.Fatal("Work succeeded after a native family failed -- the run would be " +
			"recorded complete and never redriven")
	}
	if !errors.Is(err, ErrNativeFinalizeFamilyFailed) {
		t.Fatalf("err = %v, want it to wrap ErrNativeFinalizeFamilyFailed", err)
	}
	// Refused, not partial_write: nothing landed. The distinction still matters
	// to an operator even though both outcomes now redrive.
	assertObserved(t, observer, jobruntime.DailyMetricsNativeFamilyOutcomeRefused, 0)
}

func TestASucceedingFamilyIsReportedComputedWithItsRowCount(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}

	family := &stubFinalizeFamily{rows: 7}
	handler, observer := finalizeHandlerWithFamily(t, family)

	if err := handler.Work(context.Background(), finalizeExecutionFor(testRunID)); err != nil {
		t.Fatalf("Work = %v, want success", err)
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

	handler, err := NewFinalizeHandler(finalizeStoreWithClaim())
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

	done := make(chan error, 1)
	go func() {
		done <- handler.computeNativeFinalizeFamilies(ctx, Run{})
	}()

	select {
	case err := <-done:
		if zeta.calls != 0 {
			t.Fatalf("zeta ran %d time(s) after the lease context was cancelled -- "+
				"another worker may already be computing it", zeta.calls)
		}
		if alpha.calls != 1 {
			t.Fatalf("alpha ran %d time(s), want 1", alpha.calls)
		}
		if !errors.Is(err, ErrNativeFinalizeFamilyFailed) {
			t.Fatalf("err = %v, want it to wrap ErrNativeFinalizeFamilyFailed (the cancellation)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("computeNativeFinalizeFamilies did not return after cancellation")
	}

	// zeta never ran, so it provably wrote nothing and is REFUSED -- with no
	// bridge left at all (CHAOS-3092 PR-A'), this run must be redriven for
	// zeta to ever be written.
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
