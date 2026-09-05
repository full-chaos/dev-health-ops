package daily

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// recordingFinalizeCompatibility captures the skip list the bridge was handed,
// which is the whole observable output of the mechanism at this level.
type recordingFinalizeCompatibility struct {
	fakeCompatibility
	sawSkip   []string
	callCount int
}

func (compatibility *recordingFinalizeCompatibility) Finalize(_ context.Context, _ Run, skipFamilies []string) error {
	compatibility.callCount++
	compatibility.sawSkip = skipFamilies
	return nil
}

type stubFinalizeFamily struct {
	rows  int
	err   error
	calls int
}

func (family *stubFinalizeFamily) ComputeFinalizeFamily(context.Context, Run) (int, error) {
	family.calls++
	return family.rows, family.err
}

func finalizeExecutionFor(runID string) *jobruntime.Execution[jobruntime.DailyMetricsFinalizeArgs] {
	return &jobruntime.Execution[jobruntime.DailyMetricsFinalizeArgs]{
		OrganizationID: pointer(testOrgID),
		Envelope: jobcontract.Envelope{
			OrganizationID: pointer(testOrgID),
			Domain:         jobcontract.DomainLink{Type: "daily_metrics_run", ID: runID},
		},
		Args: jobruntime.DailyMetricsFinalizeArgs{
			EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.DailyMetricsFinalizePayload]{
				OrganizationID: pointer(testOrgID),
				Domain:         jobcontract.DomainLink{Type: "daily_metrics_run", ID: runID},
				Payload:        jobcontract.DailyMetricsFinalizePayload{RunID: runID},
			},
		},
	}
}

func finalizeStoreWithClaim() *fakeStore {
	return &fakeStore{
		run: Run{ID: testRunID, OrganizationID: testOrgID, Status: "running"},
		finalizeClaim: &FinalizeClaim{
			Run:           Run{ID: testRunID, OrganizationID: testOrgID, Status: "running"},
			Token:         "token",
			LeaseDuration: 30 * time.Millisecond,
		},
	}
}

// A registered finalize family that SUCCEEDS must appear in the skip list the
// bridge receives -- that skip is the only thing stopping Python recomputing
// and silently superseding the rows Go just wrote.
func TestSucceedingNativeFinalizeFamilyIsSkippedByTheBridge(t *testing.T) {
	store := finalizeStoreWithClaim()
	compatibility := &recordingFinalizeCompatibility{}
	handler, err := NewFinalizeHandler(store, compatibility)
	if err != nil {
		t.Fatal(err)
	}
	family := &stubFinalizeFamily{rows: 7}
	if err := handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{"ic_finalize": family}); err != nil {
		t.Fatal(err)
	}

	if err := handler.Work(context.Background(), finalizeExecutionFor(testRunID)); err != nil {
		t.Fatalf("Work = %v, want success", err)
	}
	if family.calls != 1 {
		t.Fatalf("family calls = %d, want 1", family.calls)
	}
	if len(compatibility.sawSkip) != 1 || compatibility.sawSkip[0] != "ic_finalize" {
		t.Fatalf("bridge saw skip=%v, want [ic_finalize] -- without it Python "+
			"recomputes the family and its rows supersede the native ones", compatibility.sawSkip)
	}
}

// FAIL-OPEN, carried unchanged from CHAOS-4276's partition-side ruling: a
// family that errors is LEFT OUT of the skip list, so the bridge computes it
// exactly as before. The finalize itself must still succeed -- one family
// degrading to Python must not fail the run.
// REPLACES TestFailingNativeFinalizeFamilyFallsBackToTheBridge, which asserted
// the OPPOSITE and was correct only under the fail-open policy #2241 r2
// Findings 1 and 2 removed. Its reasoning -- "a failed family must not be
// skipped, or its rows are written by nobody" -- is answered by the redrive:
// the rows are written by the NEXT attempt, not by Python.
//
// Letting the bridge cover it was the hazard, because a family that failed on
// this attempt may have succeeded on a previous one.
func TestFailingNativeFinalizeFamilyFailsTheAttemptInsteadOfFallingBack(t *testing.T) {
	store := finalizeStoreWithClaim()
	compatibility := &recordingFinalizeCompatibility{}
	handler, err := NewFinalizeHandler(store, compatibility)
	if err != nil {
		t.Fatal(err)
	}
	family := &stubFinalizeFamily{err: errors.New("clickhouse hiccup")}
	if err := handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{"ic_finalize": family}); err != nil {
		t.Fatal(err)
	}

	workErr := handler.Work(context.Background(), finalizeExecutionFor(testRunID))
	if workErr == nil {
		t.Fatal("Work succeeded after a native family failed -- the run completes and " +
			"is never redriven, and the family's rows are written by nobody")
	}
	if !errors.Is(workErr, ErrNativeFinalizeFamilyFailed) {
		t.Fatalf("err = %v, want it to wrap ErrNativeFinalizeFamilyFailed", workErr)
	}
	if compatibility.callCount != 0 {
		t.Fatalf("bridge calls = %d, want 0 -- Python must never compute a family "+
			"registered as native, or a retry can reintroduce it as a second writer",
			compatibility.callCount)
	}
}

// The default is inert: no registered families means the bridge is called
// exactly as it was before this capability existed.
func TestFinalizeWithoutNativeFamiliesIsUnchanged(t *testing.T) {
	store := finalizeStoreWithClaim()
	compatibility := &recordingFinalizeCompatibility{}
	handler, err := NewFinalizeHandler(store, compatibility)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), finalizeExecutionFor(testRunID)); err != nil {
		t.Fatal(err)
	}
	if compatibility.callCount != 1 || len(compatibility.sawSkip) != 0 {
		t.Fatalf("calls=%d skip=%v, want 1 and empty", compatibility.callCount, compatibility.sawSkip)
	}
}

// Deterministic order, mirroring SetNativeFamilies. Without sorting, the skip
// list would depend on Go map iteration order and two identical runs could
// send different bytes.
func TestNativeFinalizeFamilyOrderIsDeterministic(t *testing.T) {
	store := finalizeStoreWithClaim()
	compatibility := &recordingFinalizeCompatibility{}
	handler, err := NewFinalizeHandler(store, compatibility)
	if err != nil {
		t.Fatal(err)
	}
	// The registration guard only admits names the Python bridge gates on, and
	// deterministic ordering needs more than one name to be observable at all.
	// Widening the recognised set for the duration of this test is the honest
	// way to get there: inventing three production family names would make the
	// guard's own test lie about what Python understands.
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"alpha", "mid", "zeta"}

	if err := handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{
		"zeta": &stubFinalizeFamily{}, "alpha": &stubFinalizeFamily{}, "mid": &stubFinalizeFamily{},
	}); err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), finalizeExecutionFor(testRunID)); err != nil {
		t.Fatal(err)
	}
	want := []string{"alpha", "mid", "zeta"}
	if len(compatibility.sawSkip) != len(want) {
		t.Fatalf("skip=%v, want %v", compatibility.sawSkip, want)
	}
	for i, name := range want {
		if compatibility.sawSkip[i] != name {
			t.Fatalf("skip=%v, want %v (sorted)", compatibility.sawSkip, want)
		}
	}
}

// restoreRecognisedFinalizeFamilies puts the production set back. Taken as an
// argument rather than read inside, so the deferred call captures the value at
// defer time and a test cannot accidentally restore a set another test left
// behind.
func restoreRecognisedFinalizeFamilies(original []string) {
	pythonRecognisedFinalizeFamilies = original
}
