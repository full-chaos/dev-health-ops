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
	handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{"ic_finalize": family})

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
func TestFailingNativeFinalizeFamilyFallsBackToTheBridge(t *testing.T) {
	store := finalizeStoreWithClaim()
	compatibility := &recordingFinalizeCompatibility{}
	handler, err := NewFinalizeHandler(store, compatibility)
	if err != nil {
		t.Fatal(err)
	}
	family := &stubFinalizeFamily{err: errors.New("clickhouse hiccup")}
	handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{"ic_finalize": family})

	if err := handler.Work(context.Background(), finalizeExecutionFor(testRunID)); err != nil {
		t.Fatalf("Work = %v, want success -- a native family failure is not a finalize failure", err)
	}
	if compatibility.callCount != 1 {
		t.Fatalf("bridge calls = %d, want 1", compatibility.callCount)
	}
	if len(compatibility.sawSkip) != 0 {
		t.Fatalf("bridge saw skip=%v, want EMPTY -- a failed family must not be "+
			"skipped, or its rows are written by nobody", compatibility.sawSkip)
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
	handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{
		"zeta": &stubFinalizeFamily{}, "alpha": &stubFinalizeFamily{}, "mid": &stubFinalizeFamily{},
	})
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

type recordingFinalizeObserver struct {
	calls []string
	rows  map[string]int
	err   error
}

func (observer *recordingFinalizeObserver) ObserveDailyMetricsNativeFamily(
	family string, outcome jobruntime.DailyMetricsNativeFamilyOutcome, rowsWritten int, _ time.Duration,
) error {
	if observer.rows == nil {
		observer.rows = map[string]int{}
	}
	observer.calls = append(observer.calls, family+":"+string(outcome))
	observer.rows[family] = rowsWritten
	return observer.err
}

// CHAOS-4290 shipped the mechanism with fail-open and NO counter, which its own
// RISK-NOTES admitted meant a family failing every run degraded to Python
// invisibly. This is that gap closed: a REFUSED outcome must be reported even
// though the finalize still succeeds.
func TestFailingNativeFinalizeFamilyIsReportedRefused(t *testing.T) {
	store := finalizeStoreWithClaim()
	compatibility := &recordingFinalizeCompatibility{}
	handler, err := NewFinalizeHandler(store, compatibility)
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingFinalizeObserver{}
	handler.SetNativeFinalizeFamilyObserver(observer)
	handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{
		"ic_finalize": &stubFinalizeFamily{err: errors.New("clickhouse hiccup")},
	})

	if err := handler.Work(context.Background(), finalizeExecutionFor(testRunID)); err != nil {
		t.Fatalf("Work = %v, want success -- telemetry must not gate the job", err)
	}
	if len(observer.calls) != 1 || observer.calls[0] != "ic_finalize:refused" {
		t.Fatalf("observed %v, want [ic_finalize:refused] -- a fail-open path with "+
			"no counter is indistinguishable from one that is working", observer.calls)
	}
	// And the skip list is still empty, so the bridge still computes it.
	if len(compatibility.sawSkip) != 0 {
		t.Fatalf("bridge saw skip=%v, want empty", compatibility.sawSkip)
	}
}

// A succeeding family reports computed WITH its row count, so the series can
// distinguish "ran and wrote nothing" from "did not run".
func TestSucceedingNativeFinalizeFamilyIsReportedComputedWithRows(t *testing.T) {
	store := finalizeStoreWithClaim()
	handler, err := NewFinalizeHandler(store, &recordingFinalizeCompatibility{})
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingFinalizeObserver{}
	handler.SetNativeFinalizeFamilyObserver(observer)
	handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{
		"ic_finalize": &stubFinalizeFamily{rows: 42},
	})
	if err := handler.Work(context.Background(), finalizeExecutionFor(testRunID)); err != nil {
		t.Fatal(err)
	}
	if len(observer.calls) != 1 || observer.calls[0] != "ic_finalize:computed" {
		t.Fatalf("observed %v, want [ic_finalize:computed]", observer.calls)
	}
	if observer.rows["ic_finalize"] != 42 {
		t.Fatalf("rows = %d, want 42 -- a computed outcome with no row count cannot "+
			"distinguish 'wrote nothing' from 'did not run'", observer.rows["ic_finalize"])
	}
}

// An observer that itself errors must not fail the job, matching every other
// observer in this package.
func TestFinalizeSucceedsWhenTheObserverFails(t *testing.T) {
	store := finalizeStoreWithClaim()
	handler, err := NewFinalizeHandler(store, &recordingFinalizeCompatibility{})
	if err != nil {
		t.Fatal(err)
	}
	handler.SetNativeFinalizeFamilyObserver(&recordingFinalizeObserver{err: errors.New("telemetry down")})
	handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{
		"ic_finalize": &stubFinalizeFamily{rows: 1},
	})
	if err := handler.Work(context.Background(), finalizeExecutionFor(testRunID)); err != nil {
		t.Fatalf("Work = %v, want success despite the observer failure", err)
	}
}
