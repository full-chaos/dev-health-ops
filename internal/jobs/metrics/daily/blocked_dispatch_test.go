package daily

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// reconcilingStore is a fakeStore that ALSO satisfies blockedRunReconciler,
// so these tests exercise the wired path rather than calling the reconcile
// directly.
type reconcilingStore struct {
	fakeStore
	calls   int
	sawOrg  string
	outcome BlockedReconcileOutcome
	err     error
}

func (store *reconcilingStore) ReconcileBlockedRuns(
	_ context.Context, orgID string,
) (BlockedReconcileOutcome, error) {
	store.calls++
	store.sawOrg = orgID
	return store.outcome, store.err
}

type recordingBlockedObserver struct {
	counts map[string]int
	err    error
}

func (observer *recordingBlockedObserver) ObserveDailyMetricsBlockedRun(outcome string, count int) error {
	if observer.counts == nil {
		observer.counts = map[string]int{}
	}
	observer.counts[outcome] += count
	return observer.err
}

func blockedDispatchExecution() *jobruntime.Execution[jobruntime.DailyMetricsDispatchArgs] {
	return &jobruntime.Execution[jobruntime.DailyMetricsDispatchArgs]{
		OrganizationID: pointer(testOrgID),
		Envelope: jobcontract.Envelope{
			OrganizationID: pointer(testOrgID),
			Domain:         jobcontract.DomainLink{Type: "daily_metrics_run", ID: testRunID},
		},
		Args: jobruntime.DailyMetricsDispatchArgs{
			EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.DailyMetricsDispatchPayload]{
				OrganizationID: pointer(testOrgID),
				Domain:         jobcontract.DomainLink{Type: "daily_metrics_run", ID: testRunID},
				Payload:        jobcontract.DailyMetricsDispatchPayload{RunID: testRunID},
			},
		},
	}
}

func runningRun() Run {
	return Run{ID: testRunID, OrganizationID: testOrgID, Status: "running"}
}

// The fan-out is the periodic tick this marker depends on -- the
// stranded-finalize sweep it might otherwise hang off is not periodic at all
// (FindStrandedFinalizeRuns has exactly one non-test caller, the workerctl
// CLI). If the fan-out ever stops calling it, the marker silently stops being
// maintained and the 112 already-wedged runs stay invisible, which is the
// whole failure this change exists to end.
func TestDispatchReconcilesBlockedRunsForItsOwnOrganization(t *testing.T) {
	store := &reconcilingStore{
		fakeStore: fakeStore{run: runningRun()},
		outcome:   BlockedReconcileOutcome{Marked: 2, Cleared: 1, Blocked: 5},
	}
	observer := &recordingBlockedObserver{}
	handler, err := NewDispatcher(store, fakePublisher{}, &fakeRepositoryDiscoverer{})
	if err != nil {
		t.Fatal(err)
	}
	handler.SetBlockedRunObserver(observer)

	if err := handler.Work(context.Background(), blockedDispatchExecution()); err != nil {
		t.Fatalf("Work = %v, want success", err)
	}
	if store.calls != 1 {
		t.Fatalf("reconcile calls = %d, want 1", store.calls)
	}
	if store.sawOrg != testOrgID {
		t.Fatalf("reconciled org = %q, want %q", store.sawOrg, testOrgID)
	}
	if observer.counts["marked"] != 2 || observer.counts["cleared"] != 1 {
		t.Fatalf("observed %+v, want marked 2 / cleared 1", observer.counts)
	}
}

// Zeros must still be reported: a series that vanishes when nothing changed
// cannot be distinguished from a reconcile that stopped running, and the
// CHAOS-5041 alert is built on increase() over these counters.
func TestDispatchReportsZeroTransitionsRatherThanSkippingTheSeries(t *testing.T) {
	store := &reconcilingStore{fakeStore: fakeStore{run: runningRun()}}
	observer := &recordingBlockedObserver{}
	handler, err := NewDispatcher(store, fakePublisher{}, &fakeRepositoryDiscoverer{})
	if err != nil {
		t.Fatal(err)
	}
	handler.SetBlockedRunObserver(observer)
	if err := handler.Work(context.Background(), blockedDispatchExecution()); err != nil {
		t.Fatal(err)
	}
	for _, outcome := range []string{"marked", "cleared"} {
		if _, reported := observer.counts[outcome]; !reported {
			t.Fatalf("%q was not reported at all on a no-change pass", outcome)
		}
	}
}

// Fail-open, and this is the assertion that matters most in this file: the
// marker is a VISIBILITY mechanism, and telemetry must never decide whether
// the day's metrics get computed. A reconcile failure that failed the
// dispatch job would take out the actual fan-out for that organization --
// strictly worse than the invisibility it was added to fix.
func TestDispatchSucceedsEvenWhenTheBlockedReconcileFails(t *testing.T) {
	store := &reconcilingStore{
		fakeStore: fakeStore{run: runningRun()},
		err:       errors.New("reconcile exploded"),
	}
	observer := &recordingBlockedObserver{}
	handler, err := NewDispatcher(store, fakePublisher{}, &fakeRepositoryDiscoverer{})
	if err != nil {
		t.Fatal(err)
	}
	handler.SetBlockedRunObserver(observer)
	if err := handler.Work(context.Background(), blockedDispatchExecution()); err != nil {
		t.Fatalf("Work = %v, want success despite the reconcile failure", err)
	}
	if store.calls != 1 {
		t.Fatalf("reconcile calls = %d, want 1", store.calls)
	}
	// Silent to the job, NOT to metrics: a fail-open path with no counter is
	// indistinguishable from one that is working.
	if observer.counts["failed"] != 1 {
		t.Fatalf("observed %+v on a failed reconcile, want failed=1", observer.counts)
	}
	if observer.counts["marked"] != 0 || observer.counts["cleared"] != 0 {
		t.Fatalf("observed %+v on a failed reconcile, want no transitions claimed", observer.counts)
	}
}

// An observer that itself errors must not fail the job either.
func TestDispatchSucceedsEvenWhenTheBlockedObserverFails(t *testing.T) {
	store := &reconcilingStore{
		fakeStore: fakeStore{run: runningRun()},
		outcome:   BlockedReconcileOutcome{Marked: 1},
	}
	handler, err := NewDispatcher(store, fakePublisher{}, &fakeRepositoryDiscoverer{})
	if err != nil {
		t.Fatal(err)
	}
	handler.SetBlockedRunObserver(&recordingBlockedObserver{err: errors.New("telemetry down")})
	if err := handler.Work(context.Background(), blockedDispatchExecution()); err != nil {
		t.Fatalf("Work = %v, want success despite the observer failure", err)
	}
}

// The capability is optional by construction: a Store that does not implement
// it is skipped silently rather than panicking on the type assertion. Without
// this, adding the reconcile would have forced the method onto every Store
// implementation, including fakes that have nothing to do with it.
func TestDispatchSkipsTheReconcileWhenTheStoreDoesNotSupportIt(t *testing.T) {
	store := &fakeStore{run: runningRun()}
	handler, err := NewDispatcher(store, fakePublisher{}, &fakeRepositoryDiscoverer{})
	if err != nil {
		t.Fatal(err)
	}
	handler.SetBlockedRunObserver(&recordingBlockedObserver{})
	if err := handler.Work(context.Background(), blockedDispatchExecution()); err != nil {
		t.Fatalf("Work = %v, want success with a non-reconciling store", err)
	}
	// Control: the reconciling fake DOES satisfy the interface, so the skip
	// above is the assertion discriminating, not the interface being
	// unsatisfiable by anything.
	var _ blockedRunReconciler = (*reconcilingStore)(nil)
}

// A nil observer is a silent no-op, matching every other observer here.
func TestDispatchWorksWithNoBlockedObserverAttached(t *testing.T) {
	store := &reconcilingStore{
		fakeStore: fakeStore{run: runningRun()},
		outcome:   BlockedReconcileOutcome{Marked: 3},
	}
	handler, err := NewDispatcher(store, fakePublisher{}, &fakeRepositoryDiscoverer{})
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), blockedDispatchExecution()); err != nil {
		t.Fatalf("Work = %v, want success with no observer", err)
	}
	if store.calls != 1 {
		t.Fatalf("reconcile calls = %d, want 1 even with no observer", store.calls)
	}
}
