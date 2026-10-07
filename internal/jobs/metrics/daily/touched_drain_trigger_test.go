package daily

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

type recordingTouchedDrainer struct{ passes []string }

func (drainer *recordingTouchedDrainer) DrainTouchedDays(_ context.Context, organizationID, passID string) {
	drainer.passes = append(drainer.passes, organizationID+" "+passID)
}

func dispatchExecutionForTest() *jobruntime.Execution[jobruntime.DailyMetricsDispatchArgs] {
	return &jobruntime.Execution[jobruntime.DailyMetricsDispatchArgs]{
		OrganizationID: pointer(testOrgID),
		Envelope:       jobcontract.Envelope{OrganizationID: pointer(testOrgID), Domain: jobcontract.DomainLink{Type: "daily_metrics_run", ID: testRunID}},
		Args: jobruntime.DailyMetricsDispatchArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.DailyMetricsDispatchPayload]{
			OrganizationID: pointer(testOrgID), Domain: jobcontract.DomainLink{Type: "daily_metrics_run", ID: testRunID}, Payload: jobcontract.DailyMetricsDispatchPayload{RunID: testRunID},
		}},
	}
}

// The dispatch of the nightly run is the floor trigger of the touched-day
// drain: one pass, named after the run. The dispatch of a run of any other
// generation triggers none: the END of those runs does.
func TestOnlyTheDispatchOfTheNightlyRunTriggersADrainPass(t *testing.T) {
	for generation, want := range map[string][]string{
		"fixed-schedule:daily_metrics_fanout:2026-08-12T01:00:00Z":      {testOrgID + " n:" + testRunID},
		"post-sync:00000000-0000-4000-8000-0000000000aa":                nil,
		TouchedDrainGenerationPrefix + "n:" + testRunID:                 nil,
		ManualDailyGenerationPrefix + "x":                               nil,
		ExternalRecomputeGenerationPrefix + "x":                         nil,
		"fixed-schedule:dora_daily_fanout:2026-08-12T02:15:00Z":         nil,
		"fixed-schedule:daily_metrics_fanoutX:2026-08-12T01:00:00Z":     nil,
		"prefix-fixed-schedule:daily_metrics_fanout:2026-08-12T01:00:0": nil,
	} {
		store := &fakeStore{run: Run{ID: testRunID, OrganizationID: testOrgID, Generation: generation, Status: "running"}}
		handler, err := NewDispatcher(store, fakePublisher{}, &fakeRepositoryDiscoverer{})
		if err != nil {
			t.Fatal(err)
		}
		drainer := &recordingTouchedDrainer{}
		handler.SetTouchedDaysDrainer(drainer)
		if err := handler.Work(context.Background(), dispatchExecutionForTest()); err != nil {
			t.Fatalf("%s: %v", generation, err)
		}
		if !reflect.DeepEqual(drainer.passes, want) {
			t.Errorf("generation %q: drain passes = %v, want %v", generation, drainer.passes, want)
		}
	}
}

// The end of a daily run is the continuation trigger: one pass after the run
// succeeded and one after it failed for good. An attempt that will be retried
// is not an end and triggers nothing.
func TestTheEndOfADailyRunTriggersADrainPass(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}
	endPass := []string{testOrgID + " e:" + testRunID}
	cases := []struct {
		name                 string
		familyErr            error
		attempt, maxAttempts int
		completionErr        error
		terminalWriteErr     error
		want                 []string
	}{
		{name: "the run succeeded", attempt: 1, maxAttempts: 4, want: endPass},
		{name: "the last attempt failed", familyErr: errors.New("boom"), attempt: 4, maxAttempts: 4, want: endPass},
		{name: "an attempt failed and will be retried", familyErr: errors.New("boom"), attempt: 2, maxAttempts: 4},
		{name: "the completion write failed", attempt: 1, maxAttempts: 4, completionErr: ErrUnavailable},
		{name: "the terminal write of the last attempt failed", familyErr: errors.New("boom"), attempt: 4, maxAttempts: 4, terminalWriteErr: ErrLeaseLost},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			store := finalizeStoreWithClaim()
			store.completionErr = test.completionErr
			store.failFinalizePermanentlyErr = test.terminalWriteErr
			handler, err := NewFinalizeHandler(store)
			if err != nil {
				t.Fatal(err)
			}
			if err := handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{
				"ic_finalize": &stubFinalizeFamily{err: test.familyErr},
			}); err != nil {
				t.Fatal(err)
			}
			drainer := &recordingTouchedDrainer{}
			handler.SetTouchedDaysDrainer(drainer)
			execution := finalizeExecutionFor(testRunID)
			execution.Attempt = test.attempt
			execution.Definition.MaxAttempts = test.maxAttempts
			_ = handler.Work(context.Background(), execution)
			if !reflect.DeepEqual(drainer.passes, test.want) {
				t.Fatalf("drain passes = %v, want %v", drainer.passes, test.want)
			}
		})
	}
}
