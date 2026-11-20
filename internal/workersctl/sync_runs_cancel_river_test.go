package workersctl

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/joboperator"
)

type fakeRiverCanceller struct {
	states    map[int64]joboperator.JobState
	cancelErr map[int64]error
	cancelled []int64
	reasons   []string
}

func (fake *fakeRiverCanceller) Inspect(_ context.Context, _ joboperator.Principal, id int64) (joboperator.JobSummary, error) {
	state, ok := fake.states[id]
	if !ok {
		return joboperator.JobSummary{}, errors.New("job not found")
	}
	return joboperator.JobSummary{ID: id, State: state}, nil
}

func (fake *fakeRiverCanceller) Cancel(_ context.Context, _ joboperator.Principal, id int64, reason, correlation string) (joboperator.JobSummary, error) {
	if err := fake.cancelErr[id]; err != nil {
		return joboperator.JobSummary{}, err
	}
	fake.cancelled = append(fake.cancelled, id)
	fake.reasons = append(fake.reasons, reason+"/"+correlation)
	return joboperator.JobSummary{ID: id, State: joboperator.StateCancelled}, nil
}

// Only a waiting job is cancelled, with the verb's own reason and correlation id; a running or
// finished job is left; a job that cannot be read or cancelled is counted as a failure.
func TestCancelRiverJobsCancelsOnlyWaitingJobsAndCountsFailures(t *testing.T) {
	fake := &fakeRiverCanceller{
		states: map[int64]joboperator.JobState{
			1: joboperator.StateAvailable, 2: joboperator.StateScheduled, 3: joboperator.StateRetryable, 4: joboperator.StatePending,
			5: joboperator.StateRunning, 6: joboperator.StateCompleted, 7: joboperator.StateDiscarded, 8: joboperator.StateCancelled,
			9: joboperator.StateAvailable,
		},
		cancelErr: map[int64]error{9: errors.New("cancel refused")},
	}
	count := cancelRiverJobs(context.Background(), fake, joboperator.OperatorPrincipal,
		[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, "operator_test", "corr-1")
	want := riverJobCancelCount{Cancelled: 4, SkippedRunning: 1, SkippedTerminal: 3, Failed: 2}
	if count != want {
		t.Fatalf("count %+v, want %+v", count, want)
	}
	if !slices.Equal(fake.cancelled, []int64{1, 2, 3, 4}) {
		t.Fatalf("cancelled %v, want the four waiting jobs", fake.cancelled)
	}
	for _, reason := range fake.reasons {
		if reason != "operator_test/corr-1" {
			t.Fatalf("cancel carried %q, want the verb's reason and correlation id", reason)
		}
	}
}
