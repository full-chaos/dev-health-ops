//go:build integration

package syncdispatchruntime

import (
	"context"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// resultObserver records the River result of every finished attempt.
type resultObserver struct {
	mu      sync.Mutex
	results []jobruntime.Result
}

func (*resultObserver) RuntimeRegistered(context.Context, jobruntime.RuntimeInfo) {}
func (*resultObserver) JobStarted(context.Context, jobruntime.JobLabels)          {}
func (observer *resultObserver) JobFinished(_ context.Context, _ jobruntime.JobLabels, result jobruntime.Result, _ jobruntime.ErrorCategory, _ time.Duration) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.results = append(observer.results, result)
}
func (*resultObserver) JobPanicked(context.Context, jobruntime.JobLabels) {}
func (*resultObserver) JobCancelled(context.Context, jobruntime.JobLabels, jobruntime.ErrorCategory) {
}
func (*resultObserver) DomainMismatch(context.Context, string)                                  {}
func (*resultObserver) BudgetWait(context.Context, jobruntime.JobLabels, time.Duration, string) {}
func (*resultObserver) JobWait(context.Context, jobruntime.JobLabels, time.Duration)            {}
func (*resultObserver) ObserveDeterministicFailure(context.Context, jobruntime.JobLabels, jobruntime.Reason) {
}

type cancelledQueuedTenantScope struct{}

func (cancelledQueuedTenantScope) Supports(string) bool { return true }
func (cancelledQueuedTenantScope) Resolve(ctx context.Context, _ jobruntime.ScopeRequest) (context.Context, error) {
	return ctx, nil
}

type cancelledQueuedBudget struct{}

func (cancelledQueuedBudget) Supports(string, int) bool { return true }
func (cancelledQueuedBudget) Acquire(context.Context, jobruntime.BudgetRequest) (jobruntime.BudgetLease, error) {
	return cancelledQueuedLease{}, nil
}

type cancelledQueuedLease struct{}

func (cancelledQueuedLease) Release() {}
