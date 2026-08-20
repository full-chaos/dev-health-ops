package jobruntime

import (
	"context"
	"errors"
	"time"
)

// RuntimeInfo drives worker_runtime_info{version,commit}. All fields are
// deployment identity, not job or tenant data.
type RuntimeInfo struct {
	Version string
	Commit  string
}

// JobLabels are the only dimensions exposed to metrics. They intentionally
// exclude job IDs, organizations, correlations, domains, payloads, and errors.
type JobLabels struct {
	Queue string
	Kind  string
}

// Observer maps runtime decisions to the TRD metric family:
// worker_jobs_running, worker_job_duration_seconds,
// worker_job_attempts_total, worker_job_panics_total,
// worker_job_cancellations_total, and worker_domain_state_mismatch_total.
// worker_job_wait_seconds is sampled per-handler execution from the River
// row's ScheduledAt, since that is the only place the adapter observes both
// the job's availability time and its execution start. Deployment sampling
// sets worker_execution_saturation_ratio{queue} directly from configured
// worker capacity and active executions, both scoped to this process.
//
// Pool adapters use worker_database_pool_saturation_ratio{pool} and
// worker_database_pool_acquire_seconds{pool,result}; pool is bounded to
// domain|queue_control and result to acquired|timeout|cancelled|error.
type Observer interface {
	RuntimeRegistered(context.Context, RuntimeInfo)
	JobStarted(context.Context, JobLabels)
	JobFinished(context.Context, JobLabels, Result, ErrorCategory, time.Duration)
	JobPanicked(context.Context, JobLabels)
	JobCancelled(context.Context, JobLabels, ErrorCategory)
	DomainMismatch(context.Context, string)
	BudgetWait(context.Context, JobLabels, time.Duration, string)
	// JobWait observes the time from job availability (River's ScheduledAt) to
	// execution start. It is void, like every other Observer method, so a
	// telemetry fault can never fail a job.
	JobWait(context.Context, JobLabels, time.Duration)
}

// SyncLeaseObserver is the narrower capability concrete expired-lease
// recovery implementations depend on directly, the same way concrete budget
// implementations call ObserveProviderBudgetWait directly rather than through
// the generic Observer: generic runtime middleware has no way to know
// whether a claim recovered an expired lease, only the code that performed
// the recovery does. *MetricsCollector satisfies this.
type SyncLeaseObserver interface {
	ObserveSyncLeaseExpired(SyncLeaseLabels, SyncLeaseResult) error
}

// ReportRunLeaseObserver is the narrow capability used by the report domain
// store after it durably reclaims or terminalizes an expired execution lease.
// Generic runtime middleware cannot infer this state change from a job retry.
type ReportRunLeaseObserver interface {
	ObserveReportRunLeaseExpired(ReportRunLeaseResult) error
}

// WorkGraphLeaseObserver is the narrow capability the work-graph store depends
// on after a release is fenced out by a lease the claimant has already
// outlived (CHAOS-4002, symmetric with DailyMetricsLeaseObserver's
// release_lost result). Generic runtime middleware cannot infer this from a
// job retry: only the store that ran the fenced UPDATE knows whether it
// matched zero rows because the lease had already expired.
type WorkGraphLeaseObserver interface {
	ObserveWorkGraphLeaseReleaseLost() error
}

// RemainingMetricsLeaseObserver is the remaining-metrics store's equivalent of
// WorkGraphLeaseObserver, for the same reason (CHAOS-4002).
type RemainingMetricsLeaseObserver interface {
	ObserveRemainingMetricsLeaseReleaseLost() error
}

// DailyMetricsLeaseObserver is the narrow capability the daily-metrics store
// depends on after it resolves an existing lease durably. Generic runtime
// middleware cannot infer this: a claim that parks for a live lease and a claim
// that finds nothing to do are the same job outcome, and only the code that ran
// the claim knows which happened.
type DailyMetricsLeaseObserver interface {
	ObserveDailyMetricsLease(DailyMetricsLeaseStage, DailyMetricsLeaseResult) error
}

// RegisterRuntime validates the low-cardinality scrape-presence identity
// before passing it to an Observer.
func RegisterRuntime(ctx context.Context, observer Observer, info RuntimeInfo) error {
	if observer == nil {
		return errors.New("runtime observer is required")
	}
	if !boundedIdentity(info.Version, 128) || !boundedIdentity(info.Commit, 128) {
		return errors.New("runtime identity is invalid")
	}
	observer.RuntimeRegistered(ctx, info)
	return nil
}

func boundedIdentity(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') &&
			(character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') &&
			character != '.' && character != '_' && character != '-' && character != '+' {
			return false
		}
	}
	return true
}
