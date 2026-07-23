package jobruntime

import (
	"context"
	"errors"
	"time"
)

// RuntimeInfo drives worker_runtime_info{version,commit,profile}. All fields
// are deployment identity, not job or tenant data.
type RuntimeInfo struct {
	Version string
	Commit  string
	Profile string
}

// JobLabels are the only dimensions exposed to metrics. They intentionally
// exclude job IDs, organizations, correlations, domains, payloads, and errors.
type JobLabels struct {
	Profile string
	Queue   string
	Kind    string
}

// Observer maps runtime decisions to the TRD metric family:
// worker_jobs_running, worker_job_duration_seconds,
// worker_job_attempts_total, worker_job_panics_total,
// worker_job_cancellations_total, and worker_domain_state_mismatch_total.
// Queue availability/wait metrics are sampled by the River backend rather
// than per-handler execution. Deployment sampling sets
// worker_execution_saturation_ratio{profile} directly from configured worker
// capacity and active executions.
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
}

// RegisterRuntime validates the low-cardinality scrape-presence identity
// before passing it to an Observer.
func RegisterRuntime(ctx context.Context, observer Observer, info RuntimeInfo) error {
	if observer == nil {
		return errors.New("runtime observer is required")
	}
	if !boundedIdentity(info.Version, 128) || !boundedIdentity(info.Commit, 128) || !boundedIdentity(info.Profile, 32) {
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
