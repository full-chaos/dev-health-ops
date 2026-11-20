package workersctl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboperator"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

func dispatchSyncRuns(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return writeError(stderr, "invalid_request")
	}
	switch args[0] {
	case "cancel":
		return dispatchSyncRunsCancel(ctx, runtime, args[1:], stdout, stderr)
	default:
		return writeError(stderr, "invalid_request")
	}
}

// dispatchSyncRunsCancel handles `sync-runs cancel`: it closes one unfinished sync run of one
// organization through syncdispatchruntime.CancelSyncRun on the domain pool, then cancels the
// River jobs of the provider-unit deliveries that cancel closed. The domain write is the audited
// one (resource_type sync_run); each River cancel writes its own jobs.cancel row.
func dispatchSyncRunsCancel(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	flags := quietFlags("sync-runs cancel")
	org := flags.String("org", "", "organization id")
	orgStdin := flags.Bool("org-stdin", false, "read the organization id from stdin")
	runFlag := flags.String("run", "", "sync run id")
	dryRun := flags.Bool("dry-run", false, "report the run's units and what a cancel would do, without writing anything")
	allowRunning := flags.Bool("allow-running", false, "also fail a running unit whose lease is still live; its worker loses the lease")
	mutation := addMutationFlags(flags)
	if flags.Parse(args) != nil || flags.NArg() != 0 || !mutation.valid(*dryRun) {
		return writeError(stderr, "invalid_request")
	}
	runID, err := canonicalUUID(*runFlag)
	if err != nil {
		return writeError(stderr, "invalid_request")
	}
	orgID, code := resolveOrgFlag(runtime, *org, *orgStdin, stderr)
	if code != 0 {
		return code
	}
	resource := orgID + ":" + runID
	run := func(ctx context.Context) int {
		if runtime.pools == nil || runtime.pools.Domain == nil {
			return writeError(stderr, "operator_backend_unavailable")
		}
		outcome, err := syncdispatchruntime.CancelSyncRun(ctx, runtime.pools.Domain, syncdispatchruntime.CancelSyncRunRequest{
			OrgID: orgID, SyncRunID: runID, ReasonCode: *mutation.reason,
			AllowRunning: *allowRunning, DryRun: *dryRun, Now: time.Now().UTC(),
		})
		if err != nil {
			return writeError(stderr, syncRunCancelErrorCode(err))
		}
		report := syncRunCancelReport{CancelSyncRunOutcome: outcome, DryRun: *dryRun}
		if outcome.Outcome == syncdispatchruntime.CancelOutcomeCancelled {
			report.RiverJobs = cancelRiverJobs(ctx, runtime.service, runtime.principal, outcome.RiverJobIDs, *mutation.reason, *mutation.correlation)
		}
		if code := writeResult(stdout, stderr, map[string]any{"sync_run_cancel": report}); code != 0 {
			return code
		}
		if report.RiverJobs.Failed > 0 {
			slog.Default().LogAttrs(ctx, slog.LevelWarn, "dho workers: sync run cancelled; some River jobs of its units were not cancelled",
				slog.String("correlation_id", *mutation.correlation), slog.Int("river_cancel_failed", report.RiverJobs.Failed))
			_, _ = fmt.Fprintf(stderr, "river_cancel_failed=%d\n", report.RiverJobs.Failed)
		}
		return 0
	}
	if *dryRun {
		return dryRunPreview(ctx, runtime, stderr, joboperator.ActionSyncRunCancel, "sync_run", resource, run)
	}
	return auditedWriteWith(ctx, runtime, stderr, mutation, joboperator.ActionSyncRunCancel, "sync_run", resource, !*orgStdin, run)
}

type syncRunCancelReport struct {
	syncdispatchruntime.CancelSyncRunOutcome
	DryRun    bool                `json:"dry_run"`
	RiverJobs riverJobCancelCount `json:"river_jobs"`
}

// riverJobCancelCount counts the River jobs of the cancelled units by what happened to them.
type riverJobCancelCount struct {
	Cancelled       int `json:"cancelled"`
	SkippedRunning  int `json:"skipped_running"`
	SkippedTerminal int `json:"skipped_terminal"`
	Failed          int `json:"cancel_failed"`
}

type riverJobCanceller interface {
	Inspect(ctx context.Context, principal joboperator.Principal, jobID int64) (joboperator.JobSummary, error)
	Cancel(ctx context.Context, principal joboperator.Principal, jobID int64, reasonCode, correlationID string) (joboperator.JobSummary, error)
}

// cancelRiverJobs cancels each waiting River job. It runs after the domain commit: the unit is
// failed and its run terminal, so nothing re-arms a cancelled job, and a job it misses only finds
// its unit unclaimable and retries as a no-op. A running job is left: its worker loses the lease.
func cancelRiverJobs(
	ctx context.Context, canceller riverJobCanceller, principal joboperator.Principal, ids []int64, reason, correlation string,
) riverJobCancelCount {
	var count riverJobCancelCount
	for _, id := range ids {
		job, err := canceller.Inspect(ctx, principal, id)
		if err != nil {
			count.Failed++
			continue
		}
		switch job.State {
		case joboperator.StateAvailable, joboperator.StatePending, joboperator.StateRetryable, joboperator.StateScheduled:
			if _, err := canceller.Cancel(ctx, principal, id, reason, correlation); err != nil {
				count.Failed++
				continue
			}
			count.Cancelled++
		case joboperator.StateRunning:
			count.SkippedRunning++
		default:
			count.SkippedTerminal++
		}
	}
	return count
}

func syncRunCancelErrorCode(err error) string {
	switch {
	case errors.Is(err, syncdispatchruntime.ErrCancelSyncRunInvalid):
		return "invalid_request"
	case errors.Is(err, syncdispatchruntime.ErrSyncRunNotFound):
		return "sync_run_not_found"
	case errors.Is(err, syncdispatchruntime.ErrSyncRunHasRunningUnit):
		return "sync_run_running_unit"
	case errors.Is(err, syncdispatchruntime.ErrSyncRunNotTerminal):
		return "sync_run_not_terminal"
	default:
		return "sync_run_cancel_unavailable"
	}
}
