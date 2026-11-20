package workersctl

import (
	"context"
	"errors"
	"io"
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
// organization through syncdispatchruntime.CancelSyncRun on the domain pool, audited as jobs.cancel
// on resource_type sync_run (a repeat call on a closed run writes its own audit pair and no domain
// row). It cancels no River job: a queued provider-unit job of a cancelled unit retries
// (attempts 1-4) and River discards it on attempt 5; it writes no domain row.
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
		return writeResult(stdout, stderr, map[string]any{"sync_run_cancel": syncRunCancelReport{CancelSyncRunOutcome: outcome, DryRun: *dryRun}})
	}
	if *dryRun {
		return dryRunPreview(ctx, runtime, stderr, joboperator.ActionSyncRunCancel, "sync_run", resource, run)
	}
	return auditedWriteWith(ctx, runtime, stderr, mutation, joboperator.ActionSyncRunCancel, "sync_run", resource, !*orgStdin, run)
}

type syncRunCancelReport struct {
	syncdispatchruntime.CancelSyncRunOutcome
	DryRun bool `json:"dry_run"`
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
