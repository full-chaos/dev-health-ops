// replay_external_recompute.go: `dev-health-workerctl external-recompute replay`
// (CHAOS-5296).
//
// The native drain (internal/externalrecompute/drain.go) only ever claims rows
// addressed to NativeDrainTaskName. Every row written between 2026-08-19, when
// Celery stopped, and the deploy of that drain is addressed to the retired
// Celery task name instead and is therefore unreachable from it -- on purpose,
// so that deploying the fix cannot replay ~2.5 weeks of backlog as a startup
// side effect.
//
// This command is the deliberate, operator-run alternative. It is NOT wired
// into startup, has no schedule, and collapses the backlog to one widest plan
// per (org, source system, source instance) rather than replaying every row.
//
// Start with --dry-run: it prints the identical per-group report the real run
// prints, minus the writes.

package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/externalrecompute"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
)

// defaultReplayLimit bounds one invocation. It is generous because the backlog
// this exists for is a fixed, historical set rather than a stream, but it is
// still bounded: an unbounded read of a table nobody has pruned in weeks is how
// a recovery command becomes its own incident.
const defaultReplayLimit = 5000

func dispatchExternalRecompute(
	ctx context.Context,
	runtime *operatorRuntime,
	args []string,
	stdout, stderr io.Writer,
) int {
	if len(args) == 0 {
		return writeError(stderr, "invalid_request")
	}
	switch args[0] {
	case "replay":
		return dispatchExternalRecomputeReplay(ctx, runtime, args[1:], stdout, stderr)
	default:
		return writeError(stderr, "invalid_request")
	}
}

func dispatchExternalRecomputeReplay(
	ctx context.Context,
	runtime *operatorRuntime,
	args []string,
	stdout, stderr io.Writer,
) int {
	flags := quietFlags("external-recompute replay")
	reviewEvidence := flags.String(
		"review-evidence", "",
		`REQUIRED: why this backlog is being replayed (e.g. "CHAOS-5296 -- one-shot drain of the rows written while Celery was stopped")`,
	)
	dryRun := flags.Bool("dry-run", false,
		"print the collapsed plan per (org, source, instance) with row counts and windows, without writing anything")
	limit := flags.Int("limit", defaultReplayLimit, "maximum backlog rows to read in one invocation")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return writeError(stderr, "invalid_request")
	}
	if strings.TrimSpace(*reviewEvidence) == "" || *limit < 1 {
		return writeError(stderr, "invalid_request")
	}
	if runtime == nil || runtime.pools == nil || runtime.pools.Domain == nil || runtime.registry == nil {
		return writeError(stderr, "operator_backend_unavailable")
	}

	// Each construction failure names WHICH collaborator failed and carries the
	// cause. The stderr code stays the bounded `operator_backend_unavailable`
	// the CLI contract specifies, but an operator who gets it has a log line
	// telling them whether the registry, the pool or the writer was the
	// problem -- before this, all four collapsed to one indistinguishable code
	// with the cause discarded (r1 P2).
	dailyStore, err := daily.NewPostgresStore(runtime.pools.Domain)
	if err != nil {
		return externalRecomputeBackendUnavailable(ctx, stderr, "daily_store", err)
	}
	dailyPublisher, err := daily.NewPostgresPublisher(runtime.pools.Domain, runtime.registry)
	if err != nil {
		return externalRecomputeBackendUnavailable(ctx, stderr, "daily_publisher", err)
	}
	workGraph, err := workgraph.NewRequestWriter(runtime.registry)
	if err != nil {
		return externalRecomputeBackendUnavailable(ctx, stderr, "work_graph_writer", err)
	}
	// The SAME enqueue seam the live drain uses. Constructing a second,
	// command-local one here is what would let a replay write work shaped
	// differently from the work the drain writes.
	enqueuer, err := externalrecompute.NewPostgresEnqueuer(dailyStore, dailyPublisher, workGraph)
	if err != nil {
		return externalRecomputeBackendUnavailable(ctx, stderr, "enqueuer", err)
	}

	report, err := externalrecompute.Replay(
		ctx,
		runtime.pools.Domain,
		enqueuer,
		time.Now().UTC(),
		*limit,
		*dryRun,
	)
	if err != nil {
		slog.Default().LogAttrs(ctx, slog.LevelError,
			"workerctl external recompute replay: failed",
			slog.Bool("dry_run", *dryRun), slog.Any("error", err))
		return writeError(stderr, "operator_backend_unavailable")
	}
	// Logged as well as printed: the JSON on stdout is for the operator running
	// it, this line is what a later incident review finds. Both carry the same
	// counts, and the review evidence is on the log line only -- it is why the
	// command was run, not part of its result.
	level := slog.LevelInfo
	if report.Incomplete() {
		level = slog.LevelError
	}
	slog.Default().LogAttrs(ctx, level,
		"workerctl external recompute replay: complete",
		slog.Bool("dry_run", report.DryRun),
		slog.Int("rows", report.Rows),
		slog.Int("scopeless_rows", report.ScopelessRows),
		slog.Int("unreadable_rows", report.UnreadableRows),
		slog.Int("groups", len(report.Groups)),
		slog.Int("rows_retired", report.Retired),
		slog.Int("groups_failed", report.Failed),
		slog.Bool("incomplete", report.Incomplete()),
		slog.String("review_evidence", *reviewEvidence))
	if code := writeResult(stdout, stderr, map[string]any{
		"dry_run":         report.DryRun,
		"rows":            report.Rows,
		"scopeless_rows":  report.ScopelessRows,
		"unreadable_rows": report.UnreadableRows,
		"groups":          report.Groups,
		"rows_retired":    report.Retired,
		"groups_failed":   report.Failed,
		"incomplete":      report.Incomplete(),
		"review_evidence": *reviewEvidence,
	}); code != 0 {
		return code
	}
	// A run that failed a group or could not read a row left work behind. The
	// report is still printed -- the operator needs to see WHICH grains are
	// outstanding -- but the exit code must not say success, or an incomplete
	// drain reads as a finished one to a script, a runbook step, or a tired
	// human at 2am (r1 P2). A dry run is never incomplete in this sense: it
	// deliberately does no work.
	if !report.DryRun && report.Incomplete() {
		return 1
	}
	return 0
}

func externalRecomputeBackendUnavailable(
	ctx context.Context, stderr io.Writer, stage string, cause error,
) int {
	slog.Default().LogAttrs(ctx, slog.LevelError,
		"workerctl external recompute replay: backend unavailable",
		slog.String("stage", stage), slog.Any("error", cause))
	return writeError(stderr, "operator_backend_unavailable")
}
