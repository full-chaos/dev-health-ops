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

	dailyStore, err := daily.NewPostgresStore(runtime.pools.Domain)
	if err != nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	dailyPublisher, err := daily.NewPostgresPublisher(runtime.pools.Domain, runtime.registry)
	if err != nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	workGraph, err := workgraph.NewRequestWriter(runtime.registry)
	if err != nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	// The SAME enqueue seam the live drain uses. Constructing a second,
	// command-local one here is what would let a replay write work shaped
	// differently from the work the drain writes.
	enqueuer, err := externalrecompute.NewPostgresEnqueuer(dailyStore, dailyPublisher, workGraph)
	if err != nil {
		return writeError(stderr, "operator_backend_unavailable")
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
	slog.Default().LogAttrs(ctx, slog.LevelInfo,
		"workerctl external recompute replay: complete",
		slog.Bool("dry_run", report.DryRun),
		slog.Int("rows", report.Rows),
		slog.Int("scopeless_rows", report.ScopelessRows),
		slog.Int("groups", len(report.Groups)),
		slog.Int("rows_retired", report.Retired),
		slog.Int("groups_failed", report.Failed),
		slog.String("review_evidence", *reviewEvidence))
	return writeResult(stdout, stderr, map[string]any{
		"dry_run":         report.DryRun,
		"rows":            report.Rows,
		"scopeless_rows":  report.ScopelessRows,
		"groups":          report.Groups,
		"rows_retired":    report.Retired,
		"groups_failed":   report.Failed,
		"review_evidence": *reviewEvidence,
	})
}
