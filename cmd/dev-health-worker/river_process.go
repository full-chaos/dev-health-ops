package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
	"github.com/riverqueue/rivercontrib/otelriver"
)

type riverWorkerProcess struct {
	client *river.Client[pgx.Tx]
	family workerFamily
	logger *slog.Logger
	// budget bounds how long Start retries a producer start that keeps
	// failing only on River's own fixed internal StartWorkContext deadline
	// (see Start's doc comment). Reuses cfg.PreclaimReadinessTimeout -- the
	// same roll-storm retry budget operators already tune for preclaim
	// readiness -- rather than a second knob that could drift out of sync
	// with it.
	budget time.Duration
	// now and sleep are overridden by tests so the retry loop can be driven
	// deterministically without real wall-clock waits, the same seam
	// preclaimReadinessComponent uses. Left nil, Start uses time.Now and a
	// context-aware time.Sleep.
	now   func() time.Time
	sleep func(context.Context, time.Duration)
	// startClient is process.client.Start, indirected so a test can drive the
	// retry loop with a fake that fails the way River's real producer start
	// does under a roll storm (context.DeadlineExceeded from the fixed 10s
	// StartWorkContext budget) without a live database. Left nil, Start
	// calls process.client.Start.
	startClient func(context.Context) error
}

func (riverWorkerProcess) Name() string { return "river-worker" }

// Start starts River's producers and, while the only failure is River's own
// fixed internal StartWorkContext deadline firing, retries with backoff until
// either a start succeeds or the retry budget is spent. producer.StartWork
// Context (riverqueue/river) gives fetching a queue's settings a hardcoded 10
// second budget with no retry of its own; a fleet-wide roll that restarts
// every worker group's Deployments at once can push Postgres and its pooler
// past that budget for as long as it takes the resulting connection burst to
// settle, and exiting on the very first such deadline turns an ordinary
// rollout into a crash loop. River's own startstop bookkeeping tolerates a
// failed Start being retried (a fresh attempt resets the service's run state
// -- the same in-process retry path preclaim readiness already relies on).
// A genuine, non-timeout start error is never confused for this and still
// exits on the first attempt.
func (process riverWorkerProcess) Start(ctx context.Context) error {
	if process.client == nil {
		return errWorkerDependencyUnavailable
	}
	startClient := process.startClient
	if startClient == nil {
		startClient = process.client.Start
	}
	return startupRetryBudget(
		ctx, process.budget, process.now, process.sleep,
		startClient,
		isRiverProducerStartTimeout,
		func(attempt int, elapsed, wait time.Duration, _ error) {
			process.logRetry(ctx, attempt, elapsed, wait)
		},
		func(attempt int, elapsed time.Duration, _ error, budgetExhausted bool) {
			reason := "river_producer_start_failed"
			if budgetExhausted {
				reason = "retry_budget_exhausted"
			}
			process.logGiveUp(ctx, reason, attempt, elapsed)
		},
	)
}

// isRiverProducerStartTimeout reports whether err is River's own
// producer.StartWorkContext deadline firing (a plain context.DeadlineExceeded,
// per riverqueue/river/rivershared/util/timeoututil.WithTimeoutV) rather than a
// genuine start error such as a rejected DSN or a misconfigured queue.
func isRiverProducerStartTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded)
}

// logRetry reports one non-terminal attempt at starting River's producers:
// the failure was River's own fixed internal start deadline, not a genuine
// error, and the retry budget is not yet spent. Logged at warn, the same
// level preclaim readiness retries at, because this is the expected shape of
// a roll storm, not something an operator needs to act on unless it keeps
// recurring until the budget runs out.
func (process riverWorkerProcess) logRetry(ctx context.Context, attempt int, elapsed, wait time.Duration) {
	if process.logger == nil {
		return
	}
	process.logger.WarnContext(ctx, "river workers start timed out, retrying",
		"error_category", "dependency_unavailable",
		"attempt", attempt,
		"elapsed", elapsed.String(),
		"retry_in", wait.String(),
	)
}

// logGiveUp reports the terminal failure: either a genuine start error or a
// retryable timeout whose budget ran out. Start returns an error either way,
// so the process exits and is restarted; this is the one place the attempt
// count and elapsed time are recorded before that happens.
func (process riverWorkerProcess) logGiveUp(ctx context.Context, reason string, attempt int, elapsed time.Duration) {
	if process.logger == nil {
		return
	}
	process.logger.ErrorContext(ctx, "river workers start refused",
		"error_category", "dependency_unavailable",
		"reason", reason,
		"attempts", attempt,
		"elapsed", elapsed.String(),
	)
}

func (process riverWorkerProcess) Shutdown(ctx context.Context) error {
	var stopErr error
	if process.client != nil {
		stopErr = process.client.Stop(ctx)
	}
	return errors.Join(stopErr, closeWorkerFamily(process.family))
}

func newRiverWorkerProcess(
	cfg config.Config,
	database workerDatabase,
	workers *river.Workers,
	family workerFamily,
	logger *slog.Logger,
) (lifecycle.Component, error) {
	postgresDatabase, ok := database.(*postgresWorkerDatabase)
	if !ok || postgresDatabase.pools == nil || postgresDatabase.pools.QueueControl == nil ||
		workers == nil || logger == nil || cfg.WorkerInstanceID == "" || len(family.queues) == 0 {
		return nil, errWorkerDependencyUnavailable
	}

	queues := make(map[string]river.QueueConfig, len(family.queues))
	for _, budget := range family.queues {
		if budget.Queue == "" || budget.MaxWorkers <= 0 {
			return nil, errWorkerDependencyUnavailable
		}
		if _, duplicate := queues[budget.Queue]; duplicate {
			return nil, errWorkerDependencyUnavailable
		}
		queues[budget.Queue] = river.QueueConfig{MaxWorkers: budget.MaxWorkers}
	}
	client, err := river.NewClient(
		riverpgxv5.New(postgresDatabase.pools.QueueControl),
		riverWorkerClientConfig(cfg, queues, workers, logger),
	)
	if err != nil {
		return nil, errWorkerDependencyUnavailable
	}
	return riverWorkerProcess{
		client: client,
		family: family,
		logger: logger,
		// PreclaimReadinessTimeout is already floored at HealthCheckTimeout
		// and bounded to [minimumPreclaimReadinessTimeout,
		// maximumPreclaimReadinessTimeout] by config.Load, so reusing it here
		// gives producer start the same validated, operator-tunable budget
		// preclaim readiness uses rather than a second knob that could drift
		// out of sync with it.
		budget: cfg.PreclaimReadinessTimeout,
	}, nil
}

// reindexDisabled is River's documented way to schedule no reindex work:
// river.Config.ReindexerIndexNames is "the exact list of indexes to reindex",
// and a non-nil EMPTY list means there is nothing to rebuild. Leaving it nil
// selects river.ReindexerIndexNamesDefault() instead, which is what produced
// seven `maintenance.Reindexer: Error reindexing ... permission denied`
// (SQLSTATE 42501) ERROR lines every midnight UTC from every worker process
// that runs maintenance services (CHAOS-3939).
//
// DECISION -- do not re-enable this by widening the runtime role. REINDEX needs
// either ownership of the index or, on PostgreSQL 16+, the MAINTAIN privilege,
// and this deployment's own readiness checks refuse BOTH by construction:
//
//   - Ownership: rolePostureQuery asserts the runtime identity owns nothing at
//     all -- NOT EXISTS (SELECT 1 FROM pg_class WHERE relowner = <this role>
//     AND relkind IN ('r','p','v','m','f','S')) in
//     internal/storage/postgres/domain_authorization.go. Making the role own
//     river_job so REINDEX succeeds makes that predicate false, so CheckRolePosture
//     starts FAILING rather than passing.
//   - MAINTAIN: queueRolePostureQuery asserts the queue role holds SELECT,
//     INSERT, UPDATE and DELETE on every River table and NOT MAINTAIN, guarded
//     on server_version_num >= 170000
//     (internal/storage/postgres/queue_authorization.go). This deployment runs
//     PostgreSQL 18, so that branch is live and an added MAINTAIN grant reads
//     as excess privilege -- again failing readiness.
//
// Either route would therefore need a posture change, and the derived GRANTs
// ship in the dev-hops-runner image (docker/Dockerfile), NOT the Go worker
// image, so a posture edit not accompanied by a runner rebuild breaks
// production worse than the noise it removes. Reclaiming river_job index bloat
// belongs in a privileged one-shot alongside the migration path, which already
// runs as an owner-capable role.
func reindexDisabled() []string { return []string{} }

// riverWorkerClientConfig is the exact River client configuration the worker
// process runs with. It is a named function so a test can assert the shipped
// configuration's real behaviour against a real database, instead of asserting
// against a look-alike config that could drift from this one.
func riverWorkerClientConfig(
	cfg config.Config,
	queues map[string]river.QueueConfig,
	workers *river.Workers,
	logger *slog.Logger,
) *river.Config {
	return &river.Config{
		ID:                  cfg.WorkerInstanceID,
		Logger:              logger,
		Queues:              queues,
		ReindexerIndexNames: reindexDisabled(),
		Schema:              cfg.RiverDatabaseSchema,
		Workers:             workers,
		// otelriver emits the baseline river.work span (kind, queue, status) for
		// every claimed job, picking up the global tracer provider tracing.Init
		// installs at process start (a no-op TracerProvider when tracing is
		// disabled, so this is safe with or without OTEL_ENABLED). CHAOS-3993.
		Middleware: []rivertype.Middleware{otelriver.NewMiddleware(nil)},
	}
}
