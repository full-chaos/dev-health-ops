package main

import (
	"context"
	"errors"
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

type riverWorkerProcess struct {
	client *river.Client[pgx.Tx]
	family workerFamily
}

func (riverWorkerProcess) Name() string { return "river-worker" }

func (process riverWorkerProcess) Start(ctx context.Context) error {
	if process.client == nil {
		return errWorkerDependencyUnavailable
	}
	return process.client.Start(ctx)
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
	return riverWorkerProcess{client: client, family: family}, nil
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
	}
}
