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
		&river.Config{
			ID:      cfg.WorkerInstanceID,
			Logger:  logger,
			Queues:  queues,
			Schema:  cfg.RiverDatabaseSchema,
			Workers: workers,
		},
	)
	if err != nil {
		return nil, errWorkerDependencyUnavailable
	}
	return riverWorkerProcess{client: client, family: family}, nil
}
