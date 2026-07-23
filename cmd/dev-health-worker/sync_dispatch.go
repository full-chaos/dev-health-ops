package main

import (
	"context"
	"log/slog"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

// The client type includes the driver's transaction type, but lifecycle only
// needs Start and Stop. Keep the component concrete below to avoid exposing a
// broad worker runtime interface.
type syncCoordinatorLifecycle struct {
	startStop interface {
		Start(context.Context) error
		Stop(context.Context) error
	}
}

func (component syncCoordinatorLifecycle) Name() string { return "river-sync-coordinator-worker" }
func (component syncCoordinatorLifecycle) Start(ctx context.Context) error {
	return component.startStop.Start(ctx)
}
func (component syncCoordinatorLifecycle) Shutdown(ctx context.Context) error {
	return component.startStop.Stop(ctx)
}

func buildSyncCoordinatorWorker(
	cfg config.Config,
	database workerDatabase,
	logger *slog.Logger,
) (lifecycle.Component, error) {
	if cfg.Profile != "sync" {
		return nil, nil
	}
	postgresDatabase, ok := database.(*postgresWorkerDatabase)
	if !ok || postgresDatabase.pools == nil || logger == nil {
		return nil, errWorkerDependencyUnavailable
	}
	bridge, err := syncdispatchruntime.NewHTTPBridge(syncdispatchruntime.HTTPBridgeConfig{
		BaseURL:       strings.TrimRight(cfg.OperationalBridgeURL, "/"),
		BearerToken:   cfg.OperationalBridgeToken.Reveal(),
		Timeout:       cfg.OperationalBridgeTimeout,
		AllowInsecure: cfg.OperationalBridgeAllowInsecure,
	})
	if err != nil {
		return nil, errWorkerDependencyUnavailable
	}
	workers := river.NewWorkers()
	if err := syncdispatchruntime.RegisterWorkers(workers, bridge); err != nil {
		return nil, errWorkerDependencyUnavailable
	}
	client, err := river.NewClient(riverpgxv5.New(postgresDatabase.pools.QueueControl), &river.Config{
		Logger: logger,
		Queues: map[string]river.QueueConfig{
			"sync": {MaxWorkers: 4},
		},
		Schema:  cfg.RiverDatabaseSchema,
		Workers: workers,
	})
	if err != nil {
		return nil, errWorkerDependencyUnavailable
	}
	return syncCoordinatorLifecycle{startStop: client}, nil
}
