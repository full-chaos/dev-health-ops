package main

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

type dailyWorkerComponent struct{ client *river.Client[pgx.Tx] }

func (component dailyWorkerComponent) Name() string { return "river-daily-metrics-worker" }
func (component dailyWorkerComponent) Start(ctx context.Context) error {
	return component.client.Start(ctx)
}
func (component dailyWorkerComponent) Shutdown(ctx context.Context) error {
	return component.client.Stop(ctx)
}

func buildDailyWorker(
	cfg config.Config,
	database workerDatabase,
	registry *jobruntime.Registry,
	observer jobruntime.Observer,
	logger *slog.Logger,
) (lifecycle.Component, []jobruntime.HandlerSpec, error) {
	if cfg.Profile != "heavy" || registry == nil {
		return nil, nil, nil
	}
	kinds := []string{
		jobcontract.KindDailyMetricsDispatch,
		jobcontract.KindDailyMetricsPartition,
		jobcontract.KindDailyMetricsFinalize,
	}
	specs := make([]jobruntime.HandlerSpec, 0, len(kinds))
	for _, kind := range kinds {
		descriptor, ok := registry.Descriptor(kind)
		if !ok {
			return nil, nil, errWorkerDependencyUnavailable
		}
		if descriptor.Executable() {
			specs = append(specs, descriptor)
		}
	}
	if len(specs) == 0 {
		return nil, nil, nil
	}
	if len(specs) != len(kinds) {
		return nil, nil, errWorkerDependencyUnavailable
	}
	postgresDatabase, ok := database.(*postgresWorkerDatabase)
	if !ok || postgresDatabase.pools == nil || observer == nil || logger == nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	store, err := daily.NewPostgresStore(postgresDatabase.pools.Domain)
	if err != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	publisher, err := daily.NewPostgresPublisher(postgresDatabase.pools.Domain, registry)
	if err != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	baseURL := strings.TrimRight(cfg.OperationalBridgeURL, "/")
	compatibility, err := daily.NewHTTPCompatibilityExecutor(
		&http.Client{Timeout: cfg.OperationalBridgeTimeout},
		daily.HTTPCompatibilityConfig{
			Endpoint:    baseURL + "/internal/worker/daily-metrics/v1/execute",
			BearerToken: cfg.OperationalBridgeToken.Reveal(),
		},
	)
	if err != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	idempotency, err := jobruntime.NewPostgresIdempotency(postgresDatabase.pools.Domain)
	if err != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	dependencies := jobruntime.Dependencies{
		Logger: logger, Observer: observer, TenantScope: operationalTenantScope{},
		Budget: newOperationalBudget(), Idempotency: idempotency,
	}
	workers := river.NewWorkers()
	registered := make([]jobruntime.HandlerSpec, 0, len(specs))
	for _, spec := range specs {
		switch spec.Kind {
		case jobcontract.KindDailyMetricsDispatch:
			handler, handlerErr := daily.NewDispatcher(store, publisher)
			if handlerErr != nil {
				return nil, nil, errWorkerDependencyUnavailable
			}
			adapter, adapterErr := jobruntime.NewAdapter[jobruntime.DailyMetricsDispatchArgs](
				registry, spec, handler, dependencies,
			)
			if adapterErr != nil || river.AddWorkerSafely(workers, adapter) != nil {
				return nil, nil, errWorkerDependencyUnavailable
			}
			registered = append(registered, adapter.Spec())
		case jobcontract.KindDailyMetricsPartition:
			handler, handlerErr := daily.NewPartitionHandler(store, publisher, compatibility)
			if handlerErr != nil {
				return nil, nil, errWorkerDependencyUnavailable
			}
			adapter, adapterErr := jobruntime.NewAdapter[jobruntime.DailyMetricsPartitionArgs](
				registry, spec, handler, dependencies,
			)
			if adapterErr != nil || river.AddWorkerSafely(workers, adapter) != nil {
				return nil, nil, errWorkerDependencyUnavailable
			}
			registered = append(registered, adapter.Spec())
		case jobcontract.KindDailyMetricsFinalize:
			handler, handlerErr := daily.NewFinalizeHandler(store, compatibility)
			if handlerErr != nil {
				return nil, nil, errWorkerDependencyUnavailable
			}
			adapter, adapterErr := jobruntime.NewAdapter[jobruntime.DailyMetricsFinalizeArgs](
				registry, spec, handler, dependencies,
			)
			if adapterErr != nil || river.AddWorkerSafely(workers, adapter) != nil {
				return nil, nil, errWorkerDependencyUnavailable
			}
			registered = append(registered, adapter.Spec())
		}
	}
	client, err := river.NewClient(
		riverpgxv5.New(postgresDatabase.pools.QueueControl),
		&river.Config{
			Logger: logger,
			Queues: map[string]river.QueueConfig{
				"metrics": {MaxWorkers: 2},
			},
			Schema:  cfg.RiverDatabaseSchema,
			Workers: workers,
		},
	)
	if err != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	return dailyWorkerComponent{client: client}, registered, nil
}
