package main

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

type workgraphWorkerComponent struct{ client *river.Client[pgx.Tx] }

func (component workgraphWorkerComponent) Name() string { return "river-workgraph-worker" }
func (component workgraphWorkerComponent) Start(ctx context.Context) error {
	return component.client.Start(ctx)
}
func (component workgraphWorkerComponent) Shutdown(ctx context.Context) error {
	return component.client.Stop(ctx)
}

func buildWorkgraphWorker(cfg config.Config, database workerDatabase, registry *jobruntime.Registry, observer jobruntime.Observer, logger *slog.Logger) (lifecycle.Component, []jobruntime.HandlerSpec, error) {
	if cfg.Profile != "heavy" || registry == nil {
		return nil, nil, nil
	}
	kinds := []string{jobcontract.KindWorkGraphBuild, jobcontract.KindInvestmentMaterialize, jobcontract.KindInvestmentDispatch, jobcontract.KindInvestmentChunk, jobcontract.KindInvestmentFinalize}
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
	store, err := workgraph.NewPostgresStore(postgresDatabase.pools.Domain)
	if err != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	compatibility, err := workgraph.NewHTTPCompatibilityExecutor(
		workgraphCompatibilityHTTPClient(cfg.OperationalBridgeTimeout),
		workgraph.HTTPCompatibilityConfig{
			Endpoint:    strings.TrimRight(cfg.OperationalBridgeURL, "/") + "/internal/worker/workgraph/v1/execute",
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
	dependencies := jobruntime.Dependencies{Logger: logger, Observer: observer, TenantScope: operationalTenantScope{}, Budget: newOperationalBudget(), Idempotency: idempotency}
	workers := river.NewWorkers()
	registered := make([]jobruntime.HandlerSpec, 0, len(specs))
	for _, spec := range specs {
		if err := addWorkgraphWorker(workers, registry, spec, store, compatibility, dependencies); err != nil {
			return nil, nil, errWorkerDependencyUnavailable
		}
		registered = append(registered, spec)
	}
	client, err := river.NewClient(riverpgxv5.New(postgresDatabase.pools.QueueControl), &river.Config{Logger: logger, Queues: map[string]river.QueueConfig{"workgraph": {MaxWorkers: 1}, "investment": {MaxWorkers: 1}}, Schema: cfg.RiverDatabaseSchema, Workers: workers})
	if err != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	return workgraphWorkerComponent{client: client}, registered, nil
}

func workgraphCompatibilityHTTPClient(connectTimeout time.Duration) *http.Client {
	// Work-graph and investment handler contracts have substantially different
	// execution budgets. The River execution context is the authoritative
	// deadline; the shared 30-second operational bridge timeout would abort a
	// healthy synchronous investment materialization.
	return contractDeadlineHTTPClient(connectTimeout)
}

func addWorkgraphWorker(workers *river.Workers, registry *jobruntime.Registry, spec jobruntime.HandlerSpec, store workgraph.Store, executor workgraph.CompatibilityExecutor, dependencies jobruntime.Dependencies) error {
	switch spec.Kind {
	case jobcontract.KindWorkGraphBuild:
		h, err := workgraph.NewBuildHandler(store, executor)
		if err != nil {
			return err
		}
		a, err := jobruntime.NewAdapter[jobruntime.WorkGraphBuildArgs](registry, spec, h, dependencies)
		if err != nil {
			return err
		}
		return river.AddWorkerSafely(workers, a)
	case jobcontract.KindInvestmentMaterialize:
		h, err := workgraph.NewMaterializeHandler(store, executor)
		if err != nil {
			return err
		}
		a, err := jobruntime.NewAdapter[jobruntime.InvestmentMaterializeArgs](registry, spec, h, dependencies)
		if err != nil {
			return err
		}
		return river.AddWorkerSafely(workers, a)
	case jobcontract.KindInvestmentDispatch:
		h, err := workgraph.NewDispatchHandler(store, executor)
		if err != nil {
			return err
		}
		a, err := jobruntime.NewAdapter[jobruntime.InvestmentDispatchArgs](registry, spec, h, dependencies)
		if err != nil {
			return err
		}
		return river.AddWorkerSafely(workers, a)
	case jobcontract.KindInvestmentChunk:
		h, err := workgraph.NewChunkHandler(store, executor)
		if err != nil {
			return err
		}
		a, err := jobruntime.NewAdapter[jobruntime.InvestmentChunkArgs](registry, spec, h, dependencies)
		if err != nil {
			return err
		}
		return river.AddWorkerSafely(workers, a)
	case jobcontract.KindInvestmentFinalize:
		h, err := workgraph.NewFinalizeHandler(store, executor)
		if err != nil {
			return err
		}
		a, err := jobruntime.NewAdapter[jobruntime.InvestmentFinalizeArgs](registry, spec, h, dependencies)
		if err != nil {
			return err
		}
		return river.AddWorkerSafely(workers, a)
	default:
		return errWorkerDependencyUnavailable
	}
}
