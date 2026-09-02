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
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/riverqueue/river"
)

func buildWorkgraphWorker(cfg config.Config, database workerDatabase, registry *jobruntime.Registry, observer jobruntime.Observer, logger *slog.Logger, workers *river.Workers) (workerFamily, error) {
	if !anyQueueSelected(cfg.Queues, "workgraph", "investment") || registry == nil {
		return workerFamily{}, nil
	}
	if workers == nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	kinds := []string{jobcontract.KindWorkGraphBuild, jobcontract.KindInvestmentMaterialize, jobcontract.KindInvestmentDispatch, jobcontract.KindInvestmentChunk, jobcontract.KindInvestmentFinalize}
	specs := make([]jobruntime.HandlerSpec, 0, len(kinds))
	for _, kind := range kinds {
		descriptor, ok := registry.Descriptor(kind)
		if !ok {
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		if queueSelected(cfg.Queues, descriptor.Queue) && descriptor.Executable() {
			specs = append(specs, descriptor)
		}
	}
	if len(specs) == 0 {
		return workerFamily{}, nil
	}
	postgresDatabase, ok := database.(*postgresWorkerDatabase)
	if !ok || postgresDatabase.pools == nil || observer == nil || logger == nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	// The work-graph store reports a release-lost lease directly: generic
	// middleware cannot tell that outcome apart from an ordinary release, and
	// only the store that ran the fenced UPDATE knows it matched zero rows
	// because the lease had already expired (CHAOS-4002).
	var leaseObservers []jobruntime.WorkGraphLeaseObserver
	if leaseObserver, ok := observer.(jobruntime.WorkGraphLeaseObserver); ok {
		leaseObservers = append(leaseObservers, leaseObserver)
	}
	store, err := workgraph.NewPostgresStore(postgresDatabase.pools.Domain, leaseObservers...)
	if err != nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	compatibility, err := workgraph.NewHTTPCompatibilityExecutor(
		workgraphCompatibilityHTTPClient(cfg.OperationalBridgeTimeout),
		workgraph.HTTPCompatibilityConfig{
			Endpoint:              strings.TrimRight(cfg.OperationalBridgeURL, "/") + "/internal/worker/workgraph/v1/execute",
			BearerToken:           cfg.OperationalBridgeToken.Reveal(),
			AllowInsecureInternal: cfg.OperationalBridgeAllowInsecure,
		},
	)
	if err != nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	idempotency, err := newOperationalIdempotency(postgresDatabase.pools.Domain, observer)
	if err != nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	dependencies := jobruntime.Dependencies{Logger: logger, Observer: observer, TenantScope: operationalTenantScope{}, Budget: newOperationalBudget(postgresDatabase.pools.Domain, observer), Idempotency: idempotency}

	buildPreSteps, preStepErr := workgraphBuildPreSteps(cfg, specs, observer, logger)
	if preStepErr != nil {
		return workerFamily{}, preStepErr
	}

	registered := make([]jobruntime.HandlerSpec, 0, len(specs))
	for _, spec := range specs {
		if err := addWorkgraphWorker(workers, registry, spec, store, compatibility, dependencies, buildPreSteps); err != nil {
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		registered = append(registered, spec)
	}
	budgets := selectedQueueBudgets(
		cfg.Queues, []string{"workgraph", "investment"}, cfg.WorkerQueueConcurrency,
	)
	return workerFamily{
		handlers: registered,
		queues:   budgets,
	}, nil
}

func workgraphCompatibilityHTTPClient(connectTimeout time.Duration) *http.Client {
	// Work-graph and investment handler contracts have substantially different
	// execution budgets. The River execution context is the authoritative
	// deadline; the shared 30-second operational bridge timeout would abort a
	// healthy synchronous investment materialization.
	return contractDeadlineHTTPClient(connectTimeout)
}

// workgraphBuildPreSteps constructs the native Go producers that run inside a
// build execution before the Python bridge. Returns nil when no build kind is
// selected for this worker, so a workgraph process running only the investment
// kinds takes no ClickHouse dependency it does not need.
//
// A ClickHouse failure REFUSES the family rather than registering the build
// handler without its pre-step. Once the mapping is produced natively, a build
// that skipped it would still succeed and still write edges -- just an edge set
// missing every provider-attached issue-PR link. A wrong answer that looks
// healthy is worse than a family that will not start, and `newHandler` refuses
// a nil step for the same reason.
func workgraphBuildPreSteps(
	cfg config.Config, specs []jobruntime.HandlerSpec, observer jobruntime.Observer, logger *slog.Logger,
) ([]workgraph.NativePreStep, error) {
	buildSelected := false
	for _, spec := range specs {
		if spec.Kind == jobcontract.KindWorkGraphBuild {
			buildSelected = true
			break
		}
	}
	if !buildSelected {
		return nil, nil
	}

	connection, connectionErr := clickhousestore.Open(
		context.Background(), clickhousestore.DefaultConfig(cfg.ClickHouseURI.Reveal()),
	)
	if connectionErr != nil {
		return nil, errWorkerDependencyUnavailable
	}
	loader, loaderErr := issueprlinks.NewLoader(connection)
	if loaderErr != nil {
		return nil, errWorkerDependencyUnavailable
	}
	writer, writerErr := issueprlinks.NewWriter(connection)
	if writerErr != nil {
		return nil, errWorkerDependencyUnavailable
	}
	service, serviceErr := issueprlinks.NewService(loader, writer, logger)
	if serviceErr != nil {
		return nil, errWorkerDependencyUnavailable
	}
	if candidate, ok := observer.(issueprlinks.Observer); ok {
		service.SetObserver(candidate)
	}
	step, stepErr := newIssuePRLinksPreStep(service)
	if stepErr != nil {
		return nil, errWorkerDependencyUnavailable
	}
	return []workgraph.NativePreStep{step}, nil
}

func addWorkgraphWorker(workers *river.Workers, registry *jobruntime.Registry, spec jobruntime.HandlerSpec, store workgraph.Store, executor workgraph.CompatibilityExecutor, dependencies jobruntime.Dependencies, buildPreSteps []workgraph.NativePreStep) error {
	switch spec.Kind {
	case jobcontract.KindWorkGraphBuild:
		h, err := workgraph.NewBuildHandler(store, executor, buildPreSteps...)
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
