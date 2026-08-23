package main

import (
	"context"
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/report"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/riverqueue/river"
)

const reportsQueue = "reports"

// buildReportWorker constructs the production report runtime. Before CUT-03 no
// binary constructed internal/jobs/report at all, so both report kinds were
// advertised by a compiled-kind list while nothing could execute them. The
// adapters are built here or the selected reports queue does not become ready.
func buildReportWorker(
	ctx context.Context,
	cfg config.Config,
	database workerDatabase,
	registry *jobruntime.Registry,
	observer jobruntime.Observer,
	logger *slog.Logger,
	workers *river.Workers,
) (workerFamily, error) {
	if !queueSelected(cfg.Queues, reportsQueue) || registry == nil {
		return workerFamily{}, nil
	}
	if workers == nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	kinds := []string{
		jobcontract.KindReportExecuteOnDemand,
		jobcontract.KindReportExecuteScheduled,
	}
	executable := 0
	for _, kind := range kinds {
		descriptor, ok := registry.Descriptor(kind)
		if !ok || descriptor.Queue != reportsQueue {
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		if descriptor.Executable() {
			executable++
		}
	}
	if executable == 0 {
		return workerFamily{}, nil
	}
	// On-demand and scheduled reports share one artifact store, one query
	// allowlist, and one notification lease. Consuming one route while the
	// other stays on Celery would let two runtimes race the same report_runs
	// row, so the pair is promoted together or not at all.
	if executable != len(kinds) {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	postgresDatabase, ok := database.(*postgresWorkerDatabase)
	if !ok || postgresDatabase.pools == nil || observer == nil || logger == nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	if !cfg.ClickHouseURI.Configured() {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	clickhouseConnection, err := clickhousestore.Open(
		ctx, clickhousestore.DefaultConfig(cfg.ClickHouseURI.Reveal()),
	)
	if err != nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	closeClickHouse := func() { _ = clickhouseConnection.Close() }

	idempotency, err := newOperationalIdempotency(postgresDatabase.pools.Domain, observer)
	if err != nil {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	adapters, err := report.NewProductionRuntimeAdapters(
		registry,
		postgresDatabase.pools.Domain,
		clickhouseConnection,
		jobruntime.Dependencies{
			Logger: logger, Observer: observer, TenantScope: operationalTenantScope{},
			Budget: newOperationalBudget(postgresDatabase.pools.Domain, observer), Idempotency: idempotency,
		},
	)
	if err != nil {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	if err := adapters.Register(workers); err != nil {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	specs, err := adapters.Specs()
	if err != nil || len(specs) != len(kinds) {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	return workerFamily{
		handlers: specs,
		queues: selectedQueueBudgets(
			cfg.Queues, []string{reportsQueue}, cfg.WorkerQueueConcurrency,
		),
		cleanups: []func() error{clickhouseConnection.Close},
	}, nil
}
