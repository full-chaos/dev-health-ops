package main

import (
	"context"
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/report"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

// reportsQueue and its worker budget must match the deployment manifest entry
// for the heavy process; exact startup validation compares the two.
const (
	reportsQueue        = "reports"
	reportsQueueWorkers = 2
)

type reportWorkerComponent struct {
	client     *river.Client[pgx.Tx]
	clickhouse driver.Conn
}

func (component reportWorkerComponent) Name() string { return "river-report-worker" }

func (component reportWorkerComponent) Start(ctx context.Context) error {
	return component.client.Start(ctx)
}

// Shutdown stops fetching before releasing the ClickHouse connection: a report
// still rendering must keep its query connection until River drains it.
func (component reportWorkerComponent) Shutdown(ctx context.Context) error {
	err := component.client.Stop(ctx)
	if component.clickhouse != nil {
		if closeErr := component.clickhouse.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}

// buildReportWorker constructs the production report runtime. Before CUT-03 no
// binary constructed internal/jobs/report at all, so both report kinds were
// advertised by a compiled-kind list while nothing could execute them. The
// adapters are built here or the heavy profile does not become ready.
func buildReportWorker(
	ctx context.Context,
	cfg config.Config,
	database workerDatabase,
	registry *jobruntime.Registry,
	observer jobruntime.Observer,
	logger *slog.Logger,
) (workerFamily, error) {
	if cfg.Profile != "heavy" || registry == nil {
		return workerFamily{}, nil
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

	idempotency, err := jobruntime.NewPostgresIdempotency(postgresDatabase.pools.Domain)
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
	workers := river.NewWorkers()
	if err := adapters.Register(workers); err != nil {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	specs, err := adapters.Specs()
	if err != nil || len(specs) != len(kinds) {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	if err := registerRescueCoverage(workers, registry, specs); err != nil {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	client, err := river.NewClient(
		riverpgxv5.New(postgresDatabase.pools.QueueControl),
		&river.Config{
			Logger: logger,
			Queues: map[string]river.QueueConfig{
				reportsQueue: {MaxWorkers: reportsQueueWorkers},
			},
			Schema:  cfg.RiverDatabaseSchema,
			Workers: workers,
		},
	)
	if err != nil {
		closeClickHouse()
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	return workerFamily{
		component: reportWorkerComponent{
			client: client, clickhouse: clickhouseConnection,
		},
		handlers: specs,
		queues: []jobruntime.QueueBudget{
			{Queue: reportsQueue, MaxWorkers: reportsQueueWorkers},
		},
	}, nil
}
