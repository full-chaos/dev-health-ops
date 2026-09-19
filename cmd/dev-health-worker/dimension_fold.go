// dimension_fold.go registers the system.dimension_fold worker with the
// provider-sync family.
//
// The fold runs where the rows it folds are written: this family already holds
// the ClickHouse connection that writes `repos` and `teams`, and the kind's
// queue is this family's sync_provider queue, so no other pool needs a
// ClickHouse connection or a new queue for it.

package main

import (
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/dimensionfold"
	"github.com/riverqueue/river"
)

// registerDimensionFoldWorker adds the fold worker and returns its spec. It
// returns (nil, nil) only when the registry does not route the kind to River;
// every other failure is an error, so a pool that should fold never starts
// without the worker.
func registerDimensionFoldWorker(
	registry *jobruntime.Registry,
	clickhouseConnection driver.Conn,
	database *postgresWorkerDatabase,
	observer jobruntime.Observer,
	logger *slog.Logger,
	workers *river.Workers,
) (*jobruntime.HandlerSpec, error) {
	spec, ok := registry.Descriptor(jobcontract.KindDimensionFold)
	if !ok {
		return nil, errWorkerDependencyUnavailable
	}
	if !spec.Executable() {
		return nil, nil
	}
	folder, err := dimensionfold.NewFolder(
		dimensionfold.ClickHouseConn{Conn: clickhouseConnection},
		dimensionfold.DefaultConfig(), logger,
	)
	if err != nil {
		return nil, err
	}
	handler, err := dimensionfold.NewHandler(folder, logger)
	if err != nil {
		return nil, err
	}
	idempotency, err := newOperationalIdempotency(database.pools.Domain, observer)
	if err != nil {
		return nil, err
	}
	adapter, err := jobruntime.NewAdapter[jobruntime.DimensionFoldArgs](
		registry, spec, handler, jobruntime.Dependencies{
			Logger: logger, Observer: observer,
			TenantScope: operationalTenantScope{},
			Budget:      newOperationalBudget(database.pools.Domain, observer),
			Idempotency: idempotency,
		},
	)
	if err != nil {
		return nil, err
	}
	if err := river.AddWorkerSafely(workers, adapter); err != nil {
		return nil, err
	}
	registered := adapter.Spec()
	return &registered, nil
}
