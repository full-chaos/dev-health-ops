// external_recompute.go wires the native external-ingest recompute drain
// (CHAOS-5296) and implements its enqueue seam.
//
// The consumer lives in this process, not in the stream runner that WRITES the
// rows, because everything it needs to enqueue lives here: the job registry,
// the daily-metrics store and publisher, and the work-graph request writer. The
// stream runner has none of them, and giving it a second copy would mean two
// processes able to publish daily-metrics work.
//
// The enqueue seam itself lives in internal/externalrecompute/enqueue.go, so
// this binary and workerctl's one-shot replay command publish identically.

package main

import (
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/externalrecompute"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
)

// newExternalRecomputeDrain builds the drain component, or nil when this
// process has no domain pool or registry to build it against. Returning nil
// rather than an error matches newQueueHealthMonitor: a worker that cannot host
// the drain still serves every queue it was selected for, and readiness already
// reports a missing database through its own checks.
func newExternalRecomputeDrain(
	database workerDatabase,
	registry *jobruntime.Registry,
	logger *slog.Logger,
) *externalrecompute.Drain {
	postgresDatabase, ok := database.(*postgresWorkerDatabase)
	if !ok || postgresDatabase == nil || postgresDatabase.pools == nil ||
		postgresDatabase.pools.Domain == nil || registry == nil || logger == nil {
		return nil
	}
	dailyStore, err := daily.NewPostgresStore(postgresDatabase.pools.Domain)
	if err != nil {
		logger.Error("external recompute drain unavailable", "reason", "daily_store", "error", err.Error())
		return nil
	}
	dailyPublisher, err := daily.NewPostgresPublisher(postgresDatabase.pools.Domain, registry)
	if err != nil {
		logger.Error("external recompute drain unavailable", "reason", "daily_publisher", "error", err.Error())
		return nil
	}
	workGraph, err := workgraph.NewRequestWriter(registry)
	if err != nil {
		logger.Error("external recompute drain unavailable", "reason", "work_graph_writer", "error", err.Error())
		return nil
	}
	enqueuer, err := externalrecompute.NewPostgresEnqueuer(dailyStore, dailyPublisher, workGraph)
	if err != nil {
		logger.Error("external recompute drain unavailable", "reason", "enqueuer", "error", err.Error())
		return nil
	}
	drain, err := externalrecompute.NewDrain(
		postgresDatabase.pools.Domain,
		enqueuer,
		externalrecompute.DefaultDrainConfig(),
		logger,
	)
	if err != nil {
		logger.Error("external recompute drain unavailable", "reason", "construction", "error", err.Error())
		return nil
	}
	return drain
}
