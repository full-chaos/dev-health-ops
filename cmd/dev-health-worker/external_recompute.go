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
	"fmt"
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/externalrecompute"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
)

// newExternalRecomputeDrain builds the drain component.
//
// It returns (nil, nil) only when this process has no domain pool to build
// against -- the same shape newQueueHealthMonitor uses, and the case readiness
// already reports through its own database checks.
//
// Every OTHER failure is returned as an error, which fails worker startup. That
// is deliberate and is the direct lesson of this ticket: a consumer that
// silently does not exist is indistinguishable from one that exists and finds
// nothing to do, and that indistinguishability is what let external-ingest
// recompute stop for ~2.5 weeks without anyone noticing. A drain that cannot be
// built must take the process down loudly rather than leave a live writer with
// no reader again. An operator-set cap this process cannot honour
// (externalrecompute.ErrInvalidCapEnv) reaches here through the same path.
func newExternalRecomputeDrain(
	database workerDatabase,
	registry *jobruntime.Registry,
	logger *slog.Logger,
) (*externalrecompute.Drain, error) {
	postgresDatabase, ok := database.(*postgresWorkerDatabase)
	if !ok || postgresDatabase == nil || postgresDatabase.pools == nil ||
		postgresDatabase.pools.Domain == nil || registry == nil || logger == nil {
		return nil, nil
	}
	fail := func(stage string, cause error) (*externalrecompute.Drain, error) {
		logger.Error("external recompute drain unavailable",
			"reason", stage, "error", cause.Error())
		return nil, fmt.Errorf("external recompute drain (%s): %w", stage, cause)
	}
	dailyStore, err := daily.NewPostgresStore(postgresDatabase.pools.Domain)
	if err != nil {
		return fail("daily_store", err)
	}
	dailyPublisher, err := daily.NewPostgresPublisher(postgresDatabase.pools.Domain, registry)
	if err != nil {
		return fail("daily_publisher", err)
	}
	workGraph, err := workgraph.NewRequestWriter(registry)
	if err != nil {
		return fail("work_graph_writer", err)
	}
	enqueuer, err := externalrecompute.NewPostgresEnqueuer(dailyStore, dailyPublisher, workGraph)
	if err != nil {
		return fail("enqueuer", err)
	}
	drain, err := externalrecompute.NewDrain(
		postgresDatabase.pools.Domain,
		enqueuer,
		externalrecompute.DefaultDrainConfig(),
		logger,
	)
	if err != nil {
		return fail("construction", err)
	}
	return drain, nil
}
