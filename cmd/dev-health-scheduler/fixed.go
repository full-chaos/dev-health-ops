package main

import (
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	schedulerfixed "github.com/full-chaos/dev-health-ops/internal/scheduler/fixed"
	"github.com/jackc/pgx/v5/pgxpool"
)

// defaultContractRoot matches the other production binaries.
const defaultContractRoot = "contracts/jobs/v1"

// buildFixedScheduleProducers constructs every producer this binary can
// honestly execute.
//
// The three producers below are declared but not built. Each one needs domain
// materialization that does not exist in Go yet, and registering a stub that
// quietly produced nothing would make an unmigrated schedule indistinguishable
// from a healthy one. NewNotImplementedProducer instead fails the window, which
// closes `fixed_scheduler_loop` readiness and keeps the schedule out of any
// migration evidence until its owning lane lands:
//
//   - daily_metrics_fanout needs per-organization repository discovery before
//     it can call the daily metrics store.
//   - scheduled_metrics_dispatch needs the durable metrics ScheduledJob sweep.
//   - scheduled_reports needs the ReportRun and scheduled-report-occurrence
//     writers, which Go does not have: it only executes existing report runs.
func buildFixedScheduleProducers(
	pool *pgxpool.Pool,
	registry *jobruntime.Registry,
) (*schedulerfixed.ProducerSet, error) {
	remainingStore, err := remaining.NewPostgresStore(pool)
	if err != nil {
		return nil, err
	}
	remainingPublisher, err := remaining.NewPostgresPublisher(pool, registry)
	if err != nil {
		return nil, err
	}
	// The membership safety net chains a work-graph build before its
	// projection, so the fan-out producer needs the same request writer the
	// post-sync path uses.
	graphWriter, err := workgraph.NewRequestWriter(registry)
	if err != nil {
		return nil, err
	}
	remainingFanout, err := schedulerfixed.NewRemainingMetricsFanoutProducer(
		remainingStore,
		remainingPublisher,
		schedulerfixed.NewPostgresOrganizationLister(),
		graphWriter,
	)
	if err != nil {
		return nil, err
	}
	return schedulerfixed.NewProducerSet(
		schedulerfixed.NewHeartbeatProducer(),
		schedulerfixed.NewRetentionProducer(),
		remainingFanout,
		schedulerfixed.NewNotImplementedProducer(
			schedulerfixed.ProducerDailyMetricsFanout,
			"needs Go per-organization repository discovery (CUT-12)",
		),
		schedulerfixed.NewNotImplementedProducer(
			schedulerfixed.ProducerScheduledMetricsDispatch,
			"needs the Go metrics ScheduledJob due sweep (CUT-12)",
		),
		schedulerfixed.NewNotImplementedProducer(
			schedulerfixed.ProducerScheduledReports,
			"needs Go ReportRun materialization (CUT-03)",
		),
	)
}

// buildFixedScheduleLoop constructs the fixed maintenance schedule runtime. It
// shares the scheduler process because the two schedulers must never disagree
// about which process owns periodic work.
//
// The pool is the COORDINATOR pool (CHAOS-3114). Everything constructed here
// runs inside Engine.runOccurrence's single transaction, so one pool has to
// serve the whole statement set: the occurrence ledger
// (coordinator-exclusive), the remaining-metrics store and its partition
// publisher, the work-graph request writer, and the outbox publisher. The
// stores are handed the same pool only so their non-transactional methods have
// one; the fixed engine never calls those.
func buildFixedScheduleLoop(
	pool *pgxpool.Pool,
	registry *health.Registry,
) (fixedScheduleRuntime, error) {
	if pool == nil || registry == nil {
		return nil, errSchedulerActivationUnavailable
	}
	jobs, err := jobruntime.Load(defaultContractRoot)
	if err != nil {
		return nil, err
	}
	producers, err := buildFixedScheduleProducers(pool, jobs)
	if err != nil {
		return nil, err
	}
	schedules, err := schedulerfixed.Schedules()
	if err != nil {
		return nil, err
	}
	publisher, err := schedulerfixed.NewOutboxPublisher(jobs)
	if err != nil {
		return nil, err
	}
	engine, err := schedulerfixed.NewEngine(schedulerfixed.EngineConfig{
		Schedules: schedules,
		Producers: producers,
		Ledger:    schedulerfixed.NewPostgresLedger(),
		Publisher: publisher,
		Registry:  jobs,
		Pool:      pool,
	})
	if err != nil {
		return nil, err
	}
	return schedulerfixed.NewLoop(engine, schedulerfixed.DefaultLoopConfig(registry))
}
