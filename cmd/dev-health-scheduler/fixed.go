package main

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	schedulerfixed "github.com/full-chaos/dev-health-ops/internal/scheduler/fixed"
	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/jackc/pgx/v5/pgxpool"
)

// defaultContractRoot matches the other production binaries.
const defaultContractRoot = "contracts/jobs/v1"

// buildFixedScheduleProducers constructs every producer this binary can
// honestly execute.
//
// The full account of which schedules execute, which do not, and the exact
// blocker for each lives in
// .github/docs-legacy/architecture/fixed-schedule-producers.md. Read it before
// changing anything below: the reason strings here are deliberately specific
// because their vague predecessors let two of these gaps stay invisible while
// their tickets were closed.
//
// The two producers still registered as NewNotImplementedProducer below are
// declared but not built. Registering a stub that quietly produced nothing would
// make an unmigrated schedule indistinguishable from a healthy one, so each one
// instead fails the window, which closes `fixed_scheduler_loop` readiness and
// keeps the schedule out of any migration evidence until its blocker is cleared.
//
// Both remaining gaps are ONE blocker, and it is architectural rather than
// unstarted work — the reason strings say so, because the previous wording
// ("needs Go per-organization repository discovery") read as a to-do and let the
// gap stay invisible while CUT-12 was marked done:
//
// Both schedules target metrics.daily_dispatch, which requires a
// daily_metrics_runs row whose partitions carry real repository IDs. The
// partition compute iterates scope["repo_ids"] and reports success having done
// nothing for an empty list (see _run_daily_direct in
// src/dev_health_ops/api/internal/worker_metrics.py), so an empty-repo partition
// is a false pass, not a whole-organization run. Repository identity lives only
// in the ClickHouse `repos` table — there is no repositories table anywhere in
// PostgreSQL — while this process has no ClickHouse dependency and every
// producer runs inside Engine.runOccurrence's single coordinator transaction,
// where a remote read is forbidden. The fix belongs on the worker side, which
// already holds a ClickHouse connection: discovery should materialize partitions
// from internal/jobs/metrics/daily after the run is claimed, leaving the
// scheduler to create only the run.
//
// scheduled_metrics_dispatch carries a second, independent problem worth
// recording next to the first: no code path in the product creates a
// ScheduledJob with job_type='metrics'. Every writer produces 'sync' or
// 'report'. The legacy sweep this schedule replaces has therefore never
// dispatched anything, so whether the schedule should exist at all is a product
// question, not a porting one.
// The parameter is named for the pool it actually receives, not generically:
// the coordinator grant deriver re-seeds taint by naming convention at
// function-typed struct-field call sites, and a bare `pool` reads as unattributed
// there, which is one reason this whole surface was invisible to it.
func buildFixedScheduleProducers(
	coordinatorPool *pgxpool.Pool,
	registry *jobruntime.Registry,
) (*schedulerfixed.ProducerSet, error) {
	remainingStore, err := remaining.NewPostgresStore(coordinatorPool)
	if err != nil {
		return nil, err
	}
	remainingPublisher, err := remaining.NewPostgresPublisher(coordinatorPool, registry)
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
	// The cron evaluator is the sync scheduler's reviewed croniter port rather
	// than a second parser in the fixed package: report schedules carry
	// tenant-authored cron expressions, and two dialects could disagree about
	// when one fires.
	scheduledReports, err := schedulerfixed.NewScheduledReportsProducer(schedulersync.NextOccurrence)
	if err != nil {
		return nil, err
	}
	return schedulerfixed.NewProducerSet(
		schedulerfixed.NewHeartbeatProducer(),
		schedulerfixed.NewRetentionProducer(),
		remainingFanout,
		scheduledReports,
		schedulerfixed.NewNotImplementedProducer(
			schedulerfixed.ProducerDailyMetricsFanout,
			"blocked: repository identity is ClickHouse-only and this process has no "+
				"ClickHouse connection; discovery must move to internal/jobs/metrics/daily",
		),
		schedulerfixed.NewNotImplementedProducer(
			schedulerfixed.ProducerScheduledMetricsDispatch,
			"blocked on the same ClickHouse-only repository discovery; separately, no "+
				"code path creates a job_type='metrics' ScheduledJob for it to sweep",
		),
	)
}

// errFixedScheduleUnbuilt reports that a declared schedule has no executable
// producer, so this process cannot own the fixed schedule table.
var errFixedScheduleUnbuilt = errors.New("declared fixed schedule has no built producer")

// refuseUnbuiltFixedSchedules closes readiness at STARTUP for any schedule that
// is owned on paper only.
//
// Without it the gap is only observable when an unbuilt producer actually fires,
// which for a daily schedule is up to 24 hours after the process starts: the
// window in which an operator is most likely to conclude the cutover worked.
// ScheduleCoverage cannot catch it either — it proves the legacy Beat inventory
// maps onto the schedule table with matching cadence, timezone and catch-up
// policy, an ownership property, and never constructs a producer at all.
//
// This is currently SUBSUMED by the goOwnsMarkers gate: the process already
// registers unavailable in main.go because checkedInSchedulerActivation
// .goOwnsMarkers is false, so on today's tree this check never decides anything.
// It is deliberately not dead code. It becomes load-bearing at the exact moment
// someone flips that flag, which is the moment a paper-owned schedule must stop
// the process rather than quietly never run.
func refuseUnbuiltFixedSchedules(
	producers *schedulerfixed.ProducerSet,
	schedules []schedulerfixed.Schedule,
) error {
	unbuilt := producers.Unbuilt(schedules)
	if len(unbuilt) == 0 {
		return nil
	}
	// Sorted so the failure text is stable across restarts and diffable between
	// deployments; the reason travels with each name so an operator reading a
	// refusing scheduler learns which blocker owns the gap without reading code.
	names := make([]string, 0, len(unbuilt))
	for name := range unbuilt {
		names = append(names, name)
	}
	sort.Strings(names)
	described := make([]string, 0, len(names))
	for _, name := range names {
		described = append(described, name+" ("+unbuilt[name]+")")
	}
	return fmt.Errorf("%w: %s", errFixedScheduleUnbuilt, strings.Join(described, "; "))
}

// buildFixedScheduleLoop constructs the fixed maintenance schedule runtime. It
// shares the scheduler process because the two schedulers must never disagree
// about which process owns periodic work.
//
// The pool is the COORDINATOR pool (CHAOS-3114). Everything constructed here
// runs inside Engine.runOccurrence's single transaction, so one pool has to
// serve the whole statement set: the occurrence ledger
// (coordinator-exclusive), the remaining-metrics store and its partition
// publisher, the work-graph request writer, the scheduled-report producer's
// own writers, and the outbox publisher. The stores are handed the same pool
// only so their non-transactional methods have one; the fixed engine never
// calls those.
//
// The scheduled-report producer adds three tables to that statement set —
// saved_reports (SELECT and the FOR UPDATE lock), report_runs (SELECT, INSERT),
// and scheduled_report_occurrences (SELECT, INSERT, UPDATE) — so the
// coordinator role's declared posture in
// internal/storage/postgres/domain_authorization.go must carry them or
// readiness fails closed on a missing grant. That declaration is owned outside
// this lane and is applied separately; the grant statements themselves are
// derived from it by internal/storage/river/migrate.go, so there is one list
// rather than two that could drift.
func buildFixedScheduleLoop(
	coordinatorPool *pgxpool.Pool,
	registry *health.Registry,
) (fixedScheduleRuntime, error) {
	if coordinatorPool == nil || registry == nil {
		return nil, errSchedulerActivationUnavailable
	}
	jobs, err := jobruntime.Load(defaultContractRoot)
	if err != nil {
		return nil, err
	}
	producers, err := buildFixedScheduleProducers(coordinatorPool, jobs)
	if err != nil {
		return nil, err
	}
	schedules, err := schedulerfixed.Schedules()
	if err != nil {
		return nil, err
	}
	if err := refuseUnbuiltFixedSchedules(producers, schedules); err != nil {
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
		Pool:      coordinatorPool,
	})
	if err != nil {
		return nil, err
	}
	return schedulerfixed.NewLoop(engine, schedulerfixed.DefaultLoopConfig(registry))
}
