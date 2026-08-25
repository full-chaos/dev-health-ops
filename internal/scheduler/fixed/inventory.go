package fixed

import (
	"fmt"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
)

// Producer identities. A schedule names its argument constructor here so the
// declaration table stays comparable against the legacy Beat inventory without
// linking executable producer code.
const (
	ProducerDailyMetricsFanout     = "daily_metrics_fanout"
	ProducerRemainingMetricsFanout = "remaining_metrics_fanout"
	ProducerScheduledReports       = "scheduled_reports"
	ProducerHeartbeat              = "heartbeat"
	ProducerSyncCoverageRefresh    = "sync_coverage_refresh"
	ProducerRetentionCleanup       = "retention_cleanup"
)

// UTC is the only zone the checked-in inventory uses. Every legacy Beat
// crontab ran against the Celery `timezone` setting, which is UTC, so
// preserving cadence parity means preserving the zone as well. The field
// exists because TRD section 9.2 requires a declared zone per schedule, not
// because a second zone is currently operated.
const inventoryTimezone = "UTC"

// checkedInSchedules is the complete set of fixed maintenance schedules the Go
// scheduler owns. Cadence values are byte-for-byte equivalents of the legacy
// Celery Beat entries; ScheduleCoverage proves that against the Python source.
func checkedInSchedules() []Schedule {
	return []Schedule{
		{
			ID: "daily_metrics_fanout",
			// CHAOS-4026 (2026-08-21): the legacy Beat entry "run-daily-metrics"
			// (and its dispatch_daily_metrics_for_all_orgs Celery task) was
			// deleted -- Celery is retired, zero Python celery services have run
			// in prod since 2026-08-19. This schedule has no live predecessor to
			// mirror any more, so it is Native. Its retirement is recorded in
			// RetiredBeatInventory rather than silently dropped.
			Native:   true,
			Cadence:  DailyAt(1, 0),
			Timezone: inventoryTimezone,
			// A nightly organization fan-out that is skipped leaves
			// repo_metrics_daily unpopulated for a whole day, so an outage that
			// spans 01:00 must still produce the run.
			CatchUp:          CatchUpBounded,
			UniquenessWindow: 25 * time.Hour,
			TargetKind:       jobcontract.KindDailyMetricsDispatch,
			ProducerID:       ProducerDailyMetricsFanout,
			MaxAttempts:      3,
			AlertThreshold:   25 * time.Hour,
			Rationale: "CHAOS-2849 per-organization fan-out. Blank-org runs match no tenant " +
				"rows, so the producer must enumerate active organizations.",
		},
		{
			ID: "complexity_daily_fanout",
			// CHAOS-4026 (2026-08-21): the legacy Beat entry "run-complexity-daily"
			// (and its dispatch_complexity_job Celery task) was deleted -- Celery
			// is retired. Native now; see RetiredBeatInventory.
			Native:           true,
			Cadence:          DailyAt(0, 45),
			Timezone:         inventoryTimezone,
			CatchUp:          CatchUpBounded,
			UniquenessWindow: 25 * time.Hour,
			TargetKind:       jobcontract.KindRemainingComplexity,
			ProducerID:       ProducerRemainingMetricsFanout,
			MaxAttempts:      3,
			AlertThreshold:   25 * time.Hour,
			Rationale: "CHAOS-2850 daily complexity floor. Deliberately ordered before the " +
				"01:00 daily metrics run so complexity_delta reads a fresh snapshot.",
		},
		{
			ID: "release_impact_daily_fanout",
			// CHAOS-4026 (2026-08-21): the legacy Beat entry
			// "run-release-impact-daily" (and its dispatch_release_impact /
			// run_release_impact_job Celery tasks) was deleted -- Celery is
			// retired. Native now; see RetiredBeatInventory.
			Native:           true,
			Cadence:          DailyAt(1, 30),
			Timezone:         inventoryTimezone,
			CatchUp:          CatchUpBounded,
			UniquenessWindow: 25 * time.Hour,
			TargetKind:       jobcontract.KindRemainingReleaseImpact,
			ProducerID:       ProducerRemainingMetricsFanout,
			MaxAttempts:      3,
			AlertThreshold:   25 * time.Hour,
			Rationale: "CHAOS-2381 release_impact_daily materialization. Ordered after the " +
				"01:00 daily metrics dispatch so joined deployments exist.",
		},
		// CHAOS-4243 decision note: metrics.remaining.extra_metrics/team_metrics
		// no longer exist anywhere in this codebase (not in this scheduler, not
		// in contracts/jobs/v1/registry.json, not in cmd/dev-health-worker's
		// handler wiring, not in internal/jobs/metrics/remaining/families.json).
		// They were registered handlers with zero producer anywhere -- the
		// orchestrator ruled that "registered but unbound" is itself the broken
		// state, so retirement meant deleting the kinds entirely, not wiring a
		// fixed-schedule fanout for them (which was investigated and reverted;
		// see git history). daily_metrics_fanout below already performs every
		// write both families would have performed, via its existing Python
		// compatibility bridge (daily.HTTPCompatibilityExecutor ->
		// /internal/worker/daily-metrics/v1/execute -> _run_daily_direct ->
		// run_daily_metrics_job, ops/src/dev_health_ops/metrics/job_daily.py:
		// 729-1446). That single function unconditionally computes and writes,
		// on every partition call:
		//   - compute_team_wellbeing_metrics_daily -> team_metrics_daily (line 1057)
		//   - _write_compounding_risk_for_day -> compounding_risk_daily (line 1371)
		//   - compute_release_confidence/quality_drag/pipeline_stability ->
		//     release_confidence_daily/quality_drag_daily/pipeline_stability_daily (lines 1383-1413)
		//   - run_benchmarking_for_day -> benchmarking_rollups (line 1419)
		// and, on the paired finalize call (skip_finalize=False, lines 1428-1444):
		//   - compute_ic_metrics_daily -> user_metrics_daily (ic)
		//   - compute_ic_landscape_rolling -> ic_landscape_rolling_30d
		// This was every table both families' families.json entries targeted --
		// wiring a second schedule for either would have double-computed and
		// double-written against the same ClickHouse tables every night, not
		// closed a coverage gap. testops_release_confidence/quality_drag/
		// pipeline_stability ARE measured stale (16 days, local) despite this
		// unconditional call -- that is `if release_conf:`/`if quality_drag:`/
		// `if pipeline_stab:` (job_daily.py:1408-1413) producing a falsy record
		// from the input data, a compute-level gap in the already-running
		// pathway, not a missing producer. Out of this ticket's scope.
		// TestExtraMetricsAndTeamMetricsWereFullyRetired (producers_test.go)
		// guards against either kind being reintroduced without revisiting this.
		{
			ID: "recommendations_daily_fanout",
			// CHAOS-4026 (2026-08-21): the legacy Beat entry "run-recommendations"
			// (and its run_recommendations_job Celery task) was deleted -- Celery
			// is retired. Native now; see RetiredBeatInventory.
			// _compute_recommendations_for_org itself is not dead -- it still
			// runs via api/internal/worker_metrics.py's dormant-Go bridge, which
			// this schedule's producer calls.
			Native:           true,
			Cadence:          DailyAt(2, 0),
			Timezone:         inventoryTimezone,
			CatchUp:          CatchUpBounded,
			UniquenessWindow: 25 * time.Hour,
			TargetKind:       jobcontract.KindRemainingRecommendations,
			ProducerID:       ProducerRemainingMetricsFanout,
			MaxAttempts:      3,
			AlertThreshold:   25 * time.Hour,
			Rationale: "CHAOS-2373 safety net behind the finalize-gated primary trigger. The " +
				"producer must preserve the in-flight daily_finalize skip.",
		},
		{
			ID: "dora_daily_fanout",
			// CHAOS-4242: DORA never had a legacy Beat entry -- job_dora.py ran
			// only via the post-sync remaining_metrics dispatch, never on a
			// cron. That is a genuinely new schedule, not a port, so it is
			// Native with no LegacyBeatEntry, the same shape as
			// daily_metrics_fanout. It exists because a discarded
			// metrics.remaining.dora job (CHAOS-4242's own native-executor
			// precondition regression is the concrete case that surfaced the
			// gap) had NO self-healing path at all: post-sync dispatch fires
			// once, and with no fixed-schedule catch-up behind it, a silently
			// discarded run left dora_metrics_daily stale for 5+ days before
			// anyone noticed. capacity got the same protection at R2
			// (capacity_forecast_weekly_fanout); this closes the matching gap
			// for dora.
			Native:           true,
			Cadence:          DailyAt(2, 15),
			Timezone:         inventoryTimezone,
			CatchUp:          CatchUpBounded,
			UniquenessWindow: 25 * time.Hour,
			TargetKind:       jobcontract.KindRemainingDORA,
			ProducerID:       ProducerRemainingMetricsFanout,
			MaxAttempts:      3,
			AlertThreshold:   25 * time.Hour,
			Rationale: "CHAOS-4242 self-healing catch-up. DORA reads raw synced deployment/" +
				"incident tables, not another remaining-metrics output, so it carries no " +
				"ordering dependency on its 00:45-02:00 siblings; 02:15 only keeps it out of " +
				"their slots.",
		},
		{
			ID: "membership_backfill_daily_fanout",
			// CHAOS-4026 (2026-08-21): the legacy Beat entry
			// "run-membership-backfill-daily" (and its dispatch_membership_backfill
			// Celery task) was deleted -- Celery is retired. Native now; see
			// RetiredBeatInventory. run_membership_backfill itself is not dead --
			// it still runs via api/internal/worker_workgraph.py's dormant-Go
			// bridge.
			Native:           true,
			Cadence:          DailyAt(3, 30),
			Timezone:         inventoryTimezone,
			CatchUp:          CatchUpBounded,
			UniquenessWindow: 25 * time.Hour,
			TargetKind:       jobcontract.KindRemainingMembership,
			ProducerID:       ProducerRemainingMetricsFanout,
			MaxAttempts:      3,
			AlertThreshold:   25 * time.Hour,
			Rationale: "CHAOS-2439/2433 no-LLM safety net behind the event-driven post-sync " +
				"materializer. Cheap enough that catching up a missed night is correct.",
		},
		{
			ID: "capacity_forecast_weekly_fanout",
			// CHAOS-4026 (2026-08-21): the legacy Beat entry "run-capacity-forecast"
			// (and its dispatch_capacity_forecast/run_capacity_forecast_job Celery
			// tasks, and the product_tasks.py module that held them) was deleted --
			// Celery is retired. Native now; see RetiredBeatInventory.
			Native:           true,
			Cadence:          WeeklyAt(time.Monday, 4, 0),
			Timezone:         inventoryTimezone,
			CatchUp:          CatchUpBounded,
			UniquenessWindow: 8 * 24 * time.Hour,
			TargetKind:       jobcontract.KindRemainingCapacity,
			ProducerID:       ProducerRemainingMetricsFanout,
			MaxAttempts:      3,
			AlertThreshold:   8 * 24 * time.Hour,
			Rationale: "Weekly organization fan-out. A skipped week is a week of stale " +
				"forecasts, so a bounded catch-up is the documented policy.",
		},
		{
			ID: "scheduled_reports_dispatch",
			// CHAOS-4026 (2026-08-21): the legacy Beat entry
			// "dispatch-scheduled-reports" (and its dispatch_scheduled_reports
			// Celery task) was deleted -- Celery is retired. Native now; see
			// RetiredBeatInventory. execute_saved_report itself is not dead --
			// it is still dispatched by the GraphQL triggerReport resolver.
			Native:   true,
			Cadence:  EveryInterval(300 * time.Second),
			Timezone: inventoryTimezone,
			// Report due-ness lives in the durable ScheduledJob row; the next
			// sweep re-reads it, so replaying an older bucket adds nothing.
			CatchUp:          CatchUpSkip,
			UniquenessWindow: time.Hour,
			TargetKind:       jobcontract.KindReportExecuteScheduled,
			ProducerID:       ProducerScheduledReports,
			MaxAttempts:      3,
			AlertThreshold:   30 * time.Minute,
			Rationale: "Replaces the 300s dispatch-scheduled-reports sweep. The producer " +
				"materializes a ReportRun per due SavedReport before enqueueing.",
		},
		{
			ID:               "sync_coverage_refresh",
			Native:           true,
			Cadence:          EveryInterval(300 * time.Second),
			Timezone:         inventoryTimezone,
			CatchUp:          CatchUpSkip,
			UniquenessWindow: time.Hour,
			TargetKind:       jobcontract.KindSyncCoverageRefresh,
			ProducerID:       ProducerSyncCoverageRefresh,
			MaxAttempts:      3,
			AlertThreshold:   30 * time.Minute,
			Rationale: "Rebuilds cold, invalidated, and oldest sync coverage projections " +
				"from retained PostgreSQL facts in bounded batches. Due work is re-read on the " +
				"next tick, so replaying an older bucket adds no recovery value.",
		},
		{
			ID: "phone_home_heartbeat",
			// CHAOS-4026 (2026-08-21): the legacy Beat entry "phone-home-heartbeat"
			// was deleted -- Celery is retired. Native now; see
			// RetiredBeatInventory. phone_home_heartbeat itself is not dead -- it
			// is still invoked by api/internal/worker_operational.py's dormant-Go
			// HTTP bridge (.run(), bypassing Celery entirely).
			Native:   true,
			Cadence:  DailyAt(0, 0),
			Timezone: inventoryTimezone,
			// Telemetry, not product state. A missed midnight heartbeat is
			// reported, never replayed a day late as if it were current.
			CatchUp:          CatchUpSkip,
			UniquenessWindow: 25 * time.Hour,
			TargetKind:       jobcontract.KindHeartbeat,
			ProducerID:       ProducerHeartbeat,
			// The heartbeat kind is registered with a single attempt: a retried
			// telemetry post would report the same day twice. A missed
			// heartbeat is surfaced by the alert threshold instead.
			MaxAttempts:    1,
			AlertThreshold: 25 * time.Hour,
			Rationale: "One deterministic daily heartbeat. Skip policy keeps a late " +
				"occurrence from being reported as if it were the current day.",
		},
		{
			ID:              "prune_rate_limit_observations",
			LegacyBeatEntry: "prune-rate-limit-observations",
			Cadence:         DailyAt(5, 0),
			Timezone:        inventoryTimezone,
			// Retention is cumulative: the next night deletes everything the
			// missed night would have, so replay adds nothing.
			CatchUp:          CatchUpSkip,
			UniquenessWindow: 25 * time.Hour,
			TargetKind:       jobcontract.KindRetentionCleanup,
			ProducerID:       ProducerRetentionCleanup,
			MaxAttempts:      3,
			AlertThreshold:   25 * time.Hour,
			Rationale: "CHAOS-2758 bounded retention for the provider rate-limit observation " +
				"store. Off-peak and clear of the nightly metric jobs.",
		},
		{
			ID:               "prune_external_ingest_batches",
			LegacyBeatEntry:  "prune-external-ingest-batches",
			Cadence:          DailyAt(5, 15),
			Timezone:         inventoryTimezone,
			CatchUp:          CatchUpSkip,
			UniquenessWindow: 25 * time.Hour,
			TargetKind:       jobcontract.KindRetentionCleanup,
			ProducerID:       ProducerRetentionCleanup,
			MaxAttempts:      3,
			AlertThreshold:   25 * time.Hour,
			Rationale: "CHAOS-2694 bounded retention for external-ingest status batches. " +
				"Immediately after the rate-limit prune, terminal-status rows only.",
		},
		{
			ID: "prune_ask_dev_conversations",
			// History: declared Native by CHAOS-3209, which built this schedule
			// before any Python one existed. CHAOS-3404 then added the Celery
			// beat entry `ask-dev-retention-sweep` for the SAME work at the SAME
			// 05:30 cadence, so the schedule stopped being native the moment
			// that landed (it briefly had a legacy predecessor, tracked in the
			// bidirectional inventory check below rather than left as a bypass).
			//
			// RESOLVED (CHAOS-3481, 2026-08-21, PR #1841): the two gaps a prior
			// version of this comment tracked in detail are both closed.
			// producer_version for system.retention_cleanup is now 3, matching
			// prune_ask_dev_conversations's pinned ContractVersionV3 -- Produce()
			// no longer skips on consumer_version_incompatible, and Go genuinely
			// emits and drains this cadence. The drain-completion gap (Python's
			// non-locking count_expired() vs Go's short-chunk-reports-done
			// behavior) is also closed: retention_postgres.go now wires a
			// DrainConfirmer check, red-controlled by
			// TestRetentionHandlerRefusesToReportSuccessOnAContendedShortChunk.
			//
			// RETIRED (CHAOS-4026, 2026-08-21): with Go now the genuine sole
			// purger, the Celery `ask-dev-retention-sweep` Beat entry and its
			// run_ask_dev_retention_cleanup task
			// (src/dev_health_ops/workers/ask_dev_retention.py) were deleted --
			// Celery is retired, zero Python celery services have run in prod
			// since 2026-08-19. This schedule is Native again; the retirement is
			// recorded in RetiredBeatInventory rather than silently dropped.
			Native:   true,
			Cadence:  DailyAt(5, 30),
			Timezone: inventoryTimezone,
			// Product reads enforce expires_at immediately. The scheduled pass
			// durably removes expired content and is cumulative, so a missed run
			// is repaired by the next occurrence without replaying stale buckets.
			CatchUp:          CatchUpSkip,
			UniquenessWindow: 25 * time.Hour,
			TargetKind:       jobcontract.KindRetentionCleanup,
			ProducerID:       ProducerRetentionCleanup,
			MaxAttempts:      3,
			AlertThreshold:   25 * time.Hour,
			Rationale: "CHAOS-3209 bounded Ask Dev expiry cleanup. Conversation rows carry " +
				"their exact 0/30-day expiry; this schedule adds no second horizon.",
		},
	}
}

// Schedules returns the validated checked-in schedule table. Construction
// fails rather than dropping an invalid row so a malformed declaration cannot
// silently reduce coverage.
func Schedules() ([]Schedule, error) {
	schedules := checkedInSchedules()
	seenID := make(map[string]struct{}, len(schedules))
	seenBeat := make(map[string]struct{}, len(schedules))
	for _, schedule := range schedules {
		if err := schedule.Validate(); err != nil {
			return nil, err
		}
		if _, duplicate := seenID[schedule.ID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate schedule id %s", ErrInvalidSchedule, schedule.ID)
		}
		seenID[schedule.ID] = struct{}{}
		if !schedule.Native {
			if _, duplicate := seenBeat[schedule.LegacyBeatEntry]; duplicate {
				return nil, fmt.Errorf(
					"%w: duplicate legacy beat entry %s",
					ErrInvalidSchedule, schedule.LegacyBeatEntry,
				)
			}
			seenBeat[schedule.LegacyBeatEntry] = struct{}{}
		}
	}
	sort.Slice(schedules, func(first, second int) bool {
		return schedules[first].ID < schedules[second].ID
	})
	return schedules, nil
}

// OwnerKind classifies how a legacy Beat entry is replaced. Every legacy entry
// carries exactly one owner; an entry with no owner closes readiness.
type OwnerKind string

const (
	// OwnerFixedSchedule is a schedule in this package's table.
	OwnerFixedSchedule OwnerKind = "go_fixed_schedule"
	// OwnerProductScheduler is the database-backed product scheduler in
	// internal/scheduler/sync.
	OwnerProductScheduler OwnerKind = "go_product_scheduler"
	// OwnerReconciler is the Go sync-dispatch reconciler process.
	OwnerReconciler OwnerKind = "go_reconciler"
	// OwnerStreamRunner is a continuous Go stream-runner process. Continuous
	// consumption is not a schedule: the legacy 30 second Beat launcher only
	// existed because Celery had no long-running consumer.
	OwnerStreamRunner OwnerKind = "go_stream_runner"
	// OwnerRuntimeTelemetry is a native metrics loop inside the Go runtime.
	// These entries are replaced, not ported: they must not become jobs.
	OwnerRuntimeTelemetry OwnerKind = "go_runtime_telemetry"
	// OwnerRemoved is a legacy entry that exists only to bridge Go work back
	// into Celery and disappears with the bridge.
	OwnerRemoved OwnerKind = "removed_with_bridge"
)

// LegacyEntry maps one checked Celery Beat entry to its Go owner.
type LegacyEntry struct {
	// Name is the Beat entry key in src/dev_health_ops/workers/config.py.
	Name string
	// Cadence is the legacy cadence. ScheduleCoverage compares this against
	// the Python source, so a Beat cadence edit fails the build.
	Cadence Cadence
	// Optional marks the one environment-gated entry.
	Optional bool
	// Owner and OwnerRef name the replacement.
	Owner    OwnerKind
	OwnerRef string
	// Note records why a non-schedule owner is correct.
	Note string
}

// RetiredLegacyEntry is a former Beat entry that is deliberately absent from
// the current Python configuration. It is separate from LegacyEntry because
// LegacyBeatInventory is a bidirectional mirror of live source: putting a
// retired entry there would correctly fail the source-equality gate, but would
// make the audited retirement invisible.
type RetiredLegacyEntry struct {
	Name     string
	Cadence  Cadence
	Reason   string
	Evidence string
}

// RetiredBeatInventory records reviewed Beat removals. It is a ledger, not a
// replacement map: each name must remain absent from the live Beat source.
func RetiredBeatInventory() []RetiredLegacyEntry {
	return []RetiredLegacyEntry{
		{
			Name:    "dispatch-scheduled-metrics",
			Cadence: EveryInterval(300 * time.Second),
			Reason: "No production writer creates ScheduledJob rows with job_type='metrics', " +
				"and the Go daily-metrics durable contract cannot safely turn an arbitrary " +
				"legacy configuration into a zero-repository run.",
			Evidence: "CHAOS-3128 retirement decision: source audit found zero production " +
				"writers; the local feature-stack PostgreSQL read-only audit found zero rows.",
		},
		// CHAOS-4026 (2026-08-21): Celery is retired -- zero Python celery
		// services have run in prod since the 2026-08-19 stop (owner
		// ratification). The following 14 Beat entries and their Celery task
		// implementations were deleted from src/dev_health_ops/workers/. Every
		// one already had a verified Go successor per CHAOS-4056's beat-schedule
		// inventory sweep; see that ticket's inventory comment for the full
		// per-entry mapping this ledger summarizes.
		{
			Name:    "run-daily-metrics",
			Cadence: DailyAt(1, 0),
			Reason: "Go's daily_metrics_fanout fixed schedule owns this cadence; the Python " +
				"dispatch_daily_metrics_for_all_orgs Celery task and its metrics_partitioned.py " +
				"chain (dispatch_daily_metrics_partitioned/run_daily_metrics_batch/" +
				"run_daily_metrics_finalize_task) were only ever reachable via this Beat entry.",
			Evidence: "CHAOS-4026, CHAOS-4056 beat-schedule inventory (COVERED, checked-in and " +
				"active per cmd/dev-health-scheduler/fixed.go).",
		},
		{
			Name:    "run-complexity-daily",
			Cadence: DailyAt(0, 45),
			Reason: "Go's complexity_daily_fanout fixed schedule owns this cadence; the Python " +
				"dispatch_complexity_job Celery task was only ever reachable via this Beat entry " +
				"(run_complexity_job, the per-org worker it fanned out to, is not dead -- it stays " +
				"live via post_sync_dispatch.py's event-driven chain).",
			Evidence: "CHAOS-4026, CHAOS-4056 beat-schedule inventory (COVERED).",
		},
		{
			Name:    "run-release-impact-daily",
			Cadence: DailyAt(1, 30),
			Reason: "Go's release_impact_daily_fanout fixed schedule owns this cadence; the " +
				"Python dispatch_release_impact and run_release_impact_job Celery tasks were only " +
				"ever reachable via this Beat entry (the underlying job_release_impact.py compute " +
				"function is not dead -- it stays live via the CLI and worker_metrics.py's bridge).",
			Evidence: "CHAOS-4026, CHAOS-4056 beat-schedule inventory (COVERED).",
		},
		{
			Name:    "run-recommendations",
			Cadence: DailyAt(2, 0),
			Reason: "Go's recommendations_daily_fanout fixed schedule owns this cadence; the " +
				"Python run_recommendations_job Celery task was only ever reachable via this Beat " +
				"entry and the (also-deleted) run_daily_metrics_finalize_task completion chain. " +
				"_compute_recommendations_for_org, the per-org compute it wrapped, is not dead -- " +
				"it stays live via worker_metrics.py's dormant-Go bridge.",
			Evidence: "CHAOS-4026, CHAOS-4056 beat-schedule inventory (COVERED). See CHAOS-4066 " +
				"for a pre-existing, unrelated gap this sweep found in the readiness gate.",
		},
		{
			Name:    "run-capacity-forecast",
			Cadence: WeeklyAt(time.Monday, 4, 0),
			Reason: "Go's capacity_forecast_weekly_fanout fixed schedule owns this cadence; the " +
				"Python dispatch_capacity_forecast and run_capacity_forecast_job Celery tasks (and " +
				"the product_tasks.py module that held them) were only ever reachable via this " +
				"Beat entry (job_capacity.py's compute function is not dead -- it stays live via " +
				"the CLI and worker_metrics.py's bridge).",
			Evidence: "CHAOS-4026, CHAOS-4056 beat-schedule inventory (COVERED).",
		},
		{
			Name:    "process-ingest-streams",
			Cadence: EveryInterval(30 * time.Second),
			Reason: "Go's stream-ingest process natively consumes this stream; the Python " +
				"run_ingest_consumer Celery task (a polling launcher for a short-lived consumer) " +
				"was only ever reachable via this Beat entry.",
			Evidence: "CHAOS-4026, CHAOS-4056 beat-schedule inventory (COVERED).",
		},
		{
			Name:     "process-product-telemetry-streams",
			Cadence:  EveryInterval(30 * time.Second),
			Reason:   "Same continuous-consumption replacement as process-ingest-streams (stream-ingest).",
			Evidence: "CHAOS-4026, CHAOS-4056 beat-schedule inventory (COVERED).",
		},
		{
			Name:    "process-external-ingest-streams",
			Cadence: EveryInterval(30 * time.Second),
			Reason: "Go's stream-external process (a singleton continuous consumer) natively " +
				"consumes this stream; the Python run_external_ingest_consumer Celery task was " +
				"only ever reachable via this Beat entry.",
			Evidence: "CHAOS-4026, CHAOS-4056 beat-schedule inventory (COVERED).",
		},
		{
			Name:    "external-ingest-stream-health",
			Cadence: EveryInterval(60 * time.Second),
			Reason: "Native runtime telemetry (worker_stream_lag/worker_stream_pending gauges) " +
				"replaces the Python external_ingest_stream_health Celery task, which was only " +
				"ever reachable via this Beat entry.",
			Evidence: "CHAOS-4026, CHAOS-4056 beat-schedule inventory (COVERED).",
		},
		{
			Name:    "phone-home-heartbeat",
			Cadence: DailyAt(0, 0),
			Reason: "The phone_home_heartbeat Celery task is not dead -- it stays live via " +
				"api/internal/worker_operational.py's dormant-Go HTTP bridge (.run(), bypassing " +
				"Celery entirely) -- but this Beat entry, its only Celery Beat trigger, is deleted.",
			Evidence: "CHAOS-4026, CHAOS-4056 beat-schedule inventory (COVERED).",
		},
		{
			Name:    "dispatch-scheduled-reports",
			Cadence: EveryInterval(300 * time.Second),
			Reason: "Go's scheduled_reports_dispatch fixed schedule owns this cadence; the Python " +
				"dispatch_scheduled_reports Celery task was only ever reachable via this Beat entry " +
				"(execute_saved_report, the per-report work it fanned out to, is not dead -- it " +
				"stays live via the GraphQL triggerReport resolver).",
			Evidence: "CHAOS-4026, CHAOS-4056 beat-schedule inventory (COVERED).",
		},
		{
			Name:    "run-membership-backfill-daily",
			Cadence: DailyAt(3, 30),
			Reason: "Go's membership_backfill_daily_fanout fixed schedule owns this cadence; the " +
				"Python dispatch_membership_backfill Celery task was only ever reachable via this " +
				"Beat entry (run_membership_backfill, the per-org worker it fanned out to, is not " +
				"dead -- it stays live via api/internal/worker_workgraph.py's dormant-Go bridge).",
			Evidence: "CHAOS-4026, CHAOS-4056 beat-schedule inventory (COVERED).",
		},
		{
			Name:    "ask-dev-retention-sweep",
			Cadence: DailyAt(5, 30),
			Reason: "Go's prune_ask_dev_conversations fixed schedule now genuinely owns this " +
				"cadence: producer_version for system.retention_cleanup reached 3 and the SKIP " +
				"LOCKED drain-completion gap closed (CHAOS-3481), so the Python " +
				"run_ask_dev_retention_cleanup Celery task -- the only thing purging expired " +
				"conversations while the version gate was closed -- is now redundant and deleted.",
			Evidence: "CHAOS-3481 (PR #1841, promoted producer_version to 3, wired a " +
				"DrainConfirmer check) landed before CHAOS-4026's deletion; see " +
				"prune_ask_dev_conversations's schedule-declaration comment for the full history.",
		},
		{
			Name:    "consume-pending-scheduled-sync-occurrences",
			Cadence: EveryInterval(300 * time.Second),
			Reason: "Gated by SYNC_SCHEDULED_OCCURRENCE_CONSUMER_ENABLED, defaulted False, and " +
				"per CHAOS-4056's beat-schedule inventory was never registered in Python production " +
				"at all -- functionally folded into the Go product scheduler's own " +
				"claim/materialize/retry/quarantine transaction (internal/scheduler/fixed/" +
				"inventory.go's own dispatch-scheduled-syncs entry below) by design, not by this " +
				"deletion.",
			Evidence: "CHAOS-4026, CHAOS-4056 beat-schedule inventory (N/A -- never live).",
		},
	}
}

// LegacyBeatInventory is the checked replacement map for every Celery Beat
// entry. It is the single place a reviewer reads to answer "who owns this
// now?", and the coverage test proves it stays equal to the Python source.
func LegacyBeatInventory() []LegacyEntry {
	return []LegacyEntry{
		{
			Name:     "dispatch-scheduled-syncs",
			Cadence:  EveryInterval(300 * time.Second),
			Owner:    OwnerProductScheduler,
			OwnerRef: "internal/scheduler/sync",
			Note: "Database-backed product schedule with tenant cron expressions. Owned by " +
				"the sync scheduler loop and its materializing coordinator, not by a fixed cadence.",
		},
		{
			Name:     "reconcile-sync-dispatch",
			Cadence:  EveryInterval(60 * time.Second),
			Owner:    OwnerReconciler,
			OwnerRef: "internal/syncreconciler",
			Note: "The reconciler runs its own bounded loop. Re-expressing it as a queued " +
				"job would put lease repair behind the queue it repairs.",
		},
		{
			Name:     "dispatch-go-external-ingest-recompute-bridge",
			Cadence:  EveryInterval(10 * time.Second),
			Owner:    OwnerRemoved,
			OwnerRef: "internal/externalrecompute",
			// CHAOS-4057 (2026-08-21): this Note's "Native external recompute
			// consumes the durable debounce state directly, so the entry is
			// deleted rather than replaced" is FALSE -- no such native consumer
			// exists (confirmed: the Go domain role has only SELECT/INSERT on
			// external_ingest_recompute_jobs, so it cannot self-serve). This
			// Beat entry and its Python task remain the sole reader of
			// bridge_pending rows and were deliberately NOT deleted by
			// CHAOS-4026 pending CHAOS-4057's port-vs-retire decision. Left
			// verbatim (not corrected) here because CHAOS-4026 did not touch
			// this row's content, only its position in this list -- correcting
			// the claim belongs to whoever resolves CHAOS-4057.
			Note: "Exists only to drain Go-authored compatibility bridge rows into the Python " +
				"planner. Native external recompute consumes the durable debounce state directly, " +
				"so the entry is deleted rather than replaced.",
		},
		{
			Name:     "monitor-queue-depths",
			Cadence:  EveryInterval(60 * time.Second),
			Owner:    OwnerRuntimeTelemetry,
			OwnerRef: "river runtime telemetry",
			Note: "The legacy task probes kombu/Valkey list depth, which has no River analogue. " +
				"River job age, depth, and saturation are exported natively by the runtime, so this " +
				"is a replacement rather than a port.",
		},
		{
			Name:     "prune-rate-limit-observations",
			Cadence:  DailyAt(5, 0),
			Owner:    OwnerFixedSchedule,
			OwnerRef: "prune_rate_limit_observations",
		},
		{
			Name:     "prune-external-ingest-batches",
			Cadence:  DailyAt(5, 15),
			Owner:    OwnerFixedSchedule,
			OwnerRef: "prune_external_ingest_batches",
		},
	}
}

// LegacyBeatInventoryIndex returns the inventory keyed by Beat entry name.
func LegacyBeatInventoryIndex() map[string]LegacyEntry {
	entries := LegacyBeatInventory()
	index := make(map[string]LegacyEntry, len(entries))
	for _, entry := range entries {
		index[entry.Name] = entry
	}
	return index
}

// ValidateInventory proves the legacy map and the schedule table agree: every
// schedule is claimed by exactly one legacy entry, and every legacy entry that
// claims a fixed schedule resolves to a declared one with the same cadence.
func ValidateInventory() error {
	schedules, err := Schedules()
	if err != nil {
		return err
	}
	byID := make(map[string]Schedule, len(schedules))
	for _, schedule := range schedules {
		byID[schedule.ID] = schedule
	}

	claimed := make(map[string]string, len(schedules))
	seen := make(map[string]struct{}, len(LegacyBeatInventory()))
	for _, entry := range LegacyBeatInventory() {
		if entry.Name == "" {
			return fmt.Errorf("%w: legacy inventory contains an unnamed entry", ErrInvalidSchedule)
		}
		if _, duplicate := seen[entry.Name]; duplicate {
			return fmt.Errorf("%w: duplicate legacy entry %s", ErrInvalidSchedule, entry.Name)
		}
		seen[entry.Name] = struct{}{}
		if err := entry.Cadence.Validate(); err != nil {
			return fmt.Errorf("legacy entry %s: %w", entry.Name, err)
		}
		if entry.OwnerRef == "" {
			return fmt.Errorf("%w: legacy entry %s has no owner reference", ErrInvalidSchedule, entry.Name)
		}
		switch entry.Owner {
		case OwnerFixedSchedule:
			schedule, ok := byID[entry.OwnerRef]
			if !ok {
				return fmt.Errorf(
					"%w: legacy entry %s claims undeclared schedule %s",
					ErrInvalidSchedule, entry.Name, entry.OwnerRef,
				)
			}
			if schedule.LegacyBeatEntry != entry.Name {
				return fmt.Errorf(
					"%w: schedule %s claims legacy entry %s but is owned by %s",
					ErrInvalidSchedule, schedule.ID, schedule.LegacyBeatEntry, entry.Name,
				)
			}
			if schedule.Cadence.Fingerprint() != entry.Cadence.Fingerprint() {
				return fmt.Errorf(
					"%w: schedule %s cadence %s does not match legacy entry %s cadence %s",
					ErrInvalidSchedule, schedule.ID, schedule.Cadence.Fingerprint(),
					entry.Name, entry.Cadence.Fingerprint(),
				)
			}
			if previous, duplicate := claimed[schedule.ID]; duplicate {
				return fmt.Errorf(
					"%w: schedule %s is claimed by both %s and %s",
					ErrInvalidSchedule, schedule.ID, previous, entry.Name,
				)
			}
			claimed[schedule.ID] = entry.Name
		case OwnerProductScheduler, OwnerReconciler, OwnerStreamRunner,
			OwnerRuntimeTelemetry, OwnerRemoved:
			if entry.Note == "" {
				return fmt.Errorf(
					"%w: legacy entry %s is replaced by %s without a recorded reason",
					ErrInvalidSchedule, entry.Name, entry.Owner,
				)
			}
		default:
			return fmt.Errorf(
				"%w: legacy entry %s has unknown owner %q",
				ErrInvalidSchedule, entry.Name, entry.Owner,
			)
		}
	}

	for _, schedule := range schedules {
		if schedule.Native {
			continue
		}
		if _, ok := claimed[schedule.ID]; !ok {
			return fmt.Errorf(
				"%w: schedule %s is not claimed by any legacy inventory entry",
				ErrInvalidSchedule, schedule.ID,
			)
		}
	}
	return nil
}
