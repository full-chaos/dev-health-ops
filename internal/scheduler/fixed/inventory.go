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
			ID:              "daily_metrics_fanout",
			LegacyBeatEntry: "run-daily-metrics",
			Cadence:         DailyAt(1, 0),
			Timezone:        inventoryTimezone,
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
			ID:               "complexity_daily_fanout",
			LegacyBeatEntry:  "run-complexity-daily",
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
			ID:               "release_impact_daily_fanout",
			LegacyBeatEntry:  "run-release-impact-daily",
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
		{
			ID:               "recommendations_daily_fanout",
			LegacyBeatEntry:  "run-recommendations",
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
			ID:               "membership_backfill_daily_fanout",
			LegacyBeatEntry:  "run-membership-backfill-daily",
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
			ID:               "capacity_forecast_weekly_fanout",
			LegacyBeatEntry:  "run-capacity-forecast",
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
			ID:              "scheduled_reports_dispatch",
			LegacyBeatEntry: "dispatch-scheduled-reports",
			Cadence:         EveryInterval(300 * time.Second),
			Timezone:        inventoryTimezone,
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
			ID:              "phone_home_heartbeat",
			LegacyBeatEntry: "phone-home-heartbeat",
			Cadence:         DailyAt(0, 0),
			Timezone:        inventoryTimezone,
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
			// Declared Native by CHAOS-3209, which built this schedule before
			// any Python one existed. CHAOS-3404 then added the Celery beat
			// entry `ask-dev-retention-sweep` for the SAME work at the SAME
			// 05:30 cadence, so the schedule stopped being native the moment
			// that landed: it now has a legacy predecessor, and leaving Native
			// set would have kept it out of the bidirectional inventory check
			// -- the exact bypass the Native field's own comment warns about.
			//
			// WHAT THIS OWNERSHIP CLAIM DOES *NOT* SAY, because both halves
			// were overstated once and are load-bearing for whoever decides
			// the Celery cutover (CHAOS-3481):
			//
			// 1. THIS SCHEDULE EMITS NOTHING TODAY. producers.go pins
			//    prune_ask_dev_conversations to ContractVersionV3, while
			//    contracts/jobs/v1/migration-state.json declares
			//    system.retention_cleanup at producer_version 2. Produce()
			//    therefore returns SkipReason "consumer_version_incompatible"
			//    on every occurrence, and engine.go records that as a normal
			//    skipped occurrence -- deliberately NOT promoted to a failure.
			//    So a nightly run that publishes no job reads as healthy. The
			//    Beat entry is the ONLY thing purging expired conversations
			//    until producer_version reaches 3, and must not be deleted on
			//    cadence evidence alone. The machine-checkable bar in
			//    contracts/jobs/v1/transitional-inventory.json's
			//    deletion_evidence_requirement for `ask-dev-retention-sweep`
			//    names that precondition so this cannot be missed by reading
			//    cadences alone.
			//
			// 2. THE GO DRAIN-COMPLETION CONTRACT IS STRICTLY WEAKER than
			//    Python's. The two agree on the selection predicate, the
			//    ordering, the FOR UPDATE SKIP LOCKED selection, the tombstone
			//    reason mapping and the chunked commits -- see
			//    AskDevConversationStore in
			//    internal/jobs/system/retention_postgres.go against
			//    DevPersistenceService.cleanup_expired/_purge_conversation.
			//    They do NOT agree on how a drain ENDS. Python runs a
			//    non-locking count_expired() after its batch loop and reports
			//    "partial" unless the backlog is confirmed empty, precisely
			//    because SKIP LOCKED makes a short read indistinguishable from
			//    a contended one (ask_dev_retention.py's module docstring
			//    records this as a confirmed HIGH review finding). Go has no
			//    equivalent: deleteInChunks treats a short chunk as done, and
			//    the handler discards DeleteBefore's count entirely, so a
			//    contended pass reports success. Porting that is CHAOS-3481's,
			//    not this PR's -- but the earlier version of this comment
			//    listed the five matching properties and omitted this one,
			//    which reads as full parity. It is not full parity.
			LegacyBeatEntry: "ask-dev-retention-sweep",
			Cadence:         DailyAt(5, 30),
			Timezone:        inventoryTimezone,
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
			Name:     "run-daily-metrics",
			Cadence:  DailyAt(1, 0),
			Owner:    OwnerFixedSchedule,
			OwnerRef: "daily_metrics_fanout",
		},
		{
			Name:     "run-complexity-daily",
			Cadence:  DailyAt(0, 45),
			Owner:    OwnerFixedSchedule,
			OwnerRef: "complexity_daily_fanout",
		},
		{
			Name:     "run-recommendations",
			Cadence:  DailyAt(2, 0),
			Owner:    OwnerFixedSchedule,
			OwnerRef: "recommendations_daily_fanout",
		},
		{
			Name:     "run-release-impact-daily",
			Cadence:  DailyAt(1, 30),
			Owner:    OwnerFixedSchedule,
			OwnerRef: "release_impact_daily_fanout",
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
			Name:     "run-capacity-forecast",
			Cadence:  WeeklyAt(time.Monday, 4, 0),
			Owner:    OwnerFixedSchedule,
			OwnerRef: "capacity_forecast_weekly_fanout",
		},
		{
			Name:     "process-ingest-streams",
			Cadence:  EveryInterval(30 * time.Second),
			Owner:    OwnerStreamRunner,
			OwnerRef: "stream-ingest",
			Note: "Continuous guarded at-least-once consumption replaces a Beat launcher " +
				"whose only purpose was restarting a short-lived Celery consumer.",
		},
		{
			Name:     "process-product-telemetry-streams",
			Cadence:  EveryInterval(30 * time.Second),
			Owner:    OwnerStreamRunner,
			OwnerRef: "stream-ingest",
			Note:     "Same continuous-consumption replacement as process-ingest-streams.",
		},
		{
			Name:     "process-external-ingest-streams",
			Cadence:  EveryInterval(30 * time.Second),
			Owner:    OwnerStreamRunner,
			OwnerRef: "stream-external",
			Note:     "Singleton continuous consumer; the external runner is not horizontally scaled.",
		},
		{
			Name:     "dispatch-go-external-ingest-recompute-bridge",
			Cadence:  EveryInterval(10 * time.Second),
			Owner:    OwnerRemoved,
			OwnerRef: "internal/externalrecompute",
			Note: "Exists only to drain Go-authored compatibility bridge rows into the Python " +
				"planner. Native external recompute consumes the durable debounce state directly, " +
				"so the entry is deleted rather than replaced.",
		},
		{
			Name:     "external-ingest-stream-health",
			Cadence:  EveryInterval(60 * time.Second),
			Owner:    OwnerRuntimeTelemetry,
			OwnerRef: "stream runtime telemetry",
			Note: "Lag, pending, reclaim, error, and readiness are emitted continuously by the " +
				"stream runtime. Making telemetry a queued job would hide exactly the backlog it measures.",
		},
		{
			Name:     "phone-home-heartbeat",
			Cadence:  DailyAt(0, 0),
			Owner:    OwnerFixedSchedule,
			OwnerRef: "phone_home_heartbeat",
		},
		{
			Name:     "dispatch-scheduled-reports",
			Cadence:  EveryInterval(300 * time.Second),
			Owner:    OwnerFixedSchedule,
			OwnerRef: "scheduled_reports_dispatch",
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
			Name:     "run-membership-backfill-daily",
			Cadence:  DailyAt(3, 30),
			Owner:    OwnerFixedSchedule,
			OwnerRef: "membership_backfill_daily_fanout",
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
		{
			Name:     "ask-dev-retention-sweep",
			Cadence:  DailyAt(5, 30),
			Owner:    OwnerFixedSchedule,
			OwnerRef: "prune_ask_dev_conversations",
		},
		{
			Name:     "consume-pending-scheduled-sync-occurrences",
			Cadence:  EveryInterval(300 * time.Second),
			Optional: true,
			Owner:    OwnerProductScheduler,
			OwnerRef: "internal/scheduler/sync",
			Note: "Folded into the Go materializing coordinator: claim, materialize, retry, and " +
				"quarantine all happen in the scheduler transaction, so the separate consumer is removed.",
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
