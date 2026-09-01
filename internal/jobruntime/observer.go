package jobruntime

import (
	"context"
	"errors"
	"time"
)

// RuntimeInfo drives worker_runtime_info{version,commit}. All fields are
// deployment identity, not job or tenant data.
type RuntimeInfo struct {
	Version string
	Commit  string
}

// JobLabels are the only dimensions exposed to metrics. They intentionally
// exclude job IDs, organizations, correlations, domains, payloads, and errors.
type JobLabels struct {
	Queue string
	Kind  string
}

// Observer maps runtime decisions to the TRD metric family:
// worker_jobs_running, worker_job_duration_seconds,
// worker_job_attempts_total, worker_job_panics_total,
// worker_job_cancellations_total, and worker_domain_state_mismatch_total.
// worker_job_wait_seconds is sampled per-handler execution from the River
// row's ScheduledAt, since that is the only place the adapter observes both
// the job's availability time and its execution start. Deployment sampling
// sets worker_execution_saturation_ratio{queue} directly from configured
// worker capacity and active executions, both scoped to this process.
//
// Pool adapters use worker_database_pool_saturation_ratio{pool} and
// worker_database_pool_acquire_seconds{pool,result}; pool is bounded to
// domain|queue_control and result to acquired|timeout|cancelled|error.
type Observer interface {
	RuntimeRegistered(context.Context, RuntimeInfo)
	JobStarted(context.Context, JobLabels)
	JobFinished(context.Context, JobLabels, Result, ErrorCategory, time.Duration)
	JobPanicked(context.Context, JobLabels)
	JobCancelled(context.Context, JobLabels, ErrorCategory)
	DomainMismatch(context.Context, string)
	BudgetWait(context.Context, JobLabels, time.Duration, string)
	// JobWait observes the time from job availability (River's ScheduledAt) to
	// execution start. It is void, like every other Observer method, so a
	// telemetry fault can never fail a job.
	JobWait(context.Context, JobLabels, time.Duration)
	// ObserveDeterministicFailure reports a Permanent failure that carries a
	// bounded Reason -- narrower than JobFinished/JobCancelled's category
	// alone, so one specific defect class (e.g. an invalid durable-state
	// precondition, CHAOS-4242) can be alerted on distinctly from every
	// other permanent cancellation sharing the same category. Called only
	// when a Reason was actually attached via WithReason; most permanent
	// failures carry none and never call this.
	ObserveDeterministicFailure(context.Context, JobLabels, Reason)
}

// HandlerInvocationObserver is the narrow capability CHAOS-4029's worker
// execution-liveness check depends on: proof that a job reached real handler
// code, unambiguous in a way JobStarted and JobFinished are not.
//
// JobStarted fires before row validation, tenant scope resolution, budget
// acquisition, and the idempotency claim (Adapter.execute) -- a job stuck
// failing any of those gates refreshes it on every attempt without ever
// running a handler. JobFinished's (Result, ErrorCategory) pair looked like
// it could substitute, but several categories genuinely straddle both sides
// of the boundary: a job's outer context (its registered Timeout) covers
// every gate AND the handler in one deadline, so a timeout or a drain
// cancellation can originate from tenant resolution or a budget wait just as
// easily as from the handler, and the recover() in Adapter.execute wraps the
// whole function, not just the handler call, so even CategoryPanic is not
// guaranteed handler-only. No combination of Result and ErrorCategory can
// resolve that ambiguity from outside this package.
//
// This interface exists instead of widening the required Observer interface
// (which every existing implementation -- production and test -- would have
// to grow a method to keep satisfying) precisely so it costs nothing to every
// Observer that does not need it: Adapter.execute checks for it with a plain
// type assertion, exactly like every other narrow capability in this file.
type HandlerInvocationObserver interface {
	// HandlerInvoked is called once, immediately before the job's handler
	// runs, after every pre-handler gate has already passed. It carries no
	// information about the handler's eventual outcome -- reaching this
	// call is itself the entire signal.
	HandlerInvoked(context.Context, JobLabels)
}

// SyncLeaseObserver is the narrower capability concrete expired-lease
// recovery implementations depend on directly, the same way concrete budget
// implementations call ObserveProviderBudgetWait directly rather than through
// the generic Observer: generic runtime middleware has no way to know
// whether a claim recovered an expired lease, only the code that performed
// the recovery does. *MetricsCollector satisfies this.
type SyncLeaseObserver interface {
	ObserveSyncLeaseExpired(SyncLeaseLabels, SyncLeaseResult) error
}

// ReportRunLeaseObserver is the narrow capability used by the report domain
// store after it durably reclaims or terminalizes an expired execution lease.
// Generic runtime middleware cannot infer this state change from a job retry.
type ReportRunLeaseObserver interface {
	ObserveReportRunLeaseExpired(ReportRunLeaseResult) error
}

// WorkGraphLeaseObserver is the narrow capability the work-graph store depends
// on after a release is fenced out by a lease the claimant has already
// outlived (CHAOS-4002, symmetric with DailyMetricsLeaseObserver's
// release_lost result). Generic runtime middleware cannot infer this from a
// job retry: only the store that ran the fenced UPDATE knows whether it
// matched zero rows because the lease had already expired.
type WorkGraphLeaseObserver interface {
	ObserveWorkGraphLeaseReleaseLost() error
}

// RemainingMetricsLeaseObserver is the remaining-metrics store's equivalent of
// WorkGraphLeaseObserver, for the same reason (CHAOS-4002).
type RemainingMetricsLeaseObserver interface {
	ObserveRemainingMetricsLeaseReleaseLost() error
}

// RemainingMetricsOpenDayZeroRowObserver is the narrow capability the
// remaining-metrics store depends on (CHAOS-4384) when its dora cross-trigger
// coverage check finds a 0-row succeeded partition for a day that has not
// closed yet. Generic runtime middleware cannot infer this: only the store
// that just refused to treat that partition as terminal coverage knows the
// refusal happened, and it is exactly the failure mode that froze
// dora_metrics_daily at 0 for org c6a38355 on 2026-08-26/27/28.
type RemainingMetricsOpenDayZeroRowObserver interface {
	ObserveRemainingMetricsOpenDayZeroRow(family string) error
}

// RemainingMetricsManualBackfillObserver is the narrow capability the
// remaining-metrics store depends on (CHAOS-4254) when an operator manually
// starts a backfill run for a historical (organization, family, day) that no
// automatic trigger ever dispatched -- the prod recovery path for CHAOS-4384.
// Generic runtime middleware cannot infer this: only the store that just
// decided to insert a new run, reuse an idempotent retry, or refuse an
// already-covered day knows which of the three happened.
type RemainingMetricsManualBackfillObserver interface {
	ObserveRemainingMetricsManualBackfill(family, outcome string) error
}

// ReportDedupGuardObserver is the narrow capability the weekly-report query
// adapter depends on after running a chart read over an append-only daily
// rollup through its dedup guard (CHAOS-4140). Generic runtime middleware
// cannot infer this: only the code that built and ran the guarded query knows
// how many physical rows it scanned and how many it discarded as a stale
// compute generation.
type ReportDedupGuardObserver interface {
	ObserveReportDedupGuard(table, reason string, observedRows, skippedRows int) error
}

// DailyMetricsLeaseObserver is the narrow capability the daily-metrics store
// depends on after it resolves an existing lease durably. Generic runtime
// middleware cannot infer this: a claim that parks for a live lease and a claim
// that finds nothing to do are the same job outcome, and only the code that ran
// the claim knows which happened.
type DailyMetricsLeaseObserver interface {
	ObserveDailyMetricsLease(DailyMetricsLeaseStage, DailyMetricsLeaseResult) error
}

// DailyMetricsDiscoveryObserver is the narrow capability the daily-metrics
// store depends on after it resolves live ClickHouse repository identity for
// a run (CHAOS-4263). Generic runtime middleware cannot infer this: a run
// that discovers zero repositories and a run that discovers a healthy
// non-empty set both look like an ordinary successful materialization from
// the outside, and only the store that ran the discovery knows which
// happened. Before this observer existed, a zero-repository run terminalized
// silently -- visible only as a job_daily.py log line saying every family
// produced zero rows, never as a durable, alertable signal.
type DailyMetricsDiscoveryObserver interface {
	ObserveDailyMetricsDiscovery(DailyMetricsRunTrigger, DailyMetricsDiscoveryOutcome) error
}

// PostSyncFanoutObserver is the narrow capability NativePostSyncService.Fanout
// depends on after it decides whether a completed sync warrants a daily-
// metrics re-drive (CHAOS-4263, codex adversarial-review round 2). Before
// this observer existed, Fanout's decision was invisible: a daily_dispatch
// publish and a legitimate "not daily-relevant" skip both looked, from the
// outside, like an ordinary post_sync job completing with no error -- so a
// gate that finds zero rows downstream had no way to tell "we published and
// nothing consumed it" apart from "we never published at all".
type PostSyncFanoutObserver interface {
	ObservePostSyncFanout(PostSyncFanoutOutcome) error
}

// TeamRepoOwnershipDerivationObserver is the narrow capability
// sync.team_repo_ownership_derivation's worker (CHAOS-4365 item 1b) depends
// on to report its own outcome -- distinct from PostSyncFanoutObserver, which
// only ever sees whether the FANOUT staged the handoff, never whether the
// worker that later consumed it actually wrote rows. Generic runtime
// middleware cannot infer rows_written vs no_signal vs error: only the
// worker that ran Derive() and got a row count (or an error) back knows.
type TeamRepoOwnershipDerivationObserver interface {
	ObserveTeamRepoOwnershipDerivation(TeamRepoOwnershipDerivationOutcome, int) error
	// ObserveTeamRepoOwnershipDerivationResolutionArm records, per run, how
	// many written rows came from each identity-resolution arm (CHAOS-4458
	// part (b)): the pre-existing direct project_id join, or the
	// linear_team_key join added because Linear's team_project_ownership
	// writer and its work-item normalizer disagree on what project_id means
	// (team-attribution.md §0.2). Called for every registered arm on every
	// run, including 0, so both series stay present regardless of which arm
	// (if either) actually produced rows this run.
	ObserveTeamRepoOwnershipDerivationResolutionArm(TeamRepoOwnershipResolutionArm, int) error
}

// DailyMetricsZeroRowsObserver is the narrow capability the daily-metrics
// partition handler depends on when a family's upstream source data exists
// for a partition's repositories and day, but that family's output table has
// zero rows for the same scope (CHAOS-4263, chris's ruling 2026-08-25).
// Distinct from DailyMetricsDiscoveryObserver's no_repositories outcome: this
// is a run that found real repositories and had real upstream data, yet still
// computed nothing for one or more families -- a compute-path anomaly, not an
// empty day. Generic runtime middleware cannot infer this: only the
// SourceDataChecker that cross-referenced source and output tables knows.
type DailyMetricsZeroRowsObserver interface {
	ObserveDailyMetricsFamilyZeroRowsWithSource(family string) error
}

// DailyMetricsRedriveObserver is the narrow capability
// PostgresStore.RedriveStrandedPartitions depends on (CHAOS-4358). Generic
// runtime middleware cannot infer this: only the redrive itself knows how
// many partitions it moved, and by which bounded reason -- an operator
// override of a failed_permanent terminal state versus a fresh
// metrics.daily_dispatch enqueue outside the outbox's normal per-run dedupe.
// A nil observer makes this a silent no-op, matching every other observer in
// this package: telemetry must never gate durable state.
type DailyMetricsRedriveObserver interface {
	ObserveDailyMetricsRedrive(reason string, count int) error
}

// DailyMetricsFinalizeSweepObserver is the narrow capability
// PostgresStore.FindStrandedFinalizeRuns/RedriveStrandedFinalize depend on
// (CHAOS-4389): a daily_metrics_run whose partitions are all succeeded but
// whose metrics.daily_finalize job was discarded by River (permanently
// deduped by the outbox's fixed per-run idempotency key, so nothing else
// ever re-enqueues it) stays status='running' forever unless an operator or
// reconciler sweep redrives it. "detected" counts runs a sweep pass found in
// this stranded shape; "finalized" counts runs a fresh
// metrics.daily_finalize job was actually (re-)enqueued for -- a subset of
// detected, since a run whose finalize lease is still live is detected but
// deliberately left alone. A nil observer makes this a silent no-op,
// matching every other observer in this package: telemetry must never gate
// durable state.
type DailyMetricsFinalizeSweepObserver interface {
	ObserveDailyMetricsFinalizeSweep(outcome string, count int) error
}

// DailyMetricsFinalizeLedgerRepairObserver is the narrow capability a
// CHAOS-4409 finalize-ledger bulk repair depends on: only the repair itself
// knows how many daily/finalize metric_compatibility_executions rows it
// moved to retry_authorized versus left alone because the original claim
// still read as live. A nil observer makes this a silent no-op, matching
// every other observer in this package: telemetry must never gate durable
// state. See ObserveDailyMetricsFinalizeLedgerRepair's own doc comment for
// why no production caller wires this from `dev-health-workerctl` today.
type DailyMetricsFinalizeLedgerRepairObserver interface {
	ObserveDailyMetricsFinalizeLedgerRepair(outcome string, count int) error
}

// DailyMetricsFinalizeRedriveObserver is the narrow capability
// PostgresStore.RedriveFinalizeForRange depends on (CHAOS-4405): an
// operator-invoked historical backfill that re-runs
// run_daily_metrics_finalize for one org across a [from, to] calendar-day
// range, one day at a time -- distinct from DailyMetricsFinalizeSweepObserver
// (CHAOS-4389), which counts UNATTENDED discovery/repair of a stuck run, not
// a deliberate, explicit re-execution of an already-terminal one. "redriven"
// counts calendar days a fresh metrics.daily_finalize job was actually
// enqueued for; "skipped_ineligible" counts days in the requested range that
// had no eligible run (no run at all for that day, or partitions not 100%
// succeeded); "redriven_failed" (CHAOS-4405 escalation) is observed by
// PostgresStore.transitionFinalize instead, the moment a redriven finalize's
// own claim resolves as a failure -- see that outcome's own doc comment in
// telemetry.go for why it is counted differently from the other three. A nil
// observer makes this a silent no-op, matching every other observer in this
// package: telemetry must never gate durable state.
type DailyMetricsFinalizeRedriveObserver interface {
	ObserveDailyMetricsFinalizeRedrive(outcome string, count int) error
}

// DailyMetricsPartitionRecomputeObserver is the narrow capability
// PostgresStore.RedrivePartitionsForRange depends on (CHAOS-4459): an
// operator-invoked historical recompute that resets an already-'succeeded'
// daily_metrics_run's partitions back to a claimable state for one org
// across a [from, to] calendar-day range, one day at a time -- the
// partition-level counterpart to DailyMetricsFinalizeRedriveObserver.
// "redriven" counts calendar days a fresh metrics.daily_partition job was
// actually enqueued for; "skipped_ineligible" counts days in the requested
// range with no eligible run (no run at all for that day, its partitions
// not 100% succeeded, or a concurrent caller already reset it). A nil
// observer makes this a silent no-op, matching every other observer in this
// package: telemetry must never gate durable state.
type DailyMetricsPartitionRecomputeObserver interface {
	ObserveDailyMetricsPartitionRecompute(family, outcome string, count int) error
}

// DailyMetricsNativeFamilyObserver is the narrow capability
// PartitionHandler depends on after attempting one native family compute
// inside a partition (CHAOS-4276, the daily bridge's per-partition
// counterpart to ObserveDORAPartition/ObserveDORARefused for the remaining
// bridge's per-kind native cutovers). Before this observer existed, a native
// family that silently stopped writing rows was indistinguishable from a
// healthy but quiet day: only the executor that ran the compute knows
// whether it wrote real rows, wrote nothing, or was refused and fell back to
// the compatibility bridge for this partition.
type DailyMetricsNativeFamilyObserver interface {
	ObserveDailyMetricsNativeFamily(
		family string, outcome DailyMetricsNativeFamilyOutcome, rowsWritten int, duration time.Duration,
	) error
}

// IncidentValidFromGuardObserver is the narrow capability IncidentExecutor
// depends on after every operational_service_repository_mappings join
// (CHAOS-4269/CHAOS-4295). The Python `active_incidents_query` predicate
// (`valid_from <= as_of`, no NULL-OK guard) silently drops every
// mapping_kind="repository_derived" row -- map_issue_incidents never sets
// valid_from -- making the whole `incident` family permanently zero-yield.
// IncidentExecutor's loader adds the NULL-OK guard `valid_from IS NULL OR
// valid_from <= as_of` (mirroring the symmetric valid_to clause that already
// existed) and reports, per matched mapping row, whether it would have
// survived the OLD Python predicate ("valid_from_set") or was only kept
// alive by the fix ("valid_from_null_recovered"). Without this counter, a
// deploy that silently regressed the guard (or a source that started setting
// valid_from, making the fix moot) would be indistinguishable from healthy
// steady state -- only this observer makes the fix's ongoing impact, and its
// continued necessity, visible.
type IncidentValidFromGuardObserver interface {
	ObserveIncidentValidFromGuardRows(reason IncidentValidFromGuardReason, count int) error
}

// DailyMetricsCompatRetryObserver is the narrow capability PartitionHandler
// depends on when a metrics.daily compatibility-bridge execution ends up
// stuck at "ambiguous" (CHAOS-4319: an ambiguous_refused response whose
// ledger state is "ambiguous", not the merely-transient "executing"
// collision). Retrying that partition can never succeed without a human
// repair call, so this observer's only outcome today is "persisted_failed" --
// the point at which PartitionHandler durably marks the partition
// failed_permanent instead of letting River exhaust its attempt budget on
// guaranteed 409s and silently discard the job. The "retry_authorized"
// decision this metric's name anticipates is emitted from the Python bridge
// (dev_health_metric_compat_retry_total), which is where the corresponding
// safe-to-retry authorization actually happens; this Go-side observer only
// ever reports the terminal half of the same bounded decision axis.
type DailyMetricsCompatRetryObserver interface {
	ObserveDailyMetricsCompatRetry(decision DailyMetricsCompatRetryDecision) error
}

// TeamMetricsDailyRepoCountObserver is the narrow capability
// TeamWellbeingExecutor depends on after a team_metrics_daily write lands
// (CHAOS-4329). team_metrics_daily now writes one row PER (team, repo, day)
// instead of one row per (team, day) that silently overwrote the previous
// repo's slice -- this observer makes that per-team repo fan-out an
// observable series (unlabeled: no team_id label, to avoid unbounded
// cardinality -- see ObserveTeamMetricsDailyRepoCount) instead of something
// only a bug report could surface. Direct Go counterpart of Python's
// dev_health_team_metrics_daily_repo_count (metrics/prometheus.py); the
// exposed metric NAME matches exactly so one Grafana query covers rows
// written by either language.
type TeamMetricsDailyRepoCountObserver interface {
	ObserveTeamMetricsDailyRepoCount(repoCount int) error
}

// ZeroUnitFinalizationObserver is the narrow capability the native
// finalize_sync_run port depends on (CHAOS-4175) after classifying a
// zero-unit sync run's cause. Generic runtime middleware cannot infer this:
// only the finalize implementation knows a run planned zero units, and what
// cause -- the planner's own recorded diagnosis, or the generic residual --
// it classified the run under. Direct Go counterpart of Python's
// devhealth_sync_run_zero_unit_finalizations_total (CHAOS-4159); see that
// counter's docstring for why the reason axis exists at all: a series
// dominated by the generic residual is itself the signal that an upstream
// planner path is still discarding its own diagnosis.
type ZeroUnitFinalizationObserver interface {
	ObserveZeroUnitFinalization(provider, reason string) error
}

// CoverageCacheInvalidationObserver is the narrow capability the native
// finalize_sync_run port depends on after its transaction commits
// (CHAOS-4226). Every finalize that reaches the once-only branch EMITS one
// coverage-cache invalidation; only a Valkey-acknowledged epoch bump is
// CONSUMED. The two are exposed as a pair so `emitted - consumed` is a
// number from the first sample and a non-zero gap is alertable: it means
// the home dashboard is serving pre-finalize coverage until TTL -- the
// exact hop that cost three investigation rounds on 2026-08-22.
type CoverageCacheInvalidationObserver interface {
	ObserveCoverageCacheInvalidation(provider string, consumed bool) error
}

// BudgetEstimateFailureObserver is the narrow capability the native
// dispatch_sync_run port depends on (CHAOS-4175 codex round 2): only the
// dispatch implementation knows when its BudgetGuard estimate-bridge fetch
// fell open, and why. Direct Go counterpart of the standing order that new
// fail-open logic must carry a counter with a bounded reason label.
type BudgetEstimateFailureObserver interface {
	ObserveBudgetEstimateFailure(reason string) error
}

// TeamCatalogObserver is the narrow capability CHAOS-4431's native/bridge
// team-catalog dispatch depends on to report what it did: generic runtime
// middleware cannot infer which path a provider took (native collector vs
// the Python bridge vs skipped for no CHAOS-4323 selection) or how many rows
// a native collector actually wrote per destination table -- only the
// dispatch implementation itself (TeamCatalogDiscoveryExecutor or
// teamCatalogAutoimportBridge, both cmd/dev-health-worker/internal/syncdispatchruntime)
// knows either.
type TeamCatalogObserver interface {
	// ObserveTeamCatalogDispatch records one dispatch decision: which path a
	// (provider, entry point) pair took for one call.
	ObserveTeamCatalogDispatch(provider string, entryPoint TeamCatalogEntryPoint, outcome TeamCatalogOutcome) error
	// ObserveTeamCatalogRowsWritten records rows written to one destination
	// table by a native collector. Only called on the native outcome.
	ObserveTeamCatalogRowsWritten(provider string, table TeamCatalogTable, rowsWritten int) error
}

// RegisterRuntime validates the low-cardinality scrape-presence identity
// before passing it to an Observer.
func RegisterRuntime(ctx context.Context, observer Observer, info RuntimeInfo) error {
	if observer == nil {
		return errors.New("runtime observer is required")
	}
	if !boundedIdentity(info.Version, 128) || !boundedIdentity(info.Commit, 128) {
		return errors.New("runtime identity is invalid")
	}
	observer.RuntimeRegistered(ctx, info)
	return nil
}

func boundedIdentity(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') &&
			(character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') &&
			character != '.' && character != '_' && character != '-' && character != '+' {
			return false
		}
	}
	return true
}
