package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	materializerNamespace  = uuid.MustParse("0f17e412-bca5-4cc1-a1e2-c1a6d15104a5")
	ErrInvalidMaterializer = errors.New("invalid scheduled sync materializer")
)

// NativeMaterializer owns the domain-side transaction that persists a planned
// sync run. The caller retains its coordinator transaction for policy locks,
// coordinator ledgers, and the final occurrence link.
type NativeMaterializer struct {
	domainPool        *pgxpool.Pool
	afterDomainCommit func() error
	watermarkOverlap  time.Duration
	defaultUnitCap    int
	// executedProof is the CHAOS-4060 evidence snapshot consulted by every
	// Materialize call. A nil pointer means the gate has never been wired by
	// any caller -- Materialize falls back to nil ExecutedProofEvidence
	// (pre-CHAOS-4060, gate-not-enforced pass-through), which is what every
	// existing test that never calls RefreshExecutedProof still gets. The
	// FIRST call to RefreshExecutedProof (successful or not -- see that
	// method) always installs a non-nil pointer, which is also what turns on
	// maybeRefreshExecutedProof's periodic reload below: a caller that has
	// never opted in never pays for a query it didn't ask for.
	executedProof atomic.Pointer[providersync.ExecutedProofEvidence]
	// executedProofNextRefreshAt is a UnixNano deadline, CAS-guarded so
	// concurrent Materialize calls (TestNativeMaterializerConcurrentReplayConverges
	// is a real production shape) elect exactly one refresher instead of a
	// thundering herd of identical queries.
	executedProofNextRefreshAt   atomic.Int64
	executedProofRefreshInterval time.Duration
	now                          func() time.Time
	// executedProofRefreshFailuresTotal and executedProofLastRefreshOK back
	// WritePrometheus: a codex-review finding (round 3) was that a degraded
	// gate is only visible as a log line, indistinguishable from a healthy
	// scheduler at the readiness-endpoint level. This is the "cheap half"
	// fix -- a counter plus a gauge, matching the CHAOS-4073 loud-failure
	// pattern -- not readiness-endpoint surgery.
	executedProofRefreshFailuresTotal atomic.Uint64
	executedProofLastRefreshOK        atomic.Bool
	// executedProofEverLoadedOK separates "this process has never once
	// loaded evidence" from "it loaded evidence and the LAST refresh
	// failed". CHAOS-4124 is exactly that distinction: a later refresh
	// failure is bounded (Proven facts carry forward and HasExecutedProof is
	// checked before Degraded), while a FIRST-load failure installs an empty
	// Degraded snapshot that blocks every non-waived pair -- an eight-hour
	// total planning outage. executedProofLastRefreshOK alone reads false in
	// both cases, so it cannot tell an operator which one is happening. This
	// is the Measured bit: it is what readiness and the metric key off.
	executedProofEverLoadedOK atomic.Bool
	// plannedUnitsLast and zeroPlannedOccurrencesTotal exist because the
	// 4124 outage's ONLY user-visible signal was a 17-dataset -> 1-dataset
	// collapse in planned work, and nothing measured planned work. A run
	// that plans zero units completes successfully; the scheduler stayed
	// "healthy" for eight hours while planning nothing.
	plannedUnitsLast            atomic.Int64
	zeroPlannedOccurrencesTotal atomic.Uint64
	materializedOccurrences     atomic.Uint64
	// sourceDiscovery is the CHAOS-4602 native per-occurrence source
	// (repo/project) discovery step, run before loadPlanSources reads
	// integration_sources for a provider with source-type scope
	// (github/gitlab/jira). Nil (the default from NewNativeMaterializer)
	// disables the step entirely -- every pre-CHAOS-4602 caller and test
	// keeps its exact prior behavior until WithSourceDiscovery is called.
	sourceDiscovery SourceDiscoveryExecutor
}

// WithSourceDiscovery installs the source-discovery step and returns the
// same materializer, so production wiring can chain it onto
// NewNativeMaterializer's result.
func (materializer *NativeMaterializer) WithSourceDiscovery(discovery SourceDiscoveryExecutor) *NativeMaterializer {
	if materializer != nil {
		materializer.sourceDiscovery = discovery
	}
	return materializer
}

// NewNativeMaterializer constructs the scheduled-sync materializer. The pool
// must be authenticated as the domain role; no coordinator fallback exists.
func NewNativeMaterializer(domainPool *pgxpool.Pool) (*NativeMaterializer, error) {
	if domainPool == nil {
		return nil, ErrInvalidMaterializer
	}
	overlap := boundedEnvInt("SYNC_WATERMARK_OVERLAP", 0, 0)
	cap := boundedEnvInt("SYNC_RUN_MAX_UNITS", 1000, 1)
	refreshSeconds := boundedEnvInt("SYNC_EXECUTED_PROOF_REFRESH_SECONDS", 300, 30)
	// boundedEnvInt only lower-bounds. Without an upper bound here too, a
	// value shaped like a typo (or a deliberately huge one) multiplied by
	// time.Second can overflow time.Duration's int64-nanosecond range and go
	// negative -- which makes every future "is it time to refresh yet"
	// comparison in maybeRefreshExecutedProof read as perpetually overdue,
	// turning the throttle it was meant to configure into a query storm on
	// every single Materialize call. Cap at 24h: nothing about "let a route
	// unblock without a restart" needs a longer window, and 24h*time.Second
	// is nowhere near time.Duration's ~292-year ceiling.
	const maxExecutedProofRefreshSeconds = 24 * 60 * 60
	if refreshSeconds > maxExecutedProofRefreshSeconds {
		refreshSeconds = maxExecutedProofRefreshSeconds
	}
	return &NativeMaterializer{
		domainPool: domainPool, watermarkOverlap: time.Duration(overlap) * time.Second,
		defaultUnitCap:               cap,
		executedProofRefreshInterval: time.Duration(refreshSeconds) * time.Second,
		now:                          time.Now,
	}, nil
}

// RefreshExecutedProof loads the CHAOS-4060 executed-proof snapshot from the
// domain pool and swaps it in for every subsequent Materialize call. The
// caller decides when this first runs (typically once at process startup);
// after that, Materialize itself keeps it fresh on a bounded interval (see
// maybeRefreshExecutedProof) so a route that earns its first live proof
// unblocks without a process restart.
//
// A query failure must not silently restore full pre-CHAOS-4060 permissive
// behavior for the rest of this process's lifetime -- that would defeat the
// gate this ticket exists to add. So EVERY failed refresh -- not just the
// very first load -- marks the operative snapshot Degraded (codex round 4:
// a snapshot that was healthy, then degraded by a LATER failure, must not
// keep authorizing never-attempted pairs on the strength of what it last
// successfully observed being "nothing yet" -- that reading cannot be
// trusted once the query itself starts failing). Already-PROVEN pairs are
// unaffected: Degraded only revokes the never-attempted pass-through
// (ExecutedProofSatisfied checks Proven first), never a durable fact that
// already existed, so this can never un-prove a route that already proved
// itself. The error is always returned so the caller can log it loud; this
// method never fails the whole scheduler pass over an evidence-query
// failure (CHAOS-4073's precedent: a safety layer that cannot confirm its
// own precondition fails loud, never takes the pass down -- but "loud" here
// also means "closed", not "open").
func (materializer *NativeMaterializer) RefreshExecutedProof(ctx context.Context) error {
	if materializer == nil || materializer.domainPool == nil {
		return ErrInvalidMaterializer
	}
	// Any refresh -- explicit (a caller's startup call, or a test) or
	// automatic (maybeRefreshExecutedProof) -- resets the throttle window.
	// Without this, an explicit call left executedProofNextRefreshAt at its
	// zero value, so the very next Materialize call would see "now >= 0",
	// consider a refresh immediately overdue, and fire a SECOND query right
	// away regardless of the configured interval -- silently curing a
	// just-installed Degraded snapshot with whatever the database happens to
	// answer a moment later, before the interval the caller configured ever
	// had a chance to matter.
	if materializer.now != nil {
		materializer.executedProofNextRefreshAt.Store(
			materializer.now().Add(materializer.executedProofRefreshInterval).UnixNano(),
		)
	}
	evidence, err := providersync.QueryExecutedProofEvidence(ctx, materializer.domainPool)
	if err != nil {
		materializer.executedProofRefreshFailuresTotal.Add(1)
		materializer.executedProofLastRefreshOK.Store(false)
		// Degraded, not merely empty: the query FAILED, so an empty
		// Proven/Attempted shape here would be indistinguishable from a
		// healthy database that has genuinely never attempted anything --
		// which now legitimately bootstraps every pair through. That would
		// turn an outage into full pre-CHAOS-4060 permissiveness for every
		// pair this snapshot has not already proven. Already-known Proven
		// facts carry forward unchanged (they are durable and do not stop
		// being true because a later query failed); Attempted carries
		// forward too, since it can only ever grow more conservative, never
		// less.
		degraded := &providersync.ExecutedProofEvidence{
			Proven: make(map[string]bool), Attempted: make(map[string]bool),
			Degraded: true,
		}
		if previous := materializer.executedProof.Load(); previous != nil {
			degraded.Proven = previous.Proven
			degraded.Attempted = previous.Attempted
		}
		materializer.executedProof.Store(degraded)
		return err
	}
	materializer.executedProofLastRefreshOK.Store(true)
	materializer.executedProofEverLoadedOK.Store(true)
	materializer.executedProof.Store(evidence)
	return nil
}

// HasLoadedExecutedProof reports whether at least one evidence refresh has
// ever SUCCEEDED in this process. It is deliberately not the inverse of
// "degraded": a materializer that loaded evidence once and then hit a
// refresh failure is degraded but still operating on carried-forward Proven
// facts, whereas one that has never loaded is operating on an empty Degraded
// snapshot that blocks every non-waived pair (CHAOS-4124). Only the second
// is a reason to refuse readiness.
func (materializer *NativeMaterializer) HasLoadedExecutedProof() bool {
	if materializer == nil {
		return false
	}
	return materializer.executedProofEverLoadedOK.Load()
}

// maybeRefreshExecutedProof reloads the executed-proof snapshot on a bounded
// interval, but ONLY for a materializer some caller has already opted into the
// gate via at least one explicit RefreshExecutedProof call (executedProof
// still nil means nobody asked for this, so no query fires -- every
// pre-CHAOS-4060 test that never touches this feature keeps costing zero
// queries). This is what lets a route that just earned its first live
// executed-proof row start planning again without an operator restarting the
// scheduler process, which a load-once-at-startup snapshot alone cannot do.
func (materializer *NativeMaterializer) maybeRefreshExecutedProof(ctx context.Context) {
	if materializer.executedProof.Load() == nil {
		return
	}
	now := materializer.now().UnixNano()
	next := materializer.executedProofNextRefreshAt.Load()
	if now < next {
		return
	}
	if !materializer.executedProofNextRefreshAt.CompareAndSwap(
		next, now+materializer.executedProofRefreshInterval.Nanoseconds(),
	) {
		// Another concurrent Materialize call already won the refresh race.
		return
	}
	if err := materializer.RefreshExecutedProof(ctx); err != nil {
		slog.Default().Error(
			"executed-proof evidence refresh failed; route readiness gate "+
				"keeps its last-known (or empty, conservative) snapshot until "+
				"the next refresh window (CHAOS-4060)",
			"error", err,
		)
	}
}

// WritePrometheus reports the CHAOS-4060 executed-proof gate's own health,
// separately from whatever it decides about any given route: an operator
// watching only scheduler readiness cannot tell a degraded gate (running on
// a failed or never-succeeded evidence load, silently suppressing every
// non-waived route) from a healthy one, because a degraded gate still lets
// Materialize complete a zero-unit occurrence successfully. This is the
// "cheap half" fix codex round 3 asked for: a counter and a gauge, not
// readiness-endpoint surgery.
func (materializer *NativeMaterializer) WritePrometheus(output io.Writer) error {
	if materializer == nil || output == nil {
		return errors.New("Prometheus output is required")
	}
	wired := materializer.executedProof.Load() != nil
	everLoaded := materializer.executedProofEverLoadedOK.Load()
	failures := materializer.executedProofRefreshFailuresTotal.Load()
	degraded := wired && !materializer.executedProofLastRefreshOK.Load()
	plannedUnits := materializer.plannedUnitsLast.Load()
	zeroPlanned := materializer.zeroPlannedOccurrencesTotal.Load()
	materialized := materializer.materializedOccurrences.Load()
	var text strings.Builder
	fmt.Fprintf(&text,
		"# HELP devhealth_scheduler_executed_proof_refresh_failures_total "+
			"CHAOS-4060 executed-proof evidence refresh failures (startup or periodic). "+
			"Read with devhealth_scheduler_executed_proof_evidence_state: a failure while "+
			"state is 1 is bounded (proven facts carry forward), a failure while state is "+
			"-1 means nothing has EVER loaded and every non-waived pair is blocked.\n"+
			"# TYPE devhealth_scheduler_executed_proof_refresh_failures_total counter\n"+
			"devhealth_scheduler_executed_proof_refresh_failures_total %d\n",
		failures,
	)
	// The three states are one series, not three booleans, because the whole
	// CHAOS-4124 lesson is that "not ok" is TWO different conditions with
	// wildly different blast radii and an operator has to be able to tell
	// them apart at a glance. -1 is the outage state.
	fmt.Fprint(&text,
		"# HELP devhealth_scheduler_executed_proof_evidence_state "+
			"Executed-proof evidence liveness: 1 = a refresh succeeded and the "+
			"snapshot is clean; 0 = evidence loaded at least once but the LAST refresh "+
			"failed, so the snapshot is stale-but-usable (proven pairs keep planning); "+
			"-1 = no refresh has EVER succeeded in this process, so the gate is running "+
			"on an empty Degraded snapshot and every non-waived pair is blocked "+
			"(CHAOS-4124, an 8-hour total planning outage). Alert on -1.\n"+
			"# TYPE devhealth_scheduler_executed_proof_evidence_state gauge\n"+
			"devhealth_scheduler_executed_proof_evidence_state ",
	)
	switch {
	case !wired:
		// Not wired is not a health claim at all: no caller composed the
		// gate in this process, so there is nothing to be stale about. It
		// reads as clean rather than as the outage state, exactly as
		// _gate_degraded already does.
		text.WriteString("1\n")
	case !everLoaded:
		text.WriteString("-1\n")
	case degraded:
		text.WriteString("0\n")
	default:
		text.WriteString("1\n")
	}
	fmt.Fprint(&text,
		"# HELP devhealth_scheduler_executed_proof_gate_degraded "+
			"Whether the executed-proof gate is running on a failed or "+
			"never-succeeded evidence load (1) or a healthy one (0); 0 when the "+
			"gate has never been wired by this process. Cannot distinguish a "+
			"stale snapshot from a never-loaded one -- read "+
			"devhealth_scheduler_executed_proof_evidence_state for that.\n"+
			"# TYPE devhealth_scheduler_executed_proof_gate_degraded gauge\n"+
			"devhealth_scheduler_executed_proof_gate_degraded ",
	)
	if degraded {
		text.WriteString("1\n")
	} else {
		text.WriteString("0\n")
	}
	fmt.Fprintf(&text,
		"# HELP devhealth_scheduler_planned_units Units the last SUCCESSFUL "+
			"materialization planned. Zero is a legitimate value (nothing was due), "+
			"which is why it cannot be alerted on alone -- pair it with "+
			"devhealth_scheduler_zero_planned_occurrences_total, and read both as "+
			"unproven while devhealth_scheduler_materialized_occurrences_total is flat, "+
			"since a scheduler that materializes nothing at all reports a stale gauge "+
			"identical to a healthy idle one.\n"+
			"# TYPE devhealth_scheduler_planned_units gauge\n"+
			"devhealth_scheduler_planned_units %d\n",
		plannedUnits,
	)
	fmt.Fprintf(&text,
		"# HELP devhealth_scheduler_zero_planned_occurrences_total Successful "+
			"materializations that planned ZERO units. CHAOS-4124 collapsed planning "+
			"from 17 datasets to 1 for eight hours and nothing paged, because a "+
			"zero-unit run completes successfully. A sustained climb here with "+
			"materialized_occurrences_total climbing at the same rate means every "+
			"occurrence is planning nothing -- alert on that ratio.\n"+
			"# TYPE devhealth_scheduler_zero_planned_occurrences_total counter\n"+
			"devhealth_scheduler_zero_planned_occurrences_total %d\n",
		zeroPlanned,
	)
	fmt.Fprintf(&text,
		"# HELP devhealth_scheduler_materialized_occurrences_total Occurrences "+
			"this process materialized successfully. It is the denominator that makes "+
			"the two series above readable: without it, zero-planned staying flat is "+
			"ambiguous between a healthy scheduler and one that is not running passes "+
			"at all.\n"+
			"# TYPE devhealth_scheduler_materialized_occurrences_total counter\n"+
			"devhealth_scheduler_materialized_occurrences_total %d\n",
		materialized,
	)
	if _, err := io.WriteString(output, text.String()); err != nil {
		return err
	}
	// CHAOS-4602: fold in provider_source_discovery_total when a discovery
	// step is attached, so the caller's single "does this reconciler expose
	// metrics" check (occurrences.(interface{ WritePrometheus(io.Writer)
	// error })) picks it up for free -- no separate registration site
	// needed. A materializer with no discovery service (sourceDiscovery ==
	// nil, the pre-CHAOS-4602 default) writes nothing extra here.
	if source, ok := materializer.sourceDiscovery.(interface{ WritePrometheus(io.Writer) error }); ok {
		return source.WritePrometheus(output)
	}
	return nil
}

// boundedEnvInt mirrors the two Python settings readers this materializer
// ports: _watermark_overlap_seconds' max(0, int(os.getenv(...)))
// (src/dev_health_ops/sync/watermarks.py:113-122) and _env_int's
// max(1, int(raw)) (src/dev_health_ops/sync/guard.py:296-304).
//
// The TrimSpace is load-bearing parity, not tidiness: Python's int() strips
// surrounding whitespace before parsing, so " 604800 " -- a value an operator
// gets for free from a YAML block scalar, a here-doc, or a secret file with a
// trailing newline -- is 604800 to the Python worker. A bare Atoi rejects it
// and silently takes the fallback, which for SYNC_WATERMARK_OVERLAP is 0: the
// two workers then read different incremental windows from one configuration,
// and the HEAVY ratchet's C8 overlap clamp (effectiveHeavyMaxWindow) never
// fires, because the overlap it clamps against was parsed away. Go's
// unicode.IsSpace set matches CPython's whitespace stripping across the ASCII
// controls, NBSP and the Unicode separators.
//
// Two int() acceptances are DELIBERATELY not ported, as an accepted grammar
// restriction: underscore digit separators (Python int("3_0") == 30) and
// non-ASCII decimal digits (Python int("٣٠") == 30). Neither has a
// legitimate use in a deployment env var, and honouring them would mean
// hand-rolling a parser that has to stay bug-compatible with CPython forever.
// They are not left SILENT, though: an unparseable value warns with the raw
// text, so an operator who writes one sees why the setting did not take
// effect instead of quietly running on the fallback.
func boundedEnvInt(key string, fallback, minimum int) int {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		warnUnparseableEnvInt(key, value, fallback)
		return fallback
	}
	if parsed < minimum {
		return minimum
	}
	return parsed
}

// Materialize loads and locks policy state on the coordinator transaction,
// commits the deterministic domain graph, then writes coordinator ledgers.
func (materializer *NativeMaterializer) Materialize(
	ctx context.Context,
	coordinatorTx pgx.Tx,
	occurrence PendingOccurrence,
) (PlanResult, error) {
	if materializer == nil || materializer.domainPool == nil || ctx == nil || coordinatorTx == nil {
		return PlanResult{}, ErrInvalidMaterializer
	}
	if !occurrence.ConfigActive || !occurrence.ConfigPlannerManaged || occurrence.JobType != "sync" {
		return PlanResult{}, ErrOccurrenceIneligible
	}
	// occurrence.JobStatus (scheduled_jobs.status) is checked further down, in
	// loadMaterializationPlan, conditionally: an ordinary cron tick still
	// requires an ACTIVE marker job (unchanged), but a manual/backfill
	// occurrence (a sync_manual_triggers row exists) bypasses it entirely --
	// see that check for why (codex review finding: a manual trigger on an
	// intentionally UNSCHEDULED config -- the org 70d529e0 acceptance shape
	// -- mints a deliberately PAUSED marker job, and this gate would have
	// quarantined every such occurrence, never materializing it).
	materializer.maybeRefreshExecutedProof(ctx)
	ids, err := deterministicMaterializationIDs(occurrence.ID)
	if err != nil {
		return PlanResult{}, err
	}
	loaded, err := loadMaterializationPlan(ctx, coordinatorTx, materializer.domainPool, occurrence, materializer.watermarkOverlap, materializer.defaultUnitCap, materializer.sourceDiscovery)
	if err != nil {
		return PlanResult{}, err
	}
	loaded.input.ExecutedProof = materializer.executedProof.Load()
	var units []PlannedUnit
	if loaded.terminalReason == "" {
		// CHAOS-4602: a sync_manual_triggers-backed occurrence (loaded by
		// loadMaterializationPlan above) can request backfill mode, which
		// BuildScheduledPlan explicitly rejects (ErrBackfillScheduled) --
		// route it to BuildBackfillPlan instead. Every other mode (an
		// ordinary cron tick, or a manual "Sync Now" that is NOT a backfill)
		// keeps the exact scheduled-planner path it always used.
		if loaded.input.Mode == SyncModeBackfill {
			units, err = BuildBackfillPlan(loaded.input)
		} else {
			units, err = BuildScheduledPlan(loaded.input)
		}
		if err != nil {
			return PlanResult{}, err
		}
	}
	if len(units) > loaded.totalUnitCap {
		return PlanResult{}, fmt.Errorf("%w: plan has %d units over cap %d", ErrInvalidPlan, len(units), loaded.totalUnitCap)
	}
	if len(units) > 0 && loaded.terminalReason == "" {
		if err := resolveCredentialStamp(ctx, coordinatorTx, &loaded); err != nil {
			return PlanResult{}, err
		}
	}

	domainTx, err := materializer.domainPool.Begin(ctx)
	if err != nil {
		return PlanResult{}, fmt.Errorf("begin scheduled sync domain transaction: %w", err)
	}
	domainCommitted := false
	defer func() {
		if !domainCommitted {
			_ = domainTx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	if err := applyDomainPlanMutations(ctx, domainTx, loaded, occurrence.ScheduledFor); err != nil {
		return PlanResult{}, err
	}
	if err := persistDomainGraph(ctx, domainTx, ids, loaded, units, occurrence.ScheduledFor); err != nil {
		return PlanResult{}, err
	}
	if err := domainTx.Commit(ctx); err != nil {
		return PlanResult{}, fmt.Errorf("commit scheduled sync domain graph: %w", err)
	}
	domainCommitted = true
	if materializer.afterDomainCommit != nil {
		if err := materializer.afterDomainCommit(); err != nil {
			return PlanResult{}, err
		}
	}
	if err := persistCoordinatorGraph(ctx, coordinatorTx, ids, occurrence, len(units), loaded.terminalReason, loaded.triggeredBy); err != nil {
		return PlanResult{}, err
	}
	// CHAOS-4603: telemetry is NOT recorded here. coordinatorTx is the
	// CALLER's transaction (Materialize only ever writes into it; it never
	// commits it), so anything incremented at this point would fire before
	// the caller's own commit makes this materialization durable -- exactly
	// the SAVEPOINT-vs-outer-transaction bug Python's plan_sync_run has at
	// planner.py:689-692. RecordMaterialized below exists so the caller can
	// increment only after ITS commit actually succeeds.
	return PlanResult{JobRunID: ids.JobRunID, SyncRunID: ids.SyncRunID, PlannedUnits: len(units)}, nil
}

// RecordMaterialized publishes the CHAOS-4124 planning-volume telemetry for
// one materialization. It is deliberately NOT called from inside Materialize
// (see the comment on Materialize's return above, CHAOS-4603): the caller
// must call this only after the transaction it handed to Materialize has
// itself committed successfully. A rollback of that transaction -- for any
// reason, at any later point in the caller's own commit sequence -- must
// leave these counters untouched, exactly as if Materialize had never run.
//
// Recorded only on that success path, deliberately. A pass that FAILED
// planned nothing, but it also measured nothing -- publishing a zero for it
// would be indistinguishable from a healthy occurrence that genuinely had no
// work, and the zero-planned counter exists precisely to be alertable
// without that ambiguity.
func (materializer *NativeMaterializer) RecordMaterialized(plannedUnits int) {
	if materializer == nil {
		return
	}
	materializer.materializedOccurrences.Add(1)
	materializer.plannedUnitsLast.Store(int64(plannedUnits))
	if plannedUnits == 0 {
		materializer.zeroPlannedOccurrencesTotal.Add(1)
	}
}

type loadedMaterializationPlan struct {
	input                  PlannerInput
	provider               string
	configuredCredentialID *string
	credentialID           *string
	authSource             *string
	totalUnitCap           int
	terminalReason         string
	ensureSecurityDataset  bool
	pagerDutyRepair        *pagerDutyDomainRepair
	// triggeredBy is CHAOS-4602's sync_runs.triggered_by stamp: "schedule"
	// for an ordinary cron-minted occurrence (every pre-CHAOS-4602 caller
	// keeps this value, unconditionally), or the sync_manual_triggers row's
	// own triggered_by ("manual"/"backfill") when this occurrence came from
	// one. This is what makes the org 70d529e0 acceptance proof's
	// `sync_runs.triggered_by='backfill'` observable at all -- persistDomainGraph
	// used to hardcode 'schedule' unconditionally.
	triggeredBy string
}

type syncConfigOptions struct {
	FullResync bool   `json:"full_resync"`
	Mode       string `json:"mode"`
	Schedule   string `json:"schedule_cron"`
}

type integrationOptions struct {
	InitialSyncDepth int `json:"initial_sync_depth"`
}

func loadMaterializationPlan(ctx context.Context, tx pgx.Tx, domainPool *pgxpool.Pool, occurrence PendingOccurrence, watermarkOverlap time.Duration, defaultUnitCap int, discovery SourceDiscoveryExecutor) (loadedMaterializationPlan, error) {
	var integrationID, orgID, provider string
	var credentialID *string
	var sourceID *string
	var plannerManaged bool
	var syncTargetsJSON, syncOptionsJSON, integrationOptionsJSON []byte
	err := tx.QueryRow(ctx, `
SELECT integration.id::text, integration.org_id, lower(integration.provider),
       integration.credential_id::text, config.sync_targets::jsonb, config.sync_options::jsonb,
       integration.config::jsonb,config.source_id::text,config.planner_managed
FROM public.sync_configurations AS config
JOIN public.integrations AS integration
  ON integration.id = config.integration_id AND integration.org_id = config.org_id
WHERE config.id = $1::uuid AND config.org_id = $2 AND integration.is_active`, occurrence.ConfigID, occurrence.OrgID).Scan(
		&integrationID, &orgID, &provider, &credentialID, &syncTargetsJSON, &syncOptionsJSON,
		&integrationOptionsJSON, &sourceID, &plannerManaged,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return loadedMaterializationPlan{}, ErrOccurrenceIneligible
	}
	if err != nil {
		return loadedMaterializationPlan{}, fmt.Errorf("load scheduled sync integration: %w", err)
	}
	var targets []string
	if err := json.Unmarshal(syncTargetsJSON, &targets); err != nil {
		return loadedMaterializationPlan{}, fmt.Errorf("decode sync targets: %w", err)
	}
	var options syncConfigOptions
	if err := json.Unmarshal(syncOptionsJSON, &options); err != nil {
		return loadedMaterializationPlan{}, fmt.Errorf("decode sync options: %w", err)
	}
	var integrationConfig integrationOptions
	if err := json.Unmarshal(integrationOptionsJSON, &integrationConfig); err != nil {
		return loadedMaterializationPlan{}, fmt.Errorf("decode integration options: %w", err)
	}
	// CHAOS-4602 design page: an occurrence Python minted for a manual "Sync
	// Now" or Backfill trigger (fork 1: planner-managed configs only --
	// occurrence.ConfigPlannerManaged is already asserted by Materialize's
	// own eligibility gate before this function is ever called) carries a
	// sync_manual_triggers row keyed by its own occurrence_id. Its mode
	// (and, for backfill, since/before/source_ids/dataset_keys) is
	// AUTHORITATIVE over the config's own persisted sync_options -- exactly
	// as a caller-supplied SyncPlanRequest always overrides a config's
	// stored defaults on the Python side. Finding no row here means this is
	// an ordinary cron-scheduled occurrence, and every check below runs
	// EXACTLY as it did before this table existed.
	manualTrigger, err := loadManualSyncTrigger(ctx, tx, occurrence.ID)
	if err != nil {
		return loadedMaterializationPlan{}, err
	}
	// codex review finding: Materialize's own eligibility gate no longer
	// checks occurrence.JobStatus (moved here) so it can be conditional.
	// _ensure_scheduled_job_for_config (execution_trigger.py) deliberately
	// mints a PAUSED marker job for a config with no schedule_cron -- that
	// PAUSED status means "never auto-fire a cron tick for this", which is
	// completely orthogonal to "may THIS manually-created occurrence
	// materialize". An ordinary cron-minted occurrence still requires an
	// ACTIVE marker job, unchanged.
	if manualTrigger == nil && occurrence.JobStatus != 0 {
		return loadedMaterializationPlan{}, ErrOccurrenceIneligible
	}
	mode := SyncModeIncremental
	triggeredBy := "schedule"
	var sinceOverride, beforeOverride *time.Time
	var explicitSourceIDs, explicitDatasetKeys []string
	if manualTrigger != nil {
		switch manualTrigger.Mode {
		case SyncModeIncremental, SyncModeFullResync, SyncModeBackfill:
			mode = manualTrigger.Mode
		default:
			return loadedMaterializationPlan{}, fmt.Errorf(
				"%w: unsupported manual trigger mode %q", ErrInvalidPlan, manualTrigger.Mode,
			)
		}
		triggeredBy = manualTrigger.TriggeredBy
		sinceOverride = manualTrigger.Since
		beforeOverride = manualTrigger.Before
		explicitSourceIDs = manualTrigger.SourceIDs
		explicitDatasetKeys = manualTrigger.DatasetKeys
	} else {
		// No manual trigger row: this occurrence must have come from the
		// cron scheduler, which never mints one for an unscheduled config.
		// Unchanged from pre-CHAOS-4602 behavior.
		if strings.TrimSpace(options.Schedule) == "" {
			return loadedMaterializationPlan{}, ErrOccurrenceIneligible
		}
		if options.Mode == SyncModeBackfill {
			return loadedMaterializationPlan{}, ErrBackfillScheduled
		}
		if options.Mode != "" && options.Mode != SyncModeIncremental && options.Mode != SyncModeFullResync {
			return loadedMaterializationPlan{}, fmt.Errorf("%w: unsupported scheduled mode %q", ErrInvalidPlan, options.Mode)
		}
		if options.FullResync {
			mode = SyncModeFullResync
		} else if options.Mode == SyncModeFullResync {
			mode = SyncModeFullResync
		}
	}
	if err := lockScheduledOrganization(ctx, tx, orgID); err != nil {
		return loadedMaterializationPlan{}, err
	}
	if syncTargetsRequireCanonicalIncident(targets) {
		allowed, err := canonicalIncidentAllowedForUpdate(ctx, tx, orgID, occurrence.ScheduledFor)
		if err != nil {
			return loadedMaterializationPlan{}, err
		}
		if !allowed {
			return loadedMaterializationPlan{}, ErrOccurrenceIneligible
		}
	}
	// CHAOS-4602: native per-occurrence source (repo/project) discovery,
	// before unit planning reads integration_sources below. Checking
	// sourceDiscoveryProviders here (not just inside Discover) avoids the
	// credential-resolve round trip for a provider that will always skip.
	// An explicit-scope config (sourceID set) is NOT bypassed here the same
	// way: Discover is still called, with ExplicitScope true, so its own
	// early-return (before any credential resolution -- equally cheap)
	// records the skipped-outcome telemetry too (codex review finding: the
	// old sourceID==nil bypass here made this specific, common skip case
	// invisible to provider_source_discovery_total -- no series at all, not
	// even a zero, for a config that is legitimately never expected to
	// discover). A discovery failure is loud (logged) but never fails the
	// occurrence -- it only means this pass did not widen coverage;
	// already-existing sources (from Python's config-creation-time
	// discovery, or an earlier materialize pass) are still enough to plan
	// against below.
	if discovery != nil && sourceDiscoveryProviders[provider] {
		var syncOptionsMap map[string]any
		if err := json.Unmarshal(syncOptionsJSON, &syncOptionsMap); err != nil {
			syncOptionsMap = map[string]any{}
		}
		if _, discoverErr := discovery.Discover(ctx, SourceDiscoveryArgs{
			OrgID: orgID, IntegrationID: integrationID, CredentialID: credentialID,
			Provider: provider, SyncOptions: syncOptionsMap, ExplicitScope: sourceID != nil,
			ConfigID: occurrence.ConfigID, PlannerManaged: plannerManaged,
		}); discoverErr != nil {
			slog.Default().Warn("sync.materializer.source_discovery_failed",
				slog.String("provider", provider),
				slog.String("integration_id", integrationID),
				slog.String("error", discoverErr.Error()))
		}
	}
	var pagerDutyRepair *pagerDutyDomainRepair
	pagerDutyCredentialUnavailable := false
	if provider == "pagerduty" {
		repair, unavailable, reason, err := preparePagerDutyRepair(ctx, tx, orgID, integrationID, credentialID, occurrence.ScheduledFor)
		if err != nil {
			return loadedMaterializationPlan{}, err
		}
		if reason != "" {
			return loadedMaterializationPlan{
				input:    PlannerInput{OrgID: orgID, IntegrationID: integrationID, Mode: mode, Now: occurrence.ScheduledFor.UTC()},
				provider: provider, totalUnitCap: defaultUnitCap, terminalReason: reason, triggeredBy: triggeredBy,
			}, nil
		}
		pagerDutyRepair = repair
		pagerDutyCredentialUnavailable = unavailable
	}

	var sources []PlanSource
	var datasets []PlanDataset
	ensureSecurityDataset := false
	if pagerDutyCredentialUnavailable {
		// Match the Python planner: a missing, inactive, wrong-provider, or
		// cross-tenant PagerDuty credential leaves the integration untouched and
		// deliberately produces a zero-unit plan.
		sources = nil
		datasets = nil
	} else if pagerDutyRepair != nil {
		sources = []PlanSource{pagerDutyRepair.source}
		datasets = pagerDutyRepair.datasets
	} else {
		sources, err = loadPlanSources(ctx, tx, orgID, integrationID, occurrence.ConfigID, sourceID, explicitSourceIDs, plannerManaged)
		if err != nil {
			return loadedMaterializationPlan{}, err
		}
		datasets, ensureSecurityDataset, err = loadPlanDatasets(ctx, tx, domainPool, orgID, integrationID, provider, targets, sourceID, explicitDatasetKeys)
		if err != nil {
			return loadedMaterializationPlan{}, err
		}
	}
	if planDatasetsRequireCanonicalIncident(provider, datasets) {
		allowed, err := canonicalIncidentAllowedForUpdate(ctx, tx, orgID, occurrence.ScheduledFor)
		if err != nil {
			return loadedMaterializationPlan{}, err
		}
		if !allowed {
			return loadedMaterializationPlan{}, ErrOccurrenceIneligible
		}
	}
	watermarks, err := loadPlanWatermarks(ctx, tx, orgID, sources, datasets)
	if err != nil {
		return loadedMaterializationPlan{}, err
	}
	tierCap, totalUnitCap, err := loadPlanLimits(ctx, tx, orgID, defaultUnitCap)
	if err != nil {
		return loadedMaterializationPlan{}, err
	}
	var depth *int
	if integrationConfig.InitialSyncDepth > 0 {
		depth = &integrationConfig.InitialSyncDepth
	}
	before := pointerTime(occurrence.ScheduledFor.UTC())
	if beforeOverride != nil {
		utcBefore := beforeOverride.UTC()
		before = &utcBefore
	}
	var since *time.Time
	if sinceOverride != nil {
		utcSince := sinceOverride.UTC()
		since = &utcSince
	}
	return loadedMaterializationPlan{
		input: PlannerInput{
			OrgID: orgID, IntegrationID: integrationID, Mode: mode,
			Now: occurrence.ScheduledFor.UTC(), Before: before, Since: since,
			IntegrationDepthDays: depth, TierBackfillDaysCap: tierCap,
			WatermarkOverlap: watermarkOverlap, Sources: sources, Datasets: datasets, Watermarks: watermarks,
		},
		provider:               provider,
		configuredCredentialID: credentialID,
		totalUnitCap:           totalUnitCap,
		ensureSecurityDataset:  ensureSecurityDataset,
		pagerDutyRepair:        pagerDutyRepair,
		triggeredBy:            triggeredBy,
	}, nil
}

// manualSyncTrigger is the Go read-side mirror of one sync_manual_triggers
// row (migration 0118): Python is the sole writer (a separate DB login,
// grants doc'd in domain_authorization.go's coordinatorPosture); Go only
// ever reads it, by occurrence_id, to learn what a manual "Sync Now" or
// Backfill trigger actually asked for.
type manualSyncTrigger struct {
	Mode        string
	Since       *time.Time
	Before      *time.Time
	SourceIDs   []string
	DatasetKeys []string
	// TriggeredBy is the row's own triggered_by ("manual"/"backfill",
	// CHECK-constrained by migration 0118) -- this is what
	// persistDomainGraph stamps onto sync_runs.triggered_by, replacing the
	// hardcoded 'schedule' every non-manual occurrence still gets.
	TriggeredBy string
}

// loadManualSyncTrigger returns nil, nil when no row exists for
// occurrenceID -- an ordinary cron-scheduled occurrence, the overwhelming
// majority of pickups, never has one.
func loadManualSyncTrigger(ctx context.Context, tx pgx.Tx, occurrenceID string) (*manualSyncTrigger, error) {
	var trigger manualSyncTrigger
	err := tx.QueryRow(ctx, `
SELECT mode, since, before, source_ids, dataset_keys, triggered_by
FROM public.sync_manual_triggers
WHERE occurrence_id = $1`, occurrenceID).Scan(
		&trigger.Mode, &trigger.Since, &trigger.Before, &trigger.SourceIDs, &trigger.DatasetKeys, &trigger.TriggeredBy,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load sync manual trigger: %w", err)
	}
	return &trigger, nil
}

func lockScheduledOrganization(ctx context.Context, tx pgx.Tx, orgID string) error {
	parsed, err := uuid.Parse(orgID)
	if err != nil || orgID == "default" {
		// Preserve Python's compatibility behavior for legacy/default and other
		// non-UUID organization identifiers: no UUID-scoped row exists to lock.
		return nil
	}
	var locked string
	err = tx.QueryRow(ctx, `SELECT id::text FROM public.organizations WHERE id=$1::uuid FOR KEY SHARE`, parsed).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrOccurrenceIneligible
	}
	if err != nil {
		return fmt.Errorf("lock scheduled sync organization: %w", err)
	}
	return nil
}

func resolveCredentialStamp(ctx context.Context, tx pgx.Tx, loaded *loadedMaterializationPlan) error {
	if loaded == nil {
		return ErrInvalidMaterializer
	}
	provider := strings.ToLower(loaded.provider)
	if loaded.configuredCredentialID == nil {
		if provider == "pagerduty" {
			return fmt.Errorf("%w: PagerDuty requires an active credential", ErrInvalidPlan)
		}
		auth := "environment"
		loaded.authSource = &auth
		return nil
	}
	var active bool
	var credentialOrg, credentialProvider string
	if err := tx.QueryRow(ctx, `SELECT org_id,lower(provider),is_active FROM public.integration_credentials WHERE id=$1::uuid`, *loaded.configuredCredentialID).Scan(&credentialOrg, &credentialProvider, &active); err != nil {
		return fmt.Errorf("load scheduled sync credential metadata: %w", err)
	}
	if !active || credentialOrg != loaded.input.OrgID || credentialProvider != provider {
		return ErrOccurrenceIneligible
	}
	credential := *loaded.configuredCredentialID
	auth := "integration_credential"
	loaded.credentialID = &credential
	loaded.authSource = &auth
	return nil
}

func pointerTime(value time.Time) *time.Time { return &value }

var pagerDutyOperationalDatasets = func() []string {
	keys := make([]string, 0, len(supportedProviderDatasets["pagerduty"]))
	for key := range supportedProviderDatasets["pagerduty"] {
		spec, ok := datasetSpecification("pagerduty", key)
		if ok && slices.Contains(spec.LegacyTargets, "operational") {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	return keys
}()

type pagerDutyDomainRepair struct {
	source         PlanSource
	datasets       []PlanDataset
	datasetOptions map[string][]byte
}

func preparePagerDutyRepair(ctx context.Context, tx pgx.Tx, orgID, integrationID string, credentialID *string, now time.Time) (*pagerDutyDomainRepair, bool, string, error) {
	var targetsValid bool
	if err := tx.QueryRow(ctx, `
WITH locked AS (
 SELECT sync_targets FROM public.sync_configurations
 WHERE org_id=$1 AND integration_id=$2::uuid AND lower(provider)='pagerduty'
 FOR UPDATE
)
SELECT coalesce(bool_and(sync_targets::jsonb='["operational"]'::jsonb),FALSE) FROM locked`, orgID, integrationID).Scan(&targetsValid); err != nil {
		return nil, false, "", fmt.Errorf("lock PagerDuty sync configurations: %w", err)
	}
	if !targetsValid {
		reason := "PagerDuty sync target must be operational; malformed configs were disabled"
		return nil, false, reason, disablePagerDutyConfigs(ctx, tx, orgID, integrationID, now, reason)
	}
	if credentialID == nil {
		return nil, true, "", nil
	}
	var configJSON []byte
	err := tx.QueryRow(ctx, `
SELECT config::jsonb
FROM public.integration_credentials
WHERE id=$1::uuid AND org_id=$2 AND lower(provider)='pagerduty' AND is_active`, *credentialID, orgID).Scan(&configJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, true, "", nil
	}
	if err != nil {
		return nil, false, "", fmt.Errorf("load PagerDuty credential metadata: %w", err)
	}
	var credentialConfig struct {
		AccountID string `json:"account_id"`
		Subdomain string `json:"subdomain"`
	}
	if err := json.Unmarshal(configJSON, &credentialConfig); err != nil {
		return nil, false, "", fmt.Errorf("decode PagerDuty credential metadata: %w", err)
	}
	accountID := strings.TrimSpace(credentialConfig.AccountID)
	if accountID == "" || strings.TrimSpace(credentialConfig.Subdomain) == "" {
		reason := "PagerDuty credential account identity is invalid"
		return nil, false, reason, disablePagerDutyConfigs(ctx, tx, orgID, integrationID, now, reason)
	}
	var sourceID string
	err = tx.QueryRow(ctx, `SELECT id::text FROM public.integration_sources WHERE org_id=$1 AND integration_id=$2::uuid AND lower(provider)='pagerduty' AND external_id=$3 ORDER BY id LIMIT 1`, orgID, integrationID, accountID).Scan(&sourceID)
	if errors.Is(err, pgx.ErrNoRows) {
		sourceID = uuid.NewSHA1(materializerNamespace, []byte(integrationID+":pagerduty-source:"+accountID)).String()
	} else if err != nil {
		return nil, false, "", fmt.Errorf("load PagerDuty canonical source: %w", err)
	}
	repair := &pagerDutyDomainRepair{source: PlanSource{ID: sourceID, ExternalID: accountID, Provider: "pagerduty", FullName: accountID}, datasetOptions: make(map[string][]byte)}
	rows, err := tx.Query(ctx, `SELECT dataset_key,options::jsonb FROM public.integration_datasets WHERE org_id=$1 AND integration_id=$2::uuid`, orgID, integrationID)
	if err != nil {
		return nil, false, "", fmt.Errorf("load PagerDuty datasets: %w", err)
	}
	existing := make(map[string][]byte)
	for rows.Next() {
		var key string
		var options []byte
		if err := rows.Scan(&key, &options); err != nil {
			rows.Close()
			return nil, false, "", err
		}
		existing[key] = options
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, false, "", err
	}
	rows.Close()
	for _, dataset := range pagerDutyOperationalDatasets {
		options := map[string]any{}
		if raw := existing[dataset]; len(raw) > 0 {
			_ = json.Unmarshal(raw, &options)
		}
		options["legacy_targets"] = []string{"operational"}
		encoded, err := json.Marshal(options)
		if err != nil {
			return nil, false, "", err
		}
		repair.datasetOptions[dataset] = encoded
		var depth *int
		if value, ok := options["initial_sync_depth"].(float64); ok && value > 0 {
			parsed := int(value)
			depth = &parsed
		}
		repair.datasets = append(repair.datasets, PlanDataset{Key: dataset, InitialDepthDays: depth})
	}
	return repair, false, "", nil
}

func disablePagerDutyConfigs(ctx context.Context, tx pgx.Tx, orgID, integrationID string, now time.Time, reason string) error {
	_, err := tx.Exec(ctx, `
UPDATE public.sync_configurations
SET is_active=FALSE,last_sync_at=$3,last_sync_success=FALSE,last_sync_error=$4,
    last_sync_stats='{"error_category":"pagerduty_sync_disabled"}'::jsonb,updated_at=$3
WHERE org_id=$1 AND integration_id=$2::uuid AND lower(provider)='pagerduty'`, orgID, integrationID, now, reason)
	if err != nil {
		return fmt.Errorf("disable invalid PagerDuty sync configurations: %w", err)
	}
	return nil
}

func loadPlanLimits(ctx context.Context, tx pgx.Tx, orgID string, defaultUnitCap int) (*int, int, error) {
	if _, err := uuid.Parse(orgID); err != nil {
		value := 30
		return &value, defaultUnitCap, nil
	}
	var orgTier *string
	var licenseTier *string
	var overridesJSON []byte
	err := tx.QueryRow(ctx, `
SELECT coalesce(organization.tier,'community'),license.tier,license.limits_override::jsonb
FROM public.organizations AS organization
LEFT JOIN public.org_licenses AS license ON license.org_id=organization.id
WHERE organization.id=$1::uuid`, orgID).Scan(&orgTier, &licenseTier, &overridesJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, 0, ErrOccurrenceIneligible
	}
	if err != nil {
		return nil, 0, fmt.Errorf("load scheduled sync organization limits: %w", err)
	}
	resolvedTier := "community"
	if orgTier != nil {
		resolvedTier = *orgTier
	}
	if licenseTier != nil {
		resolvedTier = *licenseTier
	}
	if resolvedTier != "community" && resolvedTier != "team" && resolvedTier != "enterprise" {
		resolvedTier = "community"
	}
	var overrides map[string]json.RawMessage
	_ = json.Unmarshal(overridesJSON, &overrides)
	resolveOverride := func(key string) (*int, bool) {
		raw, ok := overrides[key]
		if !ok {
			return nil, false
		}
		if string(raw) == "null" {
			return nil, true
		}
		var value int
		if json.Unmarshal(raw, &value) != nil || value < 1 {
			return nil, false
		}
		return &value, true
	}
	backfill, backfillSet := resolveOverride("backfill_days")
	unitCap, unitCapSet := resolveOverride("max_sync_units")
	rows, err := tx.Query(ctx, `SELECT limit_key,limit_value FROM public.tier_limits WHERE tier=$1 AND limit_key IN ('backfill_days','max_sync_units')`, resolvedTier)
	if err != nil {
		return nil, 0, fmt.Errorf("load scheduled sync tier limits: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var value *string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, 0, err
		}
		if key == "backfill_days" && !backfillSet {
			backfill, backfillSet = parseOptionalPositiveInt(value)
		}
		if key == "max_sync_units" && !unitCapSet {
			unitCap, unitCapSet = parseOptionalPositiveInt(value)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	if !backfillSet {
		switch resolvedTier {
		case "community":
			value := 30
			backfill = &value
		case "team":
			value := 90
			backfill = &value
		}
	}
	totalCap := defaultUnitCap
	if unitCap != nil {
		totalCap = *unitCap
	}
	return backfill, totalCap, nil
}

// ResolveMaxSyncUnitsCap resolves the org's tier-based max_sync_units cap --
// the same organizations/org_licenses/tier_limits resolution loadPlanLimits
// already performs for scheduling, shared (not duplicated) for
// DispatchGuard's total-cap check (CHAOS-4175 family 3, port of
// guard.py's DispatchGuard.authorize_run / TierLimitService.get_limit).
//
// Callers porting Python's _resolve_total_unit_cap must NOT propagate a
// non-nil error as a hard failure: Python's version catches every exception
// (missing org, missing tier_limits table, a malformed override) and falls
// back to defaultCap unconditionally, by design (CHAOS-2580's savepoint
// discussion). loadPlanLimits, in contrast, treats a genuinely missing
// organizations row as ErrOccurrenceIneligible -- a meaningful signal for
// the SCHEDULER's own caller, but not one DispatchGuard's fallback-on-any-
// error semantics should adopt. A caller wanting Python parity should catch
// any error from this function and substitute defaultCap itself.
func ResolveMaxSyncUnitsCap(ctx context.Context, tx pgx.Tx, orgID string, defaultCap int) (int, error) {
	_, totalCap, err := loadPlanLimits(ctx, tx, orgID, defaultCap)
	if err != nil {
		return 0, err
	}
	return totalCap, nil
}

func parseOptionalPositiveInt(value *string) (*int, bool) {
	if value == nil {
		return nil, true
	}
	var parsed int
	if _, err := fmt.Sscanf(*value, "%d", &parsed); err != nil || parsed < 1 {
		return nil, false
	}
	return &parsed, true
}

func planDatasetsRequireCanonicalIncident(provider string, datasets []PlanDataset) bool {
	for _, dataset := range datasets {
		spec, ok := datasetSpecification(provider, dataset.Key)
		if !ok {
			continue
		}
		for _, target := range spec.LegacyTargets {
			if target == "incidents" || target == "operational" {
				return true
			}
		}
	}
	return false
}

// rowQuerier is the read surface both entitlement call sites share: the
// materializer holds a pgx.Tx, the scheduler Coordinator holds a
// HandoffTransaction, and this decision must not be implemented twice.
type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// FeatureDecisionReason mirrors licensing/feature_policy.py's
// FeatureDecisionReason StrEnum verbatim (all 13 values, closed vocabulary --
// pinned against Python's exact strings in
// canonical_incident_reason_oracle_test.go). canonicalIncidentDecision only
// ever produces a subset of these: registry.py's ORG_OVERRIDE_ONLY_FEATURES
// is empty and canonical_incident_ingestion is in neither
// _TIER_BOUND_OVERRIDE_FEATURES nor EXPLICIT_PURCHASE_FEATURES, so
// ORG_OVERRIDE_EXPIRED, ORG_OVERRIDE_REQUIRED, and EXPLICIT_PURCHASE_REQUIRED
// can never be decide_feature's answer for THIS feature key -- they are
// still declared here (rather than only the reachable subset) because the
// type is meant to mirror the source enum for the oracle pin, and because a
// future caller evaluating a different feature key through this same shape
// would need them.
type FeatureDecisionReason string

const (
	FeatureDecisionReasonEnabledByOrgOverride     FeatureDecisionReason = "enabled_by_org_override"
	FeatureDecisionReasonEnabledByLicenseOverride FeatureDecisionReason = "enabled_by_license_override"
	FeatureDecisionReasonEnabledByTier            FeatureDecisionReason = "enabled_by_tier"
	FeatureDecisionReasonFeatureNotRegistered     FeatureDecisionReason = "feature_not_registered"
	FeatureDecisionReasonGlobalDisabled           FeatureDecisionReason = "global_disabled"
	FeatureDecisionReasonInvalidFeatureState      FeatureDecisionReason = "invalid_feature_state"
	FeatureDecisionReasonStorageError             FeatureDecisionReason = "storage_error"
	FeatureDecisionReasonOrgOverrideExpired       FeatureDecisionReason = "org_override_expired"
	FeatureDecisionReasonOrgOverrideDisabled      FeatureDecisionReason = "org_override_disabled"
	FeatureDecisionReasonOrgOverrideRequired      FeatureDecisionReason = "org_override_required"
	FeatureDecisionReasonLicenseOverrideDisabled  FeatureDecisionReason = "license_override_disabled"
	FeatureDecisionReasonExplicitPurchaseRequired FeatureDecisionReason = "explicit_purchase_required"
	FeatureDecisionReasonTierRequired             FeatureDecisionReason = "tier_required"
)

// CanonicalIncidentAllowedForUpdate exports canonicalIncidentAllowedForUpdate
// for cross-package reuse (CHAOS-4175). rowQuerier's own doc comment already
// states the rule this satisfies: "this decision must not be implemented
// twice." The native Go port of run_sync_reference_discovery needs the exact
// row-locking entitlement check require_canonical_incident_feature_for_update_sync
// performs, and this IS that check -- same SQL (feature_flags ->
// org_feature_overrides -> organizations/org_licenses tier ranking),
// verified line-for-line against the Python before reuse rather than
// re-derived. Delegates to CanonicalIncidentDecisionForUpdate and discards
// the reason -- this wrapper's signature is unchanged so every existing
// caller (materializer.go, coordinator.go) stays byte-identical.
func CanonicalIncidentAllowedForUpdate(ctx context.Context, tx pgx.Tx, orgID string, now time.Time) (bool, error) {
	return canonicalIncidentAllowedForUpdate(ctx, tx, orgID, now)
}

// CanonicalIncidentDecisionForUpdate is CanonicalIncidentAllowedForUpdate's
// reason-carrying sibling (CHAOS-4175): a Go-only denial reason of just
// "false" loses exactly the diagnostic specificity CHAOS-4159/#1881 exists to
// preserve on the Python side (a generic label overwriting the planner's
// specific cause is the same regression class, one layer up -- here it would
// be the native gate erasing the answer's WHY instead of a planner result's
// WHY). Reuses the identical decision path CanonicalIncidentAllowedForUpdate
// already computes; the reason was always available internally; this only
// surfaces it.
func CanonicalIncidentDecisionForUpdate(ctx context.Context, tx pgx.Tx, orgID string, now time.Time) (bool, FeatureDecisionReason, error) {
	return canonicalIncidentDecision(ctx, tx, orgID, now, true)
}

// CanonicalIncidentAllowed is the NON-LOCKING export of the same decision
// (CHAOS-4209). It exists because the class rule this package already
// documents on canonicalIncidentAllowed below -- phase-B materialization
// locks, the phase-A gate does not -- had no exported form, so the CHAOS-4175
// native ports reached for the *ForUpdate pair and dragged a FOR UPDATE onto
// the DOMAIN pool.
//
// That is not a grant gap to be closed by granting. PostgreSQL treats FOR
// UPDATE as an UPDATE-class privilege, so satisfying it would mean giving the
// domain login UPDATE on public.feature_flags and public.org_feature_overrides
// -- letting the role that handles third-party provider payloads rewrite
// global feature enablement and tenant overrides. That is precisely the
// property the Option B role split exists to protect, and a doc comment
// promising "we only take the lock" would be a comment, not a mechanism.
//
// The rule, stated once: DOMAIN-SIDE GATES READ ENTITLEMENT WITHOUT LOCKING;
// ONLY THE COORDINATOR'S MATERIALIZER TAKES FOR UPDATE.
//
// Be precise about what is given up, because "we dropped a lock" is easy to
// wave away and this one is real. FOR UPDATE NARROWED this window; it never
// closed it. Under READ COMMITTED a locking read blocks on an administrator's
// UNCOMMITTED disable and then re-reads the new row version, so that one
// interleaving was refused; a plain read sees the old snapshot and proceeds.
// The immediately adjacent interleaving -- a disable committing just after the
// read -- was never refused by either form, because the external side effect
// (executor.Discover) happens past the commit that releases the lock. So the
// lock bought one interleaving, not the property. Closing the class needs a
// re-check at the seam that actually precedes the side effect, which is
// CHAOS-4219; this gate's job is to refuse runs that were disabled BEFORE it
// looked, and that it still does.
//
// The window this gives up -- the flag changing between this read and the
// caller's commit -- is bounded in both directions and neither direction
// strands a run:
//
//   - Flag flipped OFF after a claim: the run proceeds once, and
//     internal/syncdispatchruntime/feature_disabled_termination.go terminalizes
//     it on the next pass.
//   - Flag flipped ON after a disabled-terminalize: nothing is lost, the next
//     scheduled run for that organization proceeds normally.
//
// Divergence from Python is deliberate and worth naming: Python locks at every
// site (reference_discovery.py, sync_units.py:804 for dispatch, :1289/:1311 for
// the provider unit) via require_canonical_incident_feature_for_update_sync ->
// lock_feature_rows_sync. It can afford to, because Python runs every one of
// those under ONE full-privilege role. Go does not, and the two-phase split
// this package already documents is what replaces the lock on the domain side.
func CanonicalIncidentAllowed(ctx context.Context, tx pgx.Tx, orgID string, now time.Time) (bool, error) {
	return canonicalIncidentAllowed(ctx, tx, orgID, now, false)
}

// CanonicalIncidentDecision is CanonicalIncidentAllowed's reason-carrying
// sibling, the non-locking counterpart of CanonicalIncidentDecisionForUpdate.
// A caller that terminalizes on denial needs the WHY, not just the false --
// see CanonicalIncidentDecisionForUpdate's doc comment for why the reason is
// load-bearing rather than cosmetic.
//
// Signature is identical to the ForUpdate pair so a caller moving off the lock
// changes the function name and nothing else.
func CanonicalIncidentDecision(ctx context.Context, tx pgx.Tx, orgID string, now time.Time) (bool, FeatureDecisionReason, error) {
	return canonicalIncidentDecision(ctx, tx, orgID, now, false)
}

// canonicalIncidentAllowedForUpdate is the phase-B, row-locking form.
func canonicalIncidentAllowedForUpdate(ctx context.Context, tx pgx.Tx, orgID string, now time.Time) (bool, error) {
	return canonicalIncidentAllowed(ctx, tx, orgID, now, true)
}

// canonicalIncidentAllowed resolves the canonical-incident entitlement for an
// organization. `lockRows` selects the phase it is being asked for: phase B
// materialization takes FOR UPDATE so the decision cannot change underneath the
// plan it authorizes, while the phase-A scheduler gate reads WITHOUT locking --
// mirroring Python, which uses the non-locking
// is_canonical_incident_feature_enabled_sync before minting
// (workers/sync_scheduler.py:207-219) and the locking
// require_canonical_incident_feature_for_update_sync only at materialization
// (sync/execution_trigger.py:348-355). Locking the global feature_flags row on
// every scheduler window would serialize every replica against one row.
// Thin wrapper over canonicalIncidentDecision, discarding the reason -- both
// existing call sites (coordinator.go, materializer.go) only ever needed the
// bool.
func canonicalIncidentAllowed(ctx context.Context, tx rowQuerier, orgID string, now time.Time, lockRows bool) (bool, error) {
	allowed, _, err := canonicalIncidentDecision(ctx, tx, orgID, now, lockRows)
	return allowed, err
}

// canonicalIncidentDecision is canonicalIncidentAllowed's full form: every
// return point below is annotated with the FeatureDecisionReason
// decide_feature (feature_policy.py) would produce for the identical input
// shape, for the reachable subset described on FeatureDecisionReason's doc
// comment. The bool/error control flow is UNCHANGED from before this
// reason was added -- this is a mechanical surfacing of an already-computed
// answer, not a new decision path.
func canonicalIncidentDecision(ctx context.Context, tx rowQuerier, orgID string, now time.Time, lockRows bool) (bool, FeatureDecisionReason, error) {
	if _, err := uuid.Parse(orgID); err != nil {
		// Python: require_canonical_incident_feature_for_update_sync's own
		// `except ValueError` catch on an unparseable org_id, before
		// evaluate_org_feature_sync is ever called.
		return false, FeatureDecisionReasonInvalidFeatureState, nil
	}
	lockClause := ""
	if lockRows {
		lockClause = "\nFOR UPDATE"
	}
	var featureID, minTier string
	var globallyEnabled bool
	err := tx.QueryRow(ctx, `
SELECT id::text,min_tier,is_enabled
FROM public.feature_flags
WHERE key='canonical_incident_ingestion'`+lockClause).Scan(&featureID, &minTier, &globallyEnabled)
	if errors.Is(err, pgx.ErrNoRows) {
		// Python: features_by_key.get(feature_key) is None -> is_registered=False.
		return false, FeatureDecisionReasonFeatureNotRegistered, nil
	}
	if err != nil {
		return false, "", fmt.Errorf("lock canonical incident feature: %w", err)
	}
	tierRank := map[string]int{"community": 0, "team": 1, "enterprise": 2}
	minimumRank, validMinimum := tierRank[minTier]
	if !validMinimum {
		// Python: LicenseTier(str(feature.min_tier)) raises ValueError ->
		// is_storage_valid=False, checked FIRST in decide_feature.
		return false, FeatureDecisionReasonInvalidFeatureState, nil
	}
	if !globallyEnabled {
		return false, FeatureDecisionReasonGlobalDisabled, nil
	}
	var overrideEnabled *bool
	var overrideExpires *time.Time
	err = tx.QueryRow(ctx, `
SELECT is_enabled,expires_at
FROM public.org_feature_overrides
WHERE org_id=$1::uuid AND feature_id=$2::uuid`+lockClause, orgID, featureID).Scan(&overrideEnabled, &overrideExpires)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, "", fmt.Errorf("lock canonical incident override: %w", err)
	}
	if overrideEnabled != nil && (overrideExpires == nil || overrideExpires.After(now.UTC())) {
		// Python's ORG_OVERRIDE_EXPIRED/ORG_OVERRIDE_REQUIRED and the
		// _TIER_BOUND_OVERRIDE_FEATURES-gated TIER_REQUIRED branch here are
		// unreachable for canonical_incident_ingestion -- see
		// FeatureDecisionReason's doc comment.
		if *overrideEnabled {
			return true, FeatureDecisionReasonEnabledByOrgOverride, nil
		}
		return false, FeatureDecisionReasonOrgOverrideDisabled, nil
	}
	var orgTier *string
	var licenseTier *string
	var licenseFeatures []byte
	err = tx.QueryRow(ctx, `
SELECT coalesce(organization.tier,'community'),license.tier,license.features_override::jsonb
FROM public.organizations AS organization
LEFT JOIN public.org_licenses AS license ON license.org_id=organization.id
WHERE organization.id=$1::uuid`, orgID).Scan(&orgTier, &licenseTier, &licenseFeatures)
	if errors.Is(err, pgx.ErrNoRows) {
		// No Python-observable equivalent: evaluate_org_feature_sync has no
		// "organization row missing" branch (org_tier/license resolution
		// fails soft to community there). Labeled INVALID_FEATURE_STATE as
		// the closest existing reason for "entitlement data could not be
		// resolved" -- a pre-existing behavior difference (this function
		// denies outright; Python would likely still evaluate tier), not
		// something this change alters.
		return false, FeatureDecisionReasonInvalidFeatureState, nil
	}
	if err != nil {
		return false, "", fmt.Errorf("load canonical incident entitlement: %w", err)
	}
	if len(licenseFeatures) > 0 {
		var overrides map[string]bool
		if json.Unmarshal(licenseFeatures, &overrides) == nil {
			if allowed, ok := overrides["canonical_incident_ingestion"]; ok {
				// Python's ORG_OVERRIDE_REQUIRED and TIER_REQUIRED (via
				// _TIER_BOUND_OVERRIDE_FEATURES) branches here are likewise
				// unreachable for this feature key.
				if allowed {
					return true, FeatureDecisionReasonEnabledByLicenseOverride, nil
				}
				return false, FeatureDecisionReasonLicenseOverrideDisabled, nil
			}
		}
	}
	resolvedTier := "community"
	if orgTier != nil {
		resolvedTier = *orgTier
	}
	if licenseTier != nil {
		resolvedTier = *licenseTier
	}
	orgRank, validOrgTier := tierRank[resolvedTier]
	if !validOrgTier {
		orgRank = tierRank["community"]
	}
	if orgRank >= minimumRank {
		return true, FeatureDecisionReasonEnabledByTier, nil
	}
	return false, FeatureDecisionReasonTierRequired, nil
}

// loadPlanSources loads the enabled sources a plan runs against.
// explicitSourceIDs is CHAOS-4602's manual/backfill selector
// (sync_manual_triggers.source_ids, mirroring Python's
// SyncPlanRequest.source_ids/_load_enabled_sources): nil means "no explicit
// selection", so every planner-managed source still qualifies; a non-nil
// (possibly single-element) slice narrows to exactly those ids, matching
// Python's own "empty selector plans zero sources" semantics when it is
// non-nil but empty. It is layered UNDER, never combined with, the
// config's own sourceID scope: a legacy per-source child config's
// sourceID always wins when set, exactly as it did before this parameter
// existed.
func loadPlanSources(ctx context.Context, tx pgx.Tx, orgID, integrationID, configID string, sourceID *string, explicitSourceIDs []string, plannerManaged bool) ([]PlanSource, error) {
	var idFilter []string
	switch {
	case sourceID != nil:
		idFilter = []string{*sourceID}
	case explicitSourceIDs != nil:
		idFilter = explicitSourceIDs
	}
	// metadata/sync_options are read here (not on the pure planner's own
	// path -- it has no DB access at all) purely to resolve
	// PlanSource.NonProjectJiraSource (CHAOS-4582/CHAOS-4602): a LEFT JOIN
	// against the config metadata->>'planner_managed_sync_config_id'
	// references, mirroring Python's _is_non_project_jira_source's own
	// per-source SyncConfiguration lookup.
	rows, err := tx.Query(ctx, `
SELECT src.id::text, src.external_id, lower(src.provider), src.full_name,
       src.metadata::jsonb, cfg.sync_options::jsonb
FROM public.integration_sources AS src
LEFT JOIN public.sync_configurations AS cfg
  ON cfg.id::text = src.metadata->>'planner_managed_sync_config_id'
WHERE src.org_id = $1 AND src.integration_id = $2::uuid AND src.is_enabled
  AND ($3::text[] IS NULL OR src.id::text = ANY($3::text[]))
  AND ($3::text[] IS NOT NULL OR NOT $4 OR src.metadata->>'planner_managed_sync_config_id'=$5)
ORDER BY src.full_name, src.id`, orgID, integrationID, idFilter, plannerManaged, configID)
	if err != nil {
		return nil, fmt.Errorf("load scheduled sync sources: %w", err)
	}
	defer rows.Close()
	var result []PlanSource
	for rows.Next() {
		var source PlanSource
		var metadataJSON, syncOptionsJSON []byte
		if err := rows.Scan(
			&source.ID, &source.ExternalID, &source.Provider, &source.FullName,
			&metadataJSON, &syncOptionsJSON,
		); err != nil {
			return nil, fmt.Errorf("scan scheduled sync source: %w", err)
		}
		if source.Provider == "jira" {
			var metadata, syncOptions map[string]any
			if len(metadataJSON) > 0 {
				if err := json.Unmarshal(metadataJSON, &metadata); err != nil {
					return nil, fmt.Errorf("decode source metadata: %w", err)
				}
			}
			if len(syncOptionsJSON) > 0 {
				if err := json.Unmarshal(syncOptionsJSON, &syncOptions); err != nil {
					return nil, fmt.Errorf("decode referenced config sync options: %w", err)
				}
			}
			source.NonProjectJiraSource = isNonProjectJiraSource(source.ExternalID, metadata, syncOptions)
		}
		result = append(result, source)
	}
	return result, rows.Err()
}

// isNonProjectJiraSource ports Python's _is_non_project_jira_source
// (src/dev_health_ops/sync/planner.py, CHAOS-4582) verbatim, in precedence
// order:
//
//  1. metadata["explicit_project_scope"] truthy -> NOT bad (the writer's
//     own marker for a NEW row created from an explicit project_key/id).
//     Always wins.
//  2. metadata["org_wide_placeholder"] truthy -> IS bad (Linear's org-wide
//     marker, recognized in case a future jira writer reuses it).
//  3. external_id (case-insensitive) != "jira" -> NOT bad: a real project
//     legitimately named something else.
//  4. external_id == "jira": look at the REFERENCED config's own
//     sync_options (already resolved by loadPlanSources' LEFT JOIN) for an
//     explicit project_id/project_key/team_id/repo, in that precedence --
//     if that value itself is (case-insensitively) "jira", this really is a
//     project named "JIRA", not the fallback. syncOptions is nil when the
//     source's planner_managed_sync_config_id doesn't resolve (dangling
//     reference), matching Python's `if config is not None:` guard.
//  5. Otherwise -> IS bad: the known-bad legacy shape (live evidence, org
//     70d529e0, CHAOS-4582).
func isNonProjectJiraSource(externalID string, metadata, syncOptions map[string]any) bool {
	if jsonTruthy(metadata["explicit_project_scope"]) {
		return false
	}
	if jsonTruthy(metadata["org_wide_placeholder"]) {
		return true
	}
	if !strings.EqualFold(strings.TrimSpace(externalID), "jira") {
		return false
	}
	if explicit := firstTruthyString(syncOptions, "project_id", "project_key", "team_id", "repo"); explicit != "" {
		if strings.EqualFold(strings.TrimSpace(explicit), "jira") {
			return false
		}
	}
	return true
}

// jsonTruthy mirrors Python's bare `if value:` truthiness on a value
// decoded from JSON: nil/false/""/0/empty-collection are falsy, everything
// else (including a non-empty string, a non-zero number, `true`) is truthy.
func jsonTruthy(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case string:
		return typed != ""
	case float64:
		return typed != 0
	case []any:
		return len(typed) > 0
	case map[string]any:
		return len(typed) > 0
	default:
		return true
	}
}

// firstTruthyString mirrors Python's `a.get(k1) or a.get(k2) or ...` chain:
// the first key (in order) whose value is JSON-truthy, stringified.
// Returns "" if options is nil or every key is absent/falsy.
func firstTruthyString(options map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := options[key]; ok && jsonTruthy(value) {
			return fmt.Sprintf("%v", value)
		}
	}
	return ""
}

func requestedDatasetKeys(provider string, targets []string, sourceID *string) map[string]bool {
	// Python's plan_request_for_config leaves parent dataset scope unset. Child
	// configs derive dataset keys only from the provider registry's legacy
	// targets; an empty mapping deliberately falls back to all enabled datasets.
	if sourceID == nil || len(targets) == 0 {
		return nil
	}
	requested := make(map[string]bool)
	for _, target := range targets {
		target = strings.ToLower(strings.TrimSpace(target))
		for dataset := range supportedProviderDatasets[provider] {
			if spec, ok := datasetSpecification(provider, dataset); ok {
				for _, candidate := range spec.LegacyTargets {
					if candidate == target {
						requested[dataset] = true
					}
				}
			}
		}
	}
	if len(requested) == 0 {
		return nil
	}
	return requested
}

// SyncTargetsRequireCanonicalIncident exports syncTargetsRequireCanonicalIncident
// for cross-package reuse (CHAOS-4175): the native reference_discovery port
// needs the same "does this unit's legacy targets require the canonical
// incident feature" check sync_dataset_requires_canonical_incident_feature
// makes in Python, and the answer must not diverge between the two Go
// callers of this decision.
func SyncTargetsRequireCanonicalIncident(targets []string) bool {
	return syncTargetsRequireCanonicalIncident(targets)
}

func syncTargetsRequireCanonicalIncident(targets []string) bool {
	for _, target := range targets {
		target = strings.ToLower(strings.TrimSpace(target))
		if target == "incidents" || target == "operational" {
			return true
		}
	}
	return false
}

// loadPlanDatasets loads the enabled datasets a plan runs against.
// explicitDatasetKeys is CHAOS-4602's manual/backfill selector
// (sync_manual_triggers.dataset_keys, mirroring Python's
// SyncPlanRequest.dataset_keys/_load_enabled_datasets): nil falls back to
// the legacy-target-derived scope requestedDatasetKeys already computed
// (unchanged behavior for every ordinary scheduled occurrence); a non-nil
// slice is a direct dataset_key allowlist with NO legacy-target mapping and
// NO auto security-forcing, matching _load_enabled_datasets' plain
// dataset_key IN (...) filter exactly -- an explicit selector states
// precisely what it wants, and does not get security silently added.
func loadPlanDatasets(ctx context.Context, tx pgx.Tx, domainPool *pgxpool.Pool, orgID, integrationID, provider string, targets []string, sourceID *string, explicitDatasetKeys []string) ([]PlanDataset, bool, error) {
	var requested map[string]bool
	securityRequested := false
	if explicitDatasetKeys != nil {
		requested = make(map[string]bool, len(explicitDatasetKeys))
		for _, key := range explicitDatasetKeys {
			requested[strings.ToLower(strings.TrimSpace(key))] = true
		}
		if len(requested) == 0 {
			// Explicit EMPTY selector: plan zero datasets, mirroring Python's
			// `if not dataset_keys: return []` in _load_enabled_datasets.
			return nil, false, nil
		}
		// codex review finding (gate round): Python's
		// _reconcile_explicit_requested_datasets (planner.py:858-904)
		// enables any explicitly requested dataset that has no
		// integration_datasets row yet -- "every dataset ... is opt-in and
		// reconciled uniformly here: a caller (operator, backfill, or
		// scheduled trigger) must name a dataset in request.dataset_keys
		// for it to be created/enabled." Without this, a manual/backfill
		// selector naming a dataset the config has never enabled before
		// silently plans ZERO units for it instead of creating the
		// requested work.
		//
		// MUST run on domainPool, never on tx (the COORDINATOR
		// transaction): the coordinator role's grant on integration_datasets
		// is SELECT-only (domain_authorization.go's coordinatorPosture --
		// domain has INSERT+UPDATE; coordinator does not), so an INSERT
		// here on tx would hit a Postgres insufficient-privilege error in
		// production, invisible in this package's own single-role test
		// fixtures. Matches upsertSources' own existing pattern
		// (source_discovery.go): a short-lived, separately committed
		// domain-pool transaction, ON CONFLICT DO NOTHING (no savepoint
		// needed -- this row's id is a fresh random UUID per attempt, never
		// deterministic, so a concurrent replay can only ever conflict on
		// the (org_id,integration_id,dataset_key) unique constraint the ON
		// CONFLICT clause already arbitrates). Committed before the
		// SELECT below (still on tx, coordinator-side, SELECT-only is
		// sufficient there) runs, so it sees the newly ensured row.
		for key := range requested {
			if _, ok := datasetSpecification(provider, key); !ok {
				continue
			}
			if err := ensureExplicitlyRequestedDataset(ctx, domainPool, orgID, integrationID, key); err != nil {
				return nil, false, fmt.Errorf("reconcile explicitly requested dataset %s: %w", key, err)
			}
		}
	} else {
		requested = requestedDatasetKeys(provider, targets, sourceID)
		if provider == "github" || provider == "gitlab" {
			securityRequested = true
			if requested != nil {
				requested["security"] = true
			}
		}
	}
	rows, err := tx.Query(ctx, `
SELECT dataset_key,is_enabled,options::jsonb
FROM public.integration_datasets
WHERE org_id = $1 AND integration_id = $2::uuid
ORDER BY dataset_key`, orgID, integrationID)
	if err != nil {
		return nil, false, fmt.Errorf("load scheduled sync datasets: %w", err)
	}
	defer rows.Close()
	var result []PlanDataset
	securityExists := false
	for rows.Next() {
		var key string
		var enabled bool
		var raw []byte
		if err := rows.Scan(&key, &enabled, &raw); err != nil {
			return nil, false, err
		}
		if key == "security" {
			securityExists = true
		}
		if !enabled || (requested != nil && !requested[key]) {
			continue
		}
		var option struct {
			InitialSyncDepth int `json:"initial_sync_depth"`
		}
		if err := json.Unmarshal(raw, &option); err != nil {
			return nil, false, fmt.Errorf("decode dataset %s options: %w", key, err)
		}
		var depth *int
		if option.InitialSyncDepth > 0 {
			depth = &option.InitialSyncDepth
		}
		result = append(result, PlanDataset{Key: key, InitialDepthDays: depth})
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	ensureSecurity := securityRequested && !securityExists
	if ensureSecurity {
		result = append(result, PlanDataset{Key: "security"})
	}
	return result, ensureSecurity, nil
}

// ensureExplicitlyRequestedDataset inserts one enabled integration_datasets
// row for an explicitly requested dataset key that has no row yet,
// idempotently, on domainPool -- never on the coordinator transaction (see
// loadPlanDatasets' own call site comment for why: coordinatorPosture grants
// integration_datasets SELECT only). Uses its own short-lived transaction,
// separate from and committed before the caller's coordinator transaction
// continues, so that transaction's own SELECT sees this row.
func ensureExplicitlyRequestedDataset(ctx context.Context, domainPool *pgxpool.Pool, orgID, integrationID, datasetKey string) error {
	domainTx, err := domainPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin ensure-dataset domain transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = domainTx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	if _, err := domainTx.Exec(ctx, `
INSERT INTO public.integration_datasets (id,org_id,integration_id,dataset_key,is_enabled,options)
VALUES ($1::uuid,$2,$3::uuid,$4,TRUE,'{}'::jsonb)
ON CONFLICT (org_id,integration_id,dataset_key) DO NOTHING`,
		uuid.New().String(), orgID, integrationID, datasetKey); err != nil {
		return err
	}
	if err := domainTx.Commit(ctx); err != nil {
		return fmt.Errorf("commit ensure-dataset domain transaction: %w", err)
	}
	committed = true
	return nil
}

func loadPlanWatermarks(ctx context.Context, tx pgx.Tx, orgID string, sources []PlanSource, datasets []PlanDataset) (map[WatermarkKey]time.Time, error) {
	result := make(map[WatermarkKey]time.Time)
	rows, err := tx.Query(ctx, `
SELECT source_id,dataset_key,repo_id,target,last_synced_at
FROM public.sync_watermarks
WHERE org_id=$1 AND last_synced_at IS NOT NULL`, orgID)
	if err != nil {
		return nil, fmt.Errorf("load scheduled sync watermarks: %w", err)
	}
	defer rows.Close()
	type watermarkRow struct {
		sourceID string
		dataset  string
		repoID   string
		target   string
		at       time.Time
	}
	var loaded []watermarkRow
	for rows.Next() {
		var row watermarkRow
		if err := rows.Scan(&row.sourceID, &row.dataset, &row.repoID, &row.target, &row.at); err != nil {
			return nil, err
		}
		loaded = append(loaded, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, source := range sources {
		for _, dataset := range datasets {
			key := WatermarkKey{SourceID: source.ExternalID, Dataset: dataset.Key}
			for _, row := range loaded {
				if row.sourceID == source.ExternalID && row.dataset == dataset.Key {
					result[key] = row.at.UTC()
					break
				}
			}
			if _, ok := result[key]; ok {
				continue
			}
			for _, row := range loaded {
				if row.repoID == source.ExternalID && row.target == dataset.Key {
					result[key] = row.at.UTC()
					break
				}
			}
			if _, ok := result[key]; ok {
				continue
			}
			for _, legacy := range legacyTargetsByDataset[dataset.Key] {
				for _, row := range loaded {
					if row.repoID == source.ExternalID && row.target == legacy && row.dataset == legacy {
						result[key] = row.at.UTC()
						break
					}
				}
				if _, ok := result[key]; ok {
					break
				}
			}
		}
	}
	return result, nil
}

func applyDomainPlanMutations(ctx context.Context, tx pgx.Tx, loaded loadedMaterializationPlan, now time.Time) error {
	if loaded.ensureSecurityDataset {
		// CHAOS-4203: the ON CONFLICT clause below only arbitrates the
		// (org_id,integration_id,dataset_key) unique constraint. This row's
		// id is ALSO fully deterministic (derived from IntegrationID plus a
		// fixed "security" suffix), so two concurrent replays of the same
		// occurrence race on the PRIMARY KEY too -- a conflict Postgres
		// does not silently resolve for a non-arbiter index, even though
		// it is the exact same logical "already ensured" outcome ON
		// CONFLICT DO NOTHING exists to produce. A raw 23505 on either of
		// this INSERT's two unique constraints (integration_datasets_pkey
		// or uq_integration_datasets_org_integration_dataset, alembic
		// 0015_add_integration_data_model.py) means a concurrent replay
		// already inserted this exact row; treat it like the DO NOTHING
		// branch. Any OTHER constraint name is a genuine, unrelated
		// failure and must still surface -- deliberately not a bare
		// SQLSTATE check, so a future unique constraint on this table
		// cannot be silently swallowed by accident.
		//
		// The failing INSERT must run inside its own savepoint (a nested
		// pgx.Tx): Postgres poisons the whole enclosing transaction after
		// any unhandled statement error, so swallowing the error in Go
		// without also rolling back to a savepoint would make the later
		// domain-graph writes and the outer commit fail instead (matches
		// native_finalize_sync_run.go's identical savepoint-then-swallow
		// precedent for a once-only insert).
		savepoint, err := tx.Begin(ctx)
		if err != nil {
			return fmt.Errorf("ensure scheduled security dataset: begin savepoint: %w", err)
		}
		_, execErr := savepoint.Exec(ctx, `
INSERT INTO public.integration_datasets (id,org_id,integration_id,dataset_key,is_enabled,options)
VALUES ($1::uuid,$2,$3::uuid,'security',TRUE,'{"auto_enabled_by":"scheduled_code_host_sync"}'::jsonb)
ON CONFLICT (org_id,integration_id,dataset_key) DO NOTHING`, uuid.NewSHA1(materializerNamespace, []byte(loaded.input.IntegrationID+":dataset:security")).String(), loaded.input.OrgID, loaded.input.IntegrationID)
		if execErr != nil {
			var pgErr *pgconn.PgError
			alreadyEnsured := errors.As(execErr, &pgErr) && pgErr.Code == "23505" &&
				(pgErr.ConstraintName == "integration_datasets_pkey" ||
					pgErr.ConstraintName == "uq_integration_datasets_org_integration_dataset")
			if !alreadyEnsured {
				_ = savepoint.Rollback(ctx)
				return fmt.Errorf("ensure scheduled security dataset: %w", execErr)
			}
			if err := savepoint.Rollback(ctx); err != nil {
				return fmt.Errorf("ensure scheduled security dataset: recover from concurrent replay: %w", err)
			}
		} else if err := savepoint.Commit(ctx); err != nil {
			return fmt.Errorf("ensure scheduled security dataset: %w", err)
		}
	}
	repair := loaded.pagerDutyRepair
	if repair == nil {
		return nil
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO public.integration_sources
 (id,org_id,integration_id,provider,source_type,external_id,name,full_name,metadata,is_enabled,discovered_at,last_seen_at)
VALUES ($1::uuid,$2,$3::uuid,'pagerduty','account',$4,$4,$4,'{}'::jsonb,TRUE,$5,$5)
ON CONFLICT (org_id,integration_id,provider,external_id) DO UPDATE
SET source_type='account',name=EXCLUDED.name,full_name=EXCLUDED.full_name,is_enabled=TRUE,last_seen_at=EXCLUDED.last_seen_at`, repair.source.ID, loaded.input.OrgID, loaded.input.IntegrationID, repair.source.ExternalID, now); err != nil {
		return fmt.Errorf("repair PagerDuty canonical source: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE public.integration_sources SET is_enabled=FALSE WHERE org_id=$1 AND integration_id=$2::uuid AND lower(provider)='pagerduty' AND external_id<>$3 AND is_enabled`, loaded.input.OrgID, loaded.input.IntegrationID, repair.source.ExternalID); err != nil {
		return fmt.Errorf("disable stale PagerDuty sources: %w", err)
	}
	wanted := make([]string, 0, len(repair.datasets))
	for _, dataset := range repair.datasets {
		wanted = append(wanted, dataset.Key)
		datasetID := uuid.NewSHA1(materializerNamespace, []byte(loaded.input.IntegrationID+":dataset:"+dataset.Key)).String()
		if _, err := tx.Exec(ctx, `
INSERT INTO public.integration_datasets (id,org_id,integration_id,dataset_key,is_enabled,options)
VALUES ($1::uuid,$2,$3::uuid,$4,TRUE,$5::jsonb)
ON CONFLICT (org_id,integration_id,dataset_key) DO UPDATE SET is_enabled=TRUE,options=EXCLUDED.options`, datasetID, loaded.input.OrgID, loaded.input.IntegrationID, dataset.Key, repair.datasetOptions[dataset.Key]); err != nil {
			return fmt.Errorf("repair PagerDuty dataset %s: %w", dataset.Key, err)
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE public.integration_datasets SET is_enabled=FALSE WHERE org_id=$1 AND integration_id=$2::uuid AND NOT (dataset_key=ANY($3::text[])) AND is_enabled`, loaded.input.OrgID, loaded.input.IntegrationID, wanted); err != nil {
		return fmt.Errorf("disable stale PagerDuty datasets: %w", err)
	}
	return nil
}

func persistDomainGraph(ctx context.Context, tx pgx.Tx, ids materializationIDs, loaded loadedMaterializationPlan, units []PlannedUnit, createdAt time.Time) error {
	// Replay occurs only for the crash window after this domain graph commits
	// but before the coordinator readiness fence commits. Both reconcilers now
	// exclude such scheduled graphs, so every lifecycle field must still equal
	// its initialized value. Once the fence commits, the occurrence link makes
	// the reconciler return the existing plan without calling this verifier.
	expectedStatus := "planned"
	var completedAt *time.Time
	var resultJSON []byte
	var runError *string
	if loaded.terminalReason != "" {
		expectedStatus = "failed"
		completedAt = pointerTime(createdAt)
		runError = &loaded.terminalReason
		resultJSON = []byte(`{"error_category":"pagerduty_sync_disabled"}`)
	}
	triggeredByStamp := loaded.triggeredBy
	if triggeredByStamp == "" {
		// Defensive default only: every loadMaterializationPlan return path
		// sets this explicitly. An empty value here would silently persist
		// an unlabeled sync_runs row rather than the CHAOS-4602 acceptance
		// proof's required 'schedule'/'manual'/'backfill' stamp.
		triggeredByStamp = "schedule"
	}
	_, err := tx.Exec(ctx, `
INSERT INTO public.sync_runs
 (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units,
  credential_id, credential_fingerprint, auth_source,completed_at,result,error,created_at)
VALUES ($1::uuid,$2,$3::uuid,$4,$5,$6,$7,0,0,$8::uuid,NULL,$9,$10,$11::jsonb,$12,$13)
ON CONFLICT (id) DO NOTHING`, ids.SyncRunID, loaded.input.OrgID, loaded.input.IntegrationID, triggeredByStamp,
		loaded.input.Mode, expectedStatus, len(units), loaded.credentialID, loaded.authSource, completedAt, resultJSON, runError, createdAt)
	if err != nil {
		return fmt.Errorf("persist scheduled sync run: %w", err)
	}
	var org, integration, triggeredBy, mode, status string
	var total, completed, failed int
	var credential, auth, fingerprint, persistedError *string
	var persistedStartedAt, persistedCompletedAt *time.Time
	var persistedCreatedAt time.Time
	var persistedResult []byte
	if err := tx.QueryRow(ctx, `SELECT org_id,integration_id::text,triggered_by,mode,status,total_units,completed_units,failed_units,credential_id::text,auth_source,credential_fingerprint,started_at,completed_at,result::jsonb,error,created_at FROM public.sync_runs WHERE id=$1::uuid`, ids.SyncRunID).Scan(&org, &integration, &triggeredBy, &mode, &status, &total, &completed, &failed, &credential, &auth, &fingerprint, &persistedStartedAt, &persistedCompletedAt, &persistedResult, &persistedError, &persistedCreatedAt); err != nil {
		return fmt.Errorf("verify scheduled sync run: %w", err)
	}
	if org != loaded.input.OrgID || integration != loaded.input.IntegrationID || triggeredBy != triggeredByStamp || mode != loaded.input.Mode || status != expectedStatus || total != len(units) || completed != 0 || failed != 0 || !equalOptionalString(credential, loaded.credentialID) || !equalOptionalString(auth, loaded.authSource) || fingerprint != nil || persistedStartedAt != nil || !equalOptionalTime(persistedCompletedAt, completedAt) || !equalOptionalString(persistedError, runError) || !equalOptionalJSON(persistedResult, resultJSON) || !persistedCreatedAt.Equal(createdAt) {
		return fmt.Errorf("%w: deterministic sync run identity maps to different state", ErrInvalidPlan)
	}
	type expectedUnit struct {
		unit  PlannedUnit
		flags map[string]bool
	}
	expectedUnits := make(map[string]expectedUnit, len(units))
	batch := &pgx.Batch{}
	for ordinal, unit := range units {
		unitID, err := deterministicUnitID(ids.SyncRunID, ordinal)
		if err != nil {
			return err
		}
		flags, err := json.Marshal(unit.ProcessorFlags)
		if err != nil {
			return err
		}
		batch.Queue(`
INSERT INTO public.sync_run_units
 (id,org_id,sync_run_id,integration_id,source_id,provider,dataset_key,cost_class,mode,since_at,before_at,status,attempts,processor_flags,created_at,updated_at)
VALUES ($1::uuid,$2,$3::uuid,$4::uuid,$5::uuid,$6,$7,$8,$9,$10,$11,'planned',0,$12::jsonb,$13,$13)
ON CONFLICT (id) DO NOTHING`, unitID, unit.OrgID, ids.SyncRunID, unit.IntegrationID, unit.SourceID, unit.Provider,
			unit.Dataset, unit.CostClass, unit.Mode, unit.WindowStart, unit.WindowEnd, flags, createdAt)
		expectedUnits[unitID] = expectedUnit{unit: unit, flags: unit.ProcessorFlags}
	}
	results := tx.SendBatch(ctx, batch)
	for ordinal := range units {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return fmt.Errorf("persist scheduled sync unit %d: %w", ordinal, err)
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("finish scheduled sync unit batch: %w", err)
	}
	// CHAOS-4114: every pair this plan just minted a row for is ATTEMPTED
	// from this instant, and the durable ledger has to say so in the SAME
	// transaction the rows commit in. "Attempted" is a statement about row
	// existence, not about terminalization -- the evidence query it replaces
	// had no WHERE clause at all -- so recording it any later would leave a
	// window where the gate reads a freshly-planned pair as never-attempted
	// and bootstraps it through. That is the fail-OPEN direction, which is
	// the one direction this gate must never take.
	providers := make([]string, 0, len(units))
	datasets := make([]string, 0, len(units))
	for _, unit := range units {
		providers = append(providers, unit.Provider)
		datasets = append(datasets, unit.Dataset)
	}
	if err := providersync.RecordExecutedProofAttempted(
		ctx, tx, providers, datasets, createdAt,
	); err != nil {
		return fmt.Errorf("record scheduled sync unit executed-proof attempts: %w", err)
	}
	rows, err := tx.Query(ctx, `
SELECT id::text,org_id,sync_run_id::text,integration_id::text,source_id::text,provider,dataset_key,
       cost_class,mode,since_at,before_at,status,attempts,available_at,rate_limit_deferrals,
       rate_limit_first_seen_at,expired_lease_retry_count,last_retry_reason,retry_exhausted_at,
       duration_seconds,error,result::jsonb,processor_flags::jsonb,lease_owner,lease_expires_at,
       last_heartbeat_at,created_at,updated_at
FROM public.sync_run_units WHERE sync_run_id=$1::uuid`, ids.SyncRunID)
	if err != nil {
		return fmt.Errorf("verify scheduled sync units: %w", err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var id, org, runID, integrationID, sourceID, provider, dataset, cost, mode, status string
		var since, before, availableAt, rateLimitFirstSeenAt, retryExhaustedAt *time.Time
		var leaseExpiresAt, lastHeartbeatAt *time.Time
		var attempts, rateLimitDeferrals, expiredLeaseRetryCount int
		var durationSeconds *int
		var lastRetryReason, unitError, leaseOwner *string
		var resultJSON, flagsJSON []byte
		var persistedCreatedAt, persistedUpdatedAt time.Time
		if err := rows.Scan(
			&id, &org, &runID, &integrationID, &sourceID, &provider, &dataset, &cost, &mode,
			&since, &before, &status, &attempts, &availableAt, &rateLimitDeferrals,
			&rateLimitFirstSeenAt, &expiredLeaseRetryCount, &lastRetryReason, &retryExhaustedAt,
			&durationSeconds, &unitError, &resultJSON, &flagsJSON, &leaseOwner, &leaseExpiresAt,
			&lastHeartbeatAt, &persistedCreatedAt, &persistedUpdatedAt,
		); err != nil {
			return err
		}
		expected, ok := expectedUnits[id]
		if !ok {
			return fmt.Errorf("%w: deterministic sync run contains unexpected unit %s", ErrInvalidPlan, id)
		}
		var flags map[string]bool
		if err := json.Unmarshal(flagsJSON, &flags); err != nil {
			return err
		}
		unit := expected.unit
		if org != unit.OrgID || runID != ids.SyncRunID || integrationID != unit.IntegrationID || sourceID != unit.SourceID || provider != unit.Provider || dataset != unit.Dataset || cost != unit.CostClass || mode != unit.Mode || !equalOptionalTime(since, unit.WindowStart) || !equalOptionalTime(before, unit.WindowEnd) || status != "planned" || attempts != 0 || availableAt != nil || rateLimitDeferrals != 0 || rateLimitFirstSeenAt != nil || expiredLeaseRetryCount != 0 || lastRetryReason != nil || retryExhaustedAt != nil || durationSeconds != nil || unitError != nil || len(resultJSON) != 0 || !reflect.DeepEqual(flags, expected.flags) || leaseOwner != nil || leaseExpiresAt != nil || lastHeartbeatAt != nil || !persistedCreatedAt.Equal(createdAt) || !persistedUpdatedAt.Equal(createdAt) {
			return fmt.Errorf("%w: deterministic sync unit %s maps to different state", ErrInvalidPlan, id)
		}
		seen++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if seen != len(units) {
		return fmt.Errorf("%w: deterministic sync run has %d units, expected %d", ErrInvalidPlan, seen, len(units))
	}
	return nil
}

func equalOptionalTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}
func equalOptionalJSON(left, right []byte) bool {
	if len(left) == 0 || len(right) == 0 {
		return len(left) == 0 && len(right) == 0
	}
	var a, b any
	if json.Unmarshal(left, &a) != nil || json.Unmarshal(right, &b) != nil {
		return false
	}
	return reflect.DeepEqual(a, b)
}

func persistCoordinatorGraph(ctx context.Context, tx pgx.Tx, ids materializationIDs, occurrence PendingOccurrence, totalUnits int, terminalReason, triggeredBy string) error {
	if triggeredBy == "" {
		// Defensive default only: every loadMaterializationPlan return path
		// sets this explicitly (see persistDomainGraph's identical guard).
		triggeredBy = "schedule"
	}
	jobStatus := 0
	resultValue := map[string]any{"sync_run_id": ids.SyncRunID}
	if terminalReason != "" {
		jobStatus = 3
		resultValue["terminal_status"] = "pagerduty_sync_disabled"
		resultValue["reason"] = terminalReason
		resultValue["total_units"] = totalUnits
	}
	result, _ := json.Marshal(resultValue)
	var completedAt *time.Time
	var jobError *string
	if terminalReason != "" {
		completedAt = pointerTime(occurrence.ScheduledFor)
		jobError = &terminalReason
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO public.job_runs (id,job_id,status,result,triggered_by,completed_at,error,created_at)
VALUES ($1::uuid,$2::uuid,$3,$4::jsonb,$5,$6,$7,$8)
ON CONFLICT (id) DO NOTHING`, ids.JobRunID, occurrence.JobID, jobStatus, result, triggeredBy, completedAt, jobError, occurrence.ScheduledFor); err != nil {
		return fmt.Errorf("persist scheduled job run: %w", err)
	}
	var persistedJobID, persistedTriggeredBy string
	var persistedJobStatus int
	var persistedJobResult []byte
	var persistedJobStarted, persistedJobCompleted *time.Time
	var persistedJobDuration *int
	var persistedJobError, persistedJobTraceback *string
	var persistedJobCreatedAt time.Time
	if err := tx.QueryRow(ctx, `SELECT job_id::text,status,started_at,completed_at,duration_seconds,result::jsonb,error,error_traceback,triggered_by,created_at FROM public.job_runs WHERE id=$1::uuid`, ids.JobRunID).Scan(&persistedJobID, &persistedJobStatus, &persistedJobStarted, &persistedJobCompleted, &persistedJobDuration, &persistedJobResult, &persistedJobError, &persistedJobTraceback, &persistedTriggeredBy, &persistedJobCreatedAt); err != nil {
		return fmt.Errorf("verify scheduled job run: %w", err)
	}
	if persistedJobID != occurrence.JobID || persistedJobStatus != jobStatus || persistedJobStarted != nil || !equalOptionalTime(persistedJobCompleted, completedAt) || persistedJobDuration != nil || !equalOptionalJSON(persistedJobResult, result) || !equalOptionalString(persistedJobError, jobError) || persistedJobTraceback != nil || persistedTriggeredBy != triggeredBy || !persistedJobCreatedAt.Equal(occurrence.ScheduledFor) {
		return fmt.Errorf("%w: deterministic job run identity maps to different state", ErrInvalidPlan)
	}
	if terminalReason != "" {
		return nil
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO public.sync_run_reference_discoveries
 (id,sync_run_id,org_id,status,attempts,available_at,created_at,updated_at)
VALUES ($1::uuid,$2::uuid,$3,'planned',0,$4,$4,$4)
ON CONFLICT (sync_run_id) DO NOTHING`, ids.ReferenceDiscoveryID, ids.SyncRunID, occurrence.OrgID, occurrence.ScheduledFor); err != nil {
		return fmt.Errorf("persist scheduled reference discovery: %w", err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO public.sync_dispatch_outbox
 (id,org_id,sync_run_id,kind,status,available_at,attempts,created_at,updated_at)
VALUES ($1::uuid,$2,$3::uuid,'reference_discovery','pending',$4,0,$4,$4)
ON CONFLICT (sync_run_id,kind) DO NOTHING`, ids.DispatchOutboxID, occurrence.OrgID, ids.SyncRunID, occurrence.ScheduledFor); err != nil {
		return fmt.Errorf("persist scheduled discovery outbox: %w", err)
	}
	var discoveryID, discoveryRunID, discoveryOrg, discoveryStatus string
	var discoveryAttempts int
	var discoveryAvailable, discoveryCreated, discoveryUpdated time.Time
	var discoveryLeaseOwner, discoveryError *string
	var discoveryLeaseExpires, discoveryHeartbeat, discoveryCompleted *time.Time
	var discoveryResult []byte
	if err := tx.QueryRow(ctx, `SELECT id::text,sync_run_id::text,org_id,status,attempts,available_at,lease_owner,lease_expires_at,last_heartbeat_at,completed_at,error,result::jsonb,created_at,updated_at FROM public.sync_run_reference_discoveries WHERE sync_run_id=$1::uuid`, ids.SyncRunID).Scan(&discoveryID, &discoveryRunID, &discoveryOrg, &discoveryStatus, &discoveryAttempts, &discoveryAvailable, &discoveryLeaseOwner, &discoveryLeaseExpires, &discoveryHeartbeat, &discoveryCompleted, &discoveryError, &discoveryResult, &discoveryCreated, &discoveryUpdated); err != nil {
		return fmt.Errorf("verify scheduled reference discovery: %w", err)
	}
	var outboxID, outboxOrg, outboxRunID, outboxKind, outboxStatus string
	var outboxAttempts int
	var outboxAvailable, outboxCreated, outboxUpdated time.Time
	var outboxLastError, outboxClaimToken, outboxClaimTransport *string
	var outboxDispatchedTransport, outboxTransportJobID *string
	var outboxDispatchedAt, outboxClaimExpires *time.Time
	var outboxClaimGeneration, outboxDispatchedGeneration *int64
	if err := tx.QueryRow(ctx, `SELECT id::text,org_id,sync_run_id::text,kind,status,attempts,available_at,last_error,dispatched_at,claim_token,claim_expires_at,claim_transport,claim_route_generation,dispatched_transport,dispatched_route_generation,transport_job_id,created_at,updated_at FROM public.sync_dispatch_outbox WHERE sync_run_id=$1::uuid AND kind='reference_discovery'`, ids.SyncRunID).Scan(&outboxID, &outboxOrg, &outboxRunID, &outboxKind, &outboxStatus, &outboxAttempts, &outboxAvailable, &outboxLastError, &outboxDispatchedAt, &outboxClaimToken, &outboxClaimExpires, &outboxClaimTransport, &outboxClaimGeneration, &outboxDispatchedTransport, &outboxDispatchedGeneration, &outboxTransportJobID, &outboxCreated, &outboxUpdated); err != nil {
		return fmt.Errorf("verify scheduled discovery outbox: %w", err)
	}
	expectedAt := occurrence.ScheduledFor
	if discoveryID != ids.ReferenceDiscoveryID || discoveryRunID != ids.SyncRunID || discoveryOrg != occurrence.OrgID || discoveryStatus != "planned" || discoveryAttempts != 0 || !discoveryAvailable.Equal(expectedAt) || discoveryLeaseOwner != nil || discoveryLeaseExpires != nil || discoveryHeartbeat != nil || discoveryCompleted != nil || discoveryError != nil || len(discoveryResult) != 0 || !discoveryCreated.Equal(expectedAt) || !discoveryUpdated.Equal(expectedAt) || outboxID != ids.DispatchOutboxID || outboxOrg != occurrence.OrgID || outboxRunID != ids.SyncRunID || outboxKind != "reference_discovery" || outboxStatus != "pending" || outboxAttempts != 0 || !outboxAvailable.Equal(expectedAt) || outboxLastError != nil || outboxDispatchedAt != nil || outboxClaimToken != nil || outboxClaimExpires != nil || outboxClaimTransport != nil || outboxClaimGeneration != nil || outboxDispatchedTransport != nil || outboxDispatchedGeneration != nil || outboxTransportJobID != nil || !outboxCreated.Equal(expectedAt) || !outboxUpdated.Equal(expectedAt) {
		return fmt.Errorf("%w: deterministic coordinator graph maps to different state", ErrInvalidPlan)
	}
	return nil
}

func equalOptionalString(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

type materializationIDs struct {
	JobRunID             string
	SyncRunID            string
	ReferenceDiscoveryID string
	DispatchOutboxID     string
}

func deterministicMaterializationIDs(occurrenceID string) (materializationIDs, error) {
	if occurrenceID == "" {
		return materializationIDs{}, ErrInvalidMaterializer
	}
	derive := func(kind string) string {
		return uuid.NewSHA1(materializerNamespace, []byte(occurrenceID+":"+kind)).String()
	}
	return materializationIDs{
		JobRunID:             derive("job-run"),
		SyncRunID:            derive("sync-run"),
		ReferenceDiscoveryID: derive("reference-discovery"),
		DispatchOutboxID:     derive("dispatch-outbox"),
	}, nil
}

func deterministicUnitID(syncRunID string, ordinal int) (string, error) {
	runID, err := uuid.Parse(syncRunID)
	if err != nil || ordinal < 0 {
		return "", ErrInvalidMaterializer
	}
	return uuid.NewSHA1(runID, []byte(fmt.Sprintf("unit:%d", ordinal))).String(), nil
}
