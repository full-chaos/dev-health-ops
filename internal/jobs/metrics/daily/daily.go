// Package daily owns the dormant, ID-only River boundary for daily metrics.
//
// The compatibility executor is deliberately narrow: it receives a durable
// run/partition identity after this package has reloaded and fenced it from
// PostgreSQL. It cannot receive a command, metric rows, SQL, credentials, or
// caller-selected Python module.
//
// All three kinds deliberately use the registered metrics queue. Celery's
// current all-org fanout is lightweight and uses default, but this dispatcher
// owns durable run/partition publication and must share the same bounded
// ClickHouse-facing queue as its partitions and finalizer. The checked-in route
// remains Celery until this topology and its compatibility executor are fully
// audited.
package daily

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/jackc/pgx/v5"
)

var (
	ErrInvalidState = errors.New("daily metrics durable state is invalid")
	ErrLeaseLost    = errors.New("daily metrics execution lease was lost")
	ErrLeaseActive  = errors.New("daily metrics execution lease is still active")
	ErrUnavailable  = errors.New("daily metrics dependency is unavailable")
	// ErrZeroRowsWithSourceData means a family's upstream source data existed
	// for a partition's repos+day but its output table had zero rows for that
	// scope (CHAOS-4263). Distinct from a genuinely empty day: the partition
	// must not report success. This is classified Permanent, not Retryable
	// (codex adversarial review, round 2): the compatibility bridge's
	// execution ledger (worker_metrics.py:_reserve_execution) marks this
	// exact (run, partition, family, generation, scope_digest) identity
	// "succeeded" the moment ComputePartition returns -- any later attempt at
	// the SAME identity is answered "skipped" without recomputing anything, so
	// a bare retry can never resolve this on its own. Marking it Retryable
	// would silently burn the job's attempt budget re-checking the same
	// unchanged output before failing anyway, with no recomputation ever
	// happening in between. Permanent fails loud and immediately instead,
	// which is what the RCA actually requires; real recomputation needs a new
	// execution identity (a ledger repair or a fresh run/partition), which is
	// a separate, larger change than this ticket's scope.
	ErrZeroRowsWithSourceData = errors.New("daily metrics family produced zero rows despite source data")
	// ErrDayAlreadyCovered means a deferred-discovery `metrics daily-start`
	// request (no --repo-id) found an ALREADY-succeeded run for the same
	// (org, day) under a DIFFERENT generation -- a prior scheduled fan-out,
	// post-sync re-drive, or earlier manual trigger. Refused rather than
	// dispatched (CHAOS-5055, codex adversarial review round 2, P1):
	// StartRunTx's (org_id, target_day, generation) uniqueness constraint
	// only makes ONE trigger idempotent against ITS OWN replays -- it does
	// nothing to stop a genuinely DIFFERENT trigger from inserting a second,
	// independent all-repository run for the same org+day, which would
	// recompute and duplicate-write every native daily family (file_hotspots
	// included -- an append-only table with no dedup on replay, see
	// 083_dedup_file_hotspots_windowed_view.sql) a second time. Scoped to the
	// deferred-discovery (all-repository) case only, and to StartManualDailyRun
	// only: this is the operator-facing surface CHAOS-5055 added, not a
	// change to the scheduled fan-out or post-sync triggers' own behavior
	// (which have carried this same cross-generation gap since CHAOS-4263,
	// unrelated to this diff, and are out of this fix's scope -- see the
	// doc comment on StartManualDailyRun).
	ErrDayAlreadyCovered = errors.New("daily metrics day is already covered by a succeeded run")
)

// ErrPreBridgeFamilyIncomplete means computeNativeFamilies hit an
// ErrPartialWrite for at least one pre_bridge family this partition
// (CHAOS-5078 codex round 3, astra scale review F1's pre_bridge twin -- see
// lane-ci-required-to-arc's CHAOS-5190/#2276 for the post_bridge sibling,
// same reason-code shape, no code dependency between the two PRs). Work
// uses it to hold the partition out of CompletePartition instead of letting
// it complete 'succeeded' over a silent gap.
//
// The comment this error's introduction replaces claimed a partial write
// meant "the partition is re-driven" -- that was never backed by any code:
// neither redrive.go nor postgres.go ever inspected PartialWrite, so nothing
// automatic re-drove anything. This error is what actually closes that gap.
//
// An ORDINARY (non-partial) pre_bridge refusal is UNAFFECTED by this error
// and stays fail-open exactly as before: nothing was written for that
// family yet, so the compatibility bridge remains a safe, correct fallback
// -- this error exists ONLY for the partial-write case, where the bridge is
// deliberately excluded (skipFamiliesForBridge's own contract, unchanged)
// because re-running it would duplicate the rows already written.
var ErrPreBridgeFamilyIncomplete = errors.New("daily metrics pre_bridge native family did not complete")

// ErrRepositoryCapExceeded means live ClickHouse repository discovery
// (MaterializeScheduledFanout) resolved more repositories than
// maxDailyMetricsRepositoriesPerRun for one run (CHAOS-4263, codex
// adversarial review round 3). It wraps ErrInvalidState so existing
// classification call sites (errors.Is(err, ErrInvalidState) -> Permanent)
// need no changes: retrying cannot reduce the discovered repository count,
// so this must fail loud and immediately, exactly like any other invalid
// durable state, never silently truncate to the cap.
var ErrRepositoryCapExceeded = fmt.Errorf("%w: repository count exceeds cap", ErrInvalidState)

// LeaseActiveError reports that the claim target is held by a lease that has
// not expired yet, and carries how long is left on it.
//
// It exists because "I could not take this lease" and "there is nothing to do"
// are not the same answer, even though one conditional UPDATE reports both as
// zero matched rows. A claimant that reports a live lease as success lets the
// job runtime retire the job -- and that job is the only thing that would have
// come back to reclaim the lease once it expired. The lease is bounded but the
// retry budget that could outlive it is not: it is spent in tens of seconds
// against a ten-minute lease, so an orphaned lease strands its run forever, and
// with it every handoff fenced on that run's completion key.
type LeaseActiveError struct {
	RetryAfter time.Duration
}

func (err *LeaseActiveError) Error() string { return ErrLeaseActive.Error() }
func (err *LeaseActiveError) Unwrap() error { return ErrLeaseActive }

// retryClaim maps a failed claim onto the job runtime. A live lease becomes a
// snooze that wakes when the lease expires, which does not consume an attempt,
// so the reclaim path stays reachable however long the holder takes to die.
func retryClaim(err error) error {
	var active *LeaseActiveError
	if errors.As(err, &active) {
		return jobruntime.RetryableAfter(err, active.RetryAfter)
	}
	return jobruntime.Retryable(err)
}

// retryCompatibilityError marks err Retryable and, when it is one of the
// compatibility bridge's classified sentinels (CHAOS-4264), attaches the
// matching bounded Reason so the River attempt log explains a signaled or
// resource-exhausted runner without anyone having to read Sentry/dmesg.
// Anything else (including the pre-existing ErrUnavailable) is unaffected --
// Retryable with no reason, exactly as before this ticket.
//
// ErrCompatibilityAmbiguousStuck (CHAOS-4319) is the one exception: the
// Python ledger row it names can never move again without a human /repair
// call, so marking it Retryable would only ever spend the job's whole
// attempt budget reproducing the same 409 before River discards it with no
// trace. It is marked Permanent instead -- PartitionHandler.Work has
// already durably persisted the failure (failPartitionPermanently) by the
// time this return value reaches River, so Permanent here means "stop
// retrying a lost cause," never "the failure went unrecorded."
func retryCompatibilityError(err error) error {
	if errors.Is(err, ErrCompatibilityAmbiguousStuck) {
		return jobruntime.WithReason(jobruntime.Permanent(err), jobruntime.ReasonAmbiguousRefused)
	}
	if errors.Is(err, ErrCompatibilityResourceExhaustedDeterministic) {
		// CHAOS-4543: see ErrCompatibilityResourceExhaustedDeterministic's
		// doc comment -- a KNOWN deterministic guard (not a real,
		// attempt-to-attempt-variable memory kill), so retrying 5 times
		// before River discards the job only reproduces the identical
		// refusal. Permanent stops after this one attempt; the Reason
		// stays ReasonResourceExhausted (same River-visible classification
		// as the non-deterministic case) -- only the retry BUDGET changes,
		// not what the failure is called.
		return jobruntime.WithReason(jobruntime.Permanent(err), jobruntime.ReasonResourceExhausted)
	}
	marked := jobruntime.Retryable(err)
	switch {
	case errors.Is(err, ErrCompatibilityProcessSignaled):
		return jobruntime.WithReason(marked, jobruntime.ReasonProcessSignaled)
	case errors.Is(err, ErrCompatibilityResourceExhausted):
		return jobruntime.WithReason(marked, jobruntime.ReasonResourceExhausted)
	case errors.Is(err, ErrCompatibilityAmbiguousRefused):
		return jobruntime.WithReason(marked, jobruntime.ReasonAmbiguousRefused)
	case errors.Is(err, ErrCompatibilityProgressStalled):
		return jobruntime.WithReason(marked, jobruntime.ReasonProgressStalled)
	case errors.Is(err, ErrCompatibilityCapacityExhausted):
		return jobruntime.WithReason(marked, jobruntime.ReasonCapacityExhausted)
	default:
		return marked
	}
}

// RepositoryID is a ClickHouse repos.id -- the identity space
// daily.RepositoryDiscoverer resolves against (CHAOS-4263, chris's ruling
// 2026-08-25). It is a distinct type from any Postgres integration_sources.id
// (sync_run_units.source_id) so the compiler rejects passing one for the
// other: that exact confusion -- embedding an integration_sources.id where a
// repos.id was expected -- was this ticket's root cause.
type RepositoryID string

type Run struct {
	ID             string
	OrganizationID string
	Generation     string
	Status         string
	// RepositoryDiscoveryRequired is true only for the fixed daily fan-out
	// generation while it has no durable partitions. A metrics-queue worker owns the
	// ClickHouse read and resolves this state before it can publish a partition.
	RepositoryDiscoveryRequired bool
	// DiscoveredRepoIDs is the UNION of this run's partition scopes -- the
	// set the partitions were actually cut from, read back from
	// daily_metrics_partitions rather than re-derived from a live source.
	//
	// CHAOS-4288, codex r1 on #2235. benchmarking computes ONCE per org/day and
	// picks an anchor partition to do it in. That anchor used to come from a
	// live `min(id)` read of the `repos` table, which is a DIFFERENT set from
	// the one partitions were cut from: a run over a subset of the org's repos,
	// or a repo inserted between discovery and execution, could name an anchor
	// that no partition contains. Every partition then answered "not mine",
	// returned zero rows and SUCCESS, and the org silently got no benchmarking
	// output at all.
	//
	// Choosing the anchor from this field makes anchor-and-partition agreement
	// true BY CONSTRUCTION rather than by two reads happening to agree, which
	// is the invariant that was missing. An empty union means the caller built
	// a Run without partitions and is a bug, not a quiet no-op.
	DiscoveredRepoIDs []RepositoryID
	// TargetDay is the UTC calendar day this run computes metrics for
	// (`daily_metrics_runs.target_day`). CHAOS-4276: a native family
	// executor needs this to scope its own ClickHouse reads/writes -- the
	// compatibility bridge never needed it in Go because the Python side
	// re-derives it from the run id itself.
	TargetDay time.Time
}

// StartRunRequest is the immutable post-sync input for one daily generation.
// Repository IDs are server-owned durable references and are partitioned in
// deterministic order by PostgresStore.
type StartRunRequest struct {
	OrganizationID            string
	TargetDay                 time.Time
	Generation                string
	RepositoryIDs             []RepositoryID
	PrerequisiteCompletionKey string
}

// ScheduledFanoutRequest creates the durable state for the nightly all-org
// fan-out. Repository discovery intentionally happens later in the heavy
// worker: the scheduler owns only the coordinator Postgres transaction and
// must never make a remote ClickHouse read while holding it.
type ScheduledFanoutRequest struct {
	OrganizationID string
	TargetDay      time.Time
	Generation     string
}

type Partition struct {
	ID    string
	RunID string
	// RepoIDs is the partition's repository scope
	// (`daily_metrics_partitions.repo_ids`). CHAOS-4276: a native family
	// executor needs this the same way TargetDay is needed on Run -- the
	// compatibility bridge never surfaced it in Go because Python re-derives
	// it from the partition id.
	RepoIDs []RepositoryID
}

type PartitionClaim struct {
	Partition     Partition
	Token         string
	LeaseDuration time.Duration
}

type FinalizeClaim struct {
	Run           Run
	Token         string
	LeaseDuration time.Duration
}

// Store is the authoritative execution-state boundary. Implementations must
// use bounded leases and fence renew, release, and completion transitions with
// both the current token and a live lease. An expired claimant has lost all
// mutation authority even when no replacement has claimed yet.
type Store interface {
	LoadRun(context.Context, string) (Run, error)
	ClaimDispatch(context.Context, string) (*Run, error)
	DispatchablePartitions(context.Context, string) ([]Partition, error)
	MaterializeScheduledFanout(context.Context, Run, []RepositoryID) (bool, error)
	ClaimPartition(context.Context, string) (*PartitionClaim, error)
	RenewPartition(context.Context, PartitionClaim) error
	CompletePartition(context.Context, PartitionClaim, Publisher) error
	ReleasePartition(context.Context, PartitionClaim) error
	// ReleasePartitionWithReason is ReleasePartition plus a bounded
	// failure_reason (CHAOS-4316): status stays 'failed' (silently
	// re-dispatchable, unlike FailPartitionPermanently's 'failed_permanent')
	// -- a liveness kill is not a claim this row can never satisfy, a fresh
	// attempt might simply not hang. reason must be in the same closed
	// vocabulary FailPartitionPermanently validates against.
	ReleasePartitionWithReason(ctx context.Context, claim PartitionClaim, reason string) error
	// FailPartitionPermanently durably terminalizes a partition whose
	// compatibility-bridge ledger row is stuck ambiguous (CHAOS-4319):
	// unlike ReleasePartition (status='failed', silently re-dispatchable),
	// this sets status='failed_permanent' with a bounded reason and is
	// deliberately excluded from DispatchablePartitions's reclaim set, so a
	// partition that can never succeed without a human /repair call stops
	// being retried instead of spinning forever back into the same 409.
	FailPartitionPermanently(ctx context.Context, claim PartitionClaim, reason string) error
	ClaimFinalize(context.Context, string) (*FinalizeClaim, error)
	RenewFinalize(context.Context, FinalizeClaim) error
	CompleteFinalize(context.Context, FinalizeClaim) error
	ReleaseFinalize(context.Context, FinalizeClaim) error
	// FailFinalizePermanently writes the TERMINAL finalize state --
	// status='failed' AND finalization_status='failed' -- on the final River
	// attempt (CHAOS-4290, #2241 confirmation pass).
	//
	// Nothing produced that state for a finalize before this. ReleaseFinalize
	// sets finalization_status='failed' and leaves status='running', which
	// ClaimFinalize treats as CLAIMABLE, so attempt 1 and an exhausted run were
	// indistinguishable forever. The blocked marker keyed on the only state
	// that existed and therefore fired on healthy retryable runs while never
	// seeing a stranded one.
	//
	// Named after FailPartitionPermanently, which is the same shape one layer
	// down: the point at which retrying stops and an operator has to look.
	FailFinalizePermanently(ctx context.Context, claim FinalizeClaim) error
}

// RepositoryDiscoverer reads the authoritative repository IDs for one
// organization. It is deliberately called only by the heavy worker after the
// scheduler transaction has committed the daily run and dispatch handoff.
type RepositoryDiscoverer interface {
	RepositoryIDs(context.Context, string) ([]RepositoryID, error)
}

// Publisher persists a child handoff. Its production implementation must use
// the checked-in outbox contract rather than inserting a River job directly.
type Publisher interface {
	PublishPartition(context.Context, Run, Partition) error
	PublishFinalizeTx(context.Context, pgx.Tx, Run) error
}

type RunPublisher interface {
	PublishDispatchTx(context.Context, pgx.Tx, Run, string) error
}

// CompatibilityExecutor is the only temporary Python seam. Both identities
// are loaded from Store before it is called, so it cannot expand the scope.
//
// ComputePartition's skipFamilies argument (CHAOS-4276) names families.json
// families a NativeFamilyExecutor already computed and wrote for this
// partition -- the compatibility bridge must not recompute or rewrite them.
// An empty/nil skipFamilies is a no-op: every existing caller (and the
// Python side, run_daily_metrics_job's skip_families parameter) behaves
// exactly as before this field existed.
type CompatibilityExecutor interface {
	ComputePartition(ctx context.Context, run Run, partition Partition, skipFamilies []string) error
	// Finalize takes skipFamilies for the same reason ComputePartition does
	// (CHAOS-4290): a NativeFinalizeFamilyExecutor that already computed and
	// wrote a finalize-scope family must stop the Python bridge recomputing
	// it. Without this the two writers race on an append-only table deduped
	// `ORDER BY computed_at DESC LIMIT 1 BY ...`, where the LATER writer wins
	// silently -- so a correct native family would be invisibly superseded
	// and the port would change nothing in production.
	Finalize(ctx context.Context, run Run, skipFamilies []string) error
}

// NativeFamilyExecutor computes and writes ONE families.json family's rows
// for a partition natively in Go, in place of the Python compatibility
// bridge (CHAOS-4276, the daily bridge's per-family counterpart to the
// remaining bridge's per-kind native executors). It returns how many rows it
// wrote so PartitionHandler can report it through telemetry without a
// separate readback.
//
// Unlike the remaining bridge (one River kind per family, a construction
// refusal takes only that kind out of service), the daily bridge computes
// every family inside ONE partition call. A NativeFamilyExecutor is
// therefore consulted, and can fail, PER PARTITION rather than once at
// worker startup -- PartitionHandler's fail-open policy (see Work) means a
// single family's runtime failure degrades to "Python still computes it
// this partition", not a failed partition and not a family taken out of
// service fleet-wide.
type NativeFamilyExecutor interface {
	ComputeFamily(ctx context.Context, run Run, partition Partition) (rowsWritten int, err error)
}

// CONTRACT: THE CONTEXT IS BINDING (CHAOS-4290, #2241 r1 Finding 3).
//
// The ctx handed to ComputeFinalizeFamily is the LEASE context. It is
// cancelled the moment lease renewal fails, which means another worker may
// legitimately reclaim this run and start computing the same families. An
// executor MUST pass this ctx to every ClickHouse and Postgres call it makes
// and MUST return promptly once it is cancelled.
//
// An executor that blocks past cancellation is a CONTRACT VIOLATION, not a
// slow executor. runWithLeaseRenewal waits for the work function to return
// before it reports the renewal failure, so a non-cooperative executor keeps
// FinalizeHandler.Work blocked while the lease it lost is held by someone
// else -- the two-writer hazard again, arriving through liveness rather than
// through the skip list.
//
// This cannot be enforced from the caller: Go has no way to interrupt a
// goroutine that will not look at its context. It is enforced by review, by
// the ctx-bound I/O rule above, and by the between-family cancellation check
// in computeNativeFinalizeFamilies, which at least stops the REMAINING
// families from running after the lease is gone.
//
// NativeFinalizeFamilyExecutor computes and writes ONE families.json family
// whose scope is the RUN, not a partition -- the finalize-scope families that
// run once after every partition has landed (CHAOS-4290, ic_finalize being
// the first).
//
// It is a SEPARATE interface from NativeFamilyExecutor rather than the same
// one with a nil Partition. The scopes are genuinely different: a finalize
// family reads back what the partitions wrote, so handing it a partition
// would be meaningless. Keeping them distinct makes registering a
// finalize-scope family on the partition path (or vice versa) a COMPILE
// error instead of a convention nobody enforces.
type NativeFinalizeFamilyExecutor interface {
	ComputeFinalizeFamily(ctx context.Context, run Run) (rowsWritten int, err error)
}

type Dispatcher struct {
	store      Store
	publisher  Publisher
	discoverer RepositoryDiscoverer
	// blockedObserver counts CHAOS-5040 blocked-run marker transitions.
	// Optional: nil is a silent no-op.
	blockedObserver jobruntime.DailyMetricsBlockedRunObserver
}

func NewDispatcher(store Store, publisher Publisher, discoverer RepositoryDiscoverer) (*Dispatcher, error) {
	if store == nil || publisher == nil || discoverer == nil {
		return nil, ErrUnavailable
	}
	return &Dispatcher{store: store, publisher: publisher, discoverer: discoverer}, nil
}

// SetBlockedRunObserver attaches the CHAOS-5040 blocked-run counters. Nil is
// a silent no-op, matching every other observer here.
func (handler *Dispatcher) SetBlockedRunObserver(observer jobruntime.DailyMetricsBlockedRunObserver) {
	if handler != nil {
		handler.blockedObserver = observer
	}
}

// blockedRunReconciler is the narrow, OPTIONAL capability the dispatch
// fan-out uses to keep the blocked marker current. It is deliberately NOT on
// the Store interface: this is a visibility concern, and putting it there
// would force every implementation -- including the test fakes that have
// nothing to do with it -- to carry a method they never call. The type
// assertion below mirrors how the observers in this package are wired.
type blockedRunReconciler interface {
	ReconcileBlockedRuns(ctx context.Context, orgID string) (BlockedReconcileOutcome, error)
}

// SupportsBlockedRunReconcile reports whether store carries the optional
// blocked-run reconcile capability (CHAOS-5040). It exists so the wiring can
// say so ONCE at construction rather than counting a wiring fact on every
// pass: a store lacking the method is a permanent, deployment-constant
// condition, and a per-pass counter for it would be noise that never changes.
//
// Exported deliberately rather than duplicating the interface at the call
// site: a second copy of the method set in another package is free to drift
// from this one, and the drift would be silent -- the assertion would simply
// stop matching and the reconcile would disable itself with nobody noticing.
// One definition, asked from outside.
func SupportsBlockedRunReconcile(store Store) bool {
	_, ok := store.(blockedRunReconciler)
	return ok
}

// reconcileBlockedRuns keeps this organization's blocked markers in step with
// live partition state. It runs at the END of the fan-out and is FAIL-OPEN by
// construction: the marker exists to make a wedged run visible, and a
// visibility mechanism must never be able to fail the job that computes the
// day's metrics. Every error path here returns without touching the caller's
// result -- the same rule the lease observers in this package follow, for the
// same reason.
//
// This is the periodic tick. The daily fan-out is genuinely scheduled
// (fixed-scheduler entry daily_metrics_fanout, DailyAt(1, 0), per
// organization), whereas the stranded-finalize sweep it might otherwise have
// hung off is NOT periodic -- FindStrandedFinalizeRuns has exactly one
// non-test caller, the workerctl CLI, so hanging this off it would mean a
// wedged run only becomes visible once an operator already went looking,
// which is the situation the marker exists to end.
func (handler *Dispatcher) reconcileBlockedRuns(ctx context.Context, organizationID string) {
	reconciler, ok := handler.store.(blockedRunReconciler)
	if !ok {
		return
	}
	outcome, err := reconciler.ReconcileBlockedRuns(ctx, organizationID)
	if handler.blockedObserver == nil {
		return
	}
	if err != nil {
		// Silent to the JOB, never silent to METRICS. Swallowing the error
		// here is deliberate, but a fail-open path with no counter is
		// indistinguishable from one that is working: "marked" and "cleared"
		// would both sit at zero, which reads exactly like a healthy fleet
		// with nothing wedged.
		_ = handler.blockedObserver.ObserveDailyMetricsBlockedRun("failed", 1)
		return
	}
	// Both are reported every pass, including zeros: a series that
	// disappears when nothing changed cannot be told apart from a reconcile
	// that stopped running.
	_ = handler.blockedObserver.ObserveDailyMetricsBlockedRun("marked", outcome.Marked)
	_ = handler.blockedObserver.ObserveDailyMetricsBlockedRun("cleared", outcome.Cleared)
}

func (handler *Dispatcher) Work(ctx context.Context, execution *jobruntime.Execution[jobruntime.DailyMetricsDispatchArgs]) error {
	if handler == nil || handler.store == nil || handler.publisher == nil || handler.discoverer == nil || execution == nil {
		return jobruntime.Permanent(ErrUnavailable)
	}
	runID := execution.Args.Payload.RunID
	if execution.Envelope.Domain.ID != runID {
		return jobruntime.Permanent(ErrInvalidState)
	}
	run, err := handler.store.ClaimDispatch(ctx, runID)
	if err != nil {
		if errors.Is(err, ErrInvalidState) {
			return jobruntime.Permanent(err)
		}
		return jobruntime.Retryable(err)
	}
	if run == nil {
		return nil
	}
	if run.ID != runID || run.Status != "running" || execution.OrganizationID == nil || run.OrganizationID != *execution.OrganizationID {
		return jobruntime.Permanent(ErrInvalidState)
	}
	// codex review round 3 (P2). This used to run LAST, after the publish loop,
	// on the reasoning that a visibility mechanism must never affect what the job
	// had to do. The ordering was right; placing it after the early returns was
	// not. Every failure path between here and the end -- a publish error, a
	// DispatchablePartitions error, an invalid partition -- returned before the
	// reconcile, so the marker sweep silently stopped running for that
	// organization for as long as those failures lasted.
	//
	// The reasoning error was conflating the run being DISPATCHED with the runs
	// being MARKED. I argued this was benign because a run I would mark has only
	// terminal partitions, so its publish loop cannot fail. True, and irrelevant:
	// the reconcile is ORG-scoped, so a DIFFERENT, already-wedged historical run
	// in the same organization goes unmarked because THIS run's publish failed.
	//
	// A defer covers every exit including the panic path, and keeps the
	// "cannot affect the job's outcome" property: reconcileBlockedRuns is
	// fail-open and returns nothing.
	defer handler.reconcileBlockedRuns(ctx, run.OrganizationID)
	if run.RepositoryDiscoveryRequired {
		repositoryIDs, err := handler.discoverer.RepositoryIDs(ctx, run.OrganizationID)
		if err != nil {
			return jobruntime.Retryable(err)
		}
		if _, err := handler.store.MaterializeScheduledFanout(ctx, *run, repositoryIDs); err != nil {
			if errors.Is(err, ErrInvalidState) {
				return jobruntime.Permanent(err)
			}
			return jobruntime.Retryable(err)
		}
	}
	partitions, err := handler.store.DispatchablePartitions(ctx, runID)
	if err != nil {
		return jobruntime.Retryable(err)
	}
	for _, partition := range partitions {
		if partition.ID == "" || partition.RunID != runID {
			return jobruntime.Permanent(ErrInvalidState)
		}
		if err := handler.publisher.PublishPartition(ctx, *run, partition); err != nil {
			return jobruntime.Retryable(err)
		}
	}
	return nil
}

type PartitionHandler struct {
	store             Store
	publisher         Publisher
	compatibility     CompatibilityExecutor
	sourceChecker     SourceDataChecker
	zeroRowsObserver  jobruntime.DailyMetricsZeroRowsObserver
	nativeFamilies    map[string]NativeFamilyExecutor
	nativeFamilyNames []string
	// nativeFamilyDependencies (CHAOS-5078 codex r2 F3) is each pre_bridge
	// native family's DIRECT `after` dependencies, restricted to the
	// registered subset -- see registeredDependencies. computeNativeFamilies
	// uses it to block a family's runtime execution when its dependency
	// already failed or was itself blocked this same pass, so a reader can
	// never run natively against a stale/absent snapshot its writer failed
	// to produce THIS partition.
	nativeFamilyDependencies map[string][]string
	nativeObserver           jobruntime.DailyMetricsNativeFamilyObserver
	nativeFamilyLogger       NativeFamilyRefusalLogger
	nativeFamiliesNow        func() time.Time
	compatRetryObserver      jobruntime.DailyMetricsCompatRetryObserver

	// postBridgeFamilies/postBridgeFamilyNames (CHAOS-4278) are native
	// families that must run AFTER the compatibility bridge call for the
	// SAME partition, not before -- see computePostBridgeNativeFamilies's
	// doc comment for why families.json's "pre_bridge" (default) vs
	// "post_bridge" phase exists at all.
	postBridgeFamilies    map[string]NativeFamilyExecutor
	postBridgeFamilyNames []string

	// livenessCeilingBase/PerRepo (CHAOS-4316) bound the compatibility
	// bridge call from the Go side, as a backstop behind the bridge's own
	// progress-based watchdog (worker_metrics.py _watch_progress_stall).
	// NewPartitionHandler seeds these with a nonzero, work-derived default
	// (defaultLivenessCeilingBase/PerRepo below) -- team-lead ruling
	// 2026-08-26: the fix must ship ON by default, because deployed
	// compose/helm manifests do not set the new env vars that would
	// otherwise be the only way to enable it. SetLivenessCeiling(0, 0) is
	// the one explicit, deliberate way to opt out.
	livenessCeilingBase    time.Duration
	livenessCeilingPerRepo time.Duration
}

// defaultLivenessCeilingBase/PerRepo mirror config.Config's own defaults
// (internal/platform/config/config.go, DEV_HEALTH_DAILY_PARTITION_LIVENESS_
// CEILING_BASE/PER_REPO) so that ANY caller of NewPartitionHandler -- not
// only cmd/dev-health-worker, which additionally wires the operator-tunable
// config value via SetLivenessCeiling -- gets a safe, nonzero, work-derived
// bound rather than silently unbounded behavior.
//
// Sized as queueDepthBudget(3) * the Python watchdog's own hard ceiling
// (120s base + 90s/repo, x3 multiplier -- worker_metrics.py), NOT as an
// independently-tuned Go-side number. A codex review on this ticket found
// that the compatibility bridge's per-replica runner semaphore
// (_RUNNER_CONCURRENCY_SEMAPHORE, default concurrency=1) means this
// deadline -- which starts counting from HTTP-send time, before the
// request even acquires that semaphore slot -- also has to absorb time
// spent legitimately queued behind another partition's own full compute
// window, not only this partition's own compute time. queueDepthBudget=3
// says: tolerate up to two other partitions each legitimately consuming
// their full Python hard ceiling ahead of this one on the same replica,
// plus this partition's own hard ceiling, before concluding something is
// actually wrong. At the default dailyRepositoryPartitionSize (3 repos),
// that is base(18m) + perRepo(13m30s)*3 = 58m30s, vs. a 19m30s Python hard
// ceiling -- a 3x multiple, not the earlier 25m/19.5m (~1.3x) margin that
// left no room for even one legitimately queued neighbor. If either side's
// per-repo constants are retuned, keep this ratio: the Go base/perRepo
// pair must equal 3x the Python side's own base/perRepo*multiplier, so the
// two formulas cannot silently drift apart the way an independent flat
// number would.
const (
	defaultLivenessCeilingBase    = 18 * time.Minute
	defaultLivenessCeilingPerRepo = 13*time.Minute + 30*time.Second
)

func NewPartitionHandler(store Store, publisher Publisher, compatibility CompatibilityExecutor) (*PartitionHandler, error) {
	if store == nil || publisher == nil || compatibility == nil {
		return nil, ErrUnavailable
	}
	return &PartitionHandler{
		store: store, publisher: publisher, compatibility: compatibility,
		nativeFamiliesNow:      func() time.Time { return time.Now().UTC() },
		livenessCeilingBase:    defaultLivenessCeilingBase,
		livenessCeilingPerRepo: defaultLivenessCeilingPerRepo,
	}, nil
}

// SetLivenessCeiling overrides the work-size-derived hard ceiling on the
// compatibility bridge call (CHAOS-4316): base + perRepo*len(RepoIDs), never
// a flat wall-clock number. This is a backstop, not the primary mechanism --
// the bridge's own progress-based watchdog should always win the race under
// normal conditions, since it can see per-repo progress the Go side cannot.
// It exists for when that watchdog itself cannot run (e.g. the bridge's
// event loop is wedged). The clock starts at HTTP-send time, before the
// request acquires the bridge's per-replica runner semaphore slot, so base
// and perRepo must stay sized to absorb legitimate queueing behind other
// partitions' compute time, not only this partition's own -- see the
// queueDepthBudget derivation on defaultLivenessCeilingBase/PerRepo above;
// an override here should keep the same multiple over the Python side's
// hard ceiling rather than picking an independent number.
// NewPartitionHandler already seeds a safe nonzero
// default (defaultLivenessCeilingBase/PerRepo) -- call this to tune it (a
// larger/smaller bound) or to explicitly opt out by passing base <= 0,
// which is the ONLY way to disable the ceiling; simply never calling this
// method does NOT disable it.
func (handler *PartitionHandler) SetLivenessCeiling(base, perRepo time.Duration) {
	if handler == nil {
		return
	}
	handler.livenessCeilingBase = base
	handler.livenessCeilingPerRepo = perRepo
}

// livenessCeiling returns 0 (no ceiling) only when SetLivenessCeiling(0, _)
// was called explicitly -- the constructor default is always nonzero.
func (handler *PartitionHandler) livenessCeiling(repoCount int) time.Duration {
	if handler == nil || handler.livenessCeilingBase <= 0 {
		return 0
	}
	return handler.livenessCeilingBase + handler.livenessCeilingPerRepo*time.Duration(repoCount)
}

// SetSourceDataChecker wires the optional zero-rows-with-source-data check
// (CHAOS-4263). A nil checker (the default) is a no-op, leaving the
// partition contract exactly as it was before this capability existed.
func (handler *PartitionHandler) SetSourceDataChecker(checker SourceDataChecker) {
	if handler == nil {
		return
	}
	handler.sourceChecker = checker
}

// SetZeroRowsObserver wires the optional telemetry observer for the same
// check. Never gates behavior on its own: a checker with no observer still
// fails the partition, it just fails silently on the metric.
func (handler *PartitionHandler) SetZeroRowsObserver(observer jobruntime.DailyMetricsZeroRowsObserver) {
	if handler == nil {
		return
	}
	handler.zeroRowsObserver = observer
}

// SetNativeFamilies registers the families.json families this handler
// computes natively in Go instead of the Python compatibility bridge
// (CHAOS-4276). A nil/empty map (the default) is a no-op: every family stays
// on the compatibility path exactly as before this capability existed. The
// caller (cmd/dev-health-worker/daily.go) decides per family whether its
// native executor could be built at all -- a family absent from this map
// simply never leaves the compatibility path; see NativeFamilyExecutor's
// doc comment for the runtime (per-partition) fail-open policy on top of
// that construction-time decision.
func (handler *PartitionHandler) SetNativeFamilies(families map[string]NativeFamilyExecutor) error {
	if handler == nil {
		return nil
	}
	names := make([]string, 0, len(families))
	for name := range families {
		names = append(names, name)
	}
	registry, err := LoadRegistry()
	if err != nil {
		return err
	}
	// CHAOS-4283 PR2: run order comes from families.json's `after` edges, not
	// from sort.Strings. Alphabetical order silently put the readers of
	// work_item_team_attributions ahead of the family that writes it.
	//
	// On error we register NOTHING and return it. That is deliberate on both
	// counts. Registering nothing is fail-SAFE, not fail-open: an unregistered
	// family is computed by the Python compatibility bridge exactly as it was
	// before any native executor existed, which is the same degradation policy
	// a refused executor already follows. What we must never do is fall back to
	// alphabetical -- that produces a plausible order in which a reader
	// precedes its writer, which is the very defect this ordering exists to
	// prevent, reintroduced through the error path.
	ordered, err := FamilyRunOrder(registry, names)
	if err != nil {
		handler.nativeFamilies = nil
		handler.nativeFamilyNames = nil
		handler.nativeFamilyDependencies = nil
		return err
	}
	handler.nativeFamilies = families
	handler.nativeFamilyNames = ordered
	handler.nativeFamilyDependencies = registeredDependencies(registry, names)
	return nil
}

// SetPostBridgeNativeFamilies registers the families.json families that
// declare `"phase":"post_bridge"` (CHAOS-4278) -- native families that must
// run AFTER the compatibility bridge call for the SAME partition, because
// their compute depends on a table the bridge itself writes this partition
// (see computePostBridgeNativeFamilies's doc comment). Mirrors
// SetNativeFamilies exactly (REPLACES its map on every call, sorts names for
// deterministic iteration) -- the only difference is WHEN the dispatcher
// runs these executors, in Work.
func (handler *PartitionHandler) SetPostBridgeNativeFamilies(families map[string]NativeFamilyExecutor) error {
	if handler == nil {
		return nil
	}
	names := make([]string, 0, len(families))
	for name := range families {
		names = append(names, name)
	}
	registry, err := LoadRegistry()
	if err != nil {
		return err
	}
	// post_bridge families get the SAME `after` treatment as pre_bridge ones.
	// Today no post_bridge family depends on another, so the result equals the
	// alphabetical order it replaced -- but leaving this phase on sort.Strings
	// would mean the ordering guarantee silently depended on WHICH phase a
	// family happened to be in, and CHAOS-5078 moves families between phases.
	ordered, err := FamilyRunOrder(registry, names)
	if err != nil {
		handler.postBridgeFamilies = nil
		handler.postBridgeFamilyNames = nil
		return err
	}
	handler.postBridgeFamilies = families
	handler.postBridgeFamilyNames = ordered
	return nil
}

// SetNativeFamilyObserver wires the optional telemetry observer for native
// family computation. Never gates behavior on its own. Shared by both
// pre_bridge (computeNativeFamilies) and post_bridge
// (computePostBridgeNativeFamilies) families -- the observer interface
// carries a family name, not a phase, so one wiring covers both.
func (handler *PartitionHandler) SetNativeFamilyObserver(observer jobruntime.DailyMetricsNativeFamilyObserver) {
	if handler == nil {
		return
	}
	handler.nativeObserver = observer
}

// NativeFamilyRefusalLogger is the narrow logging capability
// computeNativeFamilies/computePostBridgeNativeFamilies need to report a
// per-partition native family error (CHAOS-5139). Before this, a runtime
// refusal was recorded ONLY as an anonymous DailyMetricsNativeFamilyOutcomeRefused
// counter increment -- the wrapped error itself was discarded at the call
// site, so an operator (or an E2E gate reading /metrics) could see a family
// was refused but never learn why, and no CI artifact could ever answer that
// question without a code change (CHAOS-5138's own root cause). *slog.Logger
// satisfies this directly, matching remaining/membership_native.go's
// MembershipLogger shape.
type NativeFamilyRefusalLogger interface {
	Error(msg string, args ...any)
}

// SetNativeFamilyLogger wires optional logging for a native family's
// per-partition refusal. Nil is tolerated everywhere it is read (same
// discipline as SetNativeFamilyObserver) and never gates behavior on its
// own -- fail-open stays fail-open, this only makes the reason visible.
// Shared by both pre_bridge (computeNativeFamilies) and post_bridge
// (computePostBridgeNativeFamilies) families, same as the observer above.
func (handler *PartitionHandler) SetNativeFamilyLogger(logger NativeFamilyRefusalLogger) {
	if handler == nil {
		return
	}
	handler.nativeFamilyLogger = logger
}

// SetCompatRetryObserver wires the optional telemetry observer for a
// partition durably persisted as failed_permanent after an unresolvable
// ambiguous_refused response (CHAOS-4319). Never gates behavior on its own:
// a nil observer (the default) still fails the partition and writes the
// durable record, it just does not also increment the counter.
func (handler *PartitionHandler) SetCompatRetryObserver(observer jobruntime.DailyMetricsCompatRetryObserver) {
	if handler == nil {
		return
	}
	handler.compatRetryObserver = observer
}

// observeCompatRetry is a nil-safe helper (CHAOS-4543) around
// compatRetryObserver.ObserveDailyMetricsCompatRetry -- every
// releasePartitionWithReason branch in Work calls this immediately after,
// mirroring the ambiguous_stuck branch's own inline nil-check + call, so a
// new released-with-reason decision can never be added to one without the
// other (the risk this ticket's own root cause was: a branch existing with
// no accompanying telemetry).
func (handler *PartitionHandler) observeCompatRetry(decision jobruntime.DailyMetricsCompatRetryDecision) {
	if handler.compatRetryObserver != nil {
		_ = handler.compatRetryObserver.ObserveDailyMetricsCompatRetry(decision)
	}
}

// computeNativeFamilies runs every registered native family executor for one
// partition and returns the names that succeeded, in the same sorted order
// SetNativeFamilies stored -- deterministic so a failure in one family never
// makes another family's inclusion depend on Go map iteration order.
//
// FAIL-OPEN BY DESIGN (chris's ruling relayed via team-lead, CHAOS-4276): a
// native family's runtime failure is NOT a partition failure. It is excluded
// from the returned skip list, which means the compatibility bridge computes
// and writes that family for this partition exactly as it would have before
// any native executor existed -- one family degrading to Python must never
// take the other 22 down with it, and must never turn a transient ClickHouse
// hiccup into a Permanent partition failure.
//
// FAIL-OPEN DOES NOT MEAN "RUN ANYWAY" FOR A FAMILY'S OWN DEPENDENTS
// (CHAOS-5078 codex round 2 F3, on the PR that first put work_item_attribution
// into this SAME loop ahead of its three readers): before this fix, a
// dependency's runtime failure did not stop this loop from still calling
// ComputeFamily on the families declaring `after` on it. A reader that then
// succeeded (reading the PREVIOUS partition's attribution snapshot, since
// today's write never landed) was added to skipFamilies -- excluding it from
// the bridge's own recompute -- while its failed dependency was NOT added,
// so the bridge WOULD recompute the dependency. The correction never reaches
// the reader: the partition reports success with a stale or absent
// team-derived output. Every family is still attempted exactly once and a
// failure still degrades only that one family plus its transitive
// dependents to the bridge -- see blockedNativeDependency.
//
// A PARTIAL WRITE IS NOT FAIL-OPEN AT THE PARTITION LEVEL EITHER (CHAOS-5078
// codex round 3, astra scale review F1's pre_bridge twin -- see
// lane-ci-required-to-arc's CHAOS-5190/#2276 for the post_bridge sibling).
// The returned error is non-nil exactly when at least one family failed
// AFTER already writing rows (ErrPartialWrite): that family is excluded from
// the bridge's own recompute (skipFamiliesForBridge's contract, unchanged --
// re-running it would duplicate the rows already written to an append-only
// table), so nothing completes its rows for this partition at all. The
// comment this replaces claimed the partition was "re-driven" in that case;
// nothing in redrive.go or postgres.go ever inspected PartialWrite, so that
// was never true. Work now surfaces this error to hold the partition
// 'failed' (re-dispatchable) instead of letting it complete over the gap.
// An ORDINARY (non-partial) refusal is UNAFFECTED: the returned error is
// nil for it, and it stays fail-open to the bridge exactly as before, since
// nothing was written for it yet.
func (handler *PartitionHandler) computeNativeFamilies(ctx context.Context, run Run, partition Partition) ([]string, error) {
	if handler == nil || len(handler.nativeFamilyNames) == 0 {
		return nil, nil
	}
	skipFamilies := make([]string, 0, len(handler.nativeFamilyNames))
	// incomplete (CHAOS-5078 codex r3) names every family that failed AFTER
	// already writing rows this pass -- the partition-level signal Work uses
	// to decide whether to hold the partition out of CompletePartition.
	// Deliberately separate from `blocked`: blocked also includes ordinary
	// (non-partial) refusals, which must NOT hold the partition (they stay
	// fail-open to the bridge), so `blocked`'s membership is not the right
	// set to report here.
	var incomplete []string
	// blocked (CHAOS-5078 codex r2 F3) accumulates every family that did NOT
	// successfully compute this pass -- both an outright ComputeFamily error
	// and a family skipped here because ITS dependency is already in this
	// set. Checked before every family runs, so a failure propagates
	// transitively through the run order for free: work_item_attribution
	// fails -> added here -> work_item_state (after: work_item_attribution)
	// is blocked and added here in turn -> anything declaring `after:
	// work_item_state` would be blocked by IT, without needing to walk the
	// dependency graph more than one edge at a time.
	blocked := make(map[string]struct{}, len(handler.nativeFamilyNames))
	for _, name := range handler.nativeFamilyNames {
		executor := handler.nativeFamilies[name]
		if executor == nil {
			continue
		}
		if dependency, isBlocked := handler.blockedNativeDependency(name, blocked); isBlocked {
			// The dependency failed or was itself blocked THIS pass -- the
			// table this family reads may be stale (still holding an
			// earlier partition's snapshot) or entirely unwritten today.
			// Running anyway and then being added to skipFamilies (as
			// "computed") would permanently exclude this family from the
			// Python bridge's own recompute, so the correction the bridge
			// makes to the dependency is never propagated to this reader.
			// Not attempting it at all -- and leaving it OFF skipFamilies,
			// exactly like a direct refusal -- keeps the bridge as the
			// safety net for both the dependency AND this family together.
			if handler.nativeFamilyLogger != nil {
				handler.nativeFamilyLogger.Error(
					"native metrics.daily family blocked for this partition because its "+
						"declared dependency failed or was blocked; falling back to the "+
						"Python compatibility bridge for both (CHAOS-5078 codex r2 F3)",
					"family", name,
					"blocked_by", dependency,
					"organization_id", run.OrganizationID,
					"target_day", run.TargetDay,
					"partition_id", partition.ID,
					"repo_ids", partition.RepoIDs,
					"run_id", run.ID,
				)
			}
			if handler.nativeObserver != nil {
				_ = handler.nativeObserver.ObserveDailyMetricsNativeFamily(
					name, jobruntime.DailyMetricsNativeFamilyOutcomeRefused, 0, 0,
				)
			}
			blocked[name] = struct{}{}
			continue
		}
		started := handler.nativeFamiliesNow()
		rows, err := executor.ComputeFamily(ctx, run, partition)
		duration := handler.nativeFamiliesNow().Sub(started)
		if err != nil {
			blocked[name] = struct{}{}
			// PARTIAL WRITE IS NOT FAIL-OPEN. An executor that already wrote
			// rows before failing wraps ErrPartialWrite; fail-open there would
			// let the bridge write the same family again, and the output tables
			// are append-only MergeTrees with no version column, so the earlier
			// batches are not replaced -- they DUPLICATE. The family joins the
			// skip list instead.
			//
			// CHAOS-5078 codex round 3 (astra scale review F1's pre_bridge
			// twin): this comment used to say "the partition is re-driven"
			// here. That was false -- nothing in redrive.go or postgres.go
			// ever inspected PartialWrite, so no automatic re-drive existed.
			// `incomplete` below is what actually closes the gap: it makes
			// Work hold the partition 'failed' (re-dispatchable) instead of
			// letting it complete over a family with no writer for this
			// partition's rows.
			//
			// The rows count reported is the executor's TRUE count, not zero:
			// zero would understate what landed, which is exactly the number an
			// operator needs to judge duplication. See CHAOS-4288 / codex r1 on
			// #2235.
			//
			// Still added to `blocked` above (CHAOS-5078 codex r2 F3): a
			// partial write is an INCOMPLETE result for this partition, not a
			// trustworthy one. A family declaring `after` on it must not run
			// natively either, even though the partially-written family itself
			// is excluded from fail-open.
			if errors.Is(err, ErrPartialWrite) {
				skipFamilies = append(skipFamilies, name)
				incomplete = append(incomplete, name)
				if handler.nativeObserver != nil {
					_ = handler.nativeObserver.ObserveDailyMetricsNativeFamily(
						name, jobruntime.DailyMetricsNativeFamilyOutcomePartialWrite, rows, duration,
					)
				}
				continue
			}
			if handler.nativeFamilyLogger != nil {
				handler.nativeFamilyLogger.Error(
					"native metrics.daily family refused for this partition; "+
						"falling back to the Python compatibility bridge "+
						"(CHAOS-5139)",
					"family", name,
					"organization_id", run.OrganizationID,
					"target_day", run.TargetDay,
					"partition_id", partition.ID,
					"repo_ids", partition.RepoIDs,
					"run_id", run.ID,
					"error", err,
				)
			}
			if handler.nativeObserver != nil {
				_ = handler.nativeObserver.ObserveDailyMetricsNativeFamily(
					name, jobruntime.DailyMetricsNativeFamilyOutcomeRefused, 0, duration,
				)
			}
			continue
		}
		skipFamilies = append(skipFamilies, name)
		if handler.nativeObserver != nil {
			_ = handler.nativeObserver.ObserveDailyMetricsNativeFamily(
				name, jobruntime.DailyMetricsNativeFamilyOutcomeComputed, rows, duration,
			)
		}
	}
	if len(incomplete) > 0 {
		return skipFamilies, fmt.Errorf("%w: %s", ErrPreBridgeFamilyIncomplete, strings.Join(incomplete, ","))
	}
	return skipFamilies, nil
}

// blockedNativeDependency reports whether `name` has a registered `after`
// dependency present in `blocked` (already failed, or itself blocked
// transitively earlier in this same pass), and if so, which one -- naming
// ONE culprit is enough for an operator to start looking, and
// nativeFamilyDependencies's declaration order makes the choice
// deterministic run to run.
func (handler *PartitionHandler) blockedNativeDependency(name string, blocked map[string]struct{}) (string, bool) {
	for _, dependency := range handler.nativeFamilyDependencies[name] {
		if _, ok := blocked[dependency]; ok {
			return dependency, true
		}
	}
	return "", false
}

// skipFamiliesForBridge is what the compatibility bridge call is told NOT to
// compute for this partition: every pre_bridge family that just succeeded
// (computeNativeFamilies' own return value), PLUS every registered
// post_bridge family NAME unconditionally (CHAOS-4278) -- a post_bridge
// family has not run YET at this point (it runs after the bridge call
// returns, see computePostBridgeNativeFamilies), so there is no success/
// failure outcome to key the skip decision on. It must still be excluded
// from the bridge's own compute: the bridge is Python's LAST family compute
// this partition, if a post_bridge family were left unskipped Python would
// ALSO write it, duplicating every row the post_bridge executor writes a
// moment later. This is the one place a post_bridge family's inclusion is
// NOT fail-open the way pre_bridge families are: if the post_bridge run
// then fails, nothing computes that family for this partition (Python was
// already told to skip it) -- see computePostBridgeNativeFamilies's doc
// comment for why that tradeoff is inherent to the phase itself, not an
// oversight.
//
// The returned error (CHAOS-5078 codex round 3) is computeNativeFamilies'
// own ErrPreBridgeFamilyIncomplete, passed through unchanged -- the
// post_bridge names appended below never produce an error here, since they
// have not run yet at this point in the partition.
func (handler *PartitionHandler) skipFamiliesForBridge(ctx context.Context, run Run, partition Partition) ([]string, error) {
	skipFamilies, err := handler.computeNativeFamilies(ctx, run, partition)
	if handler == nil || len(handler.postBridgeFamilyNames) == 0 {
		return skipFamilies, err
	}
	return append(skipFamilies, handler.postBridgeFamilyNames...), err
}

// computePostBridgeNativeFamilies runs every registered POST_BRIDGE native
// family executor for one partition, AFTER the compatibility bridge call has
// already returned successfully for that same partition (see Work).
//
// # Why a post_bridge phase exists at all (CHAOS-4278)
//
// families.json's `"phase"` field is additive and defaults to `pre_bridge`
// (today's behavior, computeNativeFamilies above) for every family that does
// not declare it -- this phase exists for the narrow case of a native family
// whose OWN correctness depends on data a DIFFERENT, still-Python-bridged
// family writes during the SAME partition run. `work_item_state`
// (WorkItemStateExecutor) is the first: it reads `work_item_team_
// attributions`, which `work_item_attribution` (still Python-bridged,
// CHAOS-4283) writes fresh every partition. Running work_item_state
// pre_bridge (as it originally shipped) meant the Go read happened BEFORE
// that write -- codex round 1 (2026-09-01) caught this as a P1: a new or
// re-attributed item's freshest attribution was systematically invisible to
// the same-partition read. post_bridge fixes the ordering: pre_bridge
// natives run, then the bridge runs (computing `work_item_attribution`
// among whatever else was not skipped), THEN this method runs
// work_item_state against the now-fresh table.
//
// TEMPORARY, per family (team-lead ruling 2026-09-01): when CHAOS-4283 ports
// `work_item_attribution` to a native Go executor, that executor should run
// BEFORE `work_item_state` within the SAME (pre_bridge) native phase --
// ordinary in-process sequencing, no cross-phase call needed -- and
// `work_item_state` should move back to `pre_bridge` in families.json.
// post_bridge is a bridge (pun intended) for as long as the dependency
// crosses the Python/Go boundary, not a permanent architectural feature.
//
// Fail-open, like computeNativeFamilies, for the SAME reason (a transient
// ClickHouse hiccup must never fail the partition) -- but with a narrower
// safety net: Python was already told (via skipFamiliesForBridge) not to
// compute this family, so a post_bridge failure here means NO writer
// produces this family's rows for this partition, unlike a pre_bridge
// failure (which still has the bridge as a fallback). This method therefore
// never returns an error to Work; a failure increments the SAME
// DailyMetricsNativeFamilyOutcomeRefused telemetry pre_bridge failures use
// (the operator-visible signal for "this partition produced zero rows for
// this family, check why") and, since CHAOS-5139, is also logged via
// nativeFamilyLogger with the wrapped error -- the counter alone could never
// say WHY a family was refused (CHAOS-5138's own root cause).
func (handler *PartitionHandler) computePostBridgeNativeFamilies(ctx context.Context, run Run, partition Partition) {
	if handler == nil || len(handler.postBridgeFamilyNames) == 0 {
		return
	}
	for _, name := range handler.postBridgeFamilyNames {
		executor := handler.postBridgeFamilies[name]
		if executor == nil {
			continue
		}
		started := handler.nativeFamiliesNow()
		rows, err := executor.ComputeFamily(ctx, run, partition)
		duration := handler.nativeFamiliesNow().Sub(started)
		outcome := jobruntime.DailyMetricsNativeFamilyOutcomeComputed
		if err != nil {
			// A post_bridge partial write cannot cause BRIDGE duplication --
			// Python was already told to skip this family unconditionally. But
			// the outcome and the row count must still be truthful: reporting
			// "refused, 0 rows" for a family that wrote several thousand before
			// failing tells an operator the opposite of what happened, and the
			// re-drive decision depends on knowing rows landed. So the outcome
			// is distinguished here even though the skip decision is not.
			if errors.Is(err, ErrPartialWrite) {
				outcome = jobruntime.DailyMetricsNativeFamilyOutcomePartialWrite
			} else {
				rows = 0
				outcome = jobruntime.DailyMetricsNativeFamilyOutcomeRefused
				if handler.nativeFamilyLogger != nil {
					handler.nativeFamilyLogger.Error(
						"native metrics.daily post_bridge family refused for "+
							"this partition; no writer produces this family's "+
							"rows for this partition (CHAOS-5139)",
						"family", name,
						"organization_id", run.OrganizationID,
						"target_day", run.TargetDay,
						"partition_id", partition.ID,
						"run_id", run.ID,
						"error", err,
					)
				}
			}
		}
		if handler.nativeObserver != nil {
			_ = handler.nativeObserver.ObserveDailyMetricsNativeFamily(name, outcome, rows, duration)
		}
	}
}

func (handler *PartitionHandler) Work(ctx context.Context, execution *jobruntime.Execution[jobruntime.DailyMetricsPartitionArgs]) error {
	if handler == nil || handler.store == nil || handler.publisher == nil || handler.compatibility == nil || execution == nil {
		return jobruntime.Permanent(ErrUnavailable)
	}
	partitionID := execution.Args.Payload.PartitionID
	if execution.Envelope.Domain.ID != partitionID {
		return jobruntime.Permanent(ErrInvalidState)
	}
	claim, err := handler.store.ClaimPartition(ctx, partitionID)
	if err != nil {
		return retryClaim(err)
	}
	if claim == nil {
		return nil
	}
	run, err := handler.store.LoadRun(ctx, claim.Partition.RunID)
	if err != nil {
		_ = handler.store.ReleasePartition(ctx, *claim)
		if errors.Is(err, ErrInvalidState) {
			return jobruntime.Permanent(err)
		}
		return jobruntime.Retryable(err)
	}
	if claim.Partition.ID != partitionID || run.Status != "running" || execution.OrganizationID == nil || run.OrganizationID != *execution.OrganizationID {
		_ = handler.store.ReleasePartition(ctx, *claim)
		return jobruntime.Permanent(ErrInvalidState)
	}
	if err := runWithLeaseRenewal(
		ctx,
		claim.LeaseDuration,
		func(renewCtx context.Context) error {
			return handler.store.RenewPartition(renewCtx, *claim)
		},
		func(workCtx context.Context) error {
			skipFamilies, preBridgeErr := handler.skipFamiliesForBridge(workCtx, run, claim.Partition)
			// CHAOS-4316: bound only the compatibility bridge call, not the
			// native-family compute above (or the post_bridge native
			// compute below, CHAOS-4278, for the SAME reason -- a separate,
			// fast, ClickHouse-only path with none of the bridge's
			// liveness gap) -- bridgeCtx is scoped to this one call only,
			// workCtx stays unbounded for computePostBridgeNativeFamilies.
			bridgeCtx := workCtx
			if ceiling := handler.livenessCeiling(len(claim.Partition.RepoIDs)); ceiling > 0 {
				var cancel context.CancelFunc
				bridgeCtx, cancel = context.WithTimeout(bridgeCtx, ceiling)
				defer cancel()
			}
			// preBridgeErr (CHAOS-5078 codex round 3) is deliberately NOT
			// returned here: the bridge call must still run normally for
			// every OTHER family regardless of one pre_bridge family's
			// partial write, exactly as it already does for an ordinary
			// pre_bridge refusal. It is returned at the end of this closure
			// instead (below), once the bridge and post_bridge computes have
			// both had their normal chance to run -- mirroring how a
			// post_bridge failure is only ever discovered after its own
			// full loop completes.
			if err := handler.compatibility.ComputePartition(bridgeCtx, run, claim.Partition, skipFamilies); err != nil {
				return err
			}
			// CHAOS-4278: only after the bridge call has durably succeeded
			// for this partition -- see computePostBridgeNativeFamilies's
			// doc comment for why this ordering is the whole point of the
			// phase, and why it never returns an error here (fail-open,
			// narrower safety net than pre_bridge).
			handler.computePostBridgeNativeFamilies(workCtx, run, claim.Partition)
			return preBridgeErr
		},
	); err != nil {
		if errors.Is(err, ErrPreBridgeFamilyIncomplete) {
			// CHAOS-5078 codex round 3 (astra scale review F1's pre_bridge
			// twin -- see lane-ci-required-to-arc's CHAOS-5190/#2276 for the
			// post_bridge sibling, same shape, no code dependency): a
			// pre_bridge family failed AFTER already writing rows, and that
			// family was deliberately excluded from the bridge's own
			// recompute (skipFamiliesForBridge's contract, unchanged) to
			// avoid duplicating an append-only write. Nothing else produces
			// this family's rows for this partition, so it is released
			// 'failed' (re-dispatchable) instead of completed -- a retry can
			// still fill the gap rather than a silently-incomplete partition
			// reading as succeeded. Not wired through retryCompatibilityError:
			// this is not a compatibility bridge error (the bridge call
			// already returned successfully) -- reusing it would misclassify
			// a pre_bridge native failure as a compat-bridge one. Per-family
			// detail (which family, how many rows) already landed via
			// nativeObserver inside computeNativeFamilies.
			//
			// CHAOS-5078 codex confirmation pass (P2): the durable release
			// itself can fail (a transient DB error, or the lease already
			// expired by the time this 5s-detached call runs) -- discarding
			// that outcome with `_ =` left no trace of whether the
			// 'failed'/pre_bridge_family_incomplete row was ever actually
			// persisted, same class of silent-loss gap CHAOS-4319/CHAOS-4543
			// already closed for the compat-bridge branches below. Now gated
			// exactly like those: observeCompatRetry fires ONLY when the
			// write is confirmed to have landed, and a failed write wraps
			// the returned error with an explicit note rather than looking
			// identical to a successful release.
			if releasePartitionWithReason(handler.store, ctx, *claim, jobruntime.ReasonPreBridgeFamilyIncomplete.String(), handler.nativeFamilyLogger, run) {
				handler.observeCompatRetry(jobruntime.DailyMetricsCompatRetryDecisionReleasedPreBridgeFamilyIncomplete)
				return jobruntime.WithReason(jobruntime.Retryable(err), jobruntime.ReasonPreBridgeFamilyIncomplete)
			}
			return jobruntime.WithReason(
				jobruntime.Retryable(fmt.Errorf("%w (durable release-with-reason also failed to persist)", err)),
				jobruntime.ReasonPreBridgeFamilyIncomplete,
			)
		}
		if errors.Is(err, ErrCompatibilityAmbiguousStuck) {
			// CHAOS-4319: this ledger row will refuse every future attempt
			// identically until a human /repair call moves it -- releasing
			// back to 'failed' (silently re-dispatchable) would only queue
			// up more guaranteed 409s. Persist the terminal outcome instead
			// of letting River's eventual discard be the only trace.
			//
			// codex round 1 (P1): the durable write itself can fail (a
			// transient DB error, or the lease already expired by the time
			// this 5s-detached call runs). Permanent tells River to stop
			// retrying -- that must never be returned unless the write is
			// CONFIRMED to have landed, or the partition is lost with
			// nothing durable to show for it, exactly the failure mode this
			// ticket exists to close. A failed write falls back to ordinary
			// Retryable instead: River keeps trying, and either a later
			// attempt's write succeeds, or the existing discard-after-
			// exhausted-attempts path is no worse than before this ticket.
			if failPartitionPermanently(handler.store, ctx, *claim, jobruntime.ReasonAmbiguousRefused.String()) {
				if handler.compatRetryObserver != nil {
					_ = handler.compatRetryObserver.ObserveDailyMetricsCompatRetry(
						jobruntime.DailyMetricsCompatRetryDecisionPersistedFailed,
					)
				}
				return retryCompatibilityError(err)
			}
			releasePartition(handler.store, ctx, *claim)
			return jobruntime.WithReason(jobruntime.Retryable(err), jobruntime.ReasonAmbiguousRefused)
		}
		if errors.Is(err, ErrCompatibilityProgressStalled) {
			// CHAOS-4316: unlike the ambiguous_stuck case above, a liveness
			// kill is NOT a claim this row can never satisfy -- a fresh
			// attempt might simply not hang -- so this stays 'failed'
			// (silently re-dispatchable), with a bounded failure_reason
			// attached in the same atomic write so an operator reading the
			// partition row can tell a liveness kill apart from any other
			// 'failed' outcome without cross-referencing River's attempt
			// log or Sentry.
			if releasePartitionWithReason(handler.store, ctx, *claim, "progress_stalled", handler.nativeFamilyLogger, run) {
				handler.observeCompatRetry(jobruntime.DailyMetricsCompatRetryDecisionReleasedProgressStalled)
			}
			return retryCompatibilityError(err)
		}
		if errors.Is(err, ErrCompatibilityCapacityExhausted) {
			// CHAOS-4317: same rationale as ErrCompatibilityProgressStalled
			// above -- capacity pressure is transient container state, never
			// a claim this row can't satisfy on a fresh attempt, so this
			// stays 'failed' (silently re-dispatchable) with a bounded
			// failure_reason attached so an operator reading the partition
			// row can tell a pids-capacity refusal apart from any other
			// 'failed' outcome without cross-referencing River's attempt log.
			if releasePartitionWithReason(handler.store, ctx, *claim, "capacity_exhausted", handler.nativeFamilyLogger, run) {
				handler.observeCompatRetry(jobruntime.DailyMetricsCompatRetryDecisionReleasedCapacityExhausted)
			}
			return retryCompatibilityError(err)
		}
		if errors.Is(err, ErrCompatibilityResourceExhaustedDeterministic) {
			// CHAOS-4543: checked BEFORE the generic resource_exhausted
			// branch below -- a KNOWN deterministic guard (see
			// ErrCompatibilityResourceExhaustedDeterministic's doc comment).
			// Still releases with the SAME "resource_exhausted"
			// failure_reason (ordinarily re-dispatchable 'failed', never
			// 'failed_permanent' -- daily-redrive or a future lower-volume
			// day can still succeed), but retryCompatibilityError marks
			// this Permanent so River does not burn its whole attempt
			// budget reproducing an identical, deterministic refusal 5
			// times every cycle.
			//
			// codex review (round 2): Permanent must never be returned
			// unless the durable release is CONFIRMED to have landed --
			// mirrors the ambiguous_stuck branch's own established rule
			// above. A failed write (lease already expired, a transient DB
			// error) here would otherwise tell River to stop retrying a job
			// that recorded nothing durable at all: the exact silent-loss
			// shape CHAOS-4319 exists to close, reintroduced for this new
			// no-retry optimization if left unguarded.
			if releasePartitionWithReason(handler.store, ctx, *claim, "resource_exhausted", handler.nativeFamilyLogger, run) {
				handler.observeCompatRetry(jobruntime.DailyMetricsCompatRetryDecisionReleasedResourceExhaustedDeterministic)
				return retryCompatibilityError(err)
			}
			return jobruntime.WithReason(jobruntime.Retryable(err), jobruntime.ReasonResourceExhausted)
		}
		if errors.Is(err, ErrCompatibilityResourceExhausted) {
			// CHAOS-4543: same rationale as ErrCompatibilityProgressStalled
			// above -- a resource_exhausted kill (the runner's RSS watchdog,
			// its own RLIMIT_AS backstop, or a deliberate loader row-cap
			// guard such as TestopsRowCapExceeded) is not a claim this row
			// can never satisfy: a lower-volume day, a raised budget, or
			// simply less concurrent load on a fresh attempt could all
			// succeed later. Before this fix, this class fell through to the
			// generic releasePartition path below with no failure_reason
			// persisted -- a partition that strands this way (River
			// eventually discards the job after its attempt budget) left an
			// operator with a bare 'failed' row and no clue why, the exact
			// CHAOS-4543 "raw text not captured" gap this ticket closes.
			if releasePartitionWithReason(handler.store, ctx, *claim, "resource_exhausted", handler.nativeFamilyLogger, run) {
				handler.observeCompatRetry(jobruntime.DailyMetricsCompatRetryDecisionReleasedResourceExhausted)
			}
			return retryCompatibilityError(err)
		}
		if errors.Is(err, ErrCompatibilityProcessSignaled) {
			// CHAOS-4543: sibling of the resource_exhausted branch above -- a
			// runner subprocess killed by an external signal (kernel OOM,
			// SIGTERM) is the same non-terminal, retryable shape and hit the
			// identical missing-branch gap.
			if releasePartitionWithReason(handler.store, ctx, *claim, "process_signaled", handler.nativeFamilyLogger, run) {
				handler.observeCompatRetry(jobruntime.DailyMetricsCompatRetryDecisionReleasedProcessSignaled)
			}
			return retryCompatibilityError(err)
		}
		releasePartition(handler.store, ctx, *claim)
		return retryCompatibilityError(err)
	}
	if handler.sourceChecker != nil {
		families, checkErr := handler.sourceChecker.ZeroRowFamiliesWithSourceData(ctx, partitionID)
		if checkErr != nil {
			releasePartition(handler.store, ctx, *claim)
			if errors.Is(checkErr, ErrInvalidState) {
				return jobruntime.Permanent(checkErr)
			}
			return jobruntime.Retryable(checkErr)
		}
		if len(families) > 0 {
			if handler.zeroRowsObserver != nil {
				for _, family := range families {
					_ = handler.zeroRowsObserver.ObserveDailyMetricsFamilyZeroRowsWithSource(family)
				}
			}
			releasePartition(handler.store, ctx, *claim)
			return jobruntime.Permanent(ErrZeroRowsWithSourceData)
		}
	}
	if err := handler.store.CompletePartition(ctx, *claim, handler.publisher); err != nil {
		// The one post-claim exit that used to return without releasing. If the
		// completion failed while the lease was still ours, releasing makes the
		// partition immediately re-claimable instead of parking the retry for the
		// rest of the lease; if the lease was already lost the release is a
		// no-op, and the store records that rather than dropping it.
		releasePartition(handler.store, ctx, *claim)
		return jobruntime.Retryable(err)
	}
	return nil
}

type FinalizeHandler struct {
	store         Store
	compatibility CompatibilityExecutor
	// nativeFinalizeFamilies mirrors PartitionHandler.nativeFamilies: a
	// nil/empty map is a no-op, so every family stays on the compatibility
	// path exactly as before this capability existed.
	nativeFinalizeFamilies    map[string]NativeFinalizeFamilyExecutor
	nativeFinalizeFamilyNames []string
	// nativeFinalizeObserver reports each family's outcome. Reuses the
	// PartitionHandler observer rather than declaring a parallel interface:
	// the shape is identical and a second interface would mean two dashboards
	// answering one question.
	nativeFinalizeObserver jobruntime.DailyMetricsNativeFamilyObserver
	// nativeFinalizeNow is injected so the duration a test observes is the one
	// the test controls; nil means time.Now.
	nativeFinalizeNow func() time.Time
}

func NewFinalizeHandler(store Store, compatibility CompatibilityExecutor) (*FinalizeHandler, error) {
	if store == nil || compatibility == nil {
		return nil, ErrUnavailable
	}
	return &FinalizeHandler{store: store, compatibility: compatibility}, nil
}

// pythonRecognisedFinalizeFamilies is the set of finalize-family names the
// Python compatibility bridge actually gates on, in
// src/dev_health_ops/metrics/job_daily.py's run_daily_metrics_finalize:
//
//	if "ic_finalize" not in skip_families:
//
// It is duplicated here because the Go and Python processes cannot share a
// value, and duplicated deliberately rather than inferred: the alternative is
// trusting that whatever string a caller registers happens to be one Python
// understands. finalizeFamilyGateAgreementTest pins this slice against the
// Python source, with a negative control, so the copy cannot drift silently.
var pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}

// ErrUnknownFinalizeFamily is returned when a registered finalize family is
// not one the Python bridge gates on.
var ErrUnknownFinalizeFamily = errors.New("daily: finalize family is not recognised by the compatibility bridge")

// ErrNativeFinalizeFamilyFailed wraps any native finalize-family failure.
// retryCompatibilityError's default arm makes it Retryable, which is the whole
// policy: the run is redriven rather than completed.
var ErrNativeFinalizeFamilyFailed = errors.New("daily: native finalize family failed")

// SetNativeFinalizeFamilies registers the finalize-scope families computed
// natively in Go instead of by the Python compatibility bridge. Mirrors
// PartitionHandler.SetNativeFamilies in INTENT (deterministic iteration,
// nothing depending on Go map order) but NOT in mechanism: that one sorts by
// name because its families are independent and order-blind. Finalize
// families are not -- computeNativeFinalizeFamilies below walks
// nativeFinalizeFamilyNames and, on a mid-loop cancellation, marks every name
// from the current loop INDEX onward as refused
// (nativeFinalizeFamilyNames[index:]), so which family comes first decides
// which families get computed before a cancellation and which get marked
// refused without ever running. That is part of the contract, not an
// implementation detail a lexical sort should get to decide, so iteration
// order here is pythonRecognisedFinalizeFamilies' own DECLARED order instead
// -- see TestFinalizeFamiliesIterateInDeclaredOrderNotSortedName, which pins
// it with two families whose sorted and declared orders differ.
//
// IT VALIDATES THE NAMES (CHAOS-4290, #2241 r1 Finding 2). The names travel to
// Python as SkipFamilies and are compared there by string equality, so a name
// Python does not recognise means the native family runs AND the bridge runs:
// two writers on an append-only table, both succeeding, the later one winning
// silently. A typo -- "ic_finalise" -- is enough, and nothing downstream can
// detect it, because from Python's side an unrecognised skip entry is
// indistinguishable from one meant for a family it does not own.
//
// Registration is the last place the two vocabularies can be compared while a
// human is still watching, so it fails LOUDLY here rather than degrading. It is
// also all-or-nothing: a partial registration would leave the caller believing
// a family is native when it is not.
func (handler *FinalizeHandler) SetNativeFinalizeFamilies(families map[string]NativeFinalizeFamilyExecutor) error {
	if handler == nil {
		return nil
	}
	for name, executor := range families {
		if !slices.Contains(pythonRecognisedFinalizeFamilies, name) {
			return fmt.Errorf("%w: %q (bridge gates on %v)",
				ErrUnknownFinalizeFamily, name, pythonRecognisedFinalizeFamilies)
		}
		// A nil executor passed the name check above but would silently vanish
		// in computeNativeFinalizeFamilies (`if executor == nil { continue }`,
		// WITHOUT adding the name to skipFamilies) -- Python's bridge would then
		// compute this family too, exactly the two-writer hazard this function's
		// own name-validation exists to prevent, just reached through a nil
		// value instead of an unrecognised string. Reject it here, at the same
		// "fail loudly, all-or-nothing" boundary as the name check, rather than
		// let it degrade silently three calls later.
		if executor == nil {
			return fmt.Errorf("%w: %q registered with a nil executor", ErrUnknownFinalizeFamily, name)
		}
	}
	names := make([]string, 0, len(families))
	for _, name := range pythonRecognisedFinalizeFamilies {
		if _, registered := families[name]; registered {
			names = append(names, name)
		}
	}
	handler.nativeFinalizeFamilies = families
	handler.nativeFinalizeFamilyNames = names
	return nil
}

// SetNativeFinalizeFamilyObserver attaches the per-family outcome counters.
// Nil is a silent no-op, matching every other observer in this package: a
// handler with no observer still behaves identically.
func (handler *FinalizeHandler) SetNativeFinalizeFamilyObserver(
	observer jobruntime.DailyMetricsNativeFamilyObserver,
) {
	if handler == nil {
		return
	}
	handler.nativeFinalizeObserver = observer
}

func (handler *FinalizeHandler) finalizeNow() time.Time {
	if handler.nativeFinalizeNow != nil {
		return handler.nativeFinalizeNow()
	}
	return time.Now()
}

// observeFinalizeFamily reports one attempt. The observer's own error is
// swallowed deliberately: telemetry that can fail a job is worse than no
// telemetry, and this is the rule every observer in this package follows.
// CHAOS-5151. The observer error used to be discarded outright (`_ =
// ...ObserveDailyMetricsNativeFamily(...)`) -- exercised directly by
// TestFailingNativeFinalizeFamilyIsReportedRefused's telemetry-down fixture,
// which proved Work still succeeds when the observer errors but never checked
// whether that failure left any trace. A family whose outcome telemetry
// silently fails to record is invisible in exactly the same way an
// unregistered family is (see dailyMetricsNativeFamilies' doc comment in
// telemetry.go): the counter this call was supposed to move never moves, and
// nothing says why. run carries the identifiers (run_id, org_id, target_day)
// this call site has and the observer interface itself does not -- logged
// here, not inside the observer, because only the caller knows which run and
// family this failure belongs to.
func (handler *FinalizeHandler) observeFinalizeFamily(
	run Run, family string, outcome jobruntime.DailyMetricsNativeFamilyOutcome,
	rowsWritten int, started time.Time,
) {
	if handler.nativeFinalizeObserver == nil {
		return
	}
	if err := handler.nativeFinalizeObserver.ObserveDailyMetricsNativeFamily(
		family, outcome, rowsWritten, handler.finalizeNow().Sub(started),
	); err != nil {
		slog.Default().Error("daily finalize telemetry failed",
			"error", err,
			"run_id", run.ID,
			"organization_id", run.OrganizationID,
			"target_day", run.TargetDay.Format("2006-01-02"),
			"family", family,
			"outcome", outcome,
		)
	}
}

// computeNativeFinalizeFamilies runs every registered finalize-scope executor
// and returns the names that SUCCEEDED, in sorted order.
//
// computeNativeFinalizeFamilies runs every registered finalize-scope executor
// and returns the names to skip plus the first failure.
//
// NO FAIL-OPEN, AT ALL (CHAOS-4290, #2241 r2 Findings 1 and 2; team-lead's
// ruling). CHAOS-4276's fail-open is correct for partition scope and wrong
// here, and the r1 attempt to make it conditional on rowsWritten was wrong too
// -- for two reasons that only showed up under retry:
//
//  1. rowsWritten answers a per-ATTEMPT question. After a native write and a
//     bridge failure the whole finalize retries; if the next native attempt
//     fails before writing it reports (0, err), and a fail-open predicate reads
//     that as "nothing was written" -- when the PREVIOUS attempt already wrote.
//     Python then overwrites the native generation and the retry succeeds, so
//     nothing surfaces the duplicate writer.
//  2. Keeping a partially-written family skipped but letting the run COMPLETE
//     records success over incomplete output, with nothing to repair it.
//
// So any native error -- before, during or after a write -- fails the attempt.
// The family stays in the skip list, the bridge is not called, the run does not
// complete, and River's existing attempt machinery redrives it; after max
// attempts the CHAOS-5040 blocked-run marker records it under
// BlockedReasonFinalizeFailed. Python therefore never writes a family
// registered as native, which is what makes per-run durable state unnecessary.
//
// That last sentence about the marker was FALSE when first written (#2241 r3
// Finding 2): reconcileBlockedRunsSQL fired only on a failed_permanent
// PARTITION, and a stranded finalize has none, so blocked_at stayed NULL and
// the run was invisible to the sweep built to surface wedged runs. The
// predicate now covers it. Recording the correction rather than quietly editing
// the claim, because the failure was writing a mechanism from a ruling's
// wording instead of verifying the mechanism existed.
//
// THIS REQUIRES THE NATIVE WRITER TO BE IDEMPOTENT: a redrive must land on the
// same keys with a later computed_at so the dedup read supersedes rather than
// accumulates. That is a contract on the executor, pinned by test, not an
// assumption.
//
// The outcome vocabulary still distinguishes the two failure shapes, because
// they mean different things to an operator even though both now redrive:
// partial_write (rows already landed, a redrive may duplicate if the writer is
// not idempotent) versus refused (nothing landed).
func (handler *FinalizeHandler) computeNativeFinalizeFamilies(ctx context.Context, run Run) ([]string, error) {
	if handler == nil || len(handler.nativeFinalizeFamilyNames) == 0 {
		return nil, nil
	}
	skipFamilies := make([]string, 0, len(handler.nativeFinalizeFamilyNames))
	for index, name := range handler.nativeFinalizeFamilyNames {
		executor := handler.nativeFinalizeFamilies[name]
		if executor == nil {
			continue
		}
		// Between-family cancellation check. Once the lease is lost another
		// worker may already own this run, so continuing to write is the
		// two-writer hazard with extra steps.
		if err := ctx.Err(); err != nil {
			// Sliced from the LOOP INDEX, not from len(skipFamilies): the two
			// diverge as soon as a family is skipped for a nil executor.
			for _, remaining := range handler.nativeFinalizeFamilyNames[index:] {
				handler.observeFinalizeFamily(
					run, remaining, jobruntime.DailyMetricsNativeFamilyOutcomeRefused, 0, handler.finalizeNow(),
				)
			}
			return skipFamilies, fmt.Errorf("%w: %s: %w", ErrNativeFinalizeFamilyFailed, name, err)
		}
		started := handler.finalizeNow()
		rowsWritten, err := executor.ComputeFinalizeFamily(ctx, run)
		// The family is added to the skip list BEFORE the error is examined:
		// on the failing attempt the bridge must not compute it either, and on
		// a future change that lets the bridge run anyway this is what keeps
		// Python out.
		skipFamilies = append(skipFamilies, name)
		switch {
		case err == nil:
			handler.observeFinalizeFamily(
				run, name, jobruntime.DailyMetricsNativeFamilyOutcomeComputed, rowsWritten, started,
			)
		case rowsWritten > 0:
			handler.observeFinalizeFamily(
				run, name, jobruntime.DailyMetricsNativeFamilyOutcomePartialWrite, rowsWritten, started,
			)
			return skipFamilies, fmt.Errorf("%w: %s: %w", ErrNativeFinalizeFamilyFailed, name, err)
		default:
			handler.observeFinalizeFamily(
				run, name, jobruntime.DailyMetricsNativeFamilyOutcomeRefused, 0, started,
			)
			return skipFamilies, fmt.Errorf("%w: %s: %w", ErrNativeFinalizeFamilyFailed, name, err)
		}
	}
	return skipFamilies, nil
}

func (handler *FinalizeHandler) Work(ctx context.Context, execution *jobruntime.Execution[jobruntime.DailyMetricsFinalizeArgs]) error {
	if handler == nil || handler.store == nil || handler.compatibility == nil || execution == nil {
		return jobruntime.Permanent(ErrUnavailable)
	}
	runID := execution.Args.Payload.RunID
	if execution.Envelope.Domain.ID != runID {
		return jobruntime.Permanent(ErrInvalidState)
	}
	claim, err := handler.store.ClaimFinalize(ctx, runID)
	if err != nil {
		return retryClaim(err)
	}
	if claim == nil {
		return nil
	}
	if execution.OrganizationID == nil || claim.Run.ID != runID || claim.Run.Status != "running" || claim.Run.OrganizationID != *execution.OrganizationID {
		_ = handler.store.ReleaseFinalize(ctx, *claim)
		return jobruntime.Permanent(ErrInvalidState)
	}
	if err := runWithLeaseRenewal(
		ctx,
		claim.LeaseDuration,
		func(renewCtx context.Context) error {
			return handler.store.RenewFinalize(renewCtx, *claim)
		},
		func(workCtx context.Context) error {
			// Native families run FIRST and their names become the bridge's
			// skip list, so the bridge never recomputes what Go just wrote.
			// Inside the lease-renewal callback deliberately: a native
			// family's compute is real work and must hold the lease the same
			// way the bridge call does.
			skipFamilies, err := handler.computeNativeFinalizeFamilies(workCtx, claim.Run)
			if err != nil {
				// The bridge is NOT called. Calling it after a native failure
				// is the fail-open this ruling removed, and completing the run
				// afterwards is what recorded success over partial output.
				return err
			}
			return handler.compatibility.Finalize(workCtx, claim.Run, skipFamilies)
		},
	); err != nil {
		// LOG THE UNDERLYING ERROR BEFORE RETURNING (CHAOS-4290, #2243's
		// metrics-executed-proof failure).
		//
		// The retryable error the caller returns is wrapped by the adapter into
		// the fixed string "dev-health job failed [retryable]", and the cause is
		// serialised NOWHERE. On #2243's E2E every finalize job on every run
		// exhausted its four attempts -- 96 starts, 96 discards -- and the only
		// evidence of WHY was 96 identical wrapper strings. A family that fails
		// every attempt on every run must not be that quiet.
		//
		// Logged here rather than left to the adapter because this is the only
		// frame that still has the family scope: which run, which org, which
		// attempt of how many.
		// slog.Default() rather than skipping on a nil logger, following
		// providerunit.go's pattern. `if logger != nil { log }` would make a nil
		// logger produce SILENCE -- reintroducing, in a different place, exactly
		// the defect this block exists to remove.
		finalizeLogger := execution.Logger
		if finalizeLogger == nil {
			finalizeLogger = slog.Default()
		}
		{
			finalizeLogger.Error("daily finalize failed",
				"error", err,
				"run_id", runID,
				"organization_id", claim.Run.OrganizationID,
				"target_day", claim.Run.TargetDay.Format("2006-01-02"),
				"attempt", execution.Attempt,
				"max_attempts", execution.Definition.MaxAttempts,
				"native_families", handler.nativeFinalizeFamilyNames,
				"terminal", execution.Attempt >= execution.Definition.MaxAttempts,
			)
		}
		// FINAL ATTEMPT: write the terminal state rather than releasing for a
		// retry that will never come. River discards after this, and without a
		// terminal write the run sits at status='running',
		// finalization_status='failed' -- byte-identical to attempt 1 -- so
		// nothing downstream can tell a stranded run from a retrying one.
		//
		// Attempt and MaxAttempts come from the adapter's own pair, the same
		// values River acts on, so "final" here means what River means by it
		// rather than a parallel notion maintained beside it.
		if execution.Attempt >= execution.Definition.MaxAttempts {
			// The terminal write's OWN error is logged, not discarded (r2
			// finding #1, CHAOS-4290): a failure here means the run is left at
			// status='running' -- byte-identical to attempt 1, invisible to the
			// blocked-marker sweep -- and silently swallowing it would have made
			// this exactly the "the fix reports success while doing nothing"
			// shape FailFinalizePermanently's own RowsAffected check exists to
			// catch on the write side, just moved one frame up to the caller
			// that never looked.
			if failErr := failFinalizePermanently(handler.store, ctx, *claim); failErr != nil {
				finalizeLogger.Error("daily finalize terminal write failed",
					"error", failErr,
					"run_id", runID,
					"organization_id", claim.Run.OrganizationID,
					"target_day", claim.Run.TargetDay.Format("2006-01-02"),
				)
			}
			return retryCompatibilityError(err)
		}
		releaseFinalize(handler.store, ctx, *claim)
		return retryCompatibilityError(err)
	}
	if err := handler.store.CompleteFinalize(ctx, *claim); err != nil {
		// Symmetric with the partition layer: this exit claimed and returned
		// retryable without releasing, which is the most likely way the lease
		// behind CHAOS-3991 was orphaned in the first place.
		releaseFinalize(handler.store, ctx, *claim)
		return jobruntime.Retryable(err)
	}
	return nil
}

func runWithLeaseRenewal(
	ctx context.Context,
	leaseDuration time.Duration,
	renew func(context.Context) error,
	work func(context.Context) error,
) error {
	if ctx == nil || leaseDuration < 3*time.Millisecond || renew == nil || work == nil {
		return ErrInvalidState
	}
	workCtx, cancelWork := context.WithCancel(ctx)
	defer cancelWork()
	stop := make(chan struct{})
	renewalResult := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(leaseDuration / 3)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				renewalResult <- nil
				return
			case <-ctx.Done():
				cancelWork()
				renewalResult <- ctx.Err()
				return
			case <-ticker.C:
				// BOUNDED (CHAOS-4290, #2241 r3 Finding 1). renew(ctx) used the
				// caller's context, which has no deadline of its own, so a
				// renewal stuck in a network black hole never returned and the
				// work context was never cancelled -- because cancellation
				// happens only on the RESULT of this call.
				//
				// The production lease is 10 minutes and the adapter timeout is
				// 15, so a stalled renewal left the native executor running for
				// up to five minutes past a reclaimable lease. Another worker
				// claims the run, computes the same family, appends another
				// generation, and the dedup read silently takes the later one:
				// the two-writer hazard reached through TIMING rather than
				// through the skip list.
				//
				// The deadline is DERIVED from the lease, never a fresh
				// constant, so the two cannot drift apart. It matches the tick
				// interval: a renewal that cannot finish within one tick has
				// already missed its slot, and waiting longer only shortens the
				// margin before expiry.
				renewCtx, cancelRenew := context.WithTimeout(ctx, leaseDuration/3)
				err := renew(renewCtx)
				cancelRenew()
				if err != nil {
					// A renewal TIMEOUT is a lease loss, not a transient to
					// retry: we no longer know we hold the lease, and continuing
					// to write while another worker may own the run is the exact
					// failure this bound exists to prevent.
					cancelWork()
					renewalResult <- err
					return
				}
			}
		}
	}()
	workErr := work(workCtx)
	close(stop)
	renewalErr := <-renewalResult
	if renewalErr != nil {
		return renewalErr
	}
	return workErr
}

func releasePartition(store Store, ctx context.Context, claim PartitionClaim) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = store.ReleasePartition(releaseCtx, claim)
}

// releasePartitionWithReason mirrors releasePartition's own-context
// detachment, additionally persisting the bounded failure_reason (CHAOS-4316)
// in the SAME atomic write as the status transition -- a failed call here
// means the row was not transitioned at all (e.g. the lease already expired
// out from under this attempt), exactly releasePartition's existing
// best-effort failure semantics, not a partial write missing only the reason.
//
// Returns whether the durable write actually landed (codex review,
// CHAOS-4543): callers that also observe a released_* telemetry decision
// must gate that observation on this -- an unconditional observe would
// report a durable disposition ("this reason was persisted") even when the
// row was never transitioned at all, exactly the kind of telemetry/reality
// mismatch this ticket's own diagnosis had to work around (failure_reason
// silently NULL despite a classified failure).
//
// CHAOS-5078 codex confirmation pass: logs the underlying store error
// before returning false, covering every call site below in one place.
// Before this, a durable release-with-reason failure was invisible at this
// function's own boundary -- every caller only ever saw a bare bool, so
// none of them (including the five call sites that predate this fix) could
// distinguish "release failed, here is why" from "release failed" with no
// further trace beyond the eventual, silent lease-reclaim path. logger is
// optional (nil is a no-op, same discipline as nativeFamilyLogger
// elsewhere) -- absence of a logger must never change return behavior.
func releasePartitionWithReason(
	store Store, ctx context.Context, claim PartitionClaim, reason string,
	logger NativeFamilyRefusalLogger, run Run,
) bool {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := store.ReleasePartitionWithReason(releaseCtx, claim, reason); err != nil {
		if logger != nil {
			logger.Error(
				"daily metrics partition release-with-reason failed; the durable "+
					"failure_reason may not have been persisted",
				"organization_id", run.OrganizationID,
				"target_day", run.TargetDay,
				"partition_id", claim.Partition.ID,
				"repo_ids", claim.Partition.RepoIDs,
				"run_id", claim.Partition.RunID,
				"reason", reason,
				"error", err,
			)
		}
		return false
	}
	return true
}

// failPartitionPermanently durably terminalizes a partition stuck ambiguous
// (CHAOS-4319), mirroring releasePartition's own-context detachment so the
// write still lands even when the caller's ctx is already done.
// failPartitionPermanently durably terminalizes a partition stuck ambiguous
// (CHAOS-4319), mirroring releasePartition's own-context detachment so the
// write still lands even when the caller's ctx is already done. Returns
// whether the write actually succeeded -- the caller must not classify the
// job Permanent (stop retrying) unless this is true, or a failed durable
// write would silently drop the partition exactly like the bug this ticket
// fixes.
func failPartitionPermanently(store Store, ctx context.Context, claim PartitionClaim, reason string) bool {
	failCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return store.FailPartitionPermanently(failCtx, claim, reason) == nil
}

func releaseFinalize(store Store, ctx context.Context, claim FinalizeClaim) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = store.ReleaseFinalize(releaseCtx, claim)
}

// failFinalizePermanently writes the terminal state on a context detached from
// the failing one, exactly as releaseFinalize does: the work context may already
// be cancelled, and a terminal write that silently did not happen is the whole
// defect this fixes.
//
// Returns the store's own error (r2 finding #1, CHAOS-4290) instead of
// discarding it: this is the LAST write in the finalize failure path -- unlike
// releaseFinalize, whose failure a River-driven redrive or lease expiry can
// still recover from, a terminal write that fails here has no other
// mechanism behind it. The caller logs it with the run's own identifiers.
func failFinalizePermanently(store Store, ctx context.Context, claim FinalizeClaim) error {
	terminalCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return store.FailFinalizePermanently(terminalCtx, claim)
}
