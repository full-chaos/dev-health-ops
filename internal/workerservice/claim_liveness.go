package workerservice

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
)

// claimLiveness closes the gap two rounds of codex review on CHAOS-4029
// identified in the first design: an independent self-probe goroutine
// (internal/platform/selfprobe.Monitor) proves the domain pool is
// reachable, but it runs on its OWN goroutine and therefore keeps
// succeeding even if the real River consumer -- the thing that actually
// claims and executes jobs -- is deadlocked while the database stays
// healthy. That is precisely the CHAOS-4029 "worker whose recent jobs are
// all terminal-without-execution is not healthy" scenario the ticket's
// Wanted section calls out, and a probe disconnected from the real claim
// path cannot detect it.
//
// claimLiveness is fed by claimLivenessObserver's JobFinished tap, PER
// QUEUE (round-2 codex finding: a shared single clock lets one healthy
// queue mask a wedged sibling on a multi-queue worker), and only for
// outcomes that PROVE the job reached real execution -- see
// executionReached's doc comment for round-2's second finding (JobStarted
// fires before validation/tenant/budget/idempotency gates, so a job that
// never got past those would otherwise keep refreshing this clock forever).
type claimLiveness struct {
	mu       sync.RWMutex
	perQueue map[string]time.Time
	// inHandler counts, per queue, the jobs of THIS process that are inside a
	// handler right now (HandlerInvoked minus HandlerReturned). River counts a
	// job as running from claim -- including one failing at its idempotency
	// Begin -- so a queue whose Running count equals inHandler is genuinely busy
	// with handler work, while Running above inHandler means slots are stuck
	// BEFORE their handler. That difference is what lets a full queue stay
	// healthy for long-running jobs yet still turn red for a stale pooler.
	inHandler map[string]int64
	// preHandlerSince is, per queue, when the CURRENT unbroken run of
	// observations began in which at least one running slot was not inside a
	// handler (Running > inHandler). It is written only by preHandlerStuckFor,
	// from the readiness poll, and cleared the moment a poll sees every running
	// slot inside a handler (or none running).
	preHandlerSince map[string]time.Time
	// gateFailures is, per queue, the run of idempotency-gate failures (a job
	// that failed at its idempotency Begin, or at its completion claim) seen
	// since a handler last ran on that queue. It exists for the state the queue
	// snapshot cannot show (CHAOS-6864): River parks a failed job as retryable
	// until its backoff elapses, so between two failures the queue reads
	// Available=0 and Running=0 and every snapshot-driven arm calls it idle.
	gateFailures map[string]gateFailureRun
	// refusalLogged is, per queue, when its execution_liveness refusal was last
	// logged (claimRefusalDue).
	refusalLogged map[string]time.Time
	// staleWindow defaults to claimStalenessWindow in newClaimLiveness.
	// Exposed via SetStaleWindow so a test can shrink it from the
	// production 60s to a real-but-small duration (mirroring
	// selfprobe.Monitor.SetStaleness) instead of sleeping out the full
	// window.
	staleWindow time.Duration
	// preclaim is true from construction until markRuntimeLive reports that
	// preclaim readiness has actually passed. While true, claimLivenessReady
	// must not fail a queue on backlog-with-no-recent-claim alone: no claim
	// can possibly exist yet, by construction of the startup order itself
	// (preclaimReadinessComponent runs strictly before workerProcessComponent
	// ever starts River's producers), no matter how long preclaim's own
	// retry loop has been running against an unrelated slow dependency. The
	// zero value is false so every test fixture built directly as
	// &claimLiveness{} -- there is no other production construction path --
	// keeps its original always-enforced semantics without having to opt in.
	preclaim bool
}

// newClaimLiveness seeds every selected queue's clock to now, NOT the zero
// value, and starts the tracker in preclaim mode.
//
// The seed is deliberately different from selfprobe.Monitor's "never_proven"
// fail-closed-until-first-sample discipline: it gives the real consumer a
// full staleness window to make its first claim on each selected queue once
// running actually starts, ample for a healthy process, while a consumer
// that is ACTUALLY wedged still fails visibly once that window elapses with
// nothing claimed. It alone is not sufficient during preclaim itself,
// though: preclaim's own retry loop (startupRetryBudget) can legitimately
// run far longer than the staleness window while retrying an unrelated slow
// dependency, and no claim can exist yet regardless -- River has not
// started. preclaim being true is what actually prevents claimLivenessReady
// from mistaking that elapsed retry time for a wedged consumer; the seed
// then supplies the grace window markRuntimeLive hands off into once
// preclaim ends and a real claim becomes possible.
//
// queues is the process's selected queue set (cfg.Queues); only those
// queues are pre-seeded, so claimLivenessReady's per-queue lookup always
// finds a seeded entry for anything this process could actually be asked
// about.
func newClaimLiveness(now time.Time, queues []string) *claimLiveness {
	perQueue := make(map[string]time.Time, len(queues))
	for _, queue := range queues {
		perQueue[queue] = now
	}
	return &claimLiveness{perQueue: perQueue, staleWindow: claimStalenessWindow, preclaim: true}
}

// SetStaleWindow overrides the staleness window from the production default
// (claimStalenessWindow). Intended for tests proving staleness detection in
// real wall-clock time without waiting out the production window; safe to
// call any time since it is guarded by the same mutex as recordClaim/since.
func (c *claimLiveness) SetStaleWindow(window time.Duration) {
	if window <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.staleWindow = window
}

func (c *claimLiveness) staleness() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.staleWindow <= 0 {
		return claimStalenessWindow
	}
	return c.staleWindow
}

func (c *claimLiveness) recordClaim(queue string, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.perQueue == nil {
		c.perQueue = make(map[string]time.Time, 1)
	}
	if now.After(c.perQueue[queue]) {
		c.perQueue[queue] = now
	}
}

// reseed resets every currently-tracked queue's clock to now (codex round-3
// finding, P2). newClaimLiveness's construction-time seed only starts the
// grace period from the moment claim was ALLOCATED, early in
// configureWorkerDependenciesWithSources -- before worker-family
// composition, which can itself take real time (opening ClickHouse/Valkey
// connections, loading provider runtime config). If that takes longer than
// the staleness window and a selected queue already has backlog, the
// original seed would already be stale by the time
// preclaimReadinessComponent evaluates it, and River has still not started
// -- reproducing the exact startup deadlock newClaimLiveness's seeding
// exists to prevent, just via a slower path. The production call site
// (dependencies.go) calls this immediately before preclaimReadinessComponent
// is added to the returned components, so the grace period restarts from
// "construction has actually finished," not from struct allocation.
// handlerInvoked records a job entering a handler: it is claim evidence and one
// more slot genuinely inside a handler.
func (c *claimLiveness) handlerInvoked(queue string, now time.Time) {
	c.mu.Lock()
	if c.inHandler == nil {
		c.inHandler = make(map[string]int64, 1)
	}
	c.inHandler[queue]++
	// A job got through every gate, so the work pool reached the database: the
	// run of gate failures ends here.
	delete(c.gateFailures, queue)
	c.mu.Unlock()
	c.recordClaim(queue, now)
}

// gateFailureRun is one unbroken run of idempotency-gate failures on a queue.
type gateFailureRun struct {
	count int64
	first time.Time
	last  time.Time
	// expires is when this evidence stops counting if no further failure lands.
	expires time.Time
}

// gateFailureRetryHorizon is how long a failure that River WILL retry stays
// evidence: the longest retry delay jobruntime.NextRetryAt produces (a 5m cap
// with +/-10% jitter, i.e. 5m30s) plus slack. Within it the next failure is
// still expected; past it the job is no longer being retried and the evidence
// has nothing left to say.
const gateFailureRetryHorizon = 6 * time.Minute

// gateFailureMinimum is the fewest failures that make a run. One failure is a
// blip (the job's own retry answers it); the repeat is the signal.
const gateFailureMinimum = 2

// recordGateFailure records a job failing at its idempotency gate on queue.
// retryPending is whether River will retry the job (so another failure is due
// within gateFailureRetryHorizon); a terminal failure keeps the evidence only
// for one staleness window.
func (c *claimLiveness) recordGateFailure(queue string, retryPending bool, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gateFailures == nil {
		c.gateFailures = make(map[string]gateFailureRun, 1)
	}
	run := c.gateFailures[queue]
	if run.count == 0 || now.After(run.expires) {
		run = gateFailureRun{first: now}
	}
	run.count++
	run.last = now
	hold := c.staleWindow
	if hold <= 0 {
		hold = claimStalenessWindow
	}
	if retryPending {
		hold = gateFailureRetryHorizon
	}
	run.expires = now.Add(hold)
	c.gateFailures[queue] = run
}

// gateFailureEvidence reports queue's live run of gate failures: how many, and
// how long the run has lasted between its first and last failure. Zero when
// there is none or it has expired.
func (c *claimLiveness) gateFailureEvidence(queue string, now time.Time) (count int64, span time.Duration) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	run, ok := c.gateFailures[queue]
	if !ok || run.count == 0 || now.After(run.expires) {
		return 0, 0
	}
	return run.count, run.last.Sub(run.first)
}

// handlerReturned records a handler leaving (success, error or panic). It is
// also claim evidence: a handler that ran to its end proves the consumer path
// worked, and it covers the instant where River's row still reads running after
// the handler is already gone.
func (c *claimLiveness) handlerReturned(queue string, now time.Time) {
	c.mu.Lock()
	if c.inHandler[queue] > 0 {
		c.inHandler[queue]--
	}
	c.mu.Unlock()
	c.recordClaim(queue, now)
}

// preHandlerStuckFor reports how long queue has CONTINUOUSLY had a running slot
// that is not inside a handler, as seen by successive readiness polls; zero when
// every running slot is inside a handler (or none is running), and on the first
// observation. It exists for the case the backlog check cannot see (CHAOS-6818
// r2c): the only claimed job stalls at its idempotency Begin on a stale pooler,
// River counts it running, and nothing is available, so there is no backlog to
// fail on. A single observation proves nothing (a job is always briefly between
// claim and handler), so callers require BOTH a long unbroken run and no handler
// activity on the queue for the staleness window.
func (c *claimLiveness) preHandlerStuckFor(queue string, running int64, now time.Time) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if running <= c.inHandler[queue] {
		delete(c.preHandlerSince, queue)
		return 0
	}
	if c.preHandlerSince == nil {
		c.preHandlerSince = make(map[string]time.Time, 1)
	}
	first, observed := c.preHandlerSince[queue]
	if !observed {
		c.preHandlerSince[queue] = now
		return 0
	}
	return now.Sub(first)
}

func (c *claimLiveness) handlersInside(queue string) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.inHandler[queue]
}

func (c *claimLiveness) reseed(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for queue := range c.perQueue {
		c.perQueue[queue] = now
	}
}

// markRuntimeLive reports that preclaim readiness has actually passed, so a
// real claim is now possible on every selected queue -- called exactly once,
// by preclaimReadinessComponent.Start, immediately before it returns
// success. It does not touch the per-queue clock: the grace window from the
// most recent reseed still governs how long the real consumer now has to
// make its first claim before claimLivenessReady can fail it.
func (c *claimLiveness) markRuntimeLive() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.preclaim = false
}

// inPreclaim reports whether a real claim can plausibly exist yet. See
// markRuntimeLive and the preclaim field's doc comment.
func (c *claimLiveness) inPreclaim() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.preclaim
}

// since returns how long ago the given queue's last real claim (or the
// construction-time seed -- see newClaimLiveness) was recorded. A queue
// with no entry at all (never seeded, never claimed -- the deliberate
// zero-value-construction case some unit tests exercise directly to prove
// the fail-closed contract in isolation; production always uses
// newClaimLiveness, which seeds every selected queue) reports a duration
// large enough that any bounded staleness window is already exceeded.
func (c *claimLiveness) since(queue string, now time.Time) time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	last, ok := c.perQueue[queue]
	if !ok || last.IsZero() {
		return time.Duration(1<<63 - 1) // effectively "forever"
	}
	return now.Sub(last)
}

// claimLivenessObserver decorates the production jobruntime.Observer
// (dependencies.metrics) with one proof tap, HandlerInvoked (plus HandlerReturned and, for gate-failure
// evidence, JobFinished -- see their doc comments), an
// OPTIONAL jobruntime capability (internal/jobruntime/observer.go) that
// fires once, immediately before a job's real handler runs, after every
// pre-handler gate (validation, tenant resolution, budget acquisition, the
// idempotency claim) has already passed.
//
// Two earlier tap points were tried and rejected on codex review:
//
//   - JobStarted fires before all of those gates, so a job stuck failing
//     any one of them would refresh this clock on every attempt without
//     ever reaching real handler code -- exactly the "terminal-without-
//     execution" failure mode this check exists to catch.
//   - JobFinished's (Result, ErrorCategory) pair looked like it could
//     substitute by classifying the outcome after the fact, but several
//     categories genuinely straddle the boundary: a job's registered
//     Timeout covers every gate AND the handler under one shared deadline,
//     so CategoryTimeout/CategoryCancelled can originate from tenant
//     resolution or a budget wait just as easily as from the handler, and
//     the panic recover() in Adapter.execute wraps the whole function, not
//     only the handler call. No (Result, ErrorCategory) combination can
//     resolve that from outside internal/jobruntime.
//
// HandlerInvocationObserver's doc comment covers why this is an OPTIONAL
// capability (a type assertion Adapter.execute checks, costing nothing to
// implementations that do not need it) rather than a new required Observer
// method, which every implementation -- production and test -- would have
// had to grow just to keep compiling.
//
// claimLivenessObserver embeds the CONCRETE *jobruntime.MetricsCollector,
// not the Observer INTERFACE (a separate, round-2 codex finding):
// daily.go, operational.go, provider_sync.go, sync_dispatch.go, and
// workgraph.go all perform their own optional type assertions against the
// observer they receive -- *jobruntime.MetricsCollector itself, or one of a
// dozen narrower marker interfaces (jobruntime.DailyMetricsLeaseObserver,
// jobruntime.PostSyncFanoutObserver, jobruntime.ConcurrencyBudgetObserver,
// and others) -- to reach specialized telemetry the base Observer interface
// does not expose. Wrapping the interface in a brand-new concrete type broke
// every one of those assertions silently: the type wouldn't match, the
// assertion's own `, ok` pattern would swallow the miss, and the extended
// telemetry would just stop being recorded with no error anywhere. Embedding
// the concrete pointer instead means Go PROMOTES every one of
// *MetricsCollector's methods onto claimLivenessObserver, so it still
// satisfies every one of those narrower interfaces structurally -- the
// promoted method set is what an interface assertion checks, not the
// embedding relationship. The one exception is an assertion to the EXACT
// concrete type *jobruntime.MetricsCollector itself
// (internal/workerservice/provider_sync.go), which no amount of embedding can
// satisfy; Unwrap below is the escape hatch for that one call site.
type claimLivenessObserver struct {
	*jobruntime.MetricsCollector
	liveness *claimLiveness
}

// Unwrap exposes the embedded concrete collector for the one call site
// (provider_sync.go) that asserts against *jobruntime.MetricsCollector
// exactly rather than through an interface -- see the type's doc comment.
func (observer claimLivenessObserver) Unwrap() *jobruntime.MetricsCollector {
	return observer.MetricsCollector
}

// HandlerInvoked implements jobruntime.HandlerInvocationObserver -- see the
// type's doc comment for why this, and not JobStarted or JobFinished, is
// the tap point. *jobruntime.MetricsCollector does not implement
// HandlerInvocationObserver itself, so there is no promoted method to
// shadow here (unlike Unwrap's JobFinished-adjacent concern); this is a
// pure addition to claimLivenessObserver's method set.
func (observer claimLivenessObserver) HandlerInvoked(_ context.Context, labels jobruntime.JobLabels) {
	if observer.liveness != nil {
		observer.liveness.handlerInvoked(labels.Queue, time.Now())
	}
}

// JobFinished shadows the embedded collector's: it records the metric first,
// then notes a job that finished on an idempotency-category failure as gate
// failure evidence (CHAOS-6864). Only that one category counts. It is failure
// evidence, never proof a handler ran, so it never touches the claim clock; a
// handler that ran and then failed its completion claim is ended by the very
// HandlerInvoked that preceded it.
func (observer claimLivenessObserver) JobFinished(
	ctx context.Context, labels jobruntime.JobLabels, result jobruntime.Result,
	category jobruntime.ErrorCategory, duration time.Duration,
) {
	observer.MetricsCollector.JobFinished(ctx, labels, result, category, duration)
	if observer.liveness != nil && category == jobruntime.CategoryIdempotency {
		observer.liveness.recordGateFailure(labels.Queue, result == jobruntime.ResultRetry, time.Now())
	}
}

// HandlerReturned pairs HandlerInvoked (jobruntime.HandlerReturnObserver).
func (observer claimLivenessObserver) HandlerReturned(_ context.Context, labels jobruntime.JobLabels) {
	if observer.liveness != nil {
		observer.liveness.handlerReturned(labels.Queue, time.Now())
	}
}

var errClaimLivenessSlotStuckBeforeHandler = errors.New(
	"a running job slot has been stuck before its handler and no handler has run on this queue",
)

var errClaimLivenessGateFailing = errors.New(
	"jobs on this queue keep failing at their idempotency gate and no handler has run",
)

var errClaimLivenessStalledWithBacklog = errors.New(
	"no job has been claimed recently and this queue has available work with idle capacity to claim it",
)

// claimStalenessWindow mirrors selfprobe's own staleness sizing (three
// missed 20s intervals) so the two halves of execution_liveness -- "can the
// process reach the database" and "is the process actually claiming work
// when there is work to claim" -- share one detection-latency budget rather
// than two independently chosen numbers that could drift apart.
const claimStalenessWindow = 3 * 20 * time.Second

// claimLivenessReady is the claim-liveness half of execution_liveness's
// real closed CheckFunc (the whole of the worker's execution_liveness since CHAOS-6818), wired in
// configureWorkerDependenciesWithSources (dependencies.go). It evaluates
// EVERY selected queue independently (round-2 codex finding: a shared
// single clock lets claims on one healthy queue mask a wedged sibling), and
// for each one requires EITHER a real claim recently on THAT queue, OR
// proof the queue cannot be considered wedged right now:
//
//   - no available work on that queue (idle, not broken), OR
//   - every claim slot this process budgeted for that queue is already
//     running an existing job AND every one of those running jobs is INSIDE
//     a handler (round-2 codex finding: a fully saturated queue has no free
//     capacity to claim MORE work; registered job timeouts run up to two
//     hours, so a queue legitimately busy with long-running work must not be
//     flagged just because nothing NEW claimed in the last 60s). River counts
//     a job as running from claim, including one failing at its idempotency
//     Begin, so Running >= Capacity alone is not enough (CHAOS-6818 r1b P1):
//     a full queue with a slot stuck BEFORE its handler is the stale-pooler
//     shape and falls through to the claim-age check.
//
// A queue with available work, idle capacity to claim it, and no recent
// claim fails closed: that is exactly "recent jobs are all terminal-
// without-execution" (or, in the more severe case this also catches, no
// jobs are even being attempted), regardless of what an independent DB
// probe reports. That failure branch is itself gated by claim.inPreclaim:
// while true, no claim can exist yet by construction (River has not
// started), so a backlogged-but-unclaimed queue is logged and passed rather
// than failed -- see claim.preclaim's doc comment. Once markRuntimeLive
// flips that off, the branch enforces exactly as before.
//
// Every error this returns wraps errWorkerDependencyUnavailable via
// dependencyCheckFailed rather than replacing it outright, and is logged
// through logDependencyCheckFailure before it is returned -- the same
// discipline domainReady/queueReady/riverSchemaReady/domainTransactionReady
// already follow -- so a queue-telemetry read that failed only because its
// own bounded context expired still classifies as retryable, and an
// operator sees this member's own cause, not just its name, in the crash-
// loop log.
func (dependencies *workerDependencies) claimLivenessReady(claim *claimLiveness) health.CheckFunc {
	return func(ctx context.Context) error {
		if dependencies == nil || claim == nil {
			return errWorkerDependencyUnavailable
		}
		if !dependencies.queueTelemetryRequired {
			// No queue selection needed telemetry at all (see
			// buildQueueTelemetry) -- there is no claim path to prove live.
			return nil
		}
		if dependencies.queueTelemetryErr != nil {
			dependencies.logDependencyCheckFailure(ctx, "execution_liveness", dependencies.queueTelemetryErr)
			return dependencyCheckFailed(dependencies.queueTelemetryErr)
		}
		if dependencies.queueTelemetry == nil {
			// No error was ever recorded, so there is nothing to unwrap or log
			// -- fail closed rather than silently passing on missing evidence.
			return errWorkerDependencyUnavailable
		}
		snapshot, err := dependencies.queueTelemetry.Snapshot(ctx)
		if err != nil {
			dependencies.logDependencyCheckFailure(ctx, "execution_liveness", err)
			return dependencyCheckFailed(err)
		}
		now := time.Now()
		preclaim := claim.inPreclaim()
		for _, facts := range collectQueueFacts(snapshot, claim, now, preclaim) {
			switch judgeQueue(facts) {
			case verdictHealthy:
				// A healthy poll ends this queue's run of refusals: the next one is a
				// fresh run and logs its clause and facts again.
				claim.endRefusalRun(facts.queue)
			case verdictPreclaimSkip:
				claim.endRefusalRun(facts.queue)
				// River has not started yet, so no claim on this queue could
				// possibly exist regardless of how long preclaim's own retry
				// loop has been running against some other slow dependency.
				dependencies.logClaimLivenessPreclaimSkip(ctx, facts.queue)
			case verdictSlotStuck:
				dependencies.logClaimLivenessRefusal(ctx, claim, "slot_stuck_before_handler", facts, now)
				return fmt.Errorf("%w: queue %q", errClaimLivenessSlotStuckBeforeHandler, facts.queue)
			case verdictGateFailing:
				dependencies.logClaimLivenessRefusal(ctx, claim, "gate_failures_without_handler", facts, now)
				return fmt.Errorf("%w: queue %q", errClaimLivenessGateFailing, facts.queue)
			case verdictStalledBacklog:
				dependencies.logClaimLivenessRefusal(ctx, claim, "backlog_without_handler", facts, now)
				return fmt.Errorf("%w: queue %q", errClaimLivenessStalledWithBacklog, facts.queue)
			}
		}
		return nil
	}
}

// queueFacts is everything the liveness predicate needs to know about one
// queue at one poll, gathered in one place so the predicate itself is a pure
// function of plain values (judgeQueue) and can be tested over its whole input
// space.
type queueFacts struct {
	queue string
	// available is the queue's jobs waiting to be claimed.
	available int64
	// capacityKnown / capacity / running: this process's claim slots for the
	// queue and how many of them River counts as running. A job counts as
	// running from claim, including one that has not reached (or has left) its
	// handler.
	capacityKnown     bool
	capacity, running int64
	// inside is how many of those slots are inside a handler right now.
	inside int64
	// claimAge is how long since a handler last ran on this queue.
	claimAge time.Duration
	// stuckFor is how long successive polls have CONTINUOUSLY seen a running
	// slot outside a handler on this queue.
	stuckFor time.Duration
	// gateFails / gateFailSpan: the live run of idempotency-gate failures on the
	// queue since a handler last ran, and how long it has lasted (first to last
	// failure). Independent of what the queue snapshot shows: a job waiting out
	// its retry backoff is neither available nor running.
	gateFails    int64
	gateFailSpan time.Duration
	window       time.Duration
	preclaim     bool
}

type queueVerdict int

const (
	verdictHealthy queueVerdict = iota
	verdictPreclaimSkip
	verdictSlotStuck
	verdictGateFailing
	verdictStalledBacklog
)

// collectQueueFacts builds one queueFacts per queue the snapshot mentions
// (job telemetry first, in order, then capacity-only queues). It is the only
// place the stuck-slot tracker is advanced, and only outside preclaim.
func collectQueueFacts(snapshot riverstore.QueueTelemetrySnapshot, claim *claimLiveness, now time.Time, preclaim bool) []queueFacts {
	byQueue := map[string]*queueFacts{}
	var order []string
	get := func(queue string) *queueFacts {
		if facts, ok := byQueue[queue]; ok {
			return facts
		}
		facts := &queueFacts{queue: queue, window: claim.staleness(), preclaim: preclaim}
		byQueue[queue] = facts
		order = append(order, queue)
		return facts
	}
	for _, job := range snapshot.Jobs {
		if job.Available > 0 {
			get(job.Queue).available += job.Available
		} else {
			get(job.Queue)
		}
	}
	for _, capacity := range snapshot.QueueCapacities {
		facts := get(capacity.Queue)
		facts.capacityKnown, facts.capacity, facts.running = true, capacity.Capacity, capacity.Running
	}
	out := make([]queueFacts, 0, len(order))
	for _, queue := range order {
		facts := byQueue[queue]
		facts.inside = claim.handlersInside(queue)
		facts.claimAge = claim.since(queue, now)
		if !preclaim {
			facts.stuckFor = claim.preHandlerStuckFor(queue, facts.running, now)
		}
		facts.gateFails, facts.gateFailSpan = claim.gateFailureEvidence(queue, now)
		out = append(out, *facts)
	}
	return out
}

// judgeQueue is the ONE liveness predicate, applied to every queue whatever its
// backlog (CHAOS-6818; lead D2606):
//
//	work that should be reaching a handler is not, and nothing has run for a
//	whole window.
//
// "Should be reaching a handler" is either a claimed slot that is not inside a
// handler (running > inside) or a backlog with free capacity to claim it. The
// queue is healthy exactly when every running slot is inside a handler (long
// work, however deep the backlog behind a FULL queue) or nothing is waiting
// at all. The two ways it is not:
//
//   - a running slot outside a handler, unbroken across polls for more than the
//     window, with no handler activity for the window: a job stalled at its
//     idempotency Begin (a stale or recreated pooler, CHAOS-4029), with or
//     without a backlog behind it. The unbroken-run and no-activity conditions
//     keep a job merely between claim and handler, and a busy queue whose polls
//     each catch one, from ever counting;
//   - a backlog with a slot to claim it (or one whose full slots are not all in
//     handlers) and no handler activity for the window: the consumer is not
//     claiming.
//
// The third way is independent of the snapshot (CHAOS-6864): jobs that keep
// failing at their idempotency gate, across more than a window, with no handler
// running or inside one. River parks such a job as retryable until its backoff
// elapses (5s, 10s ... 5m), so between failures the queue reads Available=0 and
// Running=0 and neither arm above can see it. Only a run that SPANS the window
// counts (a blip answered by the job's own retry does not), and the evidence
// lapses once no further failure is due. A handler currently inside means the
// work pool is reaching the database for that job, so it is left alone.
//
// Before River starts (preclaim) no handler can have run, so no arm can
// fail; a queue that WOULD have failed the backlog arm is reported as a skip.
func judgeQueue(f queueFacts) queueVerdict {
	outside := f.running > f.inside
	if !f.preclaim && outside && f.stuckFor > f.window && f.claimAge > f.window {
		return verdictSlotStuck
	}
	if !f.preclaim && f.gateFails >= gateFailureMinimum && f.gateFailSpan > f.window &&
		f.inside == 0 && f.claimAge > f.window {
		return verdictGateFailing
	}
	if f.available <= 0 {
		return verdictHealthy // confirmed empty right now: idle, not broken.
	}
	full := f.capacityKnown && f.capacity > 0 && f.running >= f.capacity
	if full && !outside {
		return verdictHealthy // every slot is inside a handler: busy, not wedged.
	}
	if f.claimAge <= f.window {
		return verdictHealthy // a real claim landed on this queue recently.
	}
	if f.preclaim {
		return verdictPreclaimSkip
	}
	return verdictStalledBacklog
}

// claimRefusalLogInterval bounds how often one queue's execution_liveness
// refusal is logged while it keeps refusing.
const claimRefusalLogInterval = 30 * time.Second

// claimRefusalDue reports whether queue's refusal should be logged now, and
// records that it was. The first refusal after a healthy stretch is always due.
func (c *claimLiveness) claimRefusalDue(queue string, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.refusalLogged == nil {
		c.refusalLogged = make(map[string]time.Time, 1)
	}
	if last, ok := c.refusalLogged[queue]; ok && now.Sub(last) < claimRefusalLogInterval {
		return false
	}
	c.refusalLogged[queue] = now
	return true
}

// endRefusalRun forgets queue's last logged refusal, so the first refusal after
// a healthy stretch is always due (claimRefusalDue).
func (c *claimLiveness) endRefusalRun(queue string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.refusalLogged, queue)
}

// logClaimLivenessRefusal names the clause that failed execution_liveness and
// every fact the predicate judged (CHAOS-6883). The 503 body carries only the
// check name, and refusals otherwise left no log line, so a refusal that
// cleared could not be attributed to the stuck-slot arm, the backlog arm or the
// gate-failure arm afterwards. Only bounded numbers and the queue name: never
// an error string. Rate-limited per queue; the first refusal always logs.
func (dependencies *workerDependencies) logClaimLivenessRefusal(
	ctx context.Context, claim *claimLiveness, clause string, facts queueFacts, now time.Time,
) {
	if dependencies == nil || dependencies.logger == nil || claim == nil || !claim.claimRefusalDue(facts.queue, now) {
		return
	}
	dependencies.logger.WarnContext(ctx, "execution liveness refused",
		"check", "execution_liveness",
		"queue", facts.queue,
		"clause", clause,
		"available", facts.available,
		"capacity_known", facts.capacityKnown,
		"capacity", facts.capacity,
		"running", facts.running,
		"inside_handler", facts.inside,
		"claim_age_ms", facts.claimAge.Milliseconds(),
		"stuck_for_ms", facts.stuckFor.Milliseconds(),
		"gate_failures", facts.gateFails,
		"gate_failure_span_ms", facts.gateFailSpan.Milliseconds(),
		"window_ms", facts.window.Milliseconds(),
	)
}

// logClaimLivenessPreclaimSkip explains why a queue with backlog and idle
// capacity did not fail execution_liveness during preclaim: at info, not
// warn or error, because this is the expected shape of every startup with
// pre-existing backlog, not a symptom of anything wrong.
func (dependencies *workerDependencies) logClaimLivenessPreclaimSkip(ctx context.Context, queue string) {
	if dependencies == nil || dependencies.logger == nil {
		return
	}
	dependencies.logger.InfoContext(ctx, "execution liveness claim check skipped before claiming can start",
		"check", "execution_liveness",
		"queue", queue,
	)
}
