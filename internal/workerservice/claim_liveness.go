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
	c.mu.Unlock()
	c.recordClaim(queue, now)
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
// (dependencies.metrics) with exactly one extra tap: HandlerInvoked, an
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

// HandlerReturned pairs HandlerInvoked (jobruntime.HandlerReturnObserver).
func (observer claimLivenessObserver) HandlerReturned(_ context.Context, labels jobruntime.JobLabels) {
	if observer.liveness != nil {
		observer.liveness.handlerReturned(labels.Queue, time.Now())
	}
}

var errClaimLivenessSlotStuckBeforeHandler = errors.New(
	"a running job slot has been stuck before its handler and no handler has run on this queue",
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
		capacityByQueue := make(map[string]riverstore.QueueCapacityTelemetry, len(snapshot.QueueCapacities))
		for _, capacity := range snapshot.QueueCapacities {
			capacityByQueue[capacity.Queue] = capacity
		}
		now := time.Now()
		preclaim := claim.inPreclaim()
		if !preclaim {
			// A slot stuck before its handler with nothing waiting behind it
			// (CHAOS-6818 r2c): judged whether or not the queue has a backlog.
			// Both conditions are required so that a busy healthy queue (every
			// poll catches some job between claim and handler, but handlers keep
			// running) and a sparse one (a job claimed a moment ago after an
			// idle hour) never flip.
			for _, capacity := range snapshot.QueueCapacities {
				stuck := claim.preHandlerStuckFor(capacity.Queue, capacity.Running, now)
				if stuck > claim.staleness() && claim.since(capacity.Queue, now) > claim.staleness() {
					return fmt.Errorf("%w: queue %q", errClaimLivenessSlotStuckBeforeHandler, capacity.Queue)
				}
			}
		}
		for _, job := range snapshot.Jobs {
			if job.Available <= 0 {
				continue // this queue is confirmed empty right now: idle, not broken.
			}
			if capacity, ok := capacityByQueue[job.Queue]; ok && capacity.Capacity > 0 && capacity.Running >= capacity.Capacity &&
				claim.handlersInside(job.Queue) >= capacity.Running {
				// Every running slot is inside a handler (long work): healthy. A
				// full queue whose slots are NOT all in handlers has jobs stuck
				// before their handler (a stale pooler failing at Begin, CHAOS-4029)
				// and falls through to the claim-age check below.
				continue
			}
			if claim.since(job.Queue, now) <= claim.staleness() {
				continue // a real claim landed on this queue recently.
			}
			if preclaim {
				// River has not started yet, so no claim on this queue could
				// possibly exist regardless of how long preclaim's own retry
				// loop has been running against some other slow dependency.
				dependencies.logClaimLivenessPreclaimSkip(ctx, job.Queue)
				continue
			}
			return fmt.Errorf("%w: queue %q", errClaimLivenessStalledWithBacklog, job.Queue)
		}
		return nil
	}
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
