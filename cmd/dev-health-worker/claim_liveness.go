package main

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
	// staleWindow defaults to claimStalenessWindow in newClaimLiveness.
	// Exposed via SetStaleWindow so a test can shrink it from the
	// production 60s to a real-but-small duration (mirroring
	// selfprobe.Monitor.SetStaleness) instead of sleeping out the full
	// window.
	staleWindow time.Duration
}

// newClaimLiveness seeds every selected queue's clock to now, NOT the zero
// value.
//
// This is deliberately different from selfprobe.Monitor's "never_proven"
// fail-closed-until-first-sample discipline, and the difference is load-
// bearing, not cosmetic: claimLivenessReady is one of the checks
// preclaimReadinessComponent evaluates via Registry.CheckRequired BEFORE
// the River client (workerProcessComponent) ever starts -- so at true
// process construction, no real claim can possibly exist yet, by
// construction of the startup order itself. A zero-seeded clock would fail
// forever the instant any selected queue held real backlog (observed live:
// a worker restarting after a shared-stack outage, with genuine
// multi-minute backlog already queued, could never pass preclaim-readiness
// again -- the exact deadlock this seeding exists to prevent). Seeding to
// "now" treats admission itself as the starting gun and gives the real
// consumer a full staleness window to make its first claim on each
// selected queue before this signal can ever fail -- ample for a healthy
// process, while a consumer that is ACTUALLY wedged still fails visibly
// once that window elapses with nothing claimed, exactly matching the
// ticket's two-hour incident timeline rather than a from-birth deadlock.
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
	return &claimLiveness{perQueue: perQueue, staleWindow: claimStalenessWindow}
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
// (dependencies.metrics) with exactly one extra tap: JobFinished.
//
// It embeds the CONCRETE *jobruntime.MetricsCollector, not the Observer
// INTERFACE (round-2 codex finding): daily.go, operational.go,
// provider_sync.go, sync_dispatch.go, and workgraph.go all perform their
// own optional type assertions against the observer they receive --
// *jobruntime.MetricsCollector itself, or one of a dozen narrower marker
// interfaces (jobruntime.DailyMetricsLeaseObserver,
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
// (cmd/dev-health-worker/provider_sync.go), which no amount of embedding can
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

// JobFinished shadows the promoted method to add the claim-liveness tap,
// then delegates to the real collector unconditionally so every metric it
// already records is unaffected.
//
// JobFinished, not JobStarted, is the tap point (round-2 codex finding):
// JobStarted fires unconditionally before validateRow, tenant resolution,
// budget acquisition, and the idempotency claim
// (internal/jobruntime/adapter.go's Work/execute), so a job repeatedly
// failing any of those gates would refresh this clock on every attempt
// without ever reaching real handler code -- exactly the "terminal-
// without-execution" failure mode this check exists to catch. JobFinished
// carries the SAME (Result, ErrorCategory) pair the runtime already uses to
// decide what happened, and executionReached below classifies it using
// only that bounded, exported vocabulary.
func (observer claimLivenessObserver) JobFinished(
	ctx context.Context,
	labels jobruntime.JobLabels,
	result jobruntime.Result,
	category jobruntime.ErrorCategory,
	duration time.Duration,
) {
	if observer.liveness != nil && executionReached(result, category) {
		observer.liveness.recordClaim(labels.Queue, time.Now())
	}
	if observer.MetricsCollector != nil {
		observer.MetricsCollector.JobFinished(ctx, labels, result, category, duration)
	}
}

// executionReached reports whether a finished job's outcome proves it
// reached real handler code, from the bounded (Result, ErrorCategory) pair
// alone -- see internal/jobruntime/errors.go for the closed constant sets
// this switches on, and JobFinished's doc comment for why this is the tap
// point instead of JobStarted.
//
// ResultDuplicate is excluded regardless of category: it is
// PostgresIdempotency.Begin's ClaimAlreadyComplete short-circuit
// (internal/jobruntime/adapter.go's execute), which returns BEFORE the
// handler runs this attempt -- the underlying work unit may be genuinely
// done, but THIS execution never touched handler code.
//
// The category denylist below is exhaustively every category
// internal/jobruntime/errors.go's classify()/classifyBudgetWait() can
// return from BEFORE adapter.handler.Work is ever called: row validation
// (the execute() function's own default-choice fallback for a decode
// failure), tenant scope resolution, budget acquisition, and the
// idempotency claim itself. classify() -- the function that classifies
// handler-returned errors, called only AFTER Work has returned -- never
// assigns any of these four categories on its own; a handler could only
// produce one by deliberately re-marking its own error with a category
// reserved for infra gates, which is not this codebase's convention (see
// the public Retryable/Permanent constructors handlers actually use).
// CategoryTerminalDomain is included conservatively: its one confirmed
// producer (execute()'s ClaimTerminal branch) is also pre-handler, and a
// handler-returned CategoryTerminalDomain, while structurally possible, is
// rare enough that excluding it trades a small under-credit for never
// over-crediting a claim that didn't happen.
func executionReached(result jobruntime.Result, category jobruntime.ErrorCategory) bool {
	if result == jobruntime.ResultDuplicate {
		return false
	}
	switch category {
	case jobruntime.CategoryValidation,
		jobruntime.CategoryTenant,
		jobruntime.CategoryBudget,
		jobruntime.CategoryIdempotency,
		jobruntime.CategoryTerminalDomain:
		return false
	default:
		return true
	}
}

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
// real closed CheckFunc, wired alongside livenessMonitor's DB-probe half in
// configureWorkerDependenciesWithSources (dependencies.go). It evaluates
// EVERY selected queue independently (round-2 codex finding: a shared
// single clock lets claims on one healthy queue mask a wedged sibling), and
// for each one requires EITHER a real claim recently on THAT queue, OR
// proof the queue cannot be considered wedged right now:
//
//   - no available work on that queue (idle, not broken), OR
//   - every claim slot this process budgeted for that queue is already
//     running an existing job (round-2 codex finding: a fully saturated
//     queue -- Running >= Capacity -- has no free capacity to claim MORE
//     work regardless of how healthy the consumer is; registered job
//     timeouts run up to two hours, so a queue legitimately busy with
//     long-running work must not be flagged just because nothing NEW
//     claimed in the last 60s).
//
// A queue with available work, idle capacity to claim it, and no recent
// claim fails closed: that is exactly "recent jobs are all terminal-
// without-execution" (or, in the more severe case this also catches, no
// jobs are even being attempted), regardless of what an independent DB
// probe reports.
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
		if dependencies.queueTelemetryErr != nil || dependencies.queueTelemetry == nil {
			// Cannot prove idle or saturated and cannot prove a recent claim
			// -- fail closed rather than silently passing on missing evidence.
			return errWorkerDependencyUnavailable
		}
		snapshot, err := dependencies.queueTelemetry.Snapshot(ctx)
		if err != nil {
			return errWorkerDependencyUnavailable
		}
		capacityByQueue := make(map[string]riverstore.QueueCapacityTelemetry, len(snapshot.QueueCapacities))
		for _, capacity := range snapshot.QueueCapacities {
			capacityByQueue[capacity.Queue] = capacity
		}
		now := time.Now()
		for _, job := range snapshot.Jobs {
			if job.Available <= 0 {
				continue // this queue is confirmed empty right now: idle, not broken.
			}
			if capacity, ok := capacityByQueue[job.Queue]; ok && capacity.Capacity > 0 && capacity.Running >= capacity.Capacity {
				continue // fully saturated with existing work -- healthy, not wedged.
			}
			if claim.since(job.Queue, now) <= claim.staleness() {
				continue // a real claim landed on this queue recently.
			}
			return fmt.Errorf("%w: queue %q", errClaimLivenessStalledWithBacklog, job.Queue)
		}
		return nil
	}
}
