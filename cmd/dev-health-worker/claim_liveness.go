package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// claimLiveness closes the gap a codex review of CHAOS-4029 identified in
// the first design: an independent self-probe goroutine
// (internal/platform/selfprobe.Monitor) proves the domain pool is
// reachable, but it runs on its OWN goroutine and therefore keeps
// succeeding even if the real River consumer -- the thing that actually
// claims and executes jobs -- is deadlocked while the database stays
// healthy. That is precisely the CHAOS-4029 "worker whose recent jobs are
// all terminal-without-execution is not healthy" scenario the ticket's
// Wanted section calls out, and a probe disconnected from the real claim
// path cannot detect it.
//
// claimLiveness is fed by the SAME Observer every real job handler
// execution already goes through (see claimLivenessObserver below), so it
// only advances when River actually dispatched a job to a handler -- not
// when an unrelated goroutine happened to succeed at something else.
type claimLiveness struct {
	mu          sync.RWMutex
	lastClaimAt time.Time
	// staleWindow defaults to claimStalenessWindow in newClaimLiveness.
	// Exposed via SetStaleWindow so a test can shrink it from the
	// production 60s to a real-but-small duration (mirroring
	// selfprobe.Monitor.SetStaleness) instead of sleeping out the full
	// window.
	staleWindow time.Duration
}

// newClaimLiveness seeds lastClaimAt to now, NOT the zero value.
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
// consumer a full staleness window (claimStalenessWindow) to make its first
// claim before this signal can ever fail -- ample for a healthy process,
// while a consumer that is ACTUALLY wedged still fails visibly once that
// window elapses with nothing claimed, exactly matching the ticket's
// two-hour incident timeline rather than a from-birth deadlock.
func newClaimLiveness(now time.Time) *claimLiveness {
	return &claimLiveness{lastClaimAt: now, staleWindow: claimStalenessWindow}
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

func (c *claimLiveness) recordClaim(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if now.After(c.lastClaimAt) {
		c.lastClaimAt = now
	}
}

// since returns how long ago the last real claim (or the construction-time
// seed -- see newClaimLiveness) was recorded. A zero-value claimLiveness
// (constructed directly rather than via newClaimLiveness, as tests do to
// exercise the pre-seeded-forever case explicitly) reports a duration large
// enough that any bounded staleness window is already exceeded; production
// code always uses newClaimLiveness and therefore never observes this
// branch outside that deliberate test scenario.
func (c *claimLiveness) since(now time.Time) time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.lastClaimAt.IsZero() {
		return time.Duration(1<<63 - 1) // effectively "forever"
	}
	return now.Sub(c.lastClaimAt)
}

// claimLivenessObserver decorates the production jobruntime.Observer
// (dependencies.metrics) with exactly one extra tap: JobStarted, which
// River's execution wrapper calls the instant a claimed job's handler
// begins running (jobruntime.Work -> JobStarted; see
// internal/jobruntime/telemetry.go). Every other method delegates
// unchanged, so wrapping this in changes nothing about the real metrics
// collector's behavior.
type claimLivenessObserver struct {
	jobruntime.Observer
	liveness *claimLiveness
}

func (observer claimLivenessObserver) JobStarted(ctx context.Context, labels jobruntime.JobLabels) {
	if observer.liveness != nil {
		observer.liveness.recordClaim(time.Now())
	}
	if observer.Observer != nil {
		observer.Observer.JobStarted(ctx, labels)
	}
}

var errClaimLivenessStalledWithBacklog = errors.New(
	"no job has been claimed recently and at least one selected queue has available work",
)

// claimStalenessWindow mirrors selfprobe's own staleness sizing (three
// missed 20s intervals) so the two halves of execution_liveness -- "can the
// process reach the database" and "is the process actually claiming work
// when there is work to claim" -- share one detection-latency budget rather
// than two independently chosen numbers that could drift apart.
const claimStalenessWindow = 3 * 20 * time.Second

// claimLivenessReady is the idle-safe half of execution_liveness's real
// closed CheckFunc, wired in registerExecutionLivenessChecks (dependencies.go).
// It is the fix for the codex-identified gap: instead of trusting an
// unrelated goroutine's own success, it requires EITHER a real claim
// recently (claimLiveness, fed only by JobStarted) OR proof -- via the same
// queue telemetry queueHealthMonitor and queued_contract_versions already
// depend on -- that every selected queue is genuinely empty right now. A
// queue with available work and no recent claim fails closed: that is
// exactly "recent jobs are all terminal-without-execution" (or, in the more
// severe case this also catches, no jobs are even being attempted),
// regardless of what an independent DB probe reports.
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
		if claim.since(time.Now()) <= claim.staleness() {
			return nil
		}
		if dependencies.queueTelemetryErr != nil || dependencies.queueTelemetry == nil {
			// Cannot prove idle and cannot prove a recent claim -- fail
			// closed rather than silently passing on missing evidence.
			return errWorkerDependencyUnavailable
		}
		snapshot, err := dependencies.queueTelemetry.Snapshot(ctx)
		if err != nil {
			return errWorkerDependencyUnavailable
		}
		for _, job := range snapshot.Jobs {
			if job.Available > 0 {
				return errClaimLivenessStalledWithBacklog
			}
		}
		// Every selected queue is confirmed empty right now: idle, not
		// broken. No claim is expected, so the absence of one is not a
		// failure.
		return nil
	}
}
