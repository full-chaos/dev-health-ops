package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
)

// TestNewClaimLivenessSeedsGraceWindowNotZero is the direct regression test
// for a real deadlock this ticket's own development surfaced live: rebuilding
// and recreating go-worker against the shared dev stack after an
// (unrelated) pgbouncer outage, with genuine multi-minute queue backlog
// already accumulated, the worker could never pass preclaim-readiness again
// -- claimLivenessReady is evaluated by Registry.CheckRequired BEFORE the
// River client starts, so a zero-seeded claim clock demanded evidence
// (a real claim) that could not yet exist, on every single restart attempt,
// forever. Seeding to "now" at construction gives the real consumer a full
// staleness window to make its first claim before this signal can fail.
func TestNewClaimLivenessSeedsGraceWindowNotZero(t *testing.T) {
	t.Parallel()
	before := time.Now()
	claim := newClaimLiveness(before)
	// Immediately after construction -- exactly the moment preclaim-readiness
	// evaluates it, before the River client has had any chance to run -- the
	// claim must already read as "just now", not "never".
	if since := claim.since(time.Now()); since > time.Second {
		t.Fatalf("newClaimLiveness seed since() = %v, want ~0 (seeded to construction time)", since)
	}
	dependencies := &workerDependencies{
		queueTelemetryRequired: true,
		queueTelemetry: &fakeQueueTelemetry{snapshot: riverstore.QueueTelemetrySnapshot{
			// Genuine pre-existing backlog, exactly the live scenario: a
			// worker restarting into a queue that already has real work
			// waiting, before it has claimed anything in THIS process.
			Jobs: []riverstore.QueueJobTelemetry{{Queue: "sync_provider", Kind: "sync.provider_unit", Available: 6}},
		}},
	}
	if err := dependencies.claimLivenessReady(claim)(context.Background()); err != nil {
		t.Fatalf("claimLivenessReady() immediately after construction, with pre-existing backlog, = %v, want nil (must not deadlock preclaim-readiness)", err)
	}
}

// TestClaimLivenessRecordsOnlyForwardMotion proves claimLiveness.since keeps
// the latest claim even if a stale JobStarted call races in after a newer
// one -- clocks and goroutine scheduling can reorder concurrent calls, and a
// naive "always overwrite" would let a late-arriving old timestamp make a
// perfectly live process look stale.
func TestClaimLivenessRecordsOnlyForwardMotion(t *testing.T) {
	t.Parallel()
	claim := &claimLiveness{}
	now := time.Unix(1_700_000_000, 0)
	claim.recordClaim(now)
	claim.recordClaim(now.Add(-time.Minute)) // stale, must not regress
	if since := claim.since(now); since != 0 {
		t.Fatalf("since() = %v, want 0 (the later claim must win)", since)
	}
}

// TestClaimLivenessNeverClaimedReportsEffectivelyForever proves the
// pre-seeded fail-closed contract: before any real claim has ever been
// recorded, since() must report a duration so large that no bounded
// staleness window will ever read it as "recent" by accident.
func TestClaimLivenessNeverClaimedReportsEffectivelyForever(t *testing.T) {
	t.Parallel()
	claim := &claimLiveness{}
	if since := claim.since(time.Now()); since < 365*24*time.Hour {
		t.Fatalf("since() with no recorded claim = %v, want an effectively unbounded duration", since)
	}
}

// TestClaimLivenessObserverTapsJobStartedOnly proves the decorator records a
// claim on JobStarted and still delegates every call (including JobStarted
// itself) to the wrapped Observer, so metrics behavior is byte-for-byte
// identical to using the wrapped Observer directly.
func TestClaimLivenessObserverTapsJobStartedOnly(t *testing.T) {
	t.Parallel()
	recorder := &recordingJobruntimeObserver{}
	claim := &claimLiveness{}
	observer := claimLivenessObserver{Observer: recorder, liveness: claim}

	labels := jobruntime.JobLabels{Queue: "heartbeat", Kind: "system.heartbeat"}
	observer.JobStarted(context.Background(), labels)
	if claim.since(time.Now()) > time.Second {
		t.Fatal("expected JobStarted to record a claim")
	}
	if len(recorder.started) != 1 || recorder.started[0] != labels {
		t.Fatalf("expected JobStarted to delegate to the wrapped observer, got %#v", recorder.started)
	}

	observer.JobFinished(context.Background(), labels, jobruntime.Result("success"), jobruntime.ErrorCategory(""), time.Second)
	if recorder.finishedCalls != 1 {
		t.Fatalf("expected JobFinished to delegate unchanged, got %d calls", recorder.finishedCalls)
	}
}

// recordingJobruntimeObserver is a minimal jobruntime.Observer double that
// records what it was called with, so claimLivenessObserver's delegation can
// be proven directly rather than inferred from MetricsCollector's much
// larger surface.
type recordingJobruntimeObserver struct {
	started       []jobruntime.JobLabels
	finishedCalls int
}

func (o *recordingJobruntimeObserver) RuntimeRegistered(context.Context, jobruntime.RuntimeInfo) {}
func (o *recordingJobruntimeObserver) JobStarted(_ context.Context, labels jobruntime.JobLabels) {
	o.started = append(o.started, labels)
}
func (o *recordingJobruntimeObserver) JobFinished(context.Context, jobruntime.JobLabels, jobruntime.Result, jobruntime.ErrorCategory, time.Duration) {
	o.finishedCalls++
}
func (o *recordingJobruntimeObserver) JobPanicked(context.Context, jobruntime.JobLabels) {}
func (o *recordingJobruntimeObserver) JobCancelled(context.Context, jobruntime.JobLabels, jobruntime.ErrorCategory) {
}
func (o *recordingJobruntimeObserver) DomainMismatch(context.Context, string) {}
func (o *recordingJobruntimeObserver) BudgetWait(context.Context, jobruntime.JobLabels, time.Duration, string) {
}
func (o *recordingJobruntimeObserver) JobWait(context.Context, jobruntime.JobLabels, time.Duration) {}
func (o *recordingJobruntimeObserver) ObserveDeterministicFailure(context.Context, jobruntime.JobLabels, jobruntime.Reason) {
}

// TestClaimLivenessReadyRequiresProofNotJustAbsenceOfError is the direct
// reproduction of the codex-review finding on CHAOS-4029 round 1: a queue
// with available work and no recent claim must fail readiness even though
// nothing about the database or an independent probe is wrong -- the
// scenario an independent self-probe goroutine cannot detect.
func TestClaimLivenessReadyRequiresProofNotJustAbsenceOfError(t *testing.T) {
	t.Parallel()
	claim := &claimLiveness{}
	dependencies := &workerDependencies{
		queueTelemetryRequired: true,
		queueTelemetry: &fakeQueueTelemetry{snapshot: riverstore.QueueTelemetrySnapshot{
			Jobs: []riverstore.QueueJobTelemetry{{Queue: "sync", Kind: "sync.provider_unit", Available: 3}},
		}},
	}
	ready := dependencies.claimLivenessReady(claim)

	// No claim has ever been recorded, and the queue has backlog: this is
	// exactly "jobs are all terminal-without-execution" (or worse, nothing
	// is even being attempted). Must fail.
	if err := ready(context.Background()); !errors.Is(err, errClaimLivenessStalledWithBacklog) {
		t.Fatalf("ready() = %v, want errClaimLivenessStalledWithBacklog", err)
	}

	// A real claim arrives. Readiness must clear immediately, without
	// waiting out the staleness window -- the receipt is the claim itself.
	claim.recordClaim(time.Now())
	if err := ready(context.Background()); err != nil {
		t.Fatalf("ready() after a real claim = %v, want nil", err)
	}
}

// TestClaimLivenessReadyPassesWhenGenuinelyIdle is the idle-safety half of
// the same contract: a queue confirmed empty (Available == 0 everywhere)
// must pass even though no claim has ever been recorded, because there is
// no work to claim. This is what keeps a quiet fleet from reading as
// unhealthy.
func TestClaimLivenessReadyPassesWhenGenuinelyIdle(t *testing.T) {
	t.Parallel()
	claim := &claimLiveness{}
	dependencies := &workerDependencies{
		queueTelemetryRequired: true,
		queueTelemetry: &fakeQueueTelemetry{snapshot: riverstore.QueueTelemetrySnapshot{
			Jobs: []riverstore.QueueJobTelemetry{{Queue: "heartbeat", Kind: "system.heartbeat", Available: 0}},
		}},
	}
	ready := dependencies.claimLivenessReady(claim)
	if err := ready(context.Background()); err != nil {
		t.Fatalf("ready() on a genuinely idle queue = %v, want nil", err)
	}
}

// TestClaimLivenessReadyFailsClosedWhenIdleCannotBeProven proves that a
// telemetry failure -- the only way this check can confirm "genuinely
// idle" -- fails closed rather than defaulting to healthy. Absence of proof
// is never read as absence of a problem.
func TestClaimLivenessReadyFailsClosedWhenIdleCannotBeProven(t *testing.T) {
	t.Parallel()
	claim := &claimLiveness{}
	dependencies := &workerDependencies{
		queueTelemetryRequired: true,
		queueTelemetry:         &fakeQueueTelemetry{snapshotErr: errors.New("connection reset")},
	}
	ready := dependencies.claimLivenessReady(claim)
	if err := ready(context.Background()); err == nil {
		t.Fatal("expected ready() to fail closed when telemetry cannot confirm idle state")
	}
}

// TestClaimLivenessReadyPassesWithoutTelemetryRequirement proves a
// selection with no telemetry requirement at all (buildQueueTelemetry never
// ran) does not spuriously fail: there is no claim path this check could
// meaningfully assert on.
func TestClaimLivenessReadyPassesWithoutTelemetryRequirement(t *testing.T) {
	t.Parallel()
	claim := &claimLiveness{}
	dependencies := &workerDependencies{queueTelemetryRequired: false}
	ready := dependencies.claimLivenessReady(claim)
	if err := ready(context.Background()); err != nil {
		t.Fatalf("ready() with no telemetry requirement = %v, want nil", err)
	}
}
