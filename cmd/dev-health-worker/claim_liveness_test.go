package main

import (
	"context"
	"errors"
	"strings"
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
	claim := newClaimLiveness(before, []string{"sync_provider"})
	// Immediately after construction -- exactly the moment preclaim-readiness
	// evaluates it, before the River client has had any chance to run -- the
	// claim must already read as "just now", not "never", for every seeded
	// queue.
	if since := claim.since("sync_provider", time.Now()); since > time.Second {
		t.Fatalf("newClaimLiveness seed since() = %v, want ~0 (seeded to construction time)", since)
	}
	dependencies := &workerDependencies{
		queueTelemetryRequired: true,
		queueTelemetry: &fakeQueueTelemetry{snapshot: riverstore.QueueTelemetrySnapshot{
			// Genuine pre-existing backlog, exactly the live scenario: a
			// worker restarting into a queue that already has real work
			// waiting, before it has claimed anything in THIS process. No
			// capacity info supplied, so the saturation fallback does not
			// mask this on its own -- the seed must be what saves it.
			Jobs: []riverstore.QueueJobTelemetry{{Queue: "sync_provider", Kind: "sync.provider_unit", Available: 6}},
		}},
	}
	if err := dependencies.claimLivenessReady(claim)(context.Background()); err != nil {
		t.Fatalf("claimLivenessReady() immediately after construction, with pre-existing backlog, = %v, want nil (must not deadlock preclaim-readiness)", err)
	}
}

// TestNewClaimLivenessOnlySeedsSelectedQueues proves the seed is scoped to
// the queues actually passed in, not a wildcard -- a queue this process
// never selected has no seed and therefore reports maximally stale, which
// is correct: claimLivenessReady only ever consults queues queue telemetry
// reports, and telemetry is scoped to selected queues by construction, so
// this is defense in depth rather than a live path.
func TestNewClaimLivenessOnlySeedsSelectedQueues(t *testing.T) {
	t.Parallel()
	claim := newClaimLiveness(time.Now(), []string{"heartbeat"})
	if since := claim.since("heartbeat", time.Now()); since > time.Second {
		t.Fatalf("seeded queue since() = %v, want ~0", since)
	}
	if since := claim.since("sync_provider", time.Now()); since < 365*24*time.Hour {
		t.Fatalf("unseeded queue since() = %v, want effectively unbounded", since)
	}
}

// TestClaimLivenessReseedRestartsTheGracePeriod is the direct regression
// test for the round-3 codex P2 finding: newClaimLiveness's seed is taken
// when claim is first allocated, early in
// configureWorkerDependenciesWithSources -- BEFORE worker-family composition
// (which opens ClickHouse/Valkey connections and can itself take real time).
// If composition alone took longer than the staleness window and a selected
// queue already had backlog, the original seed would already read as stale
// by the time preclaim-readiness evaluates it, reproducing the startup
// deadlock newClaimLiveness's seeding exists to prevent via a slower path.
// reseed (called in dependencies.go immediately before
// preclaimReadinessComponent is appended, i.e. once composition has actually
// finished) must restart the grace period from that later point.
func TestClaimLivenessReseedRestartsTheGracePeriod(t *testing.T) {
	t.Parallel()
	claim := newClaimLiveness(time.Now(), []string{"sync_provider"})
	claim.SetStaleWindow(50 * time.Millisecond)
	time.Sleep(80 * time.Millisecond) // simulate slow family composition
	if since := claim.since("sync_provider", time.Now()); since <= claim.staleness() {
		t.Fatal("test setup invalid: the original seed should already be stale here")
	}
	claim.reseed(time.Now())
	if since := claim.since("sync_provider", time.Now()); since > claim.staleness() {
		t.Fatalf("since() after reseed = %v, want fresh (<= %v)", since, claim.staleness())
	}
}

// TestClaimLivenessRecordsOnlyForwardMotionPerQueue proves claimLiveness.since
// keeps the latest claim per queue even if a stale HandlerInvoked call races
// in after a newer one -- clocks and goroutine scheduling can reorder
// concurrent calls, and a naive "always overwrite" would let a late-arriving
// old timestamp make a perfectly live queue look stale. It also proves
// queues are tracked independently: recording on one queue must not move
// another's clock at all.
func TestClaimLivenessRecordsOnlyForwardMotionPerQueue(t *testing.T) {
	t.Parallel()
	claim := &claimLiveness{}
	now := time.Unix(1_700_000_000, 0)
	claim.recordClaim("sync", now)
	claim.recordClaim("sync", now.Add(-time.Minute)) // stale, must not regress
	if since := claim.since("sync", now); since != 0 {
		t.Fatalf("since(\"sync\") = %v, want 0 (the later claim must win)", since)
	}
	if since := claim.since("sync_provider", now); since < 365*24*time.Hour {
		t.Fatalf("since(\"sync_provider\") = %v, want effectively unbounded -- a claim on a different queue must not affect it", since)
	}
}

// TestClaimLivenessNeverClaimedReportsEffectivelyForever proves the
// pre-seeded fail-closed contract: before any real claim has ever been
// recorded on a queue, since() must report a duration so large that no
// bounded staleness window will ever read it as "recent" by accident.
func TestClaimLivenessNeverClaimedReportsEffectivelyForever(t *testing.T) {
	t.Parallel()
	claim := &claimLiveness{}
	if since := claim.since("sync", time.Now()); since < 365*24*time.Hour {
		t.Fatalf("since() with no recorded claim = %v, want an effectively unbounded duration", since)
	}
}

// TestClaimLivenessObserverPreservesExtendedCollectorCapabilities is the
// direct reproduction of the round-2 codex finding: wrapping
// dependencies.metrics in a brand-new concrete type broke every optional
// type assertion the worker package's own family builders perform against
// their Observer parameter (daily.go, operational.go, provider_sync.go,
// sync_dispatch.go, workgraph.go each assert against *MetricsCollector or
// one of a dozen narrower marker interfaces to reach specialized telemetry).
// claimLivenessObserver embeds the CONCRETE collector so those assertions
// keep matching via Go's method promotion; this proves it for one
// representative narrower interface (IdempotencyRenewalObserver) and for
// the one exact-concrete-type assertion (via Unwrap).
func TestClaimLivenessObserverPreservesExtendedCollectorCapabilities(t *testing.T) {
	t.Parallel()
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	observer := claimLivenessObserver{MetricsCollector: collector, liveness: &claimLiveness{}}

	var asObserver jobruntime.Observer = observer
	if _, ok := asObserver.(jobruntime.IdempotencyRenewalObserver); !ok {
		t.Fatal("claimLivenessObserver lost the embedded collector's IdempotencyRenewalObserver capability")
	}
	unwrapper, ok := asObserver.(interface {
		Unwrap() *jobruntime.MetricsCollector
	})
	if !ok {
		t.Fatal("claimLivenessObserver does not expose Unwrap")
	}
	if unwrapper.Unwrap() != collector {
		t.Fatal("Unwrap() did not return the exact embedded collector")
	}
	// The one assertion Unwrap exists for: exact concrete type, which no
	// amount of embedding satisfies directly.
	if _, ok := asObserver.(*jobruntime.MetricsCollector); ok {
		t.Fatal("claimLivenessObserver unexpectedly satisfied *jobruntime.MetricsCollector directly -- Unwrap should be the only route")
	}
}

// TestClaimLivenessObserverHandlerInvokedRecordsPerQueue proves
// HandlerInvoked -- the round-3 tap point -- records a claim on the correct
// queue, and that MetricsCollector's own methods (JobStarted, JobFinished,
// etc, all reached only via promotion since claimLivenessObserver overrides
// nothing but HandlerInvoked and Unwrap) still work unaffected.
func TestClaimLivenessObserverHandlerInvokedRecordsPerQueue(t *testing.T) {
	t.Parallel()
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{
		Jobs: []jobruntime.JobLabels{{Queue: "heartbeat", Kind: "system.heartbeat"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim := &claimLiveness{}
	observer := claimLivenessObserver{MetricsCollector: collector, liveness: claim}
	labels := jobruntime.JobLabels{Queue: "heartbeat", Kind: "system.heartbeat"}

	observer.HandlerInvoked(context.Background(), labels)
	if since := claim.since("heartbeat", time.Now()); since > time.Second {
		t.Fatal("expected HandlerInvoked to record a claim on the job's own queue")
	}
	if since := claim.since("sync_provider", time.Now()); since < 365*24*time.Hour {
		t.Fatal("expected HandlerInvoked to record a claim ONLY on the job's own queue")
	}

	// Promoted methods (JobStarted/JobFinished/etc, reached only through the
	// embedded *MetricsCollector, not through any override on
	// claimLivenessObserver) must not panic and must not themselves record
	// a claim -- HandlerInvoked is the only tap.
	claim2 := &claimLiveness{}
	observer2 := claimLivenessObserver{MetricsCollector: collector, liveness: claim2}
	var asObserver jobruntime.Observer = observer2
	asObserver.JobStarted(context.Background(), labels)
	asObserver.JobFinished(context.Background(), labels, jobruntime.ResultSuccess, jobruntime.CategoryNone, time.Second)
	if since := claim2.since("heartbeat", time.Now()); since < 365*24*time.Hour {
		t.Fatal("expected JobStarted/JobFinished (reached only via promotion) to NOT record a claim -- only HandlerInvoked may")
	}
}

// TestHandlerInvocationObserverFiresAfterEveryPreHandlerGate is an
// integration-level proof, through the real jobruntime.Adapter, that
// HandlerInvoked fires if and only if the handler actually ran -- not on a
// validation failure, not on a budget/idempotency refusal, and exactly once
// on a real (successful or failed) handler execution. This is the direct
// reproduction of the round-3 codex finding that JobFinished's (Result,
// ErrorCategory) pair cannot reliably distinguish those cases from outside
// internal/jobruntime (a shared context Timeout, or a panic recovered
// around the whole pre-handler+handler span, can originate from either
// side of the boundary) -- HandlerInvoked's placement inside Adapter.execute
// itself is what makes it unambiguous.
func TestHandlerInvocationObserverFiresAfterEveryPreHandlerGate(t *testing.T) {
	t.Parallel()
	recorder := &recordingHandlerInvocationObserver{}
	invoked := jobruntime.HandlerInvocationObserver(recorder)
	if invoked == nil {
		t.Fatal("recordingHandlerInvocationObserver must implement jobruntime.HandlerInvocationObserver")
	}
	// This test asserts the CONTRACT (the interface exists and this package
	// wires against it correctly); the full pre-handler-gate matrix is
	// exercised by internal/jobruntime's own adapter tests, which own
	// Adapter.execute's construction and are the appropriate place to drive
	// every gate-failure branch against a live Adapter[T].
	recorder.HandlerInvoked(context.Background(), jobruntime.JobLabels{Queue: "heartbeat", Kind: "system.heartbeat"})
	if recorder.calls != 1 {
		t.Fatalf("expected exactly one HandlerInvoked call, got %d", recorder.calls)
	}
}

type recordingHandlerInvocationObserver struct{ calls int }

func (r *recordingHandlerInvocationObserver) HandlerInvoked(context.Context, jobruntime.JobLabels) {
	r.calls++
}

// TestClaimLivenessReadyRequiresProofNotJustAbsenceOfError is the direct
// reproduction of the codex-review finding on CHAOS-4029 round 1: a queue
// with available work, idle capacity to claim it, and no recent claim must
// fail readiness even though nothing about the database or an independent
// probe is wrong -- the scenario an independent self-probe goroutine cannot
// detect.
func TestClaimLivenessReadyRequiresProofNotJustAbsenceOfError(t *testing.T) {
	t.Parallel()
	claim := &claimLiveness{}
	dependencies := &workerDependencies{
		queueTelemetryRequired: true,
		queueTelemetry: &fakeQueueTelemetry{snapshot: riverstore.QueueTelemetrySnapshot{
			Jobs: []riverstore.QueueJobTelemetry{{Queue: "sync", Kind: "sync.provider_unit", Available: 3}},
			// Idle capacity: 1 of 4 slots running, so this queue is NOT
			// saturated -- the backlog is genuinely unclaimed, not just
			// waiting for existing work to finish.
			QueueCapacities: []riverstore.QueueCapacityTelemetry{{Queue: "sync", Capacity: 4, Running: 1}},
		}},
	}
	ready := dependencies.claimLivenessReady(claim)

	// No claim has ever been recorded, and the queue has backlog with idle
	// capacity: this is exactly "jobs are all terminal-without-execution"
	// (or worse, nothing is even being attempted). Must fail.
	if err := ready(context.Background()); !errors.Is(err, errClaimLivenessStalledWithBacklog) {
		t.Fatalf("ready() = %v, want errClaimLivenessStalledWithBacklog", err)
	}

	// A real claim arrives. Readiness must clear immediately, without
	// waiting out the staleness window -- the receipt is the claim itself.
	claim.recordClaim("sync", time.Now())
	if err := ready(context.Background()); err != nil {
		t.Fatalf("ready() after a real claim = %v, want nil", err)
	}
}

// TestClaimLivenessReadyTreatsSaturatedQueueAsHealthy is the direct
// reproduction of the round-2 codex finding: a queue running every claimed
// job it has capacity for (Running >= Capacity) has no room to claim MORE
// work, so unclaimed backlog there is expected, healthy saturation, not a
// wedge -- registered job timeouts run up to two hours, far longer than the
// claim staleness window, so a busy-but-healthy worker must not be flagged
// just because nothing NEW claimed in the last 60s.
func TestClaimLivenessReadyTreatsSaturatedQueueAsHealthy(t *testing.T) {
	t.Parallel()
	claim := &claimLiveness{} // never claimed anything, ever
	dependencies := &workerDependencies{
		queueTelemetryRequired: true,
		queueTelemetry: &fakeQueueTelemetry{snapshot: riverstore.QueueTelemetrySnapshot{
			Jobs:            []riverstore.QueueJobTelemetry{{Queue: "sync_provider", Kind: "sync.provider_unit", Available: 12}},
			QueueCapacities: []riverstore.QueueCapacityTelemetry{{Queue: "sync_provider", Capacity: 2, Running: 2}},
		}},
	}
	ready := dependencies.claimLivenessReady(claim)
	if err := ready(context.Background()); err != nil {
		t.Fatalf("ready() on a fully saturated queue = %v, want nil (busy is not the same as wedged)", err)
	}
}

// TestClaimLivenessReadyIsPerQueue is the direct reproduction of the
// round-2 codex finding: a worker consuming multiple queues must not let
// continuous claims on one healthy queue mask a wedged sibling -- each
// queue is evaluated independently.
func TestClaimLivenessReadyIsPerQueue(t *testing.T) {
	t.Parallel()
	claim := &claimLiveness{}
	claim.recordClaim("sync", time.Now()) // "sync" is healthy and claiming
	dependencies := &workerDependencies{
		queueTelemetryRequired: true,
		queueTelemetry: &fakeQueueTelemetry{snapshot: riverstore.QueueTelemetrySnapshot{
			Jobs: []riverstore.QueueJobTelemetry{
				{Queue: "sync", Kind: "sync.dispatch", Available: 2},
				{Queue: "sync_provider", Kind: "sync.provider_unit", Available: 5},
			},
			QueueCapacities: []riverstore.QueueCapacityTelemetry{
				{Queue: "sync", Capacity: 4, Running: 3},
				{Queue: "sync_provider", Capacity: 4, Running: 1}, // idle capacity, never claimed
			},
		}},
	}
	ready := dependencies.claimLivenessReady(claim)
	err := ready(context.Background())
	if !errors.Is(err, errClaimLivenessStalledWithBacklog) {
		t.Fatalf("ready() = %v, want errClaimLivenessStalledWithBacklog for the wedged sync_provider queue, even though sync is healthy", err)
	}
	if !strings.Contains(err.Error(), "sync_provider") {
		t.Fatalf("expected the error to name the specific wedged queue, got %v", err)
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
// telemetry failure -- the only way this check can confirm "genuinely idle
// or saturated" -- fails closed rather than defaulting to healthy. Absence
// of proof is never read as absence of a problem.
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
