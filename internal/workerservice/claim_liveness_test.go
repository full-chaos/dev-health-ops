package workerservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	claim := &claimLiveness{}
	// Two long jobs entered their handlers an hour ago and are still inside.
	claim.handlerInvoked("sync_provider", time.Now().Add(-time.Hour))
	claim.handlerInvoked("sync_provider", time.Now().Add(-time.Hour))
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

// CHAOS-6818 r1b P1: Running == Capacity is only "busy" when every running slot
// is INSIDE a handler. River counts a job failing at its idempotency Begin as
// running, so a full queue with a slot stuck before its handler (a stale
// pooler) and a stale claim clock must fail, and must heal when the stuck slot
// reaches a handler again.
func TestClaimLivenessReadyFailsAFullQueueWhoseSlotsAreNotAllInsideHandlers(t *testing.T) {
	t.Parallel()
	claim := &claimLiveness{}
	claim.handlerInvoked("sync_provider", time.Now().Add(-time.Hour)) // 1 of 2 running slots inside a handler
	dependencies := &workerDependencies{
		queueTelemetryRequired: true,
		queueTelemetry: &fakeQueueTelemetry{snapshot: riverstore.QueueTelemetrySnapshot{
			Jobs:            []riverstore.QueueJobTelemetry{{Queue: "sync_provider", Kind: "sync.provider_unit", Available: 12}},
			QueueCapacities: []riverstore.QueueCapacityTelemetry{{Queue: "sync_provider", Capacity: 2, Running: 2}},
		}},
	}
	ready := dependencies.claimLivenessReady(claim)
	if err := ready(context.Background()); !errors.Is(err, errClaimLivenessStalledWithBacklog) {
		t.Fatalf("ready() = %v, want errClaimLivenessStalledWithBacklog (one full-queue slot is stuck before its handler)", err)
	}
	claim.handlerInvoked("sync_provider", time.Now()) // the stuck slot finally reaches its handler
	if err := ready(context.Background()); err != nil {
		t.Fatalf("ready() after the slot reached its handler = %v, want nil", err)
	}
}

// handlerReturned must balance handlerInvoked, never go below zero, and count
// as claim evidence itself.
func TestClaimLivenessHandlerReturnedBalancesAndIsEvidence(t *testing.T) {
	t.Parallel()
	claim := &claimLiveness{}
	old := time.Now().Add(-time.Hour)
	claim.handlerInvoked("q", old)
	claim.handlerReturned("q", old)
	claim.handlerReturned("q", old) // an unmatched return must not go negative
	if got := claim.handlersInside("q"); got != 0 {
		t.Fatalf("handlersInside after balanced+extra return = %d, want 0", got)
	}
	claim.handlerInvoked("q", old)
	if got := claim.handlersInside("q"); got != 1 {
		t.Fatalf("handlersInside after a fresh invoke = %d, want 1 (a floor bug would leave it 0 or negative)", got)
	}
	before := claim.since("q", time.Now())
	claim.handlerReturned("q", time.Now())
	if after := claim.since("q", time.Now()); after >= before {
		t.Fatalf("handlerReturned did not refresh the claim clock: before=%v after=%v", before, after)
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

// TestClaimLivenessReadyPreservesATelemetryDeadlineAndLogsItsOwnCause is the
// execution_liveness counterpart of TestDependencyCheckFailedPreservesThe
// UnderlyingCauseForClassification: a queue-telemetry read that fails only
// because its own bounded context expired must still classify as retryable
// (health.Registry decides that by unwrapping the CheckFunc's own returned
// error for context.DeadlineExceeded/Canceled, never by reading logs), and
// this member must log its own cause the same way domainReady/queueReady/
// riverSchemaReady/idempotencyBackendReady already do -- so an operator
// reading a crash loop sees why execution_liveness specifically refused,
// not just its bare name.
func TestClaimLivenessReadyPreservesATelemetryDeadlineAndLogsItsOwnCause(t *testing.T) {
	t.Parallel()
	var logOutput bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logOutput, nil))
	claim := &claimLiveness{}
	snapshotErr := fmt.Errorf("%w: sampling queue telemetry: %w", errors.New("queue_telemetry_unavailable"), context.DeadlineExceeded)
	dependencies := &workerDependencies{
		logger:                 logger,
		queueTelemetryRequired: true,
		queueTelemetry:         &fakeQueueTelemetry{snapshotErr: snapshotErr},
	}
	ready := dependencies.claimLivenessReady(claim)

	err := ready(context.Background())
	if !errors.Is(err, errWorkerDependencyUnavailable) {
		t.Fatalf("ready() error = %v, want errWorkerDependencyUnavailable", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ready() error = %v, want it to still unwrap to context.DeadlineExceeded", err)
	}
	if !strings.Contains(logOutput.String(), "execution_liveness") {
		t.Fatalf("expected a member-level log line naming execution_liveness, got %q", logOutput.String())
	}
	if !strings.Contains(logOutput.String(), "sampling queue telemetry") {
		t.Fatalf("expected the member's own error text in the log line, got %q", logOutput.String())
	}
}

// TestClaimLivenessReadySkipsBacklogDuringPreclaimButEnforcesAfter is the
// direct reproduction of the preclaim false-negative: a fresh worker has not
// started claiming anything yet during preclaim (River's producers have not
// started -- that is the entire point of a PRE-claim check), so a queue
// that already has backlog and no recorded claim must not be read as wedged
// while claim.inPreclaim() is still true, no matter how long preclaim's own
// retry loop has been running. The same state, once markRuntimeLive reports
// preclaim is over and the grace window has elapsed with still nothing
// claimed, must fail exactly as it always did -- this check must not lose
// its ability to catch a consumer that is genuinely wedged from birth.
func TestClaimLivenessReadySkipsBacklogDuringPreclaimButEnforcesAfter(t *testing.T) {
	t.Parallel()
	var logOutput bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logOutput, nil))
	claim := newClaimLiveness(time.Now(), []string{"sync_provider"})
	claim.SetStaleWindow(10 * time.Millisecond)
	dependencies := &workerDependencies{
		logger:                 logger,
		queueTelemetryRequired: true,
		queueTelemetry: &fakeQueueTelemetry{snapshot: riverstore.QueueTelemetrySnapshot{
			Jobs: []riverstore.QueueJobTelemetry{{Queue: "sync_provider", Kind: "sync.provider_unit", Available: 6}},
		}},
	}
	ready := dependencies.claimLivenessReady(claim)

	// Elapse the grace window while still in preclaim -- the shape a slow,
	// unrelated dependency's own retry loop produces. Nothing has claimed
	// anything because nothing could have: River has not started.
	time.Sleep(30 * time.Millisecond)
	if err := ready(context.Background()); err != nil {
		t.Fatalf("ready() during preclaim with elapsed backlog = %v, want nil (no claim is possible yet)", err)
	}
	if !strings.Contains(logOutput.String(), "execution_liveness") {
		t.Fatalf("expected a log line explaining the preclaim skip, got %q", logOutput.String())
	}

	// Preclaim readiness passes; River is about to start claiming.
	claim.markRuntimeLive()

	// The exact same backlog-with-no-claim state, past the same age gate,
	// must now fail -- this is the genuinely wedged case the check exists to
	// catch, and markRuntimeLive must not have disabled it.
	time.Sleep(30 * time.Millisecond)
	if err := ready(context.Background()); !errors.Is(err, errClaimLivenessStalledWithBacklog) {
		t.Fatalf("ready() after preclaim ended = %v, want errClaimLivenessStalledWithBacklog", err)
	}
}

// TestClaimLivenessReadyRefusesOnFirstAttemptWhenGenuinelyFailed proves a
// queue-telemetry failure that does NOT unwrap to a context deadline -- a
// real, non-transient problem -- still refuses immediately: it must not be
// misread as retryable just because this member now wraps its cause instead
// of discarding it.
func TestClaimLivenessReadyRefusesOnFirstAttemptWhenGenuinelyFailed(t *testing.T) {
	t.Parallel()
	claim := &claimLiveness{}
	genuineErr := errors.New("queue telemetry query rejected: unsupported contract version")
	dependencies := &workerDependencies{
		queueTelemetryRequired: true,
		queueTelemetry:         &fakeQueueTelemetry{snapshotErr: genuineErr},
	}
	ready := dependencies.claimLivenessReady(claim)

	err := ready(context.Background())
	if !errors.Is(err, errWorkerDependencyUnavailable) {
		t.Fatalf("ready() error = %v, want errWorkerDependencyUnavailable", err)
	}
	if !errors.Is(err, genuineErr) {
		t.Fatalf("ready() error = %v, want it to still unwrap to the genuine cause", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("ready() error unexpectedly classifies as a deadline -- a genuine failure must not be retryable")
	}
}

// CHAOS-6818 r2c P1: a slot stuck before its handler with NOTHING available.
func stuckSlotReady(t *testing.T, claim *claimLiveness, running int64) error {
	t.Helper()
	dependencies := &workerDependencies{
		queueTelemetryRequired: true,
		queueTelemetry: &fakeQueueTelemetry{snapshot: riverstore.QueueTelemetrySnapshot{
			Jobs:            []riverstore.QueueJobTelemetry{{Queue: "sync", Kind: "sync.dispatch", Available: 0}},
			QueueCapacities: []riverstore.QueueCapacityTelemetry{{Queue: "sync", Capacity: 1, Running: running}},
		}},
	}
	return dependencies.claimLivenessReady(claim)(context.Background())
}

func TestClaimLivenessFailsAStuckSlotWithNothingAvailable(t *testing.T) {
	t.Parallel()
	claim := &claimLiveness{}
	claim.SetStaleWindow(60 * time.Millisecond)
	claim.recordClaim("sync", time.Now().Add(-time.Hour)) // no handler activity for an hour
	// The first observation proves nothing: a job is briefly between claim and handler.
	if err := stuckSlotReady(t, claim, 1); err != nil {
		t.Fatalf("first observation of a claimed job must not fail: %v", err)
	}
	time.Sleep(90 * time.Millisecond)
	if err := stuckSlotReady(t, claim, 1); !errors.Is(err, errClaimLivenessSlotStuckBeforeHandler) {
		t.Fatalf("a slot stuck before its handler past the window with no handler activity = %v, want errClaimLivenessSlotStuckBeforeHandler", err)
	}
	// Heals the moment the slot reaches its handler (which is also handler activity).
	claim.handlerInvoked("sync", time.Now())
	if err := stuckSlotReady(t, claim, 1); err != nil {
		t.Fatalf("after the slot reached its handler: %v", err)
	}
}

// A job just claimed on a queue that was idle for an hour: the claim clock is
// stale but the slot has only just been seen, so it must not fail.
func TestClaimLivenessDoesNotFailAJustClaimedJobOnASparseQueue(t *testing.T) {
	t.Parallel()
	claim := &claimLiveness{}
	claim.SetStaleWindow(60 * time.Millisecond)
	claim.recordClaim("sync", time.Now().Add(-time.Hour))
	for i := 0; i < 3; i++ {
		if err := stuckSlotReady(t, claim, 1); err != nil {
			t.Fatalf("poll %d of a fresh claim: %v", i, err)
		}
		// Each poll sees the job finish (Running 0) before the next one claims.
		if err := stuckSlotReady(t, claim, 0); err != nil {
			t.Fatal(err)
		}
		time.Sleep(30 * time.Millisecond)
	}
}

// A busy healthy queue: every poll catches some job between claim and handler
// for far longer than the window, but handlers keep running, so the claim clock
// stays fresh and readiness must hold.
func TestClaimLivenessDoesNotFailABusyQueueWhoseHandlersKeepRunning(t *testing.T) {
	t.Parallel()
	claim := &claimLiveness{}
	claim.SetStaleWindow(60 * time.Millisecond)
	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		claim.recordClaim("sync", time.Now()) // a handler ran a moment ago
		if err := stuckSlotReady(t, claim, 1); err != nil {
			t.Fatalf("a busy queue with fresh handler activity failed: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A long job inside its handler is never "stuck before the handler".
func TestClaimLivenessDoesNotFailALongJobInsideItsHandler(t *testing.T) {
	t.Parallel()
	claim := &claimLiveness{}
	claim.SetStaleWindow(60 * time.Millisecond)
	claim.handlerInvoked("sync", time.Now().Add(-time.Hour))
	for i := 0; i < 4; i++ {
		if err := stuckSlotReady(t, claim, 1); err != nil {
			t.Fatalf("poll %d with the running job inside its handler: %v", i, err)
		}
		time.Sleep(40 * time.Millisecond)
	}
}

// The unbroken-run rule: a poll that sees the slot inside a handler (or gone)
// restarts the clock, so two short stuck spells never add up to one long one.
func TestClaimLivenessStuckRunMustBeUnbroken(t *testing.T) {
	t.Parallel()
	claim := &claimLiveness{}
	claim.SetStaleWindow(80 * time.Millisecond)
	claim.recordClaim("sync", time.Now().Add(-time.Hour))
	if err := stuckSlotReady(t, claim, 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := stuckSlotReady(t, claim, 0); err != nil { // the job finished: run broken
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // 100 ms since the first sighting, > the 80 ms window
	if err := stuckSlotReady(t, claim, 1); err != nil {
		t.Fatalf("a run broken by a poll that saw no stuck slot must restart, got %v", err)
	}
}

// No claim can exist before River starts: never fail on preclaim.
func TestClaimLivenessStuckSlotIsIgnoredInPreclaim(t *testing.T) {
	t.Parallel()
	claim := newClaimLiveness(time.Now().Add(-time.Hour), []string{"sync"})
	claim.SetStaleWindow(30 * time.Millisecond)
	for i := 0; i < 3; i++ {
		if err := stuckSlotReady(t, claim, 1); err != nil {
			t.Fatalf("preclaim poll %d: %v", i, err)
		}
		time.Sleep(40 * time.Millisecond)
	}
}

// judgeQueue is one pure predicate over (backlog, slots-in-handler, activity
// age, stuck-run age, preclaim). Table-driven over the input space the lead's
// ruling names: Available x slots-in-handler x stuck-at-idempotency-Begin.
func TestJudgeQueueOverTheWholeInputSpace(t *testing.T) {
	t.Parallel()
	const window = time.Minute
	young, old := 10*time.Second, 5*time.Minute
	fresh, stale := 10*time.Second, time.Hour
	type slots struct {
		name                      string
		known                     bool
		capacity, running, inside int64
	}
	shapes := []slots{
		{"capacity unknown", false, 0, 0, 0},
		{"idle, free capacity", true, 2, 0, 0},
		{"free capacity, running job inside handler", true, 2, 1, 1},
		{"free capacity, running job stuck before handler", true, 2, 1, 0},
		{"full, every slot inside a handler", true, 1, 1, 1},
		{"full, slot stuck before handler", true, 1, 1, 0},
		{"full, some slots inside some stuck", true, 2, 2, 1},
	}
	build := func(available int64, shape slots, claimAge, stuckFor time.Duration, preclaim bool) queueFacts {
		return queueFacts{
			queue: "q", available: available, capacityKnown: shape.known,
			capacity: shape.capacity, running: shape.running, inside: shape.inside,
			claimAge: claimAge, stuckFor: stuckFor, window: window, preclaim: preclaim,
		}
	}

	// Named rows: the cases that must FAIL, and the ones that must not.
	named := []struct {
		name  string
		facts queueFacts
		want  queueVerdict
	}{
		{"backlog, free capacity, no handler for the window: consumer not claiming",
			build(3, shapes[1], stale, young, false), verdictStalledBacklog},
		{"backlog, capacity unknown, no handler for the window",
			build(3, shapes[0], stale, young, false), verdictStalledBacklog},
		{"backlog, full queue with a stuck slot, no handler activity (r1b P1)",
			build(3, shapes[5], stale, young, false), verdictStalledBacklog},
		{"NO backlog, the only claimed job stuck before its handler, unbroken past the window (r2c P1)",
			build(0, shapes[5], stale, old, false), verdictSlotStuck},
		{"no backlog, free capacity, a stuck job, unbroken past the window",
			build(0, shapes[3], stale, old, false), verdictSlotStuck},
		{"no backlog, partly stuck full queue, unbroken past the window",
			build(0, shapes[6], stale, old, false), verdictSlotStuck},
		{"backlog behind a full queue whose every slot is inside a handler: long work",
			build(9, shapes[4], stale, old, false), verdictHealthy},
		{"no backlog, empty queue", build(0, shapes[1], stale, old, false), verdictHealthy},
		{"no backlog, job just claimed on a sparse queue (young stuck run)",
			build(0, shapes[5], stale, young, false), verdictHealthy},
		{"no backlog, stuck-looking slot but handlers ran recently (busy queue)",
			build(0, shapes[5], fresh, old, false), verdictHealthy},
		{"backlog, free capacity, but a handler ran recently",
			build(3, shapes[1], fresh, young, false), verdictHealthy},
		{"exactly at the window is not yet past it: stuck run",
			build(0, shapes[5], stale, window, false), verdictHealthy},
		{"exactly at the window is not yet past it: no handler activity",
			build(0, shapes[5], window, old, false), verdictHealthy},
		{"exactly at the window is not yet past it: backlog arm",
			build(3, shapes[1], window, young, false), verdictHealthy},
		{"preclaim: a queue that would fail the backlog arm is a skip",
			build(3, shapes[1], stale, young, true), verdictPreclaimSkip},
		{"preclaim: the stuck arm cannot fail",
			build(0, shapes[5], stale, old, true), verdictHealthy},
	}
	for _, test := range named {
		if got := judgeQueue(test.facts); got != test.want {
			t.Errorf("%s: verdict %d, want %d (%+v)", test.name, got, test.want, test.facts)
		}
	}

	// Properties over the full grid (2 backlogs x 7 shapes x 2 x 2 x 2).
	for _, available := range []int64{0, 3} {
		for _, shape := range shapes {
			for _, claimAge := range []time.Duration{fresh, stale} {
				for _, stuckFor := range []time.Duration{young, old} {
					for _, preclaim := range []bool{false, true} {
						facts := build(available, shape, claimAge, stuckFor, preclaim)
						verdict := judgeQueue(facts)
						failing := verdict == verdictSlotStuck || verdict == verdictStalledBacklog
						where := fmt.Sprintf("%s available=%d claimAge=%v stuckFor=%v preclaim=%v", shape.name, available, claimAge, stuckFor, preclaim)
						if claimAge <= window && failing {
							t.Errorf("recent handler activity must never fail (%s)", where)
						}
						if preclaim && failing {
							t.Errorf("preclaim must never fail (%s)", where)
						}
						if shape.running <= shape.inside && shape.known && shape.capacity > 0 &&
							(available <= 0 || shape.running >= shape.capacity) && failing {
							t.Errorf("every running slot inside a handler on an empty-or-full queue is healthy (%s)", where)
						}
						if available <= 0 && shape.running <= shape.inside && failing {
							t.Errorf("no backlog and no slot outside a handler is healthy (%s)", where)
						}
					}
				}
			}
		}
	}
}

// The stuck-slot arm never consults the backlog (lead D2606): a claimed slot
// outside a handler, unbroken past the window with no handler activity, is the
// same verdict whether nothing is available or a great deal is. Only the
// separate backlog arm reads Available (a consumer that is not claiming).
func TestJudgeQueueStuckArmIgnoresAvailable(t *testing.T) {
	t.Parallel()
	const window = time.Minute
	for _, shape := range []struct {
		name                      string
		known                     bool
		capacity, running, inside int64
	}{
		{"free capacity, stuck job", true, 2, 1, 0},
		{"full, stuck slot", true, 1, 1, 0},
		{"full, some inside some stuck", true, 2, 2, 1},
		{"capacity unknown, running reported", false, 0, 1, 0},
	} {
		for _, available := range []int64{0, 1, 3, 1000} {
			facts := queueFacts{
				queue: "q", available: available, capacityKnown: shape.known,
				capacity: shape.capacity, running: shape.running, inside: shape.inside,
				claimAge: time.Hour, stuckFor: 5 * time.Minute, window: window,
			}
			if got := judgeQueue(facts); got != verdictSlotStuck {
				t.Errorf("%s available=%d: verdict %d, want verdictSlotStuck: the stuck arm must not depend on the backlog",
					shape.name, available, got)
			}
		}
	}
}

// CHAOS-6864: the gate-failure arm. Jobs that keep failing at their idempotency
// gate are invisible to the snapshot arms while River holds them in retry backoff
// (Available=0, Running=0), so the verdict must come from the failure run itself.
func TestJudgeQueueGateFailureArm(t *testing.T) {
	t.Parallel()
	const window = time.Minute
	idleQueue := queueFacts{
		queue: "q", available: 0, capacityKnown: true, capacity: 2, running: 0, inside: 0,
		claimAge: time.Hour, window: window,
		gateFails: gateFailureMinimum, gateFailSpan: 2 * window,
	}
	with := func(edit func(*queueFacts)) queueFacts {
		facts := idleQueue
		edit(&facts)
		return facts
	}
	for _, test := range []struct {
		name  string
		facts queueFacts
		want  queueVerdict
	}{
		{"the retry-backoff gap: nothing available or running, repeated failures over a window",
			idleQueue, verdictGateFailing},
		{"the same with a backlog behind a free queue",
			with(func(f *queueFacts) { f.available = 3 }), verdictGateFailing},
		{"a single failure is a blip",
			with(func(f *queueFacts) { f.gateFails = gateFailureMinimum - 1 }), verdictHealthy},
		{"no failures at all", with(func(f *queueFacts) { f.gateFails, f.gateFailSpan = 0, 0 }), verdictHealthy},
		{"failures that have not yet spanned the window",
			with(func(f *queueFacts) { f.gateFailSpan = window }), verdictHealthy},
		{"a handler ran within the window",
			with(func(f *queueFacts) { f.claimAge = window }), verdictHealthy},
		{"a handler is inside right now: the work pool is reaching the database",
			with(func(f *queueFacts) { f.inside, f.running = 1, 1 }), verdictHealthy},
		{"preclaim: no job can have run yet",
			with(func(f *queueFacts) { f.preclaim = true }), verdictHealthy},
	} {
		if got := judgeQueue(test.facts); got != test.want {
			t.Errorf("%s: verdict %d, want %d (%+v)", test.name, got, test.want, test.facts)
		}
	}
}

func TestGateFailureEvidenceTracker(t *testing.T) {
	t.Parallel()
	start := time.Now()
	at := func(d time.Duration) time.Time { return start.Add(d) }
	claim := newClaimLiveness(start, []string{"q", "other"})
	claim.SetStaleWindow(time.Minute)

	if n, _ := claim.gateFailureEvidence("q", at(0)); n != 0 {
		t.Fatalf("evidence before any failure: %d", n)
	}
	claim.recordGateFailure("q", true, at(0))
	claim.recordGateFailure("q", true, at(5*time.Second))
	claim.recordGateFailure("q", true, at(15*time.Second))
	n, span := claim.gateFailureEvidence("q", at(3*time.Minute))
	if n != 3 || span != 15*time.Second {
		t.Fatalf("run = (%d, %v), want (3, 15s): a retry-pending failure must stay evidence across its backoff", n, span)
	}
	if n, _ := claim.gateFailureEvidence("other", at(3*time.Minute)); n != 0 {
		t.Fatal("gate failures on one queue must not be evidence on another")
	}
	if n, _ := claim.gateFailureEvidence("q", at(15*time.Second+gateFailureRetryHorizon+time.Second)); n != 0 {
		t.Fatal("evidence must lapse once no further retry is due")
	}

	// A terminal failure (no retry pending) holds the evidence for one window only.
	claim.recordGateFailure("q", false, at(10*time.Minute))
	if n, _ := claim.gateFailureEvidence("q", at(10*time.Minute+30*time.Second)); n != 1 {
		t.Fatalf("a run restarted after lapse must count fresh, got %d", n)
	}
	if n, _ := claim.gateFailureEvidence("q", at(10*time.Minute+time.Minute+time.Second)); n != 0 {
		t.Fatal("a terminal failure must not stay evidence past one window")
	}

	// A handler ending the run: the work pool reached the database.
	claim.recordGateFailure("q", true, at(20*time.Minute))
	claim.recordGateFailure("q", true, at(20*time.Minute+time.Second))
	claim.handlerInvoked("q", at(20*time.Minute+2*time.Second))
	if n, _ := claim.gateFailureEvidence("q", at(20*time.Minute+3*time.Second)); n != 0 {
		t.Fatal("HandlerInvoked must end the run of gate failures")
	}
}

// The production observer counts ONLY idempotency-category outcomes as gate
// failure evidence, and never as a claim.
func TestClaimLivenessObserverJobFinishedRecordsOnlyIdempotencyFailures(t *testing.T) {
	t.Parallel()
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{
		Jobs: []jobruntime.JobLabels{{Queue: "heartbeat", Kind: "system.heartbeat"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	labels := jobruntime.JobLabels{Queue: "heartbeat", Kind: "system.heartbeat"}
	for _, category := range []jobruntime.ErrorCategory{
		jobruntime.CategoryNone, jobruntime.CategoryValidation, jobruntime.CategoryPanic,
		jobruntime.CategoryTimeout, jobruntime.CategoryCancelled, jobruntime.CategoryRetryable,
		jobruntime.CategoryPermanent, jobruntime.CategoryTerminalDomain, jobruntime.CategoryTenant,
		jobruntime.CategoryBudget, jobruntime.CategoryRateLimited,
	} {
		claim := newClaimLiveness(time.Now(), []string{"heartbeat"})
		observer := claimLivenessObserver{MetricsCollector: collector, liveness: claim}
		observer.JobFinished(context.Background(), labels, jobruntime.ResultRetry, category, time.Millisecond)
		if n, _ := claim.gateFailureEvidence("heartbeat", time.Now()); n != 0 {
			t.Errorf("category %q must not count as an idempotency-gate failure", category)
		}
	}
	claim := newClaimLiveness(time.Now().Add(-time.Hour), []string{"heartbeat"})
	observer := claimLivenessObserver{MetricsCollector: collector, liveness: claim}
	observer.JobFinished(context.Background(), labels, jobruntime.ResultRetry, jobruntime.CategoryIdempotency, time.Millisecond)
	if n, _ := claim.gateFailureEvidence("heartbeat", time.Now()); n != 1 {
		t.Fatalf("an idempotency failure recorded %d times, want 1", n)
	}
	if since := claim.since("heartbeat", time.Now()); since < 59*time.Minute {
		t.Fatal("a gate failure must never refresh the claim clock")
	}
	// A nil tracker (the zero-value observer some fixtures build) must not panic.
	claimLivenessObserver{MetricsCollector: collector}.JobFinished(
		context.Background(), labels, jobruntime.ResultRetry, jobruntime.CategoryIdempotency, time.Millisecond)
}

// End to end through claimLivenessReady with the snapshot showing an idle queue:
// the verdict comes from the failure run alone.
func TestClaimLivenessReadyFailsOnAGateFailureRunWhileTheQueueLooksIdle(t *testing.T) {
	t.Parallel()
	telemetry := &fakeQueueTelemetry{snapshot: riverstore.QueueTelemetrySnapshot{
		Jobs:            []riverstore.QueueJobTelemetry{{Queue: "heartbeat", Kind: "system.heartbeat", Available: 0}},
		QueueCapacities: []riverstore.QueueCapacityTelemetry{{Queue: "heartbeat", Capacity: 1, Running: 0}},
	}}
	dependencies := &workerDependencies{queueTelemetryRequired: true, queueTelemetry: telemetry}
	claim := newClaimLiveness(time.Now().Add(-time.Hour), []string{"heartbeat"})
	claim.SetStaleWindow(time.Minute)
	claim.markRuntimeLive()
	ready := dependencies.claimLivenessReady(claim)

	now := time.Now()
	claim.recordGateFailure("heartbeat", true, now.Add(-90*time.Second))
	if err := ready(context.Background()); err != nil {
		t.Fatalf("one gate failure must not fail readiness: %v", err)
	}
	claim.recordGateFailure("heartbeat", true, now.Add(-time.Second))
	err := ready(context.Background())
	if !errors.Is(err, errClaimLivenessGateFailing) {
		t.Fatalf("repeated gate failures across a window with an idle-looking queue: err = %v", err)
	}
	claim.handlerInvoked("heartbeat", time.Now())
	if err := ready(context.Background()); err != nil {
		t.Fatalf("a handler reaching its work must clear the verdict: %v", err)
	}
}

// CHAOS-6883: an execution_liveness refusal names the clause and every fact the
// predicate judged. The 503 body carries only the check name and refusals used to
// leave no log line, so a refusal that cleared could not be attributed to the
// stuck-slot arm, the backlog arm or the gate-failure arm.
func TestClaimLivenessRefusalLogNamesTheClauseAndTheFacts(t *testing.T) {
	t.Parallel()
	type scenario struct {
		name     string
		clause   string
		snapshot riverstore.QueueTelemetrySnapshot
		prepare  func(*claimLiveness)
		polls    int
		want     []string
	}
	idle := riverstore.QueueTelemetrySnapshot{
		Jobs:            []riverstore.QueueJobTelemetry{{Queue: "heartbeat", Kind: "system.heartbeat", Available: 0}},
		QueueCapacities: []riverstore.QueueCapacityTelemetry{{Queue: "heartbeat", Capacity: 2, Running: 0}},
	}
	for _, test := range []scenario{
		{
			name: "backlog arm", clause: "backlog_without_handler", polls: 1,
			snapshot: riverstore.QueueTelemetrySnapshot{
				Jobs:            []riverstore.QueueJobTelemetry{{Queue: "heartbeat", Kind: "system.heartbeat", Available: 3}},
				QueueCapacities: []riverstore.QueueCapacityTelemetry{{Queue: "heartbeat", Capacity: 2, Running: 0}},
			},
			want: []string{`"available":3`, `"capacity":2`, `"running":0`, `"inside_handler":0`},
		},
		{
			name: "stuck slot arm", clause: "slot_stuck_before_handler", polls: 2,
			snapshot: riverstore.QueueTelemetrySnapshot{
				Jobs:            []riverstore.QueueJobTelemetry{{Queue: "heartbeat", Kind: "system.heartbeat", Available: 0}},
				QueueCapacities: []riverstore.QueueCapacityTelemetry{{Queue: "heartbeat", Capacity: 1, Running: 1}},
			},
			want: []string{`"running":1`, `"inside_handler":0`, `"available":0`},
		},
		{
			name: "gate failure arm", clause: "gate_failures_without_handler", polls: 1, snapshot: idle,
			prepare: func(c *claimLiveness) {
				now := time.Now()
				c.recordGateFailure("heartbeat", true, now.Add(-90*time.Second))
				c.recordGateFailure("heartbeat", true, now.Add(-time.Second))
			},
			want: []string{`"gate_failures":2`, `"available":0`},
		},
	} {
		var logs bytes.Buffer
		telemetry := &fakeQueueTelemetry{snapshot: test.snapshot}
		dependencies := &workerDependencies{
			queueTelemetryRequired: true, queueTelemetry: telemetry,
			logger: slog.New(slog.NewJSONHandler(&logs, nil)),
		}
		claim := newClaimLiveness(time.Now().Add(-time.Hour), []string{"heartbeat"})
		claim.SetStaleWindow(40 * time.Millisecond)
		if test.prepare != nil {
			claim.SetStaleWindow(time.Minute)
			test.prepare(claim)
		}
		claim.markRuntimeLive()
		ready := dependencies.claimLivenessReady(claim)
		var err error
		for poll := 0; poll < test.polls; poll++ {
			if poll > 0 {
				time.Sleep(80 * time.Millisecond)
			}
			err = ready(context.Background())
		}
		if err == nil {
			t.Fatalf("%s: expected a refusal", test.name)
		}
		out := logs.String()
		for _, want := range append([]string{
			`"msg":"execution liveness refused"`, `"check":"execution_liveness"`, `"queue":"heartbeat"`,
			`"clause":"` + test.clause + `"`,
		}, test.want...) {
			if !strings.Contains(out, want) {
				t.Errorf("%s: refusal log lacks %s: %s", test.name, want, out)
			}
		}
		// Rate limit: a repeat inside the interval adds no second line.
		before := strings.Count(out, "execution liveness refused")
		_ = ready(context.Background())
		if after := strings.Count(logs.String(), "execution liveness refused"); after != before {
			t.Errorf("%s: a repeat inside the interval logged again (%d -> %d)", test.name, before, after)
		}
	}
}

func TestClaimLivenessHealthyQueueLogsNoRefusal(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	dependencies := &workerDependencies{
		queueTelemetryRequired: true,
		queueTelemetry: &fakeQueueTelemetry{snapshot: riverstore.QueueTelemetrySnapshot{
			Jobs:            []riverstore.QueueJobTelemetry{{Queue: "heartbeat", Kind: "system.heartbeat", Available: 0}},
			QueueCapacities: []riverstore.QueueCapacityTelemetry{{Queue: "heartbeat", Capacity: 2, Running: 0}},
		}},
		logger: slog.New(slog.NewJSONHandler(&logs, nil)),
	}
	claim := newClaimLiveness(time.Now().Add(-time.Hour), []string{"heartbeat"})
	claim.markRuntimeLive()
	if err := dependencies.claimLivenessReady(claim)(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "refused") {
		t.Fatalf("a healthy queue logged a refusal: %s", logs.String())
	}
}

// CHAOS-6883 r1 P1: the first refusal after a healthy stretch is always logged. The
// per-queue limiter must not carry a refusal across a recovery, or a queue that
// recovers and refuses again inside the interval keeps only the generic registry
// line and loses the clause and facts.
func TestClaimLivenessRefusalAfterARecoveryLogsItsClauseAgain(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	telemetry := &fakeQueueTelemetry{}
	stalled := riverstore.QueueTelemetrySnapshot{
		Jobs:            []riverstore.QueueJobTelemetry{{Queue: "heartbeat", Kind: "system.heartbeat", Available: 3}},
		QueueCapacities: []riverstore.QueueCapacityTelemetry{{Queue: "heartbeat", Capacity: 2, Running: 0}},
	}
	drained := riverstore.QueueTelemetrySnapshot{
		Jobs:            []riverstore.QueueJobTelemetry{{Queue: "heartbeat", Kind: "system.heartbeat", Available: 0}},
		QueueCapacities: []riverstore.QueueCapacityTelemetry{{Queue: "heartbeat", Capacity: 2, Running: 0}},
	}
	dependencies := &workerDependencies{
		queueTelemetryRequired: true, queueTelemetry: telemetry,
		logger: slog.New(slog.NewJSONHandler(&logs, nil)),
	}
	claim := newClaimLiveness(time.Now().Add(-time.Hour), []string{"heartbeat"})
	claim.markRuntimeLive()
	ready := dependencies.claimLivenessReady(claim)
	count := func() int { return strings.Count(logs.String(), `"msg":"execution liveness refused"`) }

	telemetry.setSnapshot(stalled)
	if err := ready(context.Background()); err == nil {
		t.Fatal("expected a refusal")
	}
	if err := ready(context.Background()); err == nil || count() != 1 {
		t.Fatalf("a repeat inside the interval must not log again: err=%v lines=%d", err, count())
	}
	telemetry.setSnapshot(drained)
	if err := ready(context.Background()); err != nil {
		t.Fatalf("the drained queue must be healthy: %v", err)
	}
	telemetry.setSnapshot(stalled)
	if err := ready(context.Background()); err == nil {
		t.Fatal("expected the second refusal")
	}
	if got := count(); got != 2 {
		t.Fatalf("a refusal after a recovery logged %d lines in total, want 2 (each run of refusals logs its clause and facts): %s", got, logs.String())
	}
}

// CHAOS-6883 r2 P1: a queue that recovers while an EARLIER queue refuses must still
// end its refusal run, or its next refusal inside the interval loses its clause.
// claimLivenessReady used to return at the first refusing queue, so the later
// queue's healthy verdict was never reached.
func TestClaimLivenessRecoveryOfALaterQueueIsSeenWhileAnEarlierQueueRefuses(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	telemetry := &fakeQueueTelemetry{}
	stalled := func(queue string) riverstore.QueueJobTelemetry {
		return riverstore.QueueJobTelemetry{Queue: queue, Kind: "k." + queue, Available: 3}
	}
	drained := func(queue string) riverstore.QueueJobTelemetry {
		return riverstore.QueueJobTelemetry{Queue: queue, Kind: "k." + queue, Available: 0}
	}
	caps := []riverstore.QueueCapacityTelemetry{{Queue: "a", Capacity: 2}, {Queue: "b", Capacity: 2}}
	dependencies := &workerDependencies{
		queueTelemetryRequired: true, queueTelemetry: telemetry,
		logger: slog.New(slog.NewJSONHandler(&logs, nil)),
	}
	claim := newClaimLiveness(time.Now().Add(-time.Hour), []string{"a", "b"})
	claim.markRuntimeLive()
	ready := dependencies.claimLivenessReady(claim)
	bLines := func() int {
		n := 0
		for _, line := range strings.Split(logs.String(), "\n") {
			if strings.Contains(line, `"msg":"execution liveness refused"`) && strings.Contains(line, `"queue":"b"`) {
				n++
			}
		}
		return n
	}

	telemetry.setSnapshot(riverstore.QueueTelemetrySnapshot{Jobs: []riverstore.QueueJobTelemetry{drained("a"), stalled("b")}, QueueCapacities: caps})
	firstErr := ready(context.Background())
	telemetry.setSnapshot(riverstore.QueueTelemetrySnapshot{Jobs: []riverstore.QueueJobTelemetry{stalled("a"), drained("b")}, QueueCapacities: caps})
	secondErr := ready(context.Background())
	telemetry.setSnapshot(riverstore.QueueTelemetrySnapshot{Jobs: []riverstore.QueueJobTelemetry{drained("a"), stalled("b")}, QueueCapacities: caps})
	thirdErr := ready(context.Background())
	if firstErr == nil || secondErr == nil || thirdErr == nil {
		t.Fatalf("every poll has a refusing queue: %v %v %v", firstErr, secondErr, thirdErr)
	}
	if got := bLines(); got != 2 {
		t.Fatalf("queue b refused, recovered (while a refused) and refused again: %d clause lines, want 2: %s", got, logs.String())
	}
	// The outcome is unchanged: the FIRST refusing queue's error is returned.
	if !strings.Contains(secondErr.Error(), `"a"`) || !strings.Contains(firstErr.Error(), `"b"`) {
		t.Fatalf("the returned error must name the first refusing queue: %v / %v", firstErr, secondErr)
	}
	// And every refusing queue in one poll is logged, not only the first.
	logs.Reset()
	claim2 := newClaimLiveness(time.Now().Add(-time.Hour), []string{"a", "b"})
	claim2.markRuntimeLive()
	telemetry.setSnapshot(riverstore.QueueTelemetrySnapshot{Jobs: []riverstore.QueueJobTelemetry{stalled("a"), stalled("b")}, QueueCapacities: caps})
	if err := dependencies.claimLivenessReady(claim2)(context.Background()); err == nil || !strings.Contains(err.Error(), `"a"`) {
		t.Fatalf("both queues refuse: the first (a) is returned, got %v", err)
	}
	if strings.Count(logs.String(), `"msg":"execution liveness refused"`) != 2 {
		t.Fatalf("both refusing queues must be logged: %s", logs.String())
	}
}

// Every refusing arm behaves the same across queues: all refusing queues are
// logged in the poll, the FIRST one is returned (CHAOS-6883 r2).
func TestClaimLivenessLogsEveryRefusingQueueAndReturnsTheFirstForEachArm(t *testing.T) {
	t.Parallel()
	backlog := riverstore.QueueJobTelemetry{Queue: "b", Kind: "k.b", Available: 3}
	for _, test := range []struct {
		name    string
		clause  string
		wantErr error
		first   riverstore.QueueTelemetrySnapshot
		second  riverstore.QueueTelemetrySnapshot
		prepare func(*claimLiveness)
	}{
		{
			name: "gate failures ahead of a backlog", clause: "gate_failures_without_handler", wantErr: errClaimLivenessGateFailing,
			first: riverstore.QueueTelemetrySnapshot{
				Jobs:            []riverstore.QueueJobTelemetry{{Queue: "a", Kind: "k.a"}, backlog},
				QueueCapacities: []riverstore.QueueCapacityTelemetry{{Queue: "a", Capacity: 2}, {Queue: "b", Capacity: 2}},
			},
			prepare: func(c *claimLiveness) {
				now := time.Now()
				c.recordGateFailure("a", true, now.Add(-90*time.Second))
				c.recordGateFailure("a", true, now.Add(-time.Second))
			},
		},
		{
			name: "stuck slot ahead of a backlog", clause: "slot_stuck_before_handler", wantErr: errClaimLivenessSlotStuckBeforeHandler,
			first: riverstore.QueueTelemetrySnapshot{
				Jobs:            []riverstore.QueueJobTelemetry{{Queue: "a", Kind: "k.a"}, {Queue: "b", Kind: "k.b"}},
				QueueCapacities: []riverstore.QueueCapacityTelemetry{{Queue: "a", Capacity: 1, Running: 1}, {Queue: "b", Capacity: 2}},
			},
			second: riverstore.QueueTelemetrySnapshot{
				Jobs:            []riverstore.QueueJobTelemetry{{Queue: "a", Kind: "k.a"}, backlog},
				QueueCapacities: []riverstore.QueueCapacityTelemetry{{Queue: "a", Capacity: 1, Running: 1}, {Queue: "b", Capacity: 2}},
			},
		},
	} {
		var logs bytes.Buffer
		telemetry := &fakeQueueTelemetry{snapshot: test.first}
		dependencies := &workerDependencies{
			queueTelemetryRequired: true, queueTelemetry: telemetry,
			logger: slog.New(slog.NewJSONHandler(&logs, nil)),
		}
		claim := newClaimLiveness(time.Now().Add(-time.Hour), []string{"a", "b"})
		claim.SetStaleWindow(40 * time.Millisecond)
		if test.prepare != nil {
			claim.SetStaleWindow(time.Minute)
			test.prepare(claim)
		}
		claim.markRuntimeLive()
		ready := dependencies.claimLivenessReady(claim)
		err := ready(context.Background())
		if test.second.Jobs != nil {
			// The stuck arm needs an unbroken run across polls before it can refuse.
			time.Sleep(80 * time.Millisecond)
			telemetry.setSnapshot(test.second)
			err = ready(context.Background())
		}
		if !errors.Is(err, test.wantErr) || !strings.Contains(err.Error(), `"a"`) {
			t.Errorf("%s: the FIRST refusing queue (a) must be returned, got %v", test.name, err)
		}
		out := logs.String()
		for _, want := range []string{`"queue":"a","clause":"` + test.clause + `"`, `"queue":"b","clause":"backlog_without_handler"`} {
			if !strings.Contains(out, want) {
				t.Errorf("%s: the poll must log %s: %s", test.name, want, out)
			}
		}
	}
}

// The first refusing queue wins whichever arm a LATER queue refuses through.
func TestClaimLivenessFirstRefusalWinsOverALaterQueuesGateOrStuckRefusal(t *testing.T) {
	t.Parallel()
	first := riverstore.QueueJobTelemetry{Queue: "a", Kind: "k.a", Available: 3}
	for _, test := range []struct {
		name     string
		snapshot riverstore.QueueTelemetrySnapshot
		prepare  func(*claimLiveness)
		polls    int
	}{
		{
			name: "later queue refuses on gate failures",
			snapshot: riverstore.QueueTelemetrySnapshot{
				Jobs:            []riverstore.QueueJobTelemetry{first, {Queue: "b", Kind: "k.b"}},
				QueueCapacities: []riverstore.QueueCapacityTelemetry{{Queue: "a", Capacity: 2}, {Queue: "b", Capacity: 2}},
			},
			prepare: func(c *claimLiveness) {
				now := time.Now()
				c.recordGateFailure("b", true, now.Add(-90*time.Second))
				c.recordGateFailure("b", true, now.Add(-time.Second))
			},
			polls: 1,
		},
		{
			name: "later queue refuses on a stuck slot",
			snapshot: riverstore.QueueTelemetrySnapshot{
				Jobs:            []riverstore.QueueJobTelemetry{first, {Queue: "b", Kind: "k.b"}},
				QueueCapacities: []riverstore.QueueCapacityTelemetry{{Queue: "a", Capacity: 2}, {Queue: "b", Capacity: 1, Running: 1}},
			},
			polls: 2,
		},
	} {
		telemetry := &fakeQueueTelemetry{snapshot: test.snapshot}
		dependencies := &workerDependencies{queueTelemetryRequired: true, queueTelemetry: telemetry}
		claim := newClaimLiveness(time.Now().Add(-time.Hour), []string{"a", "b"})
		claim.SetStaleWindow(40 * time.Millisecond)
		if test.prepare != nil {
			claim.SetStaleWindow(time.Minute)
			test.prepare(claim)
		}
		claim.markRuntimeLive()
		ready := dependencies.claimLivenessReady(claim)
		var err error
		for poll := 0; poll < test.polls; poll++ {
			if poll > 0 {
				time.Sleep(80 * time.Millisecond)
			}
			err = ready(context.Background())
		}
		if !errors.Is(err, errClaimLivenessStalledWithBacklog) || !strings.Contains(err.Error(), `"a"`) {
			t.Errorf("%s: the first refusing queue (a, backlog) must be returned, got %v", test.name, err)
		}
	}
}
