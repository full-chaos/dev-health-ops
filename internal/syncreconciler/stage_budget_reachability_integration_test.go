//go:build integration

package syncreconciler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestMutationPipelineStageBudgetReachability is CHAOS-4239's reachability
// proof. It boots the real MutationPipeline + Loop composition against a
// REAL Postgres connection -- not a synthetic <-ctx.Done() wait, as the
// package's other unit tests use -- and proves the mechanism end to end:
//
//  1. A stage that genuinely blocks past its own budget (a real pg_sleep
//     query on a real pooled connection) fails only itself: it is logged and
//     counted (sync_reconciler_stage_failures_total{stage="materializer"}),
//     while its siblings (kernel, observer) still run in the same tick.
//  2. The process survives it: nothing reaches Loop.Errors().
//  3. The very next tick, with the slow query gone, succeeds on its own --
//     the loop kept ticking without any outside intervention -- and
//     readiness reopens.
//
// The other five stages are lightweight fakes here, not the production
// role-posture-fenced implementations: those are already covered end to end
// by this package's other *_integration_test.go files (lease_repair,
// terminal_delivery_repair, materializer, kernel, unreclaimable_sweep), none
// of whose SQL or pool wiring this ticket touches. This test's job is
// narrower and specific to CHAOS-4239: prove the per-stage budget and
// stage-scoped recovery mechanism against a real, context-respecting
// database call, end to end through Loop.
func TestMutationPipelineStageBudgetReachability(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	// slow gates whether the materializer's real query blocks past its
	// budget (the first tick) or returns immediately (every tick after) --
	// proving both the degrade and the self-heal against the same real
	// connection, not two different code paths.
	var slow atomic.Bool
	slow.Store(true)

	materializer := pipelineMaterializerFunc(func(stageCtx context.Context, _ time.Time, _ time.Time, _ int) (MaterializerResult, error) {
		if !slow.Load() {
			return MaterializerResult{}, nil
		}
		// A REAL, context-respecting blocking database call. pgx issues a
		// Postgres cancel request the moment stageCtx's budget expires,
		// exactly like a real slow reconciler query would be interrupted --
		// this is not a synthetic channel wait.
		_, err := pool.Exec(stageCtx, "SELECT pg_sleep(5)")
		return MaterializerResult{}, err
	})

	var kernelRan, observerRan atomic.Int64
	ticksObserved := make(chan struct{}, 4)
	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(context.Context, time.Time, int) (LeaseRepairResult, error) {
			return LeaseRepairResult{}, nil
		}),
		pipelineTerminalDeliveryRepairFunc(func(context.Context, time.Time, int) (TerminalDeliveryRepairResult, error) {
			return TerminalDeliveryRepairResult{}, nil
		}),
		materializer,
		pipelineKernelFunc(func(context.Context, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
			kernelRan.Add(1)
			return KernelResult{}, nil
		}),
		pipelineObserverFunc(func(context.Context, time.Time, int) (Observation, error) {
			observerRan.Add(1)
			defer func() { ticksObserved <- struct{}{} }()
			return testObservation(), nil
		}),
		AtLeastOncePublisher(func(context.Context, pgx.Tx, TransportClaim) (string, error) { return "", nil }),
		PostSyncHandoff(func(context.Context, TransportClaim) error { return nil }),
		nil,
		noopTerminalOutboxClose(),
		pipelineConfigWithBudget(StageMaterializer, 200*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	clock := &testClock{now: time.Now()}
	loop, registry := newTestLoop(t, pipeline, clock)
	openReadinessGate(t, registry)

	if startErr := loop.Start(context.Background()); startErr != nil {
		t.Fatalf("Start() error = %v, want nil: a degraded stage must not fail startup (CHAOS-4239)", startErr)
	}
	select {
	case <-ticksObserved:
	case <-time.After(10 * time.Second):
		t.Fatal("the initial tick never reached the observer")
	}
	// The initial tick's materializer blocked on the real pg_sleep past its
	// 200ms budget and was recorded as a stage failure -- but materializer is
	// a CONTINUE-SAFE stage (see the classification comment on
	// MutationPipeline.Step): its siblings still ran in the same tick, the
	// observer still succeeded, and the tick as a whole is a Loop success.
	// Readiness therefore stays OPEN; the degradation is visible via the
	// stage-scoped counter/log alone, exactly the "visible via its own
	// telemetry, not via process health" split CHAOS-4239 asks for. Nothing
	// reaches Loop.Errors() either way -- the process is alive regardless.
	select {
	case degradedErr := <-loop.Errors():
		t.Fatalf("a degraded stage killed the process: %v", degradedErr)
	default:
	}
	if readiness := registry.Readiness(context.Background()); !readiness.Ready {
		t.Fatalf("readiness after a continue-safe stage's failure = %#v, want open: "+
			"the observer still ran and succeeded in the same tick", readiness)
	}
	failures, _ := pipeline.stages.snapshot()
	if failures[StageMaterializer] != 1 {
		t.Fatalf("materializer stage failure count = %d, want 1", failures[StageMaterializer])
	}
	if kernelRan.Load() != 1 || observerRan.Load() != 1 {
		t.Fatalf("materializer's own budget timeout must not block its siblings in the same tick: kernel=%d observer=%d",
			kernelRan.Load(), observerRan.Load())
	}

	// Flip the materializer healthy and prove the loop kept ticking ON ITS
	// OWN -- no restart, no outside intervention -- through a second real
	// tick against the same pooled connection the first tick's cancel just
	// interrupted.
	slow.Store(false)
	clock.mu.Lock()
	clock.now = clock.now.Add(time.Second)
	ticker := clock.ticker
	clock.mu.Unlock()
	ticker.ticks <- clock.Now()

	select {
	case <-ticksObserved:
	case <-time.After(10 * time.Second):
		t.Fatal("the recovery tick never reached the observer -- the loop stopped ticking")
	}
	if kernelRan.Load() != 2 || observerRan.Load() != 2 {
		t.Fatalf("the recovery tick did not run to completion: kernel=%d observer=%d",
			kernelRan.Load(), observerRan.Load())
	}
	failuresAfterRecovery, _ := pipeline.stages.snapshot()
	if failuresAfterRecovery[StageMaterializer] != 1 {
		t.Fatalf("materializer stage failure count after a healthy tick = %d, want still 1 (unincremented)",
			failuresAfterRecovery[StageMaterializer])
	}
	if err := loop.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
