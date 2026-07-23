//go:build integration

package syncroute

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

type integrationRegistry map[string]syncdispatchcontract.Descriptor

func (registry integrationRegistry) Lookup(kind string) (syncdispatchcontract.Descriptor, bool) {
	value, ok := registry[kind]
	return value, ok
}

type integrationQuiescer struct {
	err   error
	calls int
}

func (quiescer *integrationQuiescer) Quiesce(context.Context, QuiescenceRequest) error {
	quiescer.calls++
	return quiescer.err
}

func TestRouteControlPostgresConcurrencyDrainAndRollback(t *testing.T) {
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
			t.Errorf("close PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createRouteControlSchema(t, ctx, pool)

	quiescer := &integrationQuiescer{}
	registry := integrationRegistry{
		syncdispatchcontract.KindDispatchSyncRun: {
			Kind: syncdispatchcontract.KindDispatchSyncRun, Delivery: syncdispatchcontract.DeliveryAtLeastOnce,
			Route: syncdispatchcontract.RouteCelery, RollbackRoute: syncdispatchcontract.RouteCelery,
		},
		syncdispatchcontract.KindPostSync: {
			Kind: syncdispatchcontract.KindPostSync, Delivery: syncdispatchcontract.DeliveryAtMostOnceMarkBefore,
			Route: syncdispatchcontract.RouteCelery, RollbackRoute: syncdispatchcontract.RouteCelery,
		},
		syncdispatchcontract.KindReferenceDiscovery: {
			Kind: syncdispatchcontract.KindReferenceDiscovery, Delivery: syncdispatchcontract.DeliveryAtLeastOnce,
			Route: syncdispatchcontract.RouteCelery, RollbackRoute: syncdispatchcontract.RouteCelery,
		},
	}
	capabilities, err := NewCapabilities([]Capability{
		{Kind: syncdispatchcontract.KindDispatchSyncRun, Transport: syncdispatchcontract.RouteRiver},
		{Kind: syncdispatchcontract.KindPostSync, Transport: syncdispatchcontract.RouteRiver, Quiescer: quiescer},
	})
	if err != nil {
		t.Fatal(err)
	}
	controller, err := NewController(pool, registry, capabilities)
	if err != nil {
		t.Fatal(err)
	}

	celeryTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var celeryOutboxID string
	if err := celeryTx.QueryRow(ctx, `
SELECT outbox.id::text
FROM public.sync_dispatch_transport_routes AS route
JOIN public.sync_dispatch_outbox AS outbox ON outbox.kind = route.kind
WHERE route.kind = 'reference_discovery'
FOR UPDATE OF route, outbox`).Scan(&celeryOutboxID); err != nil {
		t.Fatal(err)
	}
	celeryPauseResult := make(chan error, 1)
	go func() {
		_, pauseErr := controller.Pause(ctx, syncdispatchcontract.KindReferenceDiscovery)
		celeryPauseResult <- pauseErr
	}()
	waitForRouteRowLockWait(t, ctx, pool)
	if _, err := celeryTx.Exec(ctx, `
UPDATE public.sync_dispatch_outbox
SET status = 'dispatched'
WHERE id = $1`, celeryOutboxID); err != nil {
		t.Fatalf("Celery terminal update deadlocked with route controller: %v", err)
	}
	if err := celeryTx.Commit(ctx); err != nil {
		t.Fatalf("Celery terminal commit deadlocked with route controller: %v", err)
	}
	select {
	case err := <-celeryPauseResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("route pause did not continue after Celery terminal commit")
	}

	blockingTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := blockingTx.Exec(ctx, `
UPDATE public.sync_dispatch_outbox SET status = 'dispatched'
WHERE id = '00000000-0000-4000-8000-000000000401'`); err != nil {
		t.Fatal(err)
	}
	pauseResult := make(chan error, 1)
	go func() {
		_, pauseErr := controller.Pause(ctx, syncdispatchcontract.KindDispatchSyncRun)
		pauseResult <- pauseErr
	}()
	waitForRouteLockWait(t, ctx, pool)
	if err := blockingTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-pauseResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pause did not continue after terminal rollback")
	}
	dispatchDescriptor := registry[syncdispatchcontract.KindDispatchSyncRun]
	dispatchDescriptor.Route = syncdispatchcontract.RouteRiver
	registry[syncdispatchcontract.KindDispatchSyncRun] = dispatchDescriptor

	if _, err := pool.Exec(ctx, `
UPDATE public.sync_dispatch_outbox
SET status = 'pending', claim_token = 'claim-1', claim_expires_at = NOW() + interval '1 minute',
    claim_transport = 'celery', claim_route_generation = 1
WHERE id = '00000000-0000-4000-8000-000000000401'`); err != nil {
		t.Fatal(err)
	}
	state, err := controller.Drain(ctx, syncdispatchcontract.KindDispatchSyncRun)
	if err != nil || state.LiveClaims != 1 {
		t.Fatalf("live drain state=%+v err=%v", state, err)
	}
	if _, err := controller.Resume(ctx, syncdispatchcontract.KindDispatchSyncRun, syncdispatchcontract.RouteRiver, time.Second); !errors.Is(err, ErrLiveClaims) {
		t.Fatalf("resume with live claim error=%v", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_dispatch_outbox SET claim_expires_at = NOW() - interval '1 second'
WHERE id = '00000000-0000-4000-8000-000000000401'`); err != nil {
		t.Fatal(err)
	}
	state, err = controller.Resume(ctx, syncdispatchcontract.KindDispatchSyncRun, syncdispatchcontract.RouteRiver, time.Second)
	if err != nil || state.Transport != syncdispatchcontract.RouteRiver || state.Generation != 3 || state.Paused {
		t.Fatalf("resumed state=%+v err=%v", state, err)
	}

	if _, err := controller.Pause(ctx, syncdispatchcontract.KindPostSync); err != nil {
		t.Fatal(err)
	}
	postSyncDescriptor := registry[syncdispatchcontract.KindPostSync]
	postSyncDescriptor.Route = syncdispatchcontract.RouteRiver
	registry[syncdispatchcontract.KindPostSync] = postSyncDescriptor
	quiescer.err = errors.New("old publisher still active")
	if _, err := controller.Resume(ctx, syncdispatchcontract.KindPostSync, syncdispatchcontract.RouteRiver, time.Second); !errors.Is(err, ErrLiveClaims) {
		t.Fatalf("post_sync quiescence error=%v", err)
	}
	state, err = controller.Inspect(ctx, syncdispatchcontract.KindPostSync)
	if err != nil || !state.Paused || state.Generation != 2 || quiescer.calls != 1 {
		t.Fatalf("post_sync rollback state=%+v calls=%d err=%v", state, quiescer.calls, err)
	}
}

func waitForRouteRowLockWait(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		if err := pool.QueryRow(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM pg_stat_activity
	WHERE wait_event_type = 'Lock'
	  AND query LIKE '%FROM public.sync_dispatch_transport_routes%'
	  AND query LIKE '%FOR UPDATE%'
)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("route controller never waited on the in-flight Celery route-row lock")
}

func waitForRouteLockWait(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		if err := pool.QueryRow(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM pg_stat_activity
	WHERE wait_event_type = 'Lock'
	  AND query = 'LOCK TABLE public.sync_dispatch_outbox IN SHARE ROW EXCLUSIVE MODE'
)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("route pause never waited on the uncommitted outbox terminal transaction")
}

func createRouteControlSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE public.sync_dispatch_transport_routes (
			kind text PRIMARY KEY, transport text NOT NULL, generation bigint NOT NULL,
			paused boolean NOT NULL, paused_at timestamptz, rollback_transport text NOT NULL,
			updated_at timestamptz NOT NULL
		)`,
		`CREATE TABLE public.sync_dispatch_outbox (
			id uuid PRIMARY KEY, kind text NOT NULL, status text NOT NULL,
			claim_token text, claim_expires_at timestamptz, claim_transport text,
			claim_route_generation bigint
		)`,
		`INSERT INTO public.sync_dispatch_transport_routes
			(kind, transport, generation, paused, paused_at, rollback_transport, updated_at)
		VALUES
			('dispatch_sync_run', 'celery', 1, FALSE, NULL, 'celery', NOW()),
			('post_sync', 'celery', 1, FALSE, NULL, 'celery', NOW()),
			('reference_discovery', 'celery', 1, FALSE, NULL, 'celery', NOW())`,
		`INSERT INTO public.sync_dispatch_outbox
			(id, kind, status)
		VALUES
			('00000000-0000-4000-8000-000000000401', 'dispatch_sync_run', 'pending'),
			('00000000-0000-4000-8000-000000000402', 'reference_discovery', 'pending')`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}
