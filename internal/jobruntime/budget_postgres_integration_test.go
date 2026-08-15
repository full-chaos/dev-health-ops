//go:build integration

package jobruntime

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresConcurrencyBudgetIsFleetWideAndRecoversExpiredLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	_, err = pool.Exec(ctx, `
		CREATE TABLE public.worker_concurrency_leases (
			id uuid PRIMARY KEY,
			budget_key varchar(320) NOT NULL,
			job_kind varchar(96) NOT NULL,
			concurrency_scope varchar(16) NOT NULL,
			organization_id uuid NULL,
			owner_token uuid NOT NULL UNIQUE,
			lease_expires_at timestamptz NOT NULL,
			created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL
		)`)
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewPostgresConcurrencyBudget(pool, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewPostgresConcurrencyBudget(pool, nil)
	if err != nil {
		t.Fatal(err)
	}
	first.leaseDuration = time.Minute
	second.leaseDuration = time.Minute
	request := BudgetRequest{Kind: "system.heartbeat", ConcurrencyScope: "fleet", ConcurrencyLimit: 1}
	lease, err := first.Acquire(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	blockedCtx, blockedCancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer blockedCancel()
	if _, err := second.Acquire(blockedCtx, request); err == nil {
		t.Fatal("second independent store acquired a fleet lease while capacity was held")
	}
	lease.Release()
	released, err := second.Acquire(ctx, request)
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	released.Release()

	crashed, err := first.Acquire(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	_ = crashed
	if _, err := pool.Exec(ctx, `UPDATE public.worker_concurrency_leases SET lease_expires_at = statement_timestamp() - interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	recovered, err := second.Acquire(ctx, request)
	if err != nil {
		t.Fatalf("acquire after expiry: %v", err)
	}
	recovered.Release()
	var remaining int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.worker_concurrency_leases`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("remaining lease rows = %d, want 0", remaining)
	}
}

func TestPostgresConcurrencyBudgetAcquireIsAtomicAcrossConcurrentStores(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `
		CREATE TABLE public.worker_concurrency_leases (
			id uuid PRIMARY KEY, budget_key varchar(320) NOT NULL,
			job_kind varchar(96) NOT NULL, concurrency_scope varchar(16) NOT NULL,
			organization_id uuid NULL, owner_token uuid NOT NULL UNIQUE,
			lease_expires_at timestamptz NOT NULL, created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	first, _ := NewPostgresConcurrencyBudget(pool, nil)
	second, _ := NewPostgresConcurrencyBudget(pool, nil)
	request := BudgetRequest{Kind: "system.heartbeat", ConcurrencyScope: "fleet", ConcurrencyLimit: 1}
	start := make(chan struct{})
	results := make(chan BudgetLease, 2)
	errors := make(chan error, 2)
	for _, store := range []*PostgresConcurrencyBudget{first, second} {
		go func(store *PostgresConcurrencyBudget) {
			<-start
			acquireCtx, acquireCancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer acquireCancel()
			lease, acquireErr := store.Acquire(acquireCtx, request)
			results <- lease
			errors <- acquireErr
		}(store)
	}
	close(start)
	var leases []BudgetLease
	for range 2 {
		lease := <-results
		acquireErr := <-errors
		if acquireErr == nil && lease != nil {
			leases = append(leases, lease)
		}
	}
	if len(leases) != 1 {
		t.Fatalf("concurrent limit-one acquire produced %d leases", len(leases))
	}
	leases[0].Release()
}

func TestPostgresConcurrencyBudgetReleaseRequiresOwnerToken(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `
		CREATE TABLE public.worker_concurrency_leases (
			id uuid PRIMARY KEY, budget_key varchar(320) NOT NULL,
			job_kind varchar(96) NOT NULL, concurrency_scope varchar(16) NOT NULL,
			organization_id uuid NULL, owner_token uuid NOT NULL UNIQUE,
			lease_expires_at timestamptz NOT NULL, created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	store, _ := NewPostgresConcurrencyBudget(pool, nil)
	lease, err := store.Acquire(ctx, BudgetRequest{Kind: "system.heartbeat", ConcurrencyScope: "fleet", ConcurrencyLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	owned := lease.(*postgresConcurrencyLease)
	wrongOwner := &postgresConcurrencyLease{pool: pool, id: owned.id, token: uuid.New()}
	wrongOwner.Release()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.worker_concurrency_leases`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("wrong-token release removed %d rows, want one retained lease", 1-count)
	}
	lease.Release()
}
