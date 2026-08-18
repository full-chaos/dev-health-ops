//go:build integration

package jobruntime

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresIdempotencyPreservesDuplicateAndCrashRecoverySemantics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	}()

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createIdempotencyTable(t, ctx, pool)

	store, err := NewPostgresIdempotency(pool)
	if err != nil {
		t.Fatal(err)
	}
	store.leaseDuration = time.Minute
	request := idempotencyRequest("retention:worker_job_terminal:2026-07-14")

	first, err := store.Begin(ctx, request)
	if err != nil || first.State() != ClaimProceed {
		t.Fatalf("first Begin = %v, %v", first, err)
	}
	duplicate, err := store.Begin(ctx, request)
	if err != nil || duplicate.State() != ClaimAlreadyComplete {
		t.Fatalf("duplicate Begin = %v, %v", duplicate, err)
	}

	// A process can die after claiming but before completion. The later River
	// attempt may reclaim only after the persisted lease, never concurrently.
	if _, err := pool.Exec(ctx, "UPDATE public.worker_job_runs SET lease_expires_at = statement_timestamp() - interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := store.Begin(ctx, request)
	if err != nil || reclaimed.State() != ClaimProceed {
		t.Fatalf("reclaimed Begin = %v, %v", reclaimed, err)
	}
	if err := reclaimed.Finish(ctx, Completion{Result: ResultSuccess, Category: CategoryNone}); err != nil {
		t.Fatalf("finish reclaimed claim: %v", err)
	}
	completed, err := store.Begin(ctx, request)
	if err != nil || completed.State() != ClaimAlreadyComplete {
		t.Fatalf("completed Begin = %v, %v", completed, err)
	}

	retryRequest := idempotencyRequest("retention:worker_job_terminal:2026-07-15")
	retrying, err := store.Begin(ctx, retryRequest)
	if err != nil || retrying.State() != ClaimProceed {
		t.Fatalf("retrying Begin = %v, %v", retrying, err)
	}
	if err := retrying.Finish(ctx, Completion{Result: ResultRetry, Category: CategoryRetryable}); err != nil {
		t.Fatalf("finish retryable claim: %v", err)
	}
	nextAttempt, err := store.Begin(ctx, retryRequest)
	if err != nil || nextAttempt.State() != ClaimProceed {
		t.Fatalf("next retry Begin = %v, %v", nextAttempt, err)
	}
	// A drain or budget-lease loss also arrives as ResultCancel but is not
	// terminal: the run must stay reclaimable so River's retry can execute it
	// (CHAOS-3865). classify() sets Terminal from the same cancel flag.
	if err := nextAttempt.Finish(ctx, Completion{Result: ResultCancel, Category: CategoryCancelled}); err != nil {
		t.Fatalf("finish drained claim: %v", err)
	}
	afterDrain, err := store.Begin(ctx, retryRequest)
	if err != nil || afterDrain.State() != ClaimProceed {
		t.Fatalf("post-drain Begin = %v, %v", afterDrain, err)
	}
	// An explicit domain-terminal outcome still fences every later claim.
	if err := afterDrain.Finish(ctx, Completion{
		Result: ResultCancel, Category: CategoryPermanent, Terminal: true,
	}); err != nil {
		t.Fatalf("finish terminal claim: %v", err)
	}
	terminal, err := store.Begin(ctx, retryRequest)
	if err != nil || terminal.State() != ClaimTerminal {
		t.Fatalf("terminal Begin = %v, %v", terminal, err)
	}
}

func createIdempotencyTable(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		CREATE TABLE public.worker_job_runs (
			id uuid PRIMARY KEY,
			job_kind text NOT NULL,
			idempotency_key text NOT NULL,
			org_id uuid NULL,
			domain_type text NOT NULL,
			domain_id uuid NOT NULL,
			status text NOT NULL,
			claim_token uuid NULL,
			lease_expires_at timestamptz NULL,
			attempt_count integer NOT NULL,
			started_at timestamptz NOT NULL,
			finished_at timestamptz NULL,
			result text NULL,
			error_category text NULL,
			created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL,
			UNIQUE (job_kind, idempotency_key)
		)`)
	if err != nil {
		t.Fatal(err)
	}
}

func idempotencyRequest(key string) ClaimRequest {
	return ClaimRequest{
		Kind:           jobcontract.KindRetentionCleanup,
		IdempotencyKey: key,
		Domain: jobcontract.DomainLink{
			Type: "maintenance_run",
			ID:   "00000000-0000-4000-8000-000000000002",
		},
		Policy:  "maintenance_run_checkpoint",
		JobID:   42,
		Attempt: 1,
	}
}

// TestPostgresIdempotencyRenewsLeaseWhileTheRunIsHealthy is CHAOS-3866
// evidence. The lease (10 minutes in production) is shorter than registered
// timeouts (up to 2 hours), and Begin's running-with-expired-lease branch
// hands a duplicate a concurrent claim on the same run -- the double execution
// this store exists to fence. Renewal must keep a healthy run's lease ahead of
// the clock for as long as it executes.
func TestPostgresIdempotencyRenewsLeaseWhileTheRunIsHealthy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createIdempotencyTable(t, ctx, pool)

	store, err := NewPostgresIdempotency(pool)
	if err != nil {
		t.Fatal(err)
	}
	// Renewal ticks every leaseDuration/3, so the run outlives its original
	// lease several times over during this test.
	store.leaseDuration = 1500 * time.Millisecond
	request := idempotencyRequest("retention:worker_job_long_run:2026-07-16")

	claim, err := store.Begin(ctx, request)
	if err != nil || claim.State() != ClaimProceed {
		t.Fatalf("Begin = %v, %v", claim, err)
	}

	// Well past the original lease: without renewal the row would now be
	// claimable by a duplicate running concurrently with this one.
	time.Sleep(3 * store.leaseDuration)

	var leaseAhead bool
	if err := pool.QueryRow(ctx,
		`SELECT lease_expires_at > statement_timestamp() FROM public.worker_job_runs`,
	).Scan(&leaseAhead); err != nil {
		t.Fatal(err)
	}
	if !leaseAhead {
		t.Fatal("lease was not renewed while the run was still executing")
	}
	duplicate, err := store.Begin(ctx, request)
	if err != nil || duplicate.State() != ClaimAlreadyComplete {
		t.Fatalf("duplicate claimed a healthy long-running job: %v, %v", duplicate, err)
	}
	if err := claim.Finish(ctx, Completion{Result: ResultSuccess, Category: CategoryNone}); err != nil {
		t.Fatalf("finish renewed claim: %v", err)
	}
}
