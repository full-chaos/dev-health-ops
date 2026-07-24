//go:build integration

package pagerduty

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The receipt row, not the Redis entry ID, is what makes redelivery safe: a
// reclaimed stream entry must resolve to the same receipt and must not run the
// reconciliation twice while a live lease exists.
func TestPostgresReceiptStoreFencesDuplicateAndCrashedDeliveries(t *testing.T) {
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
	createReceiptTable(t, ctx, pool)

	store, err := NewPostgresReceiptStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 23, 18, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	const receiptID = "pagerduty:binding-1:evt-1"

	first, err := store.Begin(ctx, receiptID)
	if err != nil || !first.Proceed() || first.Token == "" {
		t.Fatalf("first claim = %#v, %v", first, err)
	}
	// A live lease is IN-FLIGHT, not completed. Collapsing the two lets a
	// reclaim mistake a peer's unfinished work for durable success and ACK the
	// entry away with no canonical effect (codex HIGH-2).
	duplicate, err := store.Begin(ctx, receiptID)
	if err != nil || duplicate.State != ReceiptInFlight {
		t.Fatalf("live-lease duplicate = %#v, %v", duplicate, err)
	}

	// A process death leaves the claim without a completion write. Only the
	// expired lease may hand the receipt to a later stream delivery, and the
	// crashed claimant must no longer be able to complete it.
	now = now.Add(store.lease + time.Second)
	reclaimed, err := store.Begin(ctx, receiptID)
	if err != nil || !reclaimed.Proceed() || reclaimed.Token == first.Token {
		t.Fatalf("reclaimed = %#v, %v", reclaimed, err)
	}
	if err := store.Complete(ctx, first); err != errUnavailable {
		t.Fatalf("stale completion = %v", err)
	}
	if err := store.Complete(ctx, reclaimed); err != nil {
		t.Fatalf("current completion = %v", err)
	}
	completed, err := store.Begin(ctx, receiptID)
	if err != nil || completed.State != ReceiptCompleted {
		t.Fatalf("completed receipt = %#v, %v", completed, err)
	}

	var kind, status string
	var attempts int
	if err := pool.QueryRow(ctx, `
SELECT job_kind, status, attempt_count FROM public.worker_job_runs WHERE idempotency_key=$1`,
		receiptID).Scan(&kind, &status, &attempts); err != nil {
		t.Fatal(err)
	}
	if kind != receiptKind || status != "succeeded" || attempts != 2 {
		t.Fatalf("receipt row kind=%s status=%s attempts=%d", kind, status, attempts)
	}
}

// createReceiptTable mirrors migration 0052 including every CHECK constraint.
//
// The constraints are the point, not decoration. The previous fixture created a
// bare table, so a Release that wrote an illegal status passed here and would
// have failed on every production call. A storage fixture that is more
// permissive than production cannot prove anything about production.
func createReceiptTable(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		CREATE TABLE public.worker_job_runs (
			id uuid PRIMARY KEY,
			job_kind varchar(96) NOT NULL,
			idempotency_key varchar(256) NOT NULL,
			org_id uuid NULL,
			domain_type varchar(64) NOT NULL,
			domain_id uuid NOT NULL,
			status varchar(16) NOT NULL,
			claim_token uuid NULL,
			lease_expires_at timestamptz NULL,
			attempt_count integer NOT NULL,
			started_at timestamptz NOT NULL,
			finished_at timestamptz NULL,
			result varchar(16) NULL,
			error_category varchar(32) NULL,
			created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL,
			CONSTRAINT uq_worker_job_run_key UNIQUE (job_kind, idempotency_key),
			CONSTRAINT ck_worker_job_run_status
				CHECK (status IN ('running', 'retryable', 'succeeded', 'terminal')),
			CONSTRAINT ck_worker_job_run_attempt_count CHECK (attempt_count >= 1),
			CONSTRAINT ck_worker_job_run_claim_state CHECK (
				(status = 'running' AND claim_token IS NOT NULL
					AND lease_expires_at IS NOT NULL AND finished_at IS NULL)
				OR (status <> 'running' AND claim_token IS NULL
					AND lease_expires_at IS NULL AND finished_at IS NOT NULL)
			),
			CONSTRAINT ck_worker_job_run_result_state CHECK (
				(result IS NULL AND error_category IS NULL)
				OR (result IS NOT NULL AND error_category IS NOT NULL)
			)
		)`)
	if err != nil {
		t.Fatal(err)
	}
}

// TestReceiptFixtureEnforcesProductionConstraints proves the fixture is not
// more permissive than production. Without it, a future edit could quietly drop
// a constraint and restore the false pass this test exists to prevent.
func TestReceiptFixtureEnforcesProductionConstraints(t *testing.T) {
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
	createReceiptTable(t, ctx, pool)

	for _, test := range []struct {
		name   string
		status string
		insert string
	}{
		{
			name:   "status outside the allowed set",
			status: "failed",
			insert: `INSERT INTO public.worker_job_runs (id, job_kind, idempotency_key, domain_type, domain_id, status, attempt_count, started_at, finished_at, created_at, updated_at) VALUES (gen_random_uuid(), 'k', 'a', 'webhook_delivery', gen_random_uuid(), 'failed', 1, now(), now(), now(), now())`,
		},
		{
			name:   "running without a lease",
			status: "running",
			insert: `INSERT INTO public.worker_job_runs (id, job_kind, idempotency_key, domain_type, domain_id, status, attempt_count, started_at, created_at, updated_at) VALUES (gen_random_uuid(), 'k', 'b', 'webhook_delivery', gen_random_uuid(), 'running', 1, now(), now(), now())`,
		},
		{
			name:   "result without an error category",
			status: "succeeded",
			insert: `INSERT INTO public.worker_job_runs (id, job_kind, idempotency_key, domain_type, domain_id, status, attempt_count, started_at, finished_at, result, created_at, updated_at) VALUES (gen_random_uuid(), 'k', 'c', 'webhook_delivery', gen_random_uuid(), 'succeeded', 1, now(), now(), 'success', now(), now())`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := pool.Exec(ctx, test.insert); err == nil {
				t.Fatalf("fixture accepted an insert production rejects (status %q)", test.status)
			}
		})
	}
}

// TestPostgresReceiptStoreReleasesForImmediateRetry covers the other half of
// codex HIGH-2: a failed reconciliation must hand the receipt back so the next
// delivery can retry, rather than leaving an abandoned lease that reads as
// another consumer's in-flight work until it expires.
func TestPostgresReceiptStoreReleasesForImmediateRetry(t *testing.T) {
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
	createReceiptTable(t, ctx, pool)

	store, err := NewPostgresReceiptStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 23, 18, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	const receiptID = "pagerduty:binding-2:evt-2"

	claim, err := store.Begin(ctx, receiptID)
	if err != nil || !claim.Proceed() {
		t.Fatalf("claim = %#v, %v", claim, err)
	}
	if err := store.Release(ctx, claim); err != nil {
		t.Fatalf("release = %v", err)
	}
	// No clock movement: the retry must be available immediately, not after
	// the lease expires.
	retry, err := store.Begin(ctx, receiptID)
	if err != nil || !retry.Proceed() || retry.Token == claim.Token {
		t.Fatalf("retry after release = %#v, %v", retry, err)
	}
	// A released claim is spent. The previous holder must not be able to
	// complete or release the receipt the new holder now owns.
	if err := store.Complete(ctx, claim); err == nil {
		t.Fatal("released claim completed the new holder's receipt")
	}
	if err := store.Release(ctx, claim); err != nil {
		t.Fatalf("releasing an already-released claim must be benign: %v", err)
	}
	stillHeld, err := store.Begin(ctx, receiptID)
	if err != nil || stillHeld.State != ReceiptInFlight {
		t.Fatalf("stale release stole the live claim: %#v, %v", stillHeld, err)
	}
	if err := store.Complete(ctx, retry); err != nil {
		t.Fatalf("current holder completion = %v", err)
	}
}
