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
	if err != nil || !first.Proceed || first.Token == "" {
		t.Fatalf("first claim = %#v, %v", first, err)
	}
	duplicate, err := store.Begin(ctx, receiptID)
	if err != nil || duplicate.Proceed {
		t.Fatalf("live-lease duplicate = %#v, %v", duplicate, err)
	}

	// A process death leaves the claim without a completion write. Only the
	// expired lease may hand the receipt to a later stream delivery, and the
	// crashed claimant must no longer be able to complete it.
	now = now.Add(store.lease + time.Second)
	reclaimed, err := store.Begin(ctx, receiptID)
	if err != nil || !reclaimed.Proceed || reclaimed.Token == first.Token {
		t.Fatalf("reclaimed = %#v, %v", reclaimed, err)
	}
	if err := store.Complete(ctx, first); err != errUnavailable {
		t.Fatalf("stale completion = %v", err)
	}
	if err := store.Complete(ctx, reclaimed); err != nil {
		t.Fatalf("current completion = %v", err)
	}
	completed, err := store.Begin(ctx, receiptID)
	if err != nil || completed.Proceed {
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

func createReceiptTable(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
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
