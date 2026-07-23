//go:build integration

package syncreconciler

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	integrationDispatchID = "00000000-0000-4000-8000-000000003301"
	integrationStaleID    = "00000000-0000-4000-8000-000000003302"
)

// This test intentionally creates the production column types instead of
// importing Python metadata. It validates the dormant Go kernel's PostgreSQL
// SQL against text claim tokens, UUID outbox identifiers, timestamptz leases,
// and bigint route generations.
func TestKernelMutationPostgresTransactionFence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
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
	if err := createKernelIntegrationFixture(ctx, pool); err != nil {
		t.Fatal(err)
	}
	kernel, err := newKernel(
		riverRegistry(t, syncdispatchcontract.KindDispatchSyncRun),
		KernelModeMutation,
		&kernelStepper{},
		pool.Begin,
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("actual CTE skips locked row and commits publisher plus terminal mark", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
		seedKernelOutbox(t, ctx, pool, integrationDispatchID, now.Add(-time.Second))

		entered := make(chan struct{})
		release := make(chan struct{})
		firstDone := make(chan error, 1)
		go func() {
			_, stepErr := kernel.Step(ctx, now, 1, time.Minute,
				func(publisherCtx context.Context, tx pgx.Tx, claim TransportClaim) (string, error) {
					if claim.ID != integrationDispatchID || claim.Kind != syncdispatchcontract.KindDispatchSyncRun ||
						claim.RouteGeneration != 7 || claim.Attempts != 1 {
						return "", fmt.Errorf("unexpected claim: %#v", claim)
					}
					var generation int64
					if err := tx.QueryRow(publisherCtx, `
						SELECT claim_route_generation
						FROM public.sync_dispatch_outbox WHERE id = $1`, claim.ID).Scan(&generation); err != nil {
						return "", err
					}
					if generation != claim.RouteGeneration {
						return "", fmt.Errorf("publisher saw claim generation %d, want %d", generation, claim.RouteGeneration)
					}
					close(entered)
					select {
					case <-release:
					case <-publisherCtx.Done():
						return "", publisherCtx.Err()
					}
					if _, err := tx.Exec(publisherCtx, `
						INSERT INTO public.river_transport_jobs (outbox_id, claim_token)
						VALUES ($1, $2)`, claim.ID, claim.ClaimToken); err != nil {
						return "", err
					}
					return "river-job-3301", nil
				}, nil)
			firstDone <- stepErr
		}()

		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		second, err := kernel.Step(ctx, now, 1, time.Minute,
			func(context.Context, pgx.Tx, TransportClaim) (string, error) {
				return "", errors.New("second reconciler reached a locked row")
			}, nil)
		if err != nil || second.Claimed != 0 || second.Dispatched != 0 {
			t.Fatalf("SKIP LOCKED second Step() = %#v, %v", second, err)
		}
		close(release)
		if err := <-firstDone; err != nil {
			t.Fatal(err)
		}

		var (
			status, dispatchedTransport, transportJobID string
			attempts                                    int
			claimToken, claimTransport                  *string
			claimGeneration, dispatchedGeneration       *int64
		)
		if err := pool.QueryRow(ctx, `
			SELECT status, attempts, claim_token, claim_transport, claim_route_generation,
				dispatched_transport, dispatched_route_generation, transport_job_id
			FROM public.sync_dispatch_outbox WHERE id = $1`, integrationDispatchID).Scan(
			&status, &attempts, &claimToken, &claimTransport, &claimGeneration,
			&dispatchedTransport, &dispatchedGeneration, &transportJobID,
		); err != nil {
			t.Fatal(err)
		}
		if status != "dispatched" || attempts != 1 || claimToken != nil || claimTransport != nil ||
			claimGeneration != nil || dispatchedTransport != "river" || dispatchedGeneration == nil ||
			*dispatchedGeneration != 7 || transportJobID != "river-job-3301" {
			t.Fatalf("terminal row = status:%s attempts:%d claim:%v/%v/%v dispatched:%s/%v/%s",
				status, attempts, claimToken, claimTransport, claimGeneration,
				dispatchedTransport, dispatchedGeneration, transportJobID)
		}
		var jobs int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM public.river_transport_jobs").Scan(&jobs); err != nil {
			t.Fatal(err)
		}
		if jobs != 1 {
			t.Fatalf("same-transaction publisher jobs = %d, want 1", jobs)
		}
	})

	t.Run("actual terminal write fences stale persisted route generation", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 23, 12, 1, 0, 0, time.UTC)
		seedKernelOutbox(t, ctx, pool, integrationStaleID, now.Add(-time.Second))
		claim := TransportClaim{
			ID:              integrationStaleID,
			Kind:            syncdispatchcontract.KindDispatchSyncRun,
			ClaimToken:      "10000000-0000-4000-8000-000000003302",
			RouteGeneration: 7,
			AvailableAt:     now.Add(-time.Second),
			Attempts:        1,
		}
		if _, err := pool.Exec(ctx, `
			UPDATE public.sync_dispatch_outbox
			SET claim_token = $2, claim_expires_at = $3, claim_transport = 'river',
				claim_route_generation = 7, attempts = 1
			WHERE id = $1`, claim.ID, claim.ClaimToken, now.Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			UPDATE public.sync_dispatch_transport_routes
			SET generation = 8 WHERE kind = $1`, syncdispatchcontract.KindDispatchSyncRun); err != nil {
			t.Fatal(err)
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if err := markRiverDispatched(ctx, tx, claim, now, "stale-river-job"); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("markRiverDispatched() error = %v", err)
		}
	})

	t.Run("publisher failure rolls back both persisted claim and River insert", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 23, 12, 2, 0, 0, time.UTC)
		seedKernelOutbox(t, ctx, pool, integrationDispatchID, now.Add(-time.Second))
		publisherErr := errors.New("injected publisher failure")
		if _, err := kernel.Step(ctx, now, 1, time.Minute,
			func(publisherCtx context.Context, tx pgx.Tx, claim TransportClaim) (string, error) {
				if _, err := tx.Exec(publisherCtx, `
					INSERT INTO public.river_transport_jobs (outbox_id, claim_token)
					VALUES ($1, $2)`, claim.ID, claim.ClaimToken); err != nil {
					return "", err
				}
				return "", publisherErr
			}, nil); !errors.Is(err, publisherErr) {
			t.Fatalf("Step() error = %v", err)
		}
		var status string
		var attempts int
		var claimToken *string
		if err := pool.QueryRow(ctx, `
			SELECT status, attempts, claim_token FROM public.sync_dispatch_outbox WHERE id = $1`,
			integrationDispatchID).Scan(&status, &attempts, &claimToken); err != nil {
			t.Fatal(err)
		}
		if status != "pending" || attempts != 0 || claimToken != nil {
			t.Fatalf("rolled-back outbox = status:%s attempts:%d claim:%v", status, attempts, claimToken)
		}
		var jobs int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM public.river_transport_jobs").Scan(&jobs); err != nil {
			t.Fatal(err)
		}
		if jobs != 0 {
			t.Fatalf("rolled-back River jobs = %d, want 0", jobs)
		}
	})
}

func createKernelIntegrationFixture(ctx context.Context, pool *pgxpool.Pool) error {
	for _, statement := range []string{
		"CREATE EXTENSION IF NOT EXISTS pgcrypto",
		`CREATE TABLE public.sync_dispatch_transport_routes (
			kind text PRIMARY KEY,
			transport text NOT NULL,
			generation bigint NOT NULL,
			paused boolean NOT NULL,
			paused_at timestamptz,
			rollback_transport text NOT NULL
		)`,
		`CREATE TABLE public.sync_dispatch_outbox (
			id uuid PRIMARY KEY,
			org_id text NOT NULL,
			sync_run_id uuid NOT NULL,
			kind text NOT NULL,
			status text NOT NULL,
			available_at timestamptz NOT NULL,
			attempts integer NOT NULL,
			last_error text,
			dispatched_at timestamptz,
			claim_token text,
			claim_expires_at timestamptz,
			claim_transport text,
			claim_route_generation bigint,
			dispatched_transport text,
			dispatched_route_generation bigint,
			transport_job_id text,
			created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL
		)`,
		`CREATE TABLE public.river_transport_jobs (
			outbox_id uuid PRIMARY KEY,
			claim_token text NOT NULL
		)`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func resetKernelIntegrationTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, statement := range []string{
		"TRUNCATE public.river_transport_jobs",
		"TRUNCATE public.sync_dispatch_outbox",
		"TRUNCATE public.sync_dispatch_transport_routes",
		`INSERT INTO public.sync_dispatch_transport_routes (
			kind, transport, generation, paused, paused_at, rollback_transport
		) VALUES
			('dispatch_sync_run', 'river', 7, FALSE, NULL, 'celery'),
			('finalize_sync_run', 'celery', 1, FALSE, NULL, 'celery'),
			('post_sync', 'celery', 1, FALSE, NULL, 'celery'),
			('reference_discovery', 'celery', 1, FALSE, NULL, 'celery')`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}

func seedKernelOutbox(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string, availableAt time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts, created_at, updated_at
		) VALUES ($1, 'org-integration', '00000000-0000-4000-8000-000000003300',
			'dispatch_sync_run', 'pending', $2, 0, $2, $2)`, id, availableAt); err != nil {
		t.Fatal(err)
	}
}
