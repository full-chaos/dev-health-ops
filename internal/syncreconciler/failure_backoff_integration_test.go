//go:build integration

package syncreconciler

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

const failureIntegrationID = "00000000-0000-4000-8000-000000003901"
const failureIntegrationRunID = "00000000-0000-4000-8000-000000003902"

func TestPublishFailureRecorderPostgresCASAndPersistence(t *testing.T) {
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
	if err := createFailureIntegrationFixture(ctx, t, pool); err != nil {
		t.Fatal(err)
	}
	recorder, err := NewPublishFailureRecorder(pool)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("two replicas exact CAS and redacted persistence", func(t *testing.T) {
		now := time.Date(2026, time.July, 23, 16, 0, 0, 0, time.UTC)
		claim := seedFailureIntegrationClaim(t, ctx, pool, now, 5)
		rawSecret := "broker rejected amqp://worker:super-secret@example.test/vhost?token=unsafe"
		start := make(chan struct{})
		results := make(chan error, 2)
		var ready sync.WaitGroup
		ready.Add(2)
		for range 2 {
			go func() {
				ready.Done()
				<-start
				results <- recorder.Record(ctx, claim, now, errors.New(rawSecret))
			}()
		}
		ready.Wait()
		close(start)
		first, second := <-results, <-results
		successes := 0
		leaseLosses := 0
		for _, result := range []error{first, second} {
			switch {
			case result == nil:
				successes++
			case errors.Is(result, ErrLeaseLost):
				leaseLosses++
			default:
				t.Fatalf("concurrent Record() error = %v", result)
			}
		}
		if successes != 1 || leaseLosses != 1 {
			t.Fatalf("concurrent results success:%d lease-lost:%d", successes, leaseLosses)
		}

		var (
			status, evidence           string
			availableAt, updatedAt     time.Time
			claimToken, claimTransport *string
			claimExpiresAt             *time.Time
			claimGeneration            *int64
		)
		if err := pool.QueryRow(ctx, `
			SELECT status, available_at, last_error, updated_at, claim_token,
				claim_expires_at, claim_transport, claim_route_generation
			FROM public.sync_dispatch_outbox WHERE id = $1`, failureIntegrationID).Scan(
			&status, &availableAt, &evidence, &updatedAt, &claimToken,
			&claimExpiresAt, &claimTransport, &claimGeneration,
		); err != nil {
			t.Fatal(err)
		}
		if status != "pending" || !availableAt.Equal(now.Add(15*time.Minute)) ||
			!updatedAt.Equal(now) || claimToken != nil || claimExpiresAt != nil ||
			claimTransport != nil || claimGeneration != nil {
			t.Fatalf("retry row status:%s available:%s updated:%s claim:%v/%v/%v/%v",
				status, availableAt, updatedAt, claimToken, claimExpiresAt, claimTransport, claimGeneration)
		}
		if evidence != transportPublishFailureEvidence || strings.Contains(evidence, "secret") ||
			strings.Contains(evidence, "token=") || len(evidence) > 64 {
			t.Fatalf("stored failure evidence = %q", evidence)
		}
	})

	t.Run("stale lease and route generation preserve committed claim", func(t *testing.T) {
		now := time.Date(2026, time.July, 23, 16, 10, 0, 0, time.UTC)
		tests := []struct {
			name   string
			mutate string
		}{
			{name: "expired lease", mutate: `
				UPDATE public.sync_dispatch_outbox SET claim_expires_at = $2
				WHERE id = $1`},
			{name: "changed route generation", mutate: `
				UPDATE public.sync_dispatch_transport_routes SET generation = 8
				WHERE kind = 'dispatch_sync_run'`},
			// The migrated schema's trigger refuses a transport change without a generation increase, so a
			// same-generation transport change is a drifted state: disable the trigger for this case only
			// (the recorder must still lose the lease on the transport mismatch alone).
			{name: "changed route transport", mutate: `
				ALTER TABLE public.sync_dispatch_transport_routes DISABLE TRIGGER trg_sync_dispatch_route_generation;
				UPDATE public.sync_dispatch_transport_routes SET transport = 'celery'
				WHERE kind = 'dispatch_sync_run'`},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				resetFailureIntegrationFixture(t, ctx, pool)
				claim := seedFailureIntegrationClaim(t, ctx, pool, now, 2)
				var mutationErr error
				if test.name == "expired lease" {
					_, mutationErr = pool.Exec(ctx, test.mutate, claim.ID, now)
				} else {
					_, mutationErr = pool.Exec(ctx, test.mutate)
				}
				if mutationErr != nil {
					t.Fatal(mutationErr)
				}
				if err := recorder.Record(ctx, claim, now, errors.New("publish failed")); !errors.Is(err, ErrLeaseLost) {
					t.Fatalf("Record() error = %v, want ErrLeaseLost", err)
				}
				var token, evidence string
				var availableAt time.Time
				if err := pool.QueryRow(ctx, `
					SELECT claim_token, available_at, COALESCE(last_error, '')
					FROM public.sync_dispatch_outbox WHERE id = $1`, claim.ID).Scan(
					&token, &availableAt, &evidence,
				); err != nil {
					t.Fatal(err)
				}
				if token != claim.ClaimToken || !availableAt.Equal(claim.AvailableAt) || evidence != "" {
					t.Fatalf("stale row mutated token:%s available:%s evidence:%q", token, availableAt, evidence)
				}
			})
		}
	})
}

func createFailureIntegrationFixture(ctx context.Context, t *testing.T, pool *pgxpool.Pool) error {
	t.Helper()
	pgschema.Apply(ctx, t, pool)
	return nil
}

func resetFailureIntegrationFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, statement := range []string{
		"TRUNCATE public.sync_dispatch_outbox",
		"TRUNCATE public.sync_dispatch_transport_routes",
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}

func seedFailureIntegrationClaim(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	now time.Time,
	attempts int64,
) TransportClaim {
	t.Helper()
	resetFailureIntegrationFixture(t, ctx, pool)
	claim := TransportClaim{
		ID:              failureIntegrationID,
		Kind:            "dispatch_sync_run",
		ClaimToken:      "10000000-0000-4000-8000-000000003901",
		RouteGeneration: 7,
		AvailableAt:     now.Add(-time.Minute),
		Attempts:        attempts,
	}
	// The migrated schema seeds every route as celery at generation 2 (the reset truncates them) and
	// the outbox row points at a real run.
	pgseed.SyncTransportRoute(ctx, t, pool, claim.Kind, "river", claim.RouteGeneration, false, "celery")
	pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: failureIntegrationRunID, OrgID: "org-failure"})
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts, claim_token,
			claim_expires_at, claim_transport, claim_route_generation, created_at, updated_at
		) VALUES ($3, 'org-failure', $8::uuid, $1, 'pending', $4, $5, $6, $7, 'river', $2, $4, $4)`,
		claim.Kind, claim.RouteGeneration, claim.ID, claim.AvailableAt, claim.Attempts,
		claim.ClaimToken, now.Add(time.Minute), failureIntegrationRunID); err != nil {
		t.Fatal(err)
	}
	return claim
}
