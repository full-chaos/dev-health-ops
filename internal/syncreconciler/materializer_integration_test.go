//go:build integration

package syncreconciler

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	materializerDispatchMissing = "00000000-0000-4000-8000-000000004101"
	materializerExpiredClaim    = "00000000-0000-4000-8000-000000004102"
	materializerLiveClaim       = "00000000-0000-4000-8000-000000004103"
	materializerTerminalDenial  = "00000000-0000-4000-8000-000000004104"
	materializerFinalize        = "00000000-0000-4000-8000-000000004105"
	materializerDiscovery       = "00000000-0000-4000-8000-000000004106"
	materializerPostSyncMissing = "00000000-0000-4000-8000-000000004107"
	materializerPostSyncExists  = "00000000-0000-4000-8000-000000004108"
)

func TestMaterializerPostgresConcurrencyAndRollback(t *testing.T) {
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
	if err := createMaterializerIntegrationFixture(ctx, pool); err != nil {
		t.Fatal(err)
	}
	materializer, err := NewMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("two replicas converge without changing delivery state", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 23, 20, 0, 0, 0, time.UTC)
		seedMaterializerIntegrationGraph(t, ctx, pool, now)

		start := make(chan struct{})
		results := make(chan error, 2)
		var ready sync.WaitGroup
		ready.Add(2)
		for replica := 0; replica < 2; replica++ {
			go func() {
				ready.Done()
				<-start
				_, stepErr := materializer.Step(ctx, now, now.Add(-15*time.Minute), 20)
				results <- stepErr
			}()
		}
		ready.Wait()
		close(start)
		for replica := 0; replica < 2; replica++ {
			if err := <-results; err != nil {
				t.Fatalf("replica %d Step(): %v", replica, err)
			}
		}

		assertMaterializerOutboxCount(t, ctx, pool, materializerDispatchMissing, "dispatch_sync_run", 1)
		assertMaterializerOutboxCount(t, ctx, pool, materializerExpiredClaim, "dispatch_sync_run", 1)
		assertMaterializerOutboxCount(t, ctx, pool, materializerLiveClaim, "dispatch_sync_run", 1)
		assertMaterializerOutboxCount(t, ctx, pool, materializerTerminalDenial, "dispatch_sync_run", 1)
		assertMaterializerOutboxCount(t, ctx, pool, materializerFinalize, "finalize_sync_run", 1)
		assertMaterializerOutboxCount(t, ctx, pool, materializerDiscovery, "reference_discovery", 1)
		assertMaterializerOutboxCount(t, ctx, pool, materializerPostSyncMissing, "post_sync", 1)
		assertMaterializerOutboxCount(t, ctx, pool, materializerPostSyncExists, "post_sync", 1)

		var (
			status         string
			availableAt    time.Time
			updatedAt      time.Time
			claimToken     *string
			claimExpiresAt *time.Time
			attempts       int
		)
		if err := pool.QueryRow(ctx, `
			SELECT status, available_at, updated_at, claim_token, claim_expires_at, attempts
			FROM public.sync_dispatch_outbox
			WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run'`,
			materializerExpiredClaim,
		).Scan(&status, &availableAt, &updatedAt, &claimToken, &claimExpiresAt, &attempts); err != nil {
			t.Fatal(err)
		}
		if status != "pending" || claimToken == nil || *claimToken != "expired-claim" ||
			claimExpiresAt == nil || !claimExpiresAt.Equal(now.Add(-time.Minute)) ||
			!availableAt.Equal(now.Add(-2*time.Hour)) || !updatedAt.Equal(now.Add(-2*time.Hour)) ||
			attempts != 4 {
			t.Fatalf("expired claim = status:%s claim:%v/%v attempts:%d available:%s updated:%s",
				status, claimToken, claimExpiresAt, attempts, availableAt, updatedAt)
		}

		var liveToken, liveTransport *string
		var liveExpiry *time.Time
		var liveGeneration *int64
		if err := pool.QueryRow(ctx, `
			SELECT claim_token, claim_expires_at, claim_transport, claim_route_generation
			FROM public.sync_dispatch_outbox
			WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run'`,
			materializerLiveClaim,
		).Scan(&liveToken, &liveExpiry, &liveTransport, &liveGeneration); err != nil {
			t.Fatal(err)
		}
		if liveToken == nil || *liveToken != "live-claim" || liveExpiry == nil || !liveExpiry.After(now) ||
			liveTransport == nil || *liveTransport != "celery" || liveGeneration == nil || *liveGeneration != 9 {
			t.Fatalf("live claim was not preserved: %v/%v/%v/%v", liveToken, liveExpiry, liveTransport, liveGeneration)
		}

		var denialStatus, denialError string
		var denialDispatchedAt time.Time
		if err := pool.QueryRow(ctx, `
			SELECT status, last_error, dispatched_at
			FROM public.sync_dispatch_outbox
			WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run'`,
			materializerTerminalDenial,
		).Scan(&denialStatus, &denialError, &denialDispatchedAt); err != nil {
			t.Fatal(err)
		}
		if denialStatus != "dispatched" || denialError != "feature_disabled" ||
			!denialDispatchedAt.Equal(now.Add(-2*time.Hour)) {
			t.Fatalf("terminal denial changed: %s/%s/%s", denialStatus, denialError, denialDispatchedAt)
		}

		var discoveryStatus string
		var discoveryDispatchedAt *time.Time
		var discoveryTransport *string
		var discoveryClaimToken, discoveryClaimTransport *string
		var discoveryClaimExpiry *time.Time
		var discoveryClaimGeneration *int64
		if err := pool.QueryRow(ctx, `
			SELECT status, dispatched_at, dispatched_transport,
				claim_token, claim_expires_at, claim_transport, claim_route_generation
			FROM public.sync_dispatch_outbox
			WHERE sync_run_id = $1 AND kind = 'reference_discovery'`,
			materializerDiscovery,
		).Scan(
			&discoveryStatus, &discoveryDispatchedAt, &discoveryTransport,
			&discoveryClaimToken, &discoveryClaimExpiry,
			&discoveryClaimTransport, &discoveryClaimGeneration,
		); err != nil {
			t.Fatal(err)
		}
		if discoveryStatus != "pending" || discoveryDispatchedAt != nil || discoveryTransport != nil ||
			discoveryClaimToken == nil || *discoveryClaimToken != "discovery-live" ||
			discoveryClaimExpiry == nil || !discoveryClaimExpiry.Equal(now.Add(time.Hour)) ||
			discoveryClaimTransport == nil || *discoveryClaimTransport != "celery" ||
			discoveryClaimGeneration == nil || *discoveryClaimGeneration != 3 {
			t.Fatalf("ordinary dispatched discovery was not rearmed with its live claim: %s/%v/%v/%v/%v/%v/%v",
				discoveryStatus, discoveryDispatchedAt, discoveryTransport,
				discoveryClaimToken, discoveryClaimExpiry,
				discoveryClaimTransport, discoveryClaimGeneration)
		}

		var postStatus string
		var postUpdatedAt time.Time
		if err := pool.QueryRow(ctx, `
			SELECT status, updated_at
			FROM public.sync_dispatch_outbox
			WHERE sync_run_id = $1 AND kind = 'post_sync'`,
			materializerPostSyncExists,
		).Scan(&postStatus, &postUpdatedAt); err != nil {
			t.Fatal(err)
		}
		if postStatus != "dispatched" || !postUpdatedAt.Equal(now.Add(-2*time.Hour)) {
			t.Fatalf("existing post_sync row changed: %s/%s", postStatus, postUpdatedAt)
		}
		var missingPostOrg string
		if err := pool.QueryRow(ctx, `
			SELECT org_id
			FROM public.sync_dispatch_outbox
			WHERE sync_run_id = $1 AND kind = 'post_sync'`,
			materializerPostSyncMissing,
		).Scan(&missingPostOrg); err != nil {
			t.Fatal(err)
		}
		if missingPostOrg != "org-materializer" {
			t.Fatalf("missing post_sync used non-authoritative org_id %q", missingPostOrg)
		}

		var unexpectedKinds int
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM public.sync_dispatch_outbox
			WHERE sync_run_id = $1 AND kind <> 'finalize_sync_run'`,
			materializerFinalize,
		).Scan(&unexpectedKinds); err != nil {
			t.Fatal(err)
		}
		if unexpectedKinds != 0 {
			t.Fatalf("finalizable run received %d non-finalize wakeups", unexpectedKinds)
		}
	})

	t.Run("later statement failure rolls back earlier materialization", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 23, 21, 0, 0, 0, time.UTC)
		seedRun(t, ctx, pool, materializerDispatchMissing, "running", now.Add(-2*time.Hour))
		seedUnit(t, ctx, pool, "00000000-0000-4000-8000-000000004201",
			materializerDispatchMissing, "planned", nil, now.Add(-time.Hour))
		seedRun(t, ctx, pool, materializerFinalize, "running", now.Add(-time.Hour))
		seedUnit(t, ctx, pool, "00000000-0000-4000-8000-000000004205",
			materializerFinalize, "success", nil, now.Add(-time.Minute))
		if _, err := pool.Exec(ctx, "INSERT INTO public.materializer_failures (kind) VALUES ('finalize_sync_run')"); err != nil {
			t.Fatal(err)
		}

		result, err := materializer.Step(ctx, now, now.Add(-15*time.Minute), 20)
		if !errors.Is(err, ErrUnavailable) || result != (MaterializerResult{}) {
			t.Fatalf("failed Step() = %#v, %v", result, err)
		}
		var rows int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM public.sync_dispatch_outbox").Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != 0 {
			t.Fatalf("failed transaction persisted %d earlier materializations", rows)
		}
	})

	t.Run("limit selects the deterministic first run only", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 23, 22, 0, 0, 0, time.UTC)
		for index, runID := range []string{materializerExpiredClaim, materializerDispatchMissing} {
			seedRun(t, ctx, pool, runID, "running", now.Add(-time.Duration(index+1)*time.Hour))
			seedUnit(t, ctx, pool,
				"00000000-0000-4000-8000-"+leftPadMaterializerID(4301+index),
				runID, "planned", nil, now.Add(-time.Hour))
		}

		result, err := materializer.Step(ctx, now, now.Add(-15*time.Minute), 1)
		if err != nil {
			t.Fatal(err)
		}
		if result.Dispatch != 1 {
			t.Fatalf("bounded Step() result = %#v", result)
		}
		assertMaterializerOutboxCount(t, ctx, pool, materializerDispatchMissing, "dispatch_sync_run", 1)
		assertMaterializerOutboxCount(t, ctx, pool, materializerExpiredClaim, "dispatch_sync_run", 0)
	})
}

func createMaterializerIntegrationFixture(ctx context.Context, pool *pgxpool.Pool) error {
	for _, statement := range []string{
		"CREATE EXTENSION IF NOT EXISTS pgcrypto",
		`CREATE TABLE public.sync_runs (
			id uuid PRIMARY KEY,
			org_id text NOT NULL,
			status text NOT NULL,
			created_at timestamptz NOT NULL
		)`,
		`CREATE TABLE public.sync_run_units (
			id uuid PRIMARY KEY,
			sync_run_id uuid NOT NULL REFERENCES public.sync_runs(id),
			status text NOT NULL,
			available_at timestamptz,
			updated_at timestamptz NOT NULL
		)`,
		`CREATE TABLE public.sync_run_reference_discoveries (
			id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
			sync_run_id uuid NOT NULL UNIQUE REFERENCES public.sync_runs(id),
			status text NOT NULL,
			available_at timestamptz NOT NULL,
			lease_expires_at timestamptz
		)`,
		`CREATE TABLE public.sync_run_post_dispatches (
			id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
			org_id text NOT NULL,
			sync_run_id uuid NOT NULL REFERENCES public.sync_runs(id),
			kind text NOT NULL,
			dispatched_at timestamptz NOT NULL,
			UNIQUE (sync_run_id, kind)
		)`,
		`CREATE TABLE public.sync_dispatch_outbox (
			id uuid PRIMARY KEY,
			org_id text NOT NULL,
			sync_run_id uuid NOT NULL REFERENCES public.sync_runs(id),
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
			updated_at timestamptz NOT NULL,
			UNIQUE (sync_run_id, kind)
		)`,
		"CREATE TABLE public.materializer_failures (kind text PRIMARY KEY)",
		`CREATE FUNCTION public.fail_materializer_insert() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF EXISTS (
				SELECT 1 FROM public.materializer_failures
				WHERE kind = NEW.kind
			) THEN
				RAISE EXCEPTION 'injected materializer failure for %', NEW.kind;
			END IF;
			RETURN NEW;
		END;
		$$`,
		`CREATE TRIGGER materializer_failure
		BEFORE INSERT OR UPDATE ON public.sync_dispatch_outbox
		FOR EACH ROW EXECUTE FUNCTION public.fail_materializer_insert()`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func resetMaterializerIntegrationTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, statement := range []string{
		"TRUNCATE public.materializer_failures",
		"TRUNCATE public.sync_dispatch_outbox",
		"TRUNCATE public.sync_run_post_dispatches",
		"TRUNCATE public.sync_run_reference_discoveries",
		"TRUNCATE public.sync_run_units",
		"TRUNCATE public.sync_runs CASCADE",
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}

func seedMaterializerIntegrationGraph(t *testing.T, ctx context.Context, pool *pgxpool.Pool, now time.Time) {
	t.Helper()
	for index, runID := range []string{
		materializerDispatchMissing,
		materializerExpiredClaim,
		materializerLiveClaim,
		materializerTerminalDenial,
	} {
		seedRun(t, ctx, pool, runID, "running", now.Add(-time.Duration(8-index)*time.Hour))
		seedUnit(t, ctx, pool,
			"00000000-0000-4000-8000-"+leftPadMaterializerID(4201+index),
			runID, "planned", nil, now.Add(-time.Hour))
	}
	seedRun(t, ctx, pool, materializerFinalize, "running", now.Add(-4*time.Hour))
	seedUnit(t, ctx, pool, "00000000-0000-4000-8000-000000004205",
		materializerFinalize, "success", nil, now.Add(-time.Minute))

	seedRun(t, ctx, pool, materializerDiscovery, "running", now.Add(-3*time.Hour))
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_run_reference_discoveries (
			sync_run_id, status, available_at, lease_expires_at
		) VALUES ($1, 'retrying', $2, NULL)`, materializerDiscovery, now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}

	for _, runID := range []string{materializerPostSyncMissing, materializerPostSyncExists} {
		seedRun(t, ctx, pool, runID, "success", now.Add(-2*time.Hour))
		if _, err := pool.Exec(ctx, `
			INSERT INTO public.sync_run_post_dispatches (
				org_id, sync_run_id, kind, dispatched_at
			) VALUES ($3, $1, 'post_sync', $2)`,
			runID, now.Add(-2*time.Hour),
			map[bool]string{true: "stale-ledger-org", false: "org-materializer"}[runID == materializerPostSyncMissing]); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			claim_token, claim_expires_at, claim_transport, claim_route_generation,
			created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-materializer', $1, 'dispatch_sync_run',
			'pending', $2, 4, 'expired-claim', $3, 'celery', 8, $2, $2
		)`, materializerExpiredClaim, now.Add(-2*time.Hour), now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			claim_token, claim_expires_at, claim_transport, claim_route_generation,
			created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-materializer', $1, 'dispatch_sync_run',
			'pending', $2, 5, 'live-claim', $3, 'celery', 9, $2, $2
		)`, materializerLiveClaim, now.Add(-2*time.Hour), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			last_error, dispatched_at, created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-materializer', $1, 'dispatch_sync_run',
			'dispatched', $2, 1, 'feature_disabled', $2, $2, $2
		)`, materializerTerminalDenial, now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			last_error, dispatched_at, dispatched_transport, dispatched_route_generation,
			transport_job_id, claim_token, claim_expires_at, claim_transport,
			claim_route_generation, created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-materializer', $1, 'reference_discovery',
			'dispatched', $2, 2, 'ordinary_failure', $2, 'celery', 3, 'celery-job',
			'discovery-live', $3, 'celery', 3, $2, $2
		)`, materializerDiscovery, now.Add(-2*time.Hour), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			dispatched_at, dispatched_transport, dispatched_route_generation,
			transport_job_id, created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-materializer', $1, 'post_sync',
			'dispatched', $2, 1, $2, 'celery', 1, 'post-job', $2, $2
		)`, materializerPostSyncExists, now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
}

func seedRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, status string, createdAt time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_runs (id, org_id, status, created_at)
		VALUES ($1, 'org-materializer', $2, $3)`, id, status, createdAt); err != nil {
		t.Fatal(err)
	}
}

func seedUnit(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	id, runID, status string,
	availableAt *time.Time,
	updatedAt time.Time,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_run_units (id, sync_run_id, status, available_at, updated_at)
		VALUES ($1, $2, $3, $4, $5)`,
		id, runID, status, availableAt, updatedAt); err != nil {
		t.Fatal(err)
	}
}

func assertMaterializerOutboxCount(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	runID, kind string,
	want int,
) {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM public.sync_dispatch_outbox
		WHERE sync_run_id = $1 AND kind = $2`, runID, kind).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("outbox count for %s/%s = %d, want %d", runID, kind, count, want)
	}
}

func leftPadMaterializerID(value int) string {
	return "00000000" + strconv.Itoa(value)
}
