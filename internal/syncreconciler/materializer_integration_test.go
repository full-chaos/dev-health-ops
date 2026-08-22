//go:build integration

package syncreconciler

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	materializerDispatchMissing  = "00000000-0000-4000-8000-000000004101"
	materializerExpiredClaim     = "00000000-0000-4000-8000-000000004102"
	materializerLiveClaim        = "00000000-0000-4000-8000-000000004103"
	materializerTerminalDenial   = "00000000-0000-4000-8000-000000004104"
	materializerFinalize         = "00000000-0000-4000-8000-000000004105"
	materializerDiscovery        = "00000000-0000-4000-8000-000000004106"
	materializerPostSyncMissing  = "00000000-0000-4000-8000-000000004107"
	materializerPostSyncExists   = "00000000-0000-4000-8000-000000004108"
	materializerRiverQueued      = "00000000-0000-4000-8000-000000004109"
	materializerStaleDispatch    = "00000000-0000-4000-8000-00000000410a"
	materializerFreshDispatch    = "00000000-0000-4000-8000-00000000410b"
	materializerRetryingDispatch = "00000000-0000-4000-8000-00000000410c"
	materializerFeatureDisabled  = "00000000-0000-4000-8000-00000000410d"
)

func TestMaterializerRedispatchesStaleUnitsExactlyOnce(t *testing.T) {
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

	t.Run("stale dispatching River row re-arms once", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 24, 1, 0, 0, 0, time.UTC)
		cutoff := now.Add(-15 * time.Minute)
		seedRun(t, ctx, pool, materializerStaleDispatch, "running", now.Add(-2*time.Hour))
		seedUnit(t, ctx, pool, "00000000-0000-4000-8000-000000004301",
			materializerStaleDispatch, "dispatching", nil, now.Add(-time.Hour))
		seedMaterializerDispatchedOutbox(t, ctx, pool, materializerStaleDispatch, "river-stale-job", now.Add(-2*time.Hour))

		result, err := materializer.Step(ctx, now, cutoff, 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Dispatch != 1 {
			t.Fatalf("stale dispatching graph did not rearm exactly once: %#v", result)
		}
		assertMaterializerDispatchState(t, ctx, pool, materializerStaleDispatch, "pending", nil, 1)

		result, err = materializer.Step(ctx, now, cutoff, 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Dispatch != 0 {
			t.Fatalf("stale dispatching graph amplified on second pass: %#v", result)
		}
		assertMaterializerDispatchState(t, ctx, pool, materializerStaleDispatch, "pending", nil, 1)
	})

	t.Run("fresh dispatching River row stays protected", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 24, 2, 0, 0, 0, time.UTC)
		cutoff := now.Add(-15 * time.Minute)
		seedRun(t, ctx, pool, materializerFreshDispatch, "running", now.Add(-2*time.Hour))
		seedUnit(t, ctx, pool, "00000000-0000-4000-8000-000000004302",
			materializerFreshDispatch, "dispatching", nil, now.Add(-5*time.Minute))
		seedMaterializerDispatchedOutbox(t, ctx, pool, materializerFreshDispatch, "river-fresh-job", now.Add(-2*time.Hour))

		result, err := materializer.Step(ctx, now, cutoff, 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Dispatch != 0 {
			t.Fatalf("fresh dispatching graph rearmed unexpectedly: %#v", result)
		}
		assertMaterializerDispatchState(t, ctx, pool, materializerFreshDispatch, "dispatched", ptrString("river"), 1)
	})

	t.Run("retrying River row re-arms once when due", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 24, 3, 0, 0, 0, time.UTC)
		cutoff := now.Add(-15 * time.Minute)
		seedRun(t, ctx, pool, materializerRetryingDispatch, "running", now.Add(-2*time.Hour))
		seedUnit(t, ctx, pool, "00000000-0000-4000-8000-000000004303",
			materializerRetryingDispatch, "retrying", ptrTime(now.Add(-time.Minute)), now.Add(-5*time.Minute))
		seedMaterializerDispatchedOutbox(t, ctx, pool, materializerRetryingDispatch, "river-retry-job", now.Add(-2*time.Hour))

		result, err := materializer.Step(ctx, now, cutoff, 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Dispatch != 1 {
			t.Fatalf("retrying graph did not rearm exactly once: %#v", result)
		}
		assertMaterializerDispatchState(t, ctx, pool, materializerRetryingDispatch, "pending", nil, 1)

		result, err = materializer.Step(ctx, now, cutoff, 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Dispatch != 0 {
			t.Fatalf("retrying graph amplified on second pass: %#v", result)
		}
		assertMaterializerDispatchState(t, ctx, pool, materializerRetryingDispatch, "pending", nil, 1)
	})

	t.Run("feature-disabled River row stays protected", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 24, 4, 0, 0, 0, time.UTC)
		cutoff := now.Add(-15 * time.Minute)
		seedRun(t, ctx, pool, materializerFeatureDisabled, "running", now.Add(-2*time.Hour))
		seedUnit(t, ctx, pool, "00000000-0000-4000-8000-000000004304",
			materializerFeatureDisabled, "dispatching", nil, now.Add(-time.Hour))
		seedMaterializerFeatureDisabledOutbox(t, ctx, pool, materializerFeatureDisabled, now.Add(-2*time.Hour))

		result, err := materializer.Step(ctx, now, cutoff, 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Dispatch != 0 {
			t.Fatalf("feature-disabled row rearmed unexpectedly: %#v", result)
		}
		assertMaterializerDispatchState(t, ctx, pool, materializerFeatureDisabled, "dispatched", ptrString("river"), 1)
	})
}

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
		assertMaterializerOutboxCount(t, ctx, pool, materializerRiverQueued, "reference_discovery", 1)
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

		for _, queued := range []struct {
			runID string
			kind  string
			jobID string
		}{
			{materializerDispatchMissing, "dispatch_sync_run", "river-dispatch-queued"},
			{materializerFinalize, "finalize_sync_run", "river-finalize-queued"},
			{materializerRiverQueued, "reference_discovery", "river-discovery-queued"},
		} {
			var riverStatus string
			var riverTransport, riverJobID *string
			var riverAttempts int
			if err := pool.QueryRow(ctx, `
				SELECT status, dispatched_transport, transport_job_id, attempts
				FROM public.sync_dispatch_outbox
				WHERE sync_run_id = $1 AND kind = $2`,
				queued.runID, queued.kind,
			).Scan(&riverStatus, &riverTransport, &riverJobID, &riverAttempts); err != nil {
				t.Fatal(err)
			}
			if riverStatus != "dispatched" || riverTransport == nil || *riverTransport != "river" ||
				riverJobID == nil || *riverJobID != queued.jobID || riverAttempts != 1 {
				t.Fatalf("queued River %s delivery was rearmed: %s/%v/%v/%d",
					queued.kind, riverStatus, riverTransport, riverJobID, riverAttempts)
			}
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
		if !errors.Is(err, ErrUnavailable) || !reflect.DeepEqual(result, MaterializerResult{}) {
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

	t.Run("scheduled graph waits for committed occurrence readiness", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 23, 23, 0, 0, 0, time.UTC)
		seedRun(t, ctx, pool, materializerDispatchMissing, "planned", now.Add(-time.Hour))
		seedUnit(t, ctx, pool, "00000000-0000-4000-8000-000000004401",
			materializerDispatchMissing, "planned", nil, now.Add(-time.Hour))
		if _, err := pool.Exec(ctx, `UPDATE public.sync_runs SET triggered_by='schedule' WHERE id=$1`, materializerDispatchMissing); err != nil {
			t.Fatal(err)
		}

		result, err := materializer.Step(ctx, now, now.Add(-15*time.Minute), 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Dispatch != 0 || result.Finalize != 0 {
			t.Fatalf("unready scheduled graph materialized a wakeup: %#v", result)
		}
		assertMaterializerOutboxCount(t, ctx, pool, materializerDispatchMissing, "dispatch_sync_run", 0)

		if _, err := pool.Exec(ctx, `
			INSERT INTO public.scheduled_sync_occurrences
				(occurrence_id,sync_run_id,job_run_id,reconcile_status)
			VALUES ('ready-occurrence',$1,'00000000-0000-4000-8000-000000004499','completed')`, materializerDispatchMissing); err != nil {
			t.Fatal(err)
		}
		result, err = materializer.Step(ctx, now, now.Add(-15*time.Minute), 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Dispatch != 1 {
			t.Fatalf("ready scheduled graph did not materialize exactly one dispatch: %#v", result)
		}
		assertMaterializerOutboxCount(t, ctx, pool, materializerDispatchMissing, "dispatch_sync_run", 1)
	})

	t.Run("scheduled zero-unit graph cannot finalize before readiness", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		now := time.Date(2026, time.July, 24, 0, 0, 0, 0, time.UTC)
		seedRun(t, ctx, pool, materializerFinalize, "planned", now.Add(-time.Hour))
		if _, err := pool.Exec(ctx, `UPDATE public.sync_runs SET triggered_by='schedule' WHERE id=$1`, materializerFinalize); err != nil {
			t.Fatal(err)
		}
		result, err := materializer.Step(ctx, now, now.Add(-15*time.Minute), 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Finalize != 0 {
			t.Fatalf("unready zero-unit scheduled graph finalized: %#v", result)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO public.scheduled_sync_occurrences
				(occurrence_id,sync_run_id,job_run_id,reconcile_status)
			VALUES ('ready-zero-occurrence',$1,'00000000-0000-4000-8000-000000004498','completed')`, materializerFinalize); err != nil {
			t.Fatal(err)
		}
		result, err = materializer.Step(ctx, now, now.Add(-15*time.Minute), 20)
		if err != nil {
			t.Fatal(err)
		}
		if result.Finalize != 1 {
			t.Fatalf("ready zero-unit scheduled graph finalize count=%d, want 1", result.Finalize)
		}
		assertMaterializerOutboxCount(t, ctx, pool, materializerFinalize, "finalize_sync_run", 1)
	})
}

func createMaterializerIntegrationFixture(ctx context.Context, pool *pgxpool.Pool) error {
	for _, statement := range []string{
		"CREATE EXTENSION IF NOT EXISTS pgcrypto",
		`CREATE TABLE public.sync_runs (
			id uuid PRIMARY KEY,
			org_id text NOT NULL,
			triggered_by text NOT NULL DEFAULT 'manual',
			status text NOT NULL,
			created_at timestamptz NOT NULL
		)`,
		`CREATE TABLE public.scheduled_sync_occurrences (
			occurrence_id text PRIMARY KEY,
			sync_run_id uuid,
			job_run_id uuid,
			reconcile_status text NOT NULL
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
		"TRUNCATE public.scheduled_sync_occurrences",
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
	for _, queued := range []struct {
		runID string
		kind  string
		jobID string
	}{
		{materializerDispatchMissing, "dispatch_sync_run", "river-dispatch-queued"},
		{materializerFinalize, "finalize_sync_run", "river-finalize-queued"},
	} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO public.sync_dispatch_outbox (
				id, org_id, sync_run_id, kind, status, available_at, attempts,
				dispatched_at, dispatched_transport, dispatched_route_generation,
				transport_job_id, created_at, updated_at
			) VALUES (
				gen_random_uuid(), 'org-materializer', $1, $2,
				'dispatched', $3, 1, $3, 'river', 2, $4, $3, $3
			)`, queued.runID, queued.kind, now.Add(-2*time.Hour), queued.jobID); err != nil {
			t.Fatal(err)
		}
	}

	seedRun(t, ctx, pool, materializerDiscovery, "running", now.Add(-3*time.Hour))
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_run_reference_discoveries (
			sync_run_id, status, available_at, lease_expires_at
		) VALUES ($1, 'retrying', $2, NULL)`, materializerDiscovery, now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	seedRun(t, ctx, pool, materializerRiverQueued, "running", now.Add(-3*time.Hour))
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_run_reference_discoveries (
			sync_run_id, status, available_at, lease_expires_at
		) VALUES ($1, 'retrying', $2, NULL)`, materializerRiverQueued, now.Add(-time.Minute)); err != nil {
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
			gen_random_uuid(), 'org-materializer', $1, 'reference_discovery',
			'dispatched', $2, 1, $2, 'river', 2, 'river-discovery-queued', $2, $2
		)`, materializerRiverQueued, now.Add(-2*time.Hour)); err != nil {
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

func seedMaterializerDispatchedOutbox(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	runID, jobID string,
	dispatchedAt time.Time,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			dispatched_at, dispatched_transport, dispatched_route_generation,
			transport_job_id, created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-materializer', $1, 'dispatch_sync_run',
			'dispatched', $2, 1, $2, 'river', 2, $3, $2, $2
		)`,
		runID, dispatchedAt, jobID); err != nil {
		t.Fatal(err)
	}
}

func seedMaterializerFeatureDisabledOutbox(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	runID string,
	dispatchedAt time.Time,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			last_error, dispatched_at, dispatched_transport,
			dispatched_route_generation, transport_job_id,
			created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-materializer', $1, 'dispatch_sync_run',
			'dispatched', $2, 1, 'feature_disabled', $2, 'river', 2, 'feature-disabled-job', $2, $2
		)`,
		runID, dispatchedAt); err != nil {
		t.Fatal(err)
	}
}

func assertMaterializerDispatchState(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	runID, wantStatus string,
	wantTransport *string,
	wantAttempts int,
) {
	t.Helper()
	var (
		status    string
		transport *string
		attempts  int
	)
	if err := pool.QueryRow(ctx, `
		SELECT status, dispatched_transport, attempts
		FROM public.sync_dispatch_outbox
		WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run'`,
		runID,
	).Scan(&status, &transport, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus || attempts != wantAttempts {
		t.Fatalf("dispatch state for %s = %s/%v/%d, want %s/%v/%d",
			runID, status, transport, attempts, wantStatus, wantTransport, wantAttempts)
	}
	if wantTransport == nil {
		if transport != nil {
			t.Fatalf("dispatch state for %s transport = %v, want nil", runID, transport)
		}
		return
	}
	if transport == nil || *transport != *wantTransport {
		t.Fatalf("dispatch state for %s transport = %v, want %s",
			runID, transport, *wantTransport)
	}
}

func ptrTime(value time.Time) *time.Time {
	return &value
}

func ptrString(value string) *string {
	return &value
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

// seedMaterializerOutboxWithAttempts writes a dispatch wakeup that has already
// been claimed and republished `attempts` times. That column is durable and
// already persisted; nothing new is instrumented to read it.
func seedMaterializerOutboxWithAttempts(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	runID string,
	attempts int64,
	at time.Time,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-materializer', $1, 'dispatch_sync_run',
			'pending', $3, $2, $3, $3
		)`, runID, attempts, at); err != nil {
		t.Fatal(err)
	}
}

// CHAOS-4097 item 2. One production sync_dispatch_outbox row reached
// attempts = 72601, generating roughly 1500 no-op River jobs a minute for
// twenty-two hours, and nothing anywhere said a word about it.
//
// The report is what makes that loud. It changes no predicate and no write --
// the LOOP is bounded by the sweep removing the stuck units the re-arm
// predicate keeps matching, not by this -- so every assertion here is about
// visibility, and the negative cases matter as much as the positive one: a
// threshold that fires on healthy rows would be turned off within a week and
// then this would be exactly as silent as it was before.
func TestMaterializerReportsRunawayDispatchWakeups(t *testing.T) {
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
	now := time.Now().UTC()
	step := func() MaterializerResult {
		t.Helper()
		result, err := materializer.Step(ctx, now, now.Add(-15*time.Minute), 10)
		if err != nil {
			t.Fatalf("materializer step: %v", err)
		}
		return result
	}

	t.Run("a looping wakeup on a live run is reported", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		run := "00000000-0000-4000-8000-" + leftPadMaterializerID(9001)
		seedRun(t, ctx, pool, run, "dispatching", now.Add(-24*time.Hour))
		seedMaterializerOutboxWithAttempts(t, ctx, pool, run, 72601, now.Add(-time.Hour))

		result := step()
		if len(result.Runaway) != 1 || result.RunawayTruncated {
			t.Fatalf("runaway report = %#v, want exactly the looping row", result.Runaway)
		}
		if result.Runaway[0].SyncRunID != run || result.Runaway[0].Attempts != 72601 {
			t.Fatalf("runaway row = %#v, want the run and its durable attempt count",
				result.Runaway[0])
		}
	})

	// NON-VACUITY, and the reason the threshold is not lower. Every healthy
	// dispatch_sync_run row in production sat at a p99 of 43-72 attempts; a
	// report that fired there would be noise, and noise gets muted.
	t.Run("an ordinary attempt count is not reported", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		run := "00000000-0000-4000-8000-" + leftPadMaterializerID(9002)
		seedRun(t, ctx, pool, run, "dispatching", now.Add(-time.Hour))
		seedMaterializerOutboxWithAttempts(t, ctx, pool, run,
			runawayDispatchAttempts-1, now.Add(-time.Hour))

		if result := step(); len(result.Runaway) != 0 {
			t.Fatalf("runaway report = %#v, want a healthy row left unreported", result.Runaway)
		}
	})

	// A finished run's row is inert: its count is archaeology, not an
	// operational signal. Production holds 3397 completed rows that would
	// otherwise be re-reported on every single pass, forever.
	t.Run("a terminal run is not reported however high its count", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		run := "00000000-0000-4000-8000-" + leftPadMaterializerID(9003)
		seedRun(t, ctx, pool, run, "success", now.Add(-48*time.Hour))
		seedMaterializerOutboxWithAttempts(t, ctx, pool, run, 72601, now.Add(-time.Hour))

		if result := step(); len(result.Runaway) != 0 {
			t.Fatalf("runaway report = %#v, want a finished run left unreported", result.Runaway)
		}
	})

	// A widespread degradation must not turn one pass into an unbounded burst
	// of log lines -- and the cap must SAY it capped. A silently truncated
	// report reads as "these are all of them", which is the same quiet
	// under-reporting this whole ticket is about.
	t.Run("the report is capped and says so", func(t *testing.T) {
		resetMaterializerIntegrationTables(t, ctx, pool)
		for index := 0; index < runawayDispatchScan+3; index++ {
			run := "00000000-0000-4000-8000-" + leftPadMaterializerID(9100+index)
			seedRun(t, ctx, pool, run, "dispatching", now.Add(-24*time.Hour))
			seedMaterializerOutboxWithAttempts(t, ctx, pool, run,
				int64(runawayDispatchAttempts+index), now.Add(-time.Hour))
		}

		result := step()
		if len(result.Runaway) != runawayDispatchScan || !result.RunawayTruncated {
			t.Fatalf("runaway report = %d rows truncated=%t, want %d and a truncation flag",
				len(result.Runaway), result.RunawayTruncated, runawayDispatchScan)
		}
		// Worst first, so a capped report is still the most useful rows.
		if result.Runaway[0].Attempts <= result.Runaway[len(result.Runaway)-1].Attempts {
			t.Fatalf("runaway report is not ordered worst-first: %#v", result.Runaway)
		}
	})
}
