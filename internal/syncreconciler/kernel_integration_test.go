//go:build integration

package syncreconciler

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"testing"
	"time"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

const (
	integrationDispatchID = "00000000-0000-4000-8000-000000003301"
	integrationStaleID    = "00000000-0000-4000-8000-000000003302"
	kernelDomainRole      = "kernel_domain_runtime"
	kernelQueueRole       = "kernel_queue_runtime"
	kernelDomainPassword  = "kernel_domain_password"
	kernelQueuePassword   = "kernel_queue_password"
)

type kernelRiverArgs struct {
	OutboxID string `json:"outbox_id"`
}

func (kernelRiverArgs) Kind() string { return "test.sync_dispatch" }

// This test intentionally creates the production column types instead of
// importing Python metadata. It validates the dormant Go kernel's PostgreSQL
// SQL against text claim tokens, UUID outbox identifiers, timestamptz leases,
// and bigint route generations through the actual least-privilege runtime
// roles and River InsertTx API.
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

	adminPool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer adminPool.Close()
	if err := createKernelIntegrationFixture(ctx, adminPool); err != nil {
		t.Fatal(err)
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, adminPool, riverstore.MigrationOptions{
		Schema:     "river",
		DomainRole: kernelDomainRole,
		QueueRole:  kernelQueueRole,
	}); err != nil {
		t.Fatal(err)
	}

	domainPool, err := pgxpool.New(
		ctx,
		kernelRoleURI(t, instance.URI, kernelDomainRole, kernelDomainPassword),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer domainPool.Close()
	queuePool, err := pgxpool.New(
		ctx,
		kernelRoleURI(t, instance.URI, kernelQueueRole, kernelQueuePassword),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer queuePool.Close()
	if err := postgresstore.CheckDomainAuthorization(ctx, domainPool, kernelDomainRole, "river"); err != nil {
		t.Fatalf("domain authorization: %v", err)
	}
	if err := postgresstore.CheckQueueAuthorization(ctx, queuePool, kernelQueueRole, "river"); err != nil {
		t.Fatalf("queue authorization: %v", err)
	}
	if _, err := queuePool.Exec(
		ctx,
		"UPDATE public.sync_dispatch_transport_routes SET generation = generation + 1",
	); err == nil {
		t.Fatal("queue role unexpectedly has route UPDATE")
	}

	kernel, err := NewKernel(
		domainPool,
		queuePool,
		riverRegistry(t, syncdispatchcontract.KindDispatchSyncRun),
		KernelModeMutation,
	)
	if err != nil {
		t.Fatal(err)
	}
	riverClient, err := river.NewClient(
		riverpgxv5.New(queuePool),
		&river.Config{Schema: "river"},
	)
	if err != nil {
		t.Fatal(err)
	}
	publish := func(
		publisherCtx context.Context,
		tx pgx.Tx,
		claim TransportClaim,
	) (string, error) {
		inserted, insertErr := riverClient.InsertTx(
			publisherCtx,
			tx,
			kernelRiverArgs{OutboxID: claim.ID},
			&river.InsertOpts{Queue: "sync"},
		)
		if insertErr != nil {
			return "", insertErr
		}
		return strconv.FormatInt(inserted.Job.ID, 10), nil
	}

	t.Run("queue role atomically commits River insert plus terminal mark", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, adminPool)
		now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
		seedKernelOutbox(t, ctx, adminPool, integrationDispatchID, now.Add(-time.Second))

		entered := make(chan struct{})
		release := make(chan struct{})
		firstDone := make(chan error, 1)
		go func() {
			_, stepErr := kernel.Step(
				ctx,
				now,
				1,
				time.Minute,
				func(publisherCtx context.Context, tx pgx.Tx, claim TransportClaim) (string, error) {
					if claim.ID != integrationDispatchID ||
						claim.Kind != syncdispatchcontract.KindDispatchSyncRun ||
						claim.RouteGeneration != 7 ||
						claim.Attempts != 1 {
						return "", fmt.Errorf("unexpected claim: %#v", claim)
					}
					var generation int64
					if err := tx.QueryRow(
						publisherCtx,
						`SELECT claim_route_generation
						FROM public.sync_dispatch_outbox WHERE id = $1`,
						claim.ID,
					).Scan(&generation); err != nil {
						return "", err
					}
					if generation != claim.RouteGeneration {
						return "", fmt.Errorf(
							"publisher saw claim generation %d, want %d",
							generation,
							claim.RouteGeneration,
						)
					}
					close(entered)
					select {
					case <-release:
					case <-publisherCtx.Done():
						return "", publisherCtx.Err()
					}
					return publish(publisherCtx, tx, claim)
				},
				nil,
			)
			firstDone <- stepErr
		}()

		select {
		case <-entered:
		case stepErr := <-firstDone:
			t.Fatalf("first reconciler failed before publisher: %v", stepErr)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}

		second, err := kernel.Step(
			ctx,
			now,
			1,
			time.Minute,
			func(context.Context, pgx.Tx, TransportClaim) (string, error) {
				return "", errors.New("second reconciler reached a locked row")
			},
			nil,
		)
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
		if err := adminPool.QueryRow(
			ctx,
			`SELECT status, attempts, claim_token, claim_transport, claim_route_generation,
				dispatched_transport, dispatched_route_generation, transport_job_id
			FROM public.sync_dispatch_outbox WHERE id = $1`,
			integrationDispatchID,
		).Scan(
			&status,
			&attempts,
			&claimToken,
			&claimTransport,
			&claimGeneration,
			&dispatchedTransport,
			&dispatchedGeneration,
			&transportJobID,
		); err != nil {
			t.Fatal(err)
		}
		if status != "dispatched" ||
			attempts != 1 ||
			claimToken != nil ||
			claimTransport != nil ||
			claimGeneration != nil ||
			dispatchedTransport != "river" ||
			dispatchedGeneration == nil ||
			*dispatchedGeneration != 7 {
			t.Fatalf(
				"terminal row = status:%s attempts:%d claim:%v/%v/%v dispatched:%s/%v/%s",
				status,
				attempts,
				claimToken,
				claimTransport,
				claimGeneration,
				dispatchedTransport,
				dispatchedGeneration,
				transportJobID,
			)
		}
		var jobs int
		if err := adminPool.QueryRow(
			ctx,
			"SELECT count(*) FROM river.river_job WHERE id::text = $1",
			transportJobID,
		).Scan(&jobs); err != nil {
			t.Fatal(err)
		}
		if jobs != 1 {
			t.Fatalf("same-transaction River jobs = %d, want 1", jobs)
		}
	})

	t.Run("concurrent route change rolls back River insert and outbox claim", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, adminPool)
		now := time.Date(2026, time.July, 23, 12, 0, 30, 0, time.UTC)
		seedKernelOutbox(t, ctx, adminPool, integrationDispatchID, now.Add(-time.Second))
		entered := make(chan struct{})
		release := make(chan struct{})
		firstDone := make(chan error, 1)
		go func() {
			_, stepErr := kernel.Step(
				ctx,
				now,
				1,
				time.Minute,
				func(publisherCtx context.Context, tx pgx.Tx, claim TransportClaim) (string, error) {
					jobID, insertErr := publish(publisherCtx, tx, claim)
					if insertErr != nil {
						return "", insertErr
					}
					close(entered)
					select {
					case <-release:
					case <-publisherCtx.Done():
						return "", publisherCtx.Err()
					}
					return jobID, nil
				},
				nil,
			)
			firstDone <- stepErr
		}()
		select {
		case <-entered:
		case stepErr := <-firstDone:
			t.Fatalf("first reconciler failed before route change: %v", stepErr)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if _, err := adminPool.Exec(
			ctx,
			`UPDATE public.sync_dispatch_transport_routes
			SET generation = generation + 1
			WHERE kind = $1`,
			syncdispatchcontract.KindDispatchSyncRun,
		); err != nil {
			t.Fatalf("concurrent route update: %v", err)
		}
		close(release)
		if err := <-firstDone; !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("Step() after route change error = %v", err)
		}
		var status string
		var attempts int
		var claimToken *string
		if err := adminPool.QueryRow(
			ctx,
			`SELECT status, attempts, claim_token
			FROM public.sync_dispatch_outbox WHERE id = $1`,
			integrationDispatchID,
		).Scan(&status, &attempts, &claimToken); err != nil {
			t.Fatal(err)
		}
		if status != "pending" || attempts != 0 || claimToken != nil {
			t.Fatalf(
				"route-fenced outbox = status:%s attempts:%d claim:%v",
				status,
				attempts,
				claimToken,
			)
		}
		var jobs int
		if err := adminPool.QueryRow(ctx, "SELECT count(*) FROM river.river_job").Scan(&jobs); err != nil {
			t.Fatal(err)
		}
		if jobs != 0 {
			t.Fatalf("route-fenced River jobs = %d, want 0", jobs)
		}
	})

	t.Run("actual terminal write fences stale persisted route generation", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, adminPool)
		now := time.Date(2026, time.July, 23, 12, 1, 0, 0, time.UTC)
		seedKernelOutbox(t, ctx, adminPool, integrationStaleID, now.Add(-time.Second))
		claim := TransportClaim{
			ID:              integrationStaleID,
			Kind:            syncdispatchcontract.KindDispatchSyncRun,
			ClaimToken:      "10000000-0000-4000-8000-000000003302",
			RouteGeneration: 7,
			AvailableAt:     now.Add(-time.Second),
			Attempts:        1,
		}
		if _, err := adminPool.Exec(
			ctx,
			`UPDATE public.sync_dispatch_outbox
			SET claim_token = $2, claim_expires_at = $3, claim_transport = 'river',
				claim_route_generation = 7, attempts = 1
			WHERE id = $1`,
			claim.ID,
			claim.ClaimToken,
			now.Add(time.Minute),
		); err != nil {
			t.Fatal(err)
		}
		if _, err := adminPool.Exec(
			ctx,
			`UPDATE public.sync_dispatch_transport_routes
			SET generation = 8 WHERE kind = $1`,
			syncdispatchcontract.KindDispatchSyncRun,
		); err != nil {
			t.Fatal(err)
		}
		tx, err := queuePool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if err := markRiverDispatched(
			ctx,
			tx,
			claim,
			now,
			"stale-river-job",
		); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("markRiverDispatched() error = %v", err)
		}
	})

	t.Run("failure after River InsertTx rolls back claim, job, and terminal mark", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, adminPool)
		now := time.Date(2026, time.July, 23, 12, 2, 0, 0, time.UTC)
		seedKernelOutbox(t, ctx, adminPool, integrationDispatchID, now.Add(-time.Second))
		publisherErr := errors.New("injected publisher failure")
		if _, err := kernel.Step(
			ctx,
			now,
			1,
			time.Minute,
			func(publisherCtx context.Context, tx pgx.Tx, claim TransportClaim) (string, error) {
				if _, err := publish(publisherCtx, tx, claim); err != nil {
					return "", err
				}
				return "", publisherErr
			},
			nil,
		); !errors.Is(err, publisherErr) {
			t.Fatalf("Step() error = %v", err)
		}
		var status string
		var attempts int
		var claimToken *string
		if err := adminPool.QueryRow(
			ctx,
			`SELECT status, attempts, claim_token
			FROM public.sync_dispatch_outbox WHERE id = $1`,
			integrationDispatchID,
		).Scan(&status, &attempts, &claimToken); err != nil {
			t.Fatal(err)
		}
		if status != "pending" || attempts != 0 || claimToken != nil {
			t.Fatalf(
				"rolled-back outbox = status:%s attempts:%d claim:%v",
				status,
				attempts,
				claimToken,
			)
		}
		var jobs int
		if err := adminPool.QueryRow(ctx, "SELECT count(*) FROM river.river_job").Scan(&jobs); err != nil {
			t.Fatal(err)
		}
		if jobs != 0 {
			t.Fatalf("rolled-back River jobs = %d, want 0", jobs)
		}
	})
}

func createKernelIntegrationFixture(ctx context.Context, pool *pgxpool.Pool) error {
	for _, statement := range []string{
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		"CREATE ROLE " + kernelDomainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + kernelDomainPassword + "'",
		"CREATE ROLE " + kernelQueueRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + kernelQueuePassword + "'",
		"GRANT CONNECT ON DATABASE worker_test TO " + kernelDomainRole + ", " + kernelQueueRole,
		"CREATE TABLE public.worker_job_outbox (id uuid PRIMARY KEY, state text NOT NULL)",
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
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func resetKernelIntegrationTables(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
) {
	t.Helper()
	for _, statement := range []string{
		"TRUNCATE river.river_job",
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

func seedKernelOutbox(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	id string,
	availableAt time.Time,
) {
	t.Helper()
	if _, err := pool.Exec(
		ctx,
		`INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts, created_at, updated_at
		) VALUES ($1, 'org-integration', '00000000-0000-4000-8000-000000003300',
			'dispatch_sync_run', 'pending', $2, 0, $2, $2)`,
		id,
		availableAt,
	); err != nil {
		t.Fatal(err)
	}
}

func kernelRoleURI(t *testing.T, rawURI, role, password string) string {
	t.Helper()
	parsed, err := url.Parse(rawURI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(role, password)
	return parsed.String()
}
