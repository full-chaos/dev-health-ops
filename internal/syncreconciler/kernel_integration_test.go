//go:build integration

package syncreconciler

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboperator"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
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

func (kernelRiverArgs) Kind() string { return syncdispatchcontract.KindDispatchSyncRun }

type kernelOperatorRegistry struct{}

func (kernelOperatorRegistry) Descriptor(string) (jobruntime.Descriptor, bool) {
	return jobruntime.Descriptor{}, false
}
func (kernelOperatorRegistry) Profile(string) []jobruntime.Descriptor { return nil }
func (kernelOperatorRegistry) HasProfile(string) bool                 { return false }
func (kernelOperatorRegistry) HasQueue(string) bool                   { return false }

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
		again, err := kernel.Step(ctx, now, 1, time.Minute, publish, nil)
		if err != nil || again.Claimed != 0 || again.Dispatched != 0 {
			t.Fatalf("terminal row was reclaimed: %#v, %v", again, err)
		}
		if err := adminPool.QueryRow(ctx, "SELECT count(*) FROM river.river_job").Scan(&jobs); err != nil {
			t.Fatal(err)
		}
		if jobs != 1 {
			t.Fatalf("repeat Step() inserted %d River jobs, want 1", jobs)
		}
	})

	t.Run("crash after claim commit leaves a durable bounded lease for later retry", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, adminPool)
		now := time.Date(2026, time.July, 23, 12, 0, 15, 0, time.UTC)
		seedKernelOutbox(t, ctx, adminPool, integrationDispatchID, now.Add(-time.Second))

		claims, err := kernel.commitClaims(ctx, now, 1, time.Minute)
		if err != nil || len(claims) != 1 {
			t.Fatalf("commitClaims() = %#v, %v", claims, err)
		}
		var token string
		var attempts int
		var expiresAt time.Time
		if err := adminPool.QueryRow(ctx, `
			SELECT claim_token, claim_expires_at, attempts
			FROM public.sync_dispatch_outbox WHERE id = $1`, integrationDispatchID).Scan(
			&token, &expiresAt, &attempts,
		); err != nil {
			t.Fatal(err)
		}
		if token != claims[0].ClaimToken || !expiresAt.Equal(now.Add(time.Minute)) || attempts != 1 {
			t.Fatalf("durable claim token:%s expires:%s attempts:%d", token, expiresAt, attempts)
		}
		var jobs int
		if err := adminPool.QueryRow(ctx, "SELECT count(*) FROM river.river_job").Scan(&jobs); err != nil {
			t.Fatal(err)
		}
		if jobs != 0 {
			t.Fatalf("claim-only crash window inserted %d River jobs", jobs)
		}
		blocked, err := kernel.Step(ctx, now, 1, time.Minute, publish, nil)
		if err != nil || blocked.Claimed != 0 {
			t.Fatalf("live durable lease was reclaimed: %#v, %v", blocked, err)
		}
		retryNow := now.Add(time.Minute + time.Nanosecond)
		retried, err := kernel.Step(ctx, retryNow, 1, time.Minute, publish, nil)
		if err != nil || retried.Claimed != 1 || retried.Dispatched != 1 {
			t.Fatalf("expired durable claim retry = %#v, %v", retried, err)
		}
		if err := adminPool.QueryRow(ctx, `
			SELECT attempts FROM public.sync_dispatch_outbox WHERE id = $1`,
			integrationDispatchID,
		).Scan(&attempts); err != nil {
			t.Fatal(err)
		}
		if attempts != 2 {
			t.Fatalf("retry attempts = %d, want 2", attempts)
		}
	})

	t.Run("concurrent route change rolls back River insert but preserves durable claim", func(t *testing.T) {
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
		if err := <-firstDone; err != nil {
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
		if status != "pending" || attempts != 1 || claimToken == nil {
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

	t.Run("stale committed lease is rejected before River insertion", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, adminPool)
		now := time.Date(2026, time.July, 23, 12, 1, 30, 0, time.UTC)
		seedKernelOutbox(t, ctx, adminPool, integrationStaleID, now.Add(-time.Second))
		claims, err := kernel.commitClaims(ctx, now, 1, time.Minute)
		if err != nil || len(claims) != 1 {
			t.Fatalf("commitClaims() = %#v, %v", claims, err)
		}
		if _, err := adminPool.Exec(ctx, `
			UPDATE public.sync_dispatch_outbox SET claim_expires_at = $2
			WHERE id = $1`, claims[0].ID, now); err != nil {
			t.Fatal(err)
		}
		published := false
		outcome, err := kernel.deliverAtLeastOnce(
			ctx,
			claims[0],
			now,
			func(context.Context, pgx.Tx, TransportClaim) (string, error) {
				published = true
				return "", nil
			},
		)
		if !errors.Is(err, ErrLeaseLost) || outcome != deliveryLeaseLost || published {
			t.Fatalf("stale delivery outcome=%d published=%t error=%v", outcome, published, err)
		}
		var jobs int
		if err := adminPool.QueryRow(ctx, "SELECT count(*) FROM river.river_job").Scan(&jobs); err != nil {
			t.Fatal(err)
		}
		if jobs != 0 {
			t.Fatalf("stale lease inserted %d River jobs", jobs)
		}
	})

	t.Run("failure after River InsertTx rolls back job and records durable retry", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, adminPool)
		now := time.Date(2026, time.July, 23, 12, 2, 0, 0, time.UTC)
		seedKernelOutbox(t, ctx, adminPool, integrationDispatchID, now.Add(-time.Second))
		publisherErr := errors.New("injected publisher failure")
		result, err := kernel.Step(
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
		)
		if err != nil || result.Retried != 1 || result.Dispatched != 0 {
			t.Fatalf("Step() = %#v, %v", result, err)
		}
		var status string
		var attempts int
		var claimToken *string
		var availableAt time.Time
		var lastError string
		if err := adminPool.QueryRow(
			ctx,
			`SELECT status, attempts, claim_token, available_at, COALESCE(last_error, '')
			FROM public.sync_dispatch_outbox WHERE id = $1`,
			integrationDispatchID,
		).Scan(&status, &attempts, &claimToken, &availableAt, &lastError); err != nil {
			t.Fatal(err)
		}
		if status != "pending" || attempts != 1 || claimToken != nil ||
			!availableAt.Equal(now.Add(time.Minute)) ||
			lastError != transportPublishFailureEvidence {
			t.Fatalf(
				"retried outbox = status:%s attempts:%d claim:%v available:%s error:%s",
				status,
				attempts,
				claimToken,
				availableAt,
				lastError,
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

	t.Run("partial batch commits successful claims and records failed claim retry", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, adminPool)
		now := time.Date(2026, time.July, 23, 12, 3, 0, 0, time.UTC)
		seedKernelOutbox(t, ctx, adminPool, integrationDispatchID, now.Add(-2*time.Second))
		seedKernelOutbox(t, ctx, adminPool, integrationStaleID, now.Add(-time.Second))
		result, err := kernel.Step(
			ctx,
			now,
			2,
			time.Minute,
			func(publisherCtx context.Context, tx pgx.Tx, claim TransportClaim) (string, error) {
				jobID, insertErr := publish(publisherCtx, tx, claim)
				if insertErr != nil {
					return "", insertErr
				}
				if claim.ID == integrationStaleID {
					return "", errors.New("injected second publish failure")
				}
				return jobID, nil
			},
			nil,
		)
		if err != nil || result.Claimed != 2 || result.Dispatched != 1 || result.Retried != 1 {
			t.Fatalf("partial Step() = %#v, %v", result, err)
		}
		var (
			firstStatus, secondStatus string
			secondToken               *string
			secondAvailable           time.Time
			jobs                      int
		)
		if err := adminPool.QueryRow(ctx, `
			SELECT status FROM public.sync_dispatch_outbox WHERE id = $1`,
			integrationDispatchID,
		).Scan(&firstStatus); err != nil {
			t.Fatal(err)
		}
		if err := adminPool.QueryRow(ctx, `
			SELECT status, claim_token, available_at
			FROM public.sync_dispatch_outbox WHERE id = $1`,
			integrationStaleID,
		).Scan(&secondStatus, &secondToken, &secondAvailable); err != nil {
			t.Fatal(err)
		}
		if err := adminPool.QueryRow(ctx, "SELECT count(*) FROM river.river_job").Scan(&jobs); err != nil {
			t.Fatal(err)
		}
		if firstStatus != "dispatched" || secondStatus != "pending" || secondToken != nil ||
			!secondAvailable.Equal(now.Add(time.Minute)) || jobs != 1 {
			t.Fatalf(
				"partial batch first:%s second:%s/%v/%s jobs:%d",
				firstStatus,
				secondStatus,
				secondToken,
				secondAvailable,
				jobs,
			)
		}
	})

	t.Run("post_sync failed insert releases its guarded claim for retry", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, adminPool)
		now := time.Date(2026, time.July, 23, 12, 4, 0, 0, time.UTC)
		if _, err := adminPool.Exec(ctx, `
			UPDATE public.sync_dispatch_transport_routes
			SET transport = 'river', generation = 7
			WHERE kind = $1`, syncdispatchcontract.KindPostSync); err != nil {
			t.Fatal(err)
		}
		seedKernelOutboxKind(
			t,
			ctx,
			adminPool,
			integrationStaleID,
			syncdispatchcontract.KindPostSync,
			now.Add(-time.Second),
		)
		postKernel, err := NewKernel(
			domainPool,
			queuePool,
			riverRegistry(t, syncdispatchcontract.KindPostSync),
			KernelModeMutation,
		)
		if err != nil {
			t.Fatal(err)
		}
		publishErr := errors.New("injected post-sync River insert failure")
		result, err := postKernel.Step(
			ctx,
			now,
			1,
			time.Minute,
			func(context.Context, pgx.Tx, TransportClaim) (string, error) {
				return "", publishErr
			},
			nil,
		)
		if err != nil || result.Claimed != 1 || result.Retried != 1 || result.Dispatched != 0 {
			t.Fatalf("post_sync Step() = %#v, %v", result, err)
		}
		var status string
		var claimToken *string
		var availableAt time.Time
		if err := adminPool.QueryRow(ctx, `
			SELECT status, claim_token, available_at
			FROM public.sync_dispatch_outbox WHERE id = $1`,
			integrationStaleID,
		).Scan(&status, &claimToken, &availableAt); err != nil {
			t.Fatal(err)
		}
		if status != "pending" || claimToken != nil || !availableAt.Equal(now.Add(time.Minute)) {
			t.Fatalf("post_sync retry row status:%s claim:%v available:%s", status, claimToken, availableAt)
		}
	})

	t.Run("unhandled River rescuer discard is recovered without reopening real terminal failures", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, adminPool)
		now := time.Date(2026, time.July, 23, 12, 5, 0, 0, time.UTC)
		seedKernelOutbox(t, ctx, adminPool, integrationDispatchID, now.Add(-time.Second))
		first, err := kernel.Step(ctx, now, 1, time.Minute, publish, nil)
		if err != nil || first.Dispatched != 1 {
			t.Fatalf("initial Step() = %#v, %v", first, err)
		}
		var firstJobID string
		if err := adminPool.QueryRow(ctx, `
			SELECT transport_job_id FROM public.sync_dispatch_outbox WHERE id = $1`,
			integrationDispatchID,
		).Scan(&firstJobID); err != nil {
			t.Fatal(err)
		}
		if _, err := adminPool.Exec(ctx, `
			UPDATE river.river_job
			SET state = 'discarded', attempt = 1, max_attempts = 5,
				attempted_at = $2::timestamptz - interval '61 minutes', finalized_at = $2::timestamptz,
				errors = ARRAY[jsonb_build_object(
					'at', $2, 'attempt', 1,
					'error', 'Stuck job rescued by JobRescuer', 'trace', ''
				)]
			WHERE id::text = $1`, firstJobID, now); err != nil {
			t.Fatal(err)
		}

		repair, err := NewTerminalDeliveryRepair(queuePool, "river")
		if err != nil {
			t.Fatal(err)
		}
		recovered, err := repair.Step(ctx, now.Add(time.Second), 10)
		if err != nil || recovered.Recovered != 1 {
			t.Fatalf("repair Step() = %#v, %v", recovered, err)
		}
		operatorBackend, err := joboperator.NewDirectPostgresBackend(
			queuePool,
			"river",
			kernelOperatorRegistry{},
		)
		if err != nil {
			t.Fatal(err)
		}
		parsedFirstJobID, err := strconv.ParseInt(firstJobID, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := operatorBackend.Retry(ctx, parsedFirstJobID, joboperator.Mutation{
			ExpectedState: joboperator.StateDiscarded,
		}); !errors.Is(err, joboperator.ErrStateConflict) {
			t.Fatalf("stale operator retry error = %v, want state conflict", err)
		}
		second, err := kernel.Step(ctx, now.Add(2*time.Second), 1, time.Minute, publish, nil)
		if err != nil || second.Dispatched != 1 {
			t.Fatalf("replacement Step() = %#v, %v", second, err)
		}
		var (
			status      string
			attempts    int
			secondJobID string
		)
		if err := adminPool.QueryRow(ctx, `
			SELECT status, attempts, transport_job_id
			FROM public.sync_dispatch_outbox WHERE id = $1`,
			integrationDispatchID,
		).Scan(&status, &attempts, &secondJobID); err != nil {
			t.Fatal(err)
		}
		if status != "dispatched" || attempts != 2 || secondJobID == firstJobID {
			t.Fatalf("replacement = status:%s attempts:%d jobs:%s/%s", status, attempts, firstJobID, secondJobID)
		}

		if _, err := adminPool.Exec(ctx, `
			UPDATE river.river_job
			SET state = 'discarded', attempt = 1, finalized_at = $2::timestamptz,
				errors = ARRAY[jsonb_build_object(
					'at', $2::timestamptz, 'attempt', 1,
					'error', 'provider request permanently rejected', 'trace', ''
				)]
			WHERE id::text = $1`, secondJobID, now.Add(3*time.Second)); err != nil {
			t.Fatal(err)
		}
		ordinaryFailure, err := repair.Step(ctx, now.Add(4*time.Second), 10)
		if err != nil || ordinaryFailure.Recovered != 0 {
			t.Fatalf("ordinary failure repair Step() = %#v, %v", ordinaryFailure, err)
		}
		if _, err := adminPool.Exec(ctx, `
			UPDATE river.river_job
			SET attempt = max_attempts,
				errors = ARRAY[jsonb_build_object(
					'at', $2::timestamptz, 'attempt', max_attempts,
					'error', 'Stuck job rescued by JobRescuer', 'trace', ''
				)]
			WHERE id::text = $1`, secondJobID, now.Add(5*time.Second)); err != nil {
			t.Fatal(err)
		}
		exhausted, err := repair.Step(ctx, now.Add(6*time.Second), 10)
		if err != nil || exhausted.Recovered != 0 {
			t.Fatalf("exhausted rescue repair Step() = %#v, %v", exhausted, err)
		}
		if err := adminPool.QueryRow(ctx, `
			SELECT status, transport_job_id FROM public.sync_dispatch_outbox WHERE id = $1`,
			integrationDispatchID,
		).Scan(&status, &secondJobID); err != nil {
			t.Fatal(err)
		}
		if status != "dispatched" {
			t.Fatalf("real terminal failure reopened: status=%s job=%s", status, secondJobID)
		}
	})

	t.Run("terminal delivery repair uses active-run indexes against retained history", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, adminPool)
		now := time.Date(2026, time.July, 23, 13, 0, 0, 0, time.UTC)
		if _, err := adminPool.Exec(ctx, `
			INSERT INTO public.sync_runs (id, status)
			SELECT (lpad(to_hex(series), 8, '0') || '-0000-4000-8000-' || lpad(to_hex(series), 12, '0'))::uuid,
				'success'
			FROM generate_series(1, 50000) AS series`); err != nil {
			t.Fatal(err)
		}
		if _, err := adminPool.Exec(ctx, `
			INSERT INTO public.sync_dispatch_outbox (
				id, org_id, sync_run_id, kind, status, available_at, attempts,
				dispatched_at, dispatched_transport, dispatched_route_generation,
				transport_job_id, created_at, updated_at
			)
			SELECT gen_random_uuid(), 'org-history', run.id, 'dispatch_sync_run',
				'dispatched', $1, 1, $1, 'river', 7, '1', $1, $1
			FROM public.sync_runs AS run WHERE run.status = 'success'`, now.Add(-24*time.Hour)); err != nil {
			t.Fatal(err)
		}
		for _, statement := range []string{
			"ANALYZE public.sync_runs",
			"ANALYZE public.sync_dispatch_outbox",
		} {
			if _, err := adminPool.Exec(ctx, statement); err != nil {
				t.Fatal(err)
			}
		}
		seedKernelOutbox(t, ctx, adminPool, integrationDispatchID, now.Add(-time.Second))
		first, err := kernel.Step(ctx, now, 1, time.Minute, publish, nil)
		if err != nil || first.Dispatched != 1 {
			t.Fatalf("initial Step() = %#v, %v", first, err)
		}
		var jobID string
		if err := adminPool.QueryRow(ctx, `
			SELECT transport_job_id FROM public.sync_dispatch_outbox WHERE id = $1`,
			integrationDispatchID,
		).Scan(&jobID); err != nil {
			t.Fatal(err)
		}
		if _, err := adminPool.Exec(ctx, `
			UPDATE river.river_job
			SET state = 'discarded', attempt = 1, max_attempts = 5, finalized_at = $2::timestamptz,
				errors = ARRAY[jsonb_build_object('at', $2::timestamptz, 'attempt', 1,
					'error', 'Stuck job rescued by JobRescuer', 'trace', '')]
			WHERE id::text = $1`, jobID, now); err != nil {
			t.Fatal(err)
		}
		repair, err := NewTerminalDeliveryRepair(queuePool, "river")
		if err != nil {
			t.Fatal(err)
		}
		tx, err := queuePool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		explainQuery := repair.query
		explainQuery = strings.ReplaceAll(explainQuery, "$1", "'"+now.Add(time.Second).Format(time.RFC3339Nano)+"'::timestamptz")
		explainQuery = strings.ReplaceAll(explainQuery, "$2", "10")
		explainQuery = strings.ReplaceAll(explainQuery, "$3", "'Stuck job rescued by JobRescuer'::text")
		explainQuery = strings.ReplaceAll(explainQuery, "$4", "'river_unhandled_rescue'::text")
		rows, err := tx.Query(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) "+explainQuery)
		if err != nil {
			t.Fatal(err)
		}
		var plan string
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			plan += line + "\n"
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(plan, "ix_sync_runs_status_id") ||
			!strings.Contains(plan, "uq_sync_dispatch_outbox_run_kind") {
			t.Fatalf("repair plan did not use bounded active-run indexes:\n%s", plan)
		}
	})
}

func createKernelIntegrationFixture(ctx context.Context, pool *pgxpool.Pool) error {
	for _, statement := range []string{
		"REVOKE TEMPORARY ON DATABASE worker_test FROM PUBLIC",
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		"CREATE ROLE " + kernelDomainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + kernelDomainPassword + "'",
		"CREATE ROLE " + kernelQueueRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + kernelQueuePassword + "'",
		"GRANT CONNECT ON DATABASE worker_test TO " + kernelDomainRole + ", " + kernelQueueRole,
		"CREATE TABLE public.integrations (id uuid PRIMARY KEY)",
		"CREATE TABLE public.integration_sources (id uuid PRIMARY KEY)",
		"CREATE TABLE public.integration_datasets (id uuid PRIMARY KEY)",
		"CREATE TABLE public.integration_credentials (id uuid PRIMARY KEY)",
		"CREATE TABLE public.provider_oauth_credentials (id uuid PRIMARY KEY)",
		"CREATE TABLE public.sync_runs (id uuid PRIMARY KEY, status text NOT NULL)",
		"CREATE INDEX ix_sync_runs_status_id ON public.sync_runs (status, id)",
		// worker_job_routes is coordinator-exclusive under the Option B split
		// (role-partition manifest, removed in e23ede618; see git history at
		// eda2d6b91) — created but never granted to the domain role here.
		"CREATE TABLE public.worker_job_routes (id uuid PRIMARY KEY)",
		"CREATE TABLE public.sync_run_units (id uuid PRIMARY KEY, state text NOT NULL)",
		// Migration 0088 (#1529) added this table to domainPosture's manifest,
		// so the CheckDomainAuthorization readiness call each test in this
		// package makes fails closed on its absence — the
		// table has to exist here even though this fixture never writes to it,
		// exactly as it has to exist in a deployment before domain workers
		// start. ApplyPinnedMigrations' domain GRANT for it is guarded by
		// `IF to_regclass(...) IS NOT NULL`, so without this CREATE the role
		// silently receives no grant and readiness reports the opaque
		// "PostgreSQL readiness check failed".
		"CREATE TABLE public.sync_run_unit_effect_snapshots (sync_run_unit_id uuid PRIMARY KEY)",
		"CREATE TABLE public.sync_watermarks (id uuid PRIMARY KEY, state text NOT NULL)",
		"CREATE TABLE public.worker_job_outbox (id uuid PRIMARY KEY, state text NOT NULL)",
		"CREATE TABLE public.worker_job_completion_fences (completion_key text PRIMARY KEY)",
		"CREATE TABLE public.sync_configurations (id bigint PRIMARY KEY)",
		"CREATE TABLE public.scheduled_jobs (id uuid PRIMARY KEY)",
		"CREATE TABLE public.scheduled_report_occurrences (occurrence_id text PRIMARY KEY)",
		"CREATE TABLE public.backfill_jobs (id uuid PRIMARY KEY)",
		"CREATE TABLE public.sync_coverage_projections (id uuid PRIMARY KEY)",
		"CREATE TABLE public.organizations (id bigint PRIMARY KEY)",
		"CREATE TABLE public.remaining_metric_runs (id bigint PRIMARY KEY)",
		"CREATE TABLE public.remaining_metric_partitions (id bigint PRIMARY KEY)",
		"CREATE TABLE public.work_graph_execution_requests (id bigint PRIMARY KEY)",
		"CREATE TABLE public.work_graph_execution_ledger (id bigint PRIMARY KEY)",
		// CHAOS-3033 Option B manifest additions — domain-exclusive tables
		// (role-partition manifest, removed in e23ede618; see git history at eda2d6b91).
		"CREATE TABLE public.billing_notifications (id bigint PRIMARY KEY)",
		"CREATE TABLE public.daily_metrics_partitions (id bigint PRIMARY KEY)",
		"CREATE TABLE public.daily_metrics_runs (id bigint PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_batch_payloads (id bigint PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_batches (id bigint PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_recompute_jobs (id bigint PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_rejections (id bigint PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_sources (id bigint PRIMARY KEY)",
		"CREATE TABLE public.feature_flags (id bigint PRIMARY KEY)",
		"CREATE TABLE public.org_feature_overrides (id bigint PRIMARY KEY)",
		"CREATE TABLE public.org_licenses (id bigint PRIMARY KEY)",
		"CREATE TABLE public.dev_conversations (id uuid PRIMARY KEY)",
		"CREATE TABLE public.dev_conversation_tombstones (id uuid PRIMARY KEY)",
		"CREATE TABLE public.provider_rate_limit_observations (id bigint PRIMARY KEY)",
		"CREATE TABLE public.report_runs (id bigint PRIMARY KEY)",
		"CREATE TABLE public.saved_reports (id bigint PRIMARY KEY)",
		"CREATE TABLE public.webhook_deliveries (id bigint PRIMARY KEY)",
		"CREATE TABLE public.worker_job_runs (id bigint PRIMARY KEY)",
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
		`CREATE UNIQUE INDEX uq_sync_dispatch_outbox_run_kind
		 ON public.sync_dispatch_outbox (sync_run_id, kind)`,
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
		"TRUNCATE public.sync_runs",
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
	seedKernelOutboxKind(
		t,
		ctx,
		pool,
		id,
		syncdispatchcontract.KindDispatchSyncRun,
		availableAt,
	)
}

func seedKernelOutboxKind(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	id string,
	kind string,
	availableAt time.Time,
) {
	t.Helper()
	if _, err := pool.Exec(
		ctx,
		`INSERT INTO public.sync_runs (id, status)
		 VALUES ($1, 'dispatching') ON CONFLICT (id) DO NOTHING`,
		id,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(
		ctx,
		`INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts, created_at, updated_at
		) VALUES ($1, 'org-integration', $1,
			$3, 'pending', $2, 0, $2, $2)`,
		id,
		availableAt,
		kind,
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
