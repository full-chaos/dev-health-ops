//go:build integration

package syncreconciler

import (
	"context"
	"strconv"
	"testing"
	"time"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

const (
	exhaustionRunID = "00000000-0000-4000-8000-000000004401"
	// A coordinator bridge failure carries the transport error text, never
	// River's maintenance-rescue sentinel. CHAOS-3951's wedge is exactly the
	// case where these two differ.
	exhaustionBridgeError = "sync dispatch bridge request failed: status=503"
)

// TestTerminalDeliveryRepairReclaimsExhaustedCoordinatorDelivery pins
// CHAOS-3951 against the real River schema and the least-privilege queue role.
//
// The subtests deliberately assert WHICH recovery branch fired by reading the
// durable evidence code back off the row, not merely that the row returned to
// 'pending'. A repair that stamped one shared code, or that reclaimed every
// discarded job, would drive the outbox to the same status and pass a
// state-only assertion while being wrong in a way that either hides the wedge
// or resurrects a deliberately permanent failure.
func TestTerminalDeliveryRepairReclaimsExhaustedCoordinatorDelivery(t *testing.T) {
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
	queuePool, err := pgxpool.New(
		ctx,
		kernelRoleURI(t, instance.URI, kernelQueueRole, kernelQueuePassword),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer queuePool.Close()
	if err := postgresstore.CheckQueueAuthorization(ctx, queuePool, kernelQueueRole, "river"); err != nil {
		t.Fatalf("queue authorization: %v", err)
	}
	riverClient, err := river.NewClient(
		riverpgxv5.New(adminPool),
		&river.Config{Schema: "river"},
	)
	if err != nil {
		t.Fatal(err)
	}
	repair, err := NewTerminalDeliveryRepair(queuePool, "river")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)

	t.Run("exhausted delivery is reclaimed under its own evidence code", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, adminPool)
		jobID := seedExhaustionDelivery(t, ctx, adminPool, riverClient, now, "running")
		discardRiverJob(t, ctx, adminPool, jobID, now, 5, 5, exhaustionBridgeError)

		result, err := repair.Step(ctx, now, 10)
		if err != nil {
			t.Fatalf("repair step: %v", err)
		}
		if result.Recovered != 1 || result.ExhaustedRecovered != 1 {
			t.Fatalf("result = %#v, want one recovery counted as exhausted", result)
		}
		assertOutboxDelivery(t, ctx, adminPool, "pending", riverDeliveryExhaustedEvidence, false)
	})

	t.Run("permanent discard before exhaustion stays excluded", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, adminPool)
		jobID := seedExhaustionDelivery(t, ctx, adminPool, riverClient, now, "running")
		// River discards a jobruntime.Permanent failure with attempts still on
		// the clock. That is a deliberate terminal outcome, not a wedge, and
		// reclaiming it would loop a failure the worker declared unretryable.
		discardRiverJob(t, ctx, adminPool, jobID, now, 2, 5, exhaustionBridgeError)

		result, err := repair.Step(ctx, now, 10)
		if err != nil {
			t.Fatalf("repair step: %v", err)
		}
		if result != (TerminalDeliveryRepairResult{}) {
			t.Fatalf("result = %#v, want no recovery", result)
		}
		assertOutboxDelivery(t, ctx, adminPool, "dispatched", "", true)
	})

	t.Run("unhandled rescue keeps the pre-existing rescue evidence code", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, adminPool)
		jobID := seedExhaustionDelivery(t, ctx, adminPool, riverClient, now, "running")
		discardRiverJob(t, ctx, adminPool, jobID, now, 2, 5, riverUnhandledRescueError)

		result, err := repair.Step(ctx, now, 10)
		if err != nil {
			t.Fatalf("repair step: %v", err)
		}
		if result.Recovered != 1 || result.ExhaustedRecovered != 0 {
			t.Fatalf("result = %#v, want one recovery not counted as exhausted", result)
		}
		assertOutboxDelivery(t, ctx, adminPool, "pending", riverUnhandledRescueEvidence, false)
	})

	t.Run("exhausted delivery on a terminal run is never reclaimed", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, adminPool)
		jobID := seedExhaustionDelivery(t, ctx, adminPool, riverClient, now, "success")
		discardRiverJob(t, ctx, adminPool, jobID, now, 5, 5, exhaustionBridgeError)

		result, err := repair.Step(ctx, now, 10)
		if err != nil {
			t.Fatalf("repair step: %v", err)
		}
		if result != (TerminalDeliveryRepairResult{}) {
			t.Fatalf("result = %#v, want no recovery", result)
		}
		assertOutboxDelivery(t, ctx, adminPool, "dispatched", "", true)
	})

	t.Run("live job at the attempt ceiling is never reclaimed", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, adminPool)
		jobID := seedExhaustionDelivery(t, ctx, adminPool, riverClient, now, "running")
		// Same attempt arithmetic as an exhausted job, but River has not
		// finalized it: the fifth attempt is still executing.
		if _, err := adminPool.Exec(
			ctx,
			`UPDATE river.river_job
			 SET attempt = 5, max_attempts = 5, state = 'running'
			 WHERE id = $1`,
			jobID,
		); err != nil {
			t.Fatal(err)
		}

		result, err := repair.Step(ctx, now, 10)
		if err != nil {
			t.Fatalf("repair step: %v", err)
		}
		if result != (TerminalDeliveryRepairResult{}) {
			t.Fatalf("result = %#v, want no recovery", result)
		}
		assertOutboxDelivery(t, ctx, adminPool, "dispatched", "", true)
	})

	t.Run("exhausted delivery on a paused route is never reclaimed", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, adminPool)
		jobID := seedExhaustionDelivery(t, ctx, adminPool, riverClient, now, "running")
		discardRiverJob(t, ctx, adminPool, jobID, now, 5, 5, exhaustionBridgeError)
		if _, err := adminPool.Exec(
			ctx,
			`UPDATE public.sync_dispatch_transport_routes
			 SET paused = TRUE, paused_at = $1
			 WHERE kind = 'dispatch_sync_run'`,
			now,
		); err != nil {
			t.Fatal(err)
		}

		result, err := repair.Step(ctx, now, 10)
		if err != nil {
			t.Fatalf("repair step: %v", err)
		}
		if result != (TerminalDeliveryRepairResult{}) {
			t.Fatalf("result = %#v, want no recovery", result)
		}
		assertOutboxDelivery(t, ctx, adminPool, "dispatched", "", true)
	})
}

// seedExhaustionDelivery reproduces the state a coordinator delivery is left in
// once the Kernel has published it: the outbox row is 'dispatched' and points
// at a live River job through transport_job_id and the route generation.
func seedExhaustionDelivery(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	client *river.Client[pgx.Tx],
	now time.Time,
	runStatus string,
) int64 {
	t.Helper()
	seedKernelOutbox(t, ctx, pool, exhaustionRunID, now.Add(-time.Hour))
	if _, err := pool.Exec(
		ctx,
		`UPDATE public.sync_runs SET status = $2 WHERE id = $1`,
		exhaustionRunID,
		runStatus,
	); err != nil {
		t.Fatal(err)
	}
	inserted, err := client.Insert(ctx, kernelRiverArgs{OutboxID: exhaustionRunID}, &river.InsertOpts{
		Queue: "sync",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(
		ctx,
		`UPDATE public.sync_dispatch_outbox
		 SET status = 'dispatched',
			 dispatched_at = $2,
			 dispatched_transport = 'river',
			 dispatched_route_generation = 7,
			 transport_job_id = $3,
			 claim_token = NULL,
			 claim_expires_at = NULL,
			 claim_transport = NULL,
			 claim_route_generation = NULL,
			 last_error = NULL,
			 updated_at = $2
		 WHERE id = $1`,
		exhaustionRunID,
		now.Add(-time.Hour),
		strconv.FormatInt(inserted.Job.ID, 10),
	); err != nil {
		t.Fatal(err)
	}
	return inserted.Job.ID
}

// discardRiverJob drives a River row into the exact terminal shape the repair
// reads: discarded, finalized, with a bounded error history whose last entry
// carries the supplied text.
func discardRiverJob(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	jobID int64,
	now time.Time,
	attempt int,
	maxAttempts int,
	errorText string,
) {
	t.Helper()
	if _, err := pool.Exec(
		ctx,
		`UPDATE river.river_job
		 SET state = 'discarded',
			 finalized_at = $2,
			 attempt = $3,
			 max_attempts = $4,
			 errors = ARRAY[jsonb_build_object('error', $5::text, 'attempt', $6::int)]
		 WHERE id = $1`,
		jobID,
		now.Add(-time.Minute),
		attempt,
		maxAttempts,
		errorText,
		attempt,
	); err != nil {
		t.Fatal(err)
	}
}

func assertOutboxDelivery(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	wantStatus string,
	wantError string,
	wantDeliveryRetained bool,
) {
	t.Helper()
	var (
		status      string
		lastError   *string
		transportID *string
		dispatchAt  *time.Time
	)
	if err := pool.QueryRow(
		ctx,
		`SELECT status, last_error, transport_job_id, dispatched_at
		 FROM public.sync_dispatch_outbox WHERE id = $1`,
		exhaustionRunID,
	).Scan(&status, &lastError, &transportID, &dispatchAt); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus {
		t.Fatalf("outbox status = %q, want %q", status, wantStatus)
	}
	if wantError == "" {
		if lastError != nil {
			t.Fatalf("outbox last_error = %q, want unset", *lastError)
		}
	} else if lastError == nil || *lastError != wantError {
		t.Fatalf("outbox last_error = %v, want %q", lastError, wantError)
	}
	if wantDeliveryRetained {
		if transportID == nil || dispatchAt == nil {
			t.Fatal("delivery linkage was cleared on a row that must stay dispatched")
		}
		return
	}
	if transportID != nil || dispatchAt != nil {
		t.Fatalf(
			"reclaimed row still points at its delivery: transport_job_id=%v dispatched_at=%v",
			transportID,
			dispatchAt,
		)
	}
}
