//go:build integration

package syncreconciler

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

// postSyncJobArgs are the real post_sync River args of the seeded delivery: the repair finds the outbox row through
// the job's sync_run_id.
func postSyncJobArgs(runID string, attempt int64) syncdispatchruntime.PostSyncArgs {
	return syncdispatchruntime.PostSyncArgs{TransportArgs: syncdispatchruntime.TransportArgs{
		Version: 1, OrgID: "00000000-0000-4000-8000-0000000044f0", RunID: runID,
		DispatchOutbox: runID, DeliveryAttempt: attempt, RouteGeneration: 7,
	}}
}

type postSyncRepairRig struct {
	admin, domain, queue *pgxpool.Pool
	client               *river.Client[pgx.Tx]
	repair               *TerminalDeliveryRepair
}

func startPostSyncRepairRig(t *testing.T, ctx context.Context) *postSyncRepairRig {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	domainRole, err := containers.RoleName("kernel_domain_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err := containers.RoleName("kernel_queue_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	t.Cleanup(func() { containers.DropRole(admin, domainRole, t.Logf) })
	t.Cleanup(func() { containers.DropRole(admin, queueRole, t.Logf) })
	if err := createKernelIntegrationFixture(ctx, admin, domainRole, queueRole, dbName); err != nil {
		t.Fatal(err)
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema: "river", DomainRole: domainRole, QueueRole: queueRole,
	}); err != nil {
		t.Fatal(err)
	}
	queue, err := pgxpool.New(ctx, kernelRoleURI(t, instance.URI, queueRole, kernelQueuePassword))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(queue.Close)
	if err := postgresstore.CheckQueueAuthorization(ctx, queue, queueRole, "river"); err != nil {
		t.Fatalf("queue authorization: %v", err)
	}
	domain, err := pgxpool.New(ctx, kernelRoleURI(t, instance.URI, domainRole, kernelDomainPassword))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(domain.Close)
	client, err := river.NewClient(riverpgxv5.New(admin), &river.Config{Schema: "river"})
	if err != nil {
		t.Fatal(err)
	}
	repair, err := NewTerminalDeliveryRepair(queue, admin, "river")
	if err != nil {
		t.Fatal(err)
	}
	return &postSyncRepairRig{admin: admin, domain: domain, queue: queue, client: client, repair: repair}
}

// seedPostSyncDelivery resets the tables, routes post_sync to River at generation 7 and seeds one post_sync outbox
// row created at createdAt, dispatched at attempt 1 to a River job of its kind, on a run in runStatus. It returns
// the job id.
func (rig *postSyncRepairRig) seedPostSyncDelivery(
	t *testing.T, ctx context.Context, now time.Time, runStatus string, createdAt time.Time,
) int64 {
	t.Helper()
	resetKernelIntegrationTables(t, ctx, rig.admin)
	if _, err := rig.admin.Exec(ctx,
		`UPDATE public.sync_dispatch_transport_routes SET transport = 'river', generation = 7 WHERE kind = 'post_sync'`,
	); err != nil {
		t.Fatal(err)
	}
	seedKernelOutboxKind(t, ctx, rig.admin, exhaustionRunID, syncdispatchcontract.KindPostSync, createdAt)
	inserted, err := rig.client.Insert(ctx, postSyncJobArgs(exhaustionRunID, 1), &river.InsertOpts{Queue: "sync"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rig.admin.Exec(ctx, `UPDATE public.sync_runs SET status = $2 WHERE id = $1`, exhaustionRunID, runStatus); err != nil {
		t.Fatal(err)
	}
	if _, err := rig.admin.Exec(ctx,
		`UPDATE public.sync_dispatch_outbox
		 SET status = 'dispatched', attempts = 1, created_at = $2, dispatched_at = $3, dispatched_transport = 'river',
		     dispatched_route_generation = 7, transport_job_id = $4, updated_at = $3
		 WHERE id = $1`,
		exhaustionRunID, createdAt, now.Add(-time.Hour), strconv.FormatInt(inserted.Job.ID, 10),
	); err != nil {
		t.Fatal(err)
	}
	return inserted.Job.ID
}

func (rig *postSyncRepairRig) step(t *testing.T, ctx context.Context, now time.Time) TerminalDeliveryRepairResult {
	t.Helper()
	result, err := rig.repair.Step(ctx, now, 10)
	if err != nil {
		t.Fatalf("repair step: %v", err)
	}
	return result
}

// TestTerminalDeliveryRepairReclaimsExhaustedPostSyncDeliveryOnTerminalRun pins CHAOS-14115: a post_sync delivery
// whose River job spent its attempt budget is re-armed on the terminal run it always belongs to, only inside the
// re-drive window, only on the exhausted proof, and an older one is marked and counted once. The other kinds keep
// the live-run rule.
func TestTerminalDeliveryRepairReclaimsExhaustedPostSyncDeliveryOnTerminalRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	rig := startPostSyncRepairRig(t, ctx)
	now := time.Date(2026, time.November, 18, 12, 0, 0, 0, time.UTC)
	inWindow := now.Add(-6 * 24 * time.Hour)

	t.Run("control: exhausted dispatch_sync_run on a running run is repaired", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, rig.admin)
		jobID := seedExhaustionDelivery(t, ctx, rig.admin, rig.client, now, "running")
		discardRiverJob(t, ctx, rig.admin, jobID, now, 5, 5, exhaustionBridgeError)
		result := rig.step(t, ctx, now)
		if result.Recovered != 1 || result.ExhaustedRecovered != 1 || result.PostSyncRearmed != 0 {
			t.Fatalf("result = %#v, want one exhausted recovery, no post_sync", result)
		}
		assertOutboxDelivery(t, ctx, rig.admin, "pending", riverDeliveryExhaustedEvidence, false)
	})

	for _, runStatus := range []string{"success", "partial_failed", "failed"} {
		t.Run("A1 exhausted post_sync on a "+runStatus+" run is re-armed", func(t *testing.T) {
			jobID := rig.seedPostSyncDelivery(t, ctx, now, runStatus, inWindow)
			discardRiverJob(t, ctx, rig.admin, jobID, now, 5, 5, "post_sync fanout unavailable")
			result := rig.step(t, ctx, now)
			if result.Recovered != 1 || result.ExhaustedRecovered != 1 || result.PostSyncRearmed != 1 || result.PostSyncSkippedWindow != 0 {
				t.Fatalf("result = %#v, want one exhausted post_sync re-arm", result)
			}
			assertOutboxDelivery(t, ctx, rig.admin, "pending", riverDeliveryExhaustedEvidence, false)
		})
	}

	t.Run("A2 exhausted post_sync on a running run is not re-armed", func(t *testing.T) {
		jobID := rig.seedPostSyncDelivery(t, ctx, now, "running", inWindow)
		discardRiverJob(t, ctx, rig.admin, jobID, now, 5, 5, "post_sync fanout unavailable")
		if result := rig.step(t, ctx, now); result != (TerminalDeliveryRepairResult{}) {
			t.Fatalf("result = %#v, want nothing", result)
		}
		assertOutboxDelivery(t, ctx, rig.admin, "dispatched", "", true)
	})

	t.Run("A3 exhausted dispatch_sync_run on a success run is not re-armed", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, rig.admin)
		jobID := seedExhaustionDelivery(t, ctx, rig.admin, rig.client, now, "success")
		discardRiverJob(t, ctx, rig.admin, jobID, now, 5, 5, exhaustionBridgeError)
		if result := rig.step(t, ctx, now); result != (TerminalDeliveryRepairResult{}) {
			t.Fatalf("result = %#v, want nothing", result)
		}
		assertOutboxDelivery(t, ctx, rig.admin, "dispatched", "", true)
	})

	t.Run("A4 outside the window: marked once, counted once, never re-armed", func(t *testing.T) {
		jobID := rig.seedPostSyncDelivery(t, ctx, now, "success", now.Add(-8*24*time.Hour))
		discardRiverJob(t, ctx, rig.admin, jobID, now, 5, 5, "post_sync fanout unavailable")
		result := rig.step(t, ctx, now)
		if result.Recovered != 0 || result.PostSyncRearmed != 0 || result.PostSyncSkippedWindow != 1 {
			t.Fatalf("first pass result = %#v, want one skipped_window and no recovery", result)
		}
		assertOutboxDelivery(t, ctx, rig.admin, "dispatched", riverDeliveryExhaustedOutsideWindowEvidence, true)
		var generation int64
		var transportJob string
		if err := rig.admin.QueryRow(ctx,
			`SELECT dispatched_route_generation, transport_job_id FROM public.sync_dispatch_outbox WHERE id = $1`, exhaustionRunID,
		).Scan(&generation, &transportJob); err != nil {
			t.Fatal(err)
		}
		if generation != 7 || transportJob != strconv.FormatInt(jobID, 10) {
			t.Fatalf("marked row linkage gen=%d job=%s, want 7/%d", generation, transportJob, jobID)
		}
		if again := rig.step(t, ctx, now.Add(time.Minute)); again != (TerminalDeliveryRepairResult{}) {
			t.Fatalf("second pass result = %#v, want nothing: a marked row is counted once", again)
		}
	})

	t.Run("A4 window edge: one hour inside is re-armed", func(t *testing.T) {
		jobID := rig.seedPostSyncDelivery(t, ctx, now, "success", now.Add(-postSyncRedriveWindow+time.Hour))
		discardRiverJob(t, ctx, rig.admin, jobID, now, 5, 5, "post_sync fanout unavailable")
		if result := rig.step(t, ctx, now); result.PostSyncRearmed != 1 || result.PostSyncSkippedWindow != 0 {
			t.Fatalf("result = %#v, want one re-arm", result)
		}
		assertOutboxDelivery(t, ctx, rig.admin, "pending", riverDeliveryExhaustedEvidence, false)
	})

	t.Run("A5 permanent discard before exhaustion is not re-armed", func(t *testing.T) {
		jobID := rig.seedPostSyncDelivery(t, ctx, now, "success", inWindow)
		discardRiverJob(t, ctx, rig.admin, jobID, now, 2, 5, "post_sync fanout unavailable")
		if result := rig.step(t, ctx, now); result != (TerminalDeliveryRepairResult{}) {
			t.Fatalf("result = %#v, want nothing", result)
		}
		assertOutboxDelivery(t, ctx, rig.admin, "dispatched", "", true)
	})

	t.Run("A5 unhandled rescue on a terminal run is not re-armed", func(t *testing.T) {
		jobID := rig.seedPostSyncDelivery(t, ctx, now, "success", inWindow)
		discardRiverJob(t, ctx, rig.admin, jobID, now, 2, 5, riverUnhandledRescueError)
		if result := rig.step(t, ctx, now); result != (TerminalDeliveryRepairResult{}) {
			t.Fatalf("result = %#v, want nothing", result)
		}
		assertOutboxDelivery(t, ctx, rig.admin, "dispatched", "", true)
	})

	t.Run("A5 rescue-only cancel on a terminal run is not re-armed", func(t *testing.T) {
		jobID := rig.seedPostSyncDelivery(t, ctx, now, "success", inWindow)
		cancelRiverJob(t, ctx, rig.admin, jobID, now, riverRescueOnlyCancelError, false)
		if result := rig.step(t, ctx, now); result != (TerminalDeliveryRepairResult{}) {
			t.Fatalf("result = %#v, want nothing", result)
		}
		assertOutboxDelivery(t, ctx, rig.admin, "dispatched", "", true)
	})

	t.Run("A5 row linked to a newer job is not re-armed or marked", func(t *testing.T) {
		for _, createdAt := range []time.Time{inWindow, now.Add(-8 * 24 * time.Hour)} {
			jobID := rig.seedPostSyncDelivery(t, ctx, now, "success", createdAt)
			discardRiverJob(t, ctx, rig.admin, jobID, now, 5, 5, "post_sync fanout unavailable")
			newer, err := rig.client.Insert(ctx, postSyncJobArgs(exhaustionRunID, 2), &river.InsertOpts{Queue: "sync"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := rig.admin.Exec(ctx, `UPDATE public.sync_dispatch_outbox SET attempts = 2, transport_job_id = $2 WHERE id = $1`,
				exhaustionRunID, strconv.FormatInt(newer.Job.ID, 10)); err != nil {
				t.Fatal(err)
			}
			if result := rig.step(t, ctx, now); result != (TerminalDeliveryRepairResult{}) {
				t.Fatalf("created_at %s: result = %#v, want nothing", createdAt, result)
			}
			assertOutboxDelivery(t, ctx, rig.admin, "dispatched", "", true)
		}
	})

	t.Run("A6 completed post_sync delivery is not re-armed or marked", func(t *testing.T) {
		for _, createdAt := range []time.Time{inWindow, now.Add(-8 * 24 * time.Hour)} {
			jobID := rig.seedPostSyncDelivery(t, ctx, now, "success", createdAt)
			if _, err := rig.admin.Exec(ctx,
				`UPDATE river.river_job SET state = 'completed', finalized_at = $2, attempt = 5, max_attempts = 5,
				 errors = ARRAY[jsonb_build_object('error', 'earlier attempt', 'attempt', 4)] WHERE id = $1`,
				jobID, now.Add(-time.Minute),
			); err != nil {
				t.Fatal(err)
			}
			if result := rig.step(t, ctx, now); result != (TerminalDeliveryRepairResult{}) {
				t.Fatalf("created_at %s: result = %#v, want nothing", createdAt, result)
			}
			assertOutboxDelivery(t, ctx, rig.admin, "dispatched", "", true)
		}
	})
}

// TestReclaimedPostSyncDeliveryRepublishesThroughTheKernel (A7): after the repair re-arms an exhausted post_sync
// delivery, the Kernel claims it at the next attempt and the real Publisher inserts a NEW River job: the retired
// job's unique key (ByArgs incl. delivery_attempt) does not swallow it.
func TestReclaimedPostSyncDeliveryRepublishesThroughTheKernel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	rig := startPostSyncRepairRig(t, ctx)
	now := time.Date(2026, time.November, 18, 12, 0, 0, 0, time.UTC)

	publisher, err := syncdispatchruntime.NewPublisher(rig.client, syncdispatchruntime.PublisherOptions{
		Queue: syncdispatchcontract.RiverQueue, MaxAttempts: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	reference := syncdispatchruntime.DomainReference{
		OrganizationID: "00000000-0000-4000-8000-0000000044f0",
		SyncRunID:      exhaustionRunID,
	}
	claims := []TransportClaim{}
	publish := func(publishCtx context.Context, tx pgx.Tx, claim TransportClaim) (string, error) {
		claims = append(claims, claim)
		return publisher.Publish(publishCtx, tx, syncdispatchruntime.Claim{
			OutboxID: claim.ID, Kind: claim.Kind, RouteGeneration: claim.RouteGeneration, DeliveryAttempt: claim.Attempts,
		}, reference)
	}
	kernel, err := NewKernel(rig.domain, rig.queue, riverRegistry(t, syncdispatchcontract.KindPostSync), KernelModeMutation)
	if err != nil {
		t.Fatal(err)
	}

	resetKernelIntegrationTables(t, ctx, rig.admin)
	if _, err := rig.admin.Exec(ctx,
		`UPDATE public.sync_dispatch_transport_routes SET transport = 'river', generation = 7 WHERE kind = 'post_sync'`,
	); err != nil {
		t.Fatal(err)
	}
	seedKernelOutboxKind(t, ctx, rig.admin, exhaustionRunID, syncdispatchcontract.KindPostSync, now.Add(-2*time.Hour))
	if _, err := rig.admin.Exec(ctx, `UPDATE public.sync_runs SET status = 'success' WHERE id = $1`, exhaustionRunID); err != nil {
		t.Fatal(err)
	}

	first, err := kernel.Step(ctx, now.Add(-time.Hour), 1, time.Minute, publish, nil)
	if err != nil || first.Dispatched != 1 || len(claims) != 1 || claims[0].Attempts != 1 {
		t.Fatalf("first delivery = %#v, %v, claims %#v; want one dispatch at attempt 1", first, err, claims)
	}
	var firstJob string
	if err := rig.admin.QueryRow(ctx, `SELECT transport_job_id FROM public.sync_dispatch_outbox WHERE id = $1`, exhaustionRunID).Scan(&firstJob); err != nil {
		t.Fatal(err)
	}
	discardRiverJob(t, ctx, rig.admin, mustParseJobID(t, firstJob), now, 5, 5, "post_sync fanout unavailable")

	if result := rig.step(t, ctx, now); result.PostSyncRearmed != 1 {
		t.Fatalf("repair = %#v, want one post_sync re-arm", result)
	}
	second, err := kernel.Step(ctx, now, 1, time.Minute, publish, nil)
	if err != nil || second.Dispatched != 1 || len(claims) != 2 || claims[1].Attempts != 2 {
		t.Fatalf("redelivery = %#v, %v, claims %#v; want one dispatch at attempt 2", second, err, claims)
	}
	var secondJob, status string
	var attempts int64
	if err := rig.admin.QueryRow(ctx,
		`SELECT transport_job_id, status, attempts FROM public.sync_dispatch_outbox WHERE id = $1`, exhaustionRunID,
	).Scan(&secondJob, &status, &attempts); err != nil {
		t.Fatal(err)
	}
	if secondJob == firstJob || status != "dispatched" || attempts != 2 {
		t.Fatalf("outbox job=%s status=%s attempts=%d, want a new job (not %s), dispatched, attempt 2", secondJob, status, attempts, firstJob)
	}
	var state string
	var argsAttempt int64
	if err := rig.admin.QueryRow(ctx,
		`SELECT state::text, (args->>'delivery_attempt')::bigint FROM river.river_job WHERE id = $1`, mustParseJobID(t, secondJob),
	).Scan(&state, &argsAttempt); err != nil {
		t.Fatal(err)
	}
	if state != "available" || argsAttempt != 2 {
		t.Fatalf("new job state=%s delivery_attempt=%d, want available/2", state, argsAttempt)
	}
}

// TestPostSyncRedriveUnderTheRealRouteFence runs both post_sync outcomes on the migrated schema, whose route-fence
// trigger rewrites the delivery columns on every outbox UPDATE, as the production queue role: the re-arm clears the
// linkage, and the outside-window mark keeps it.
func TestPostSyncRedriveUnderTheRealRouteFence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	h := startFinalizeBackstopHarness(t, ctx)
	now := time.Date(2026, time.November, 18, 12, 0, 0, 0, time.UTC)

	seed := func(t *testing.T, createdAt time.Time) (int64, int64) {
		t.Helper()
		resetMaterializerIntegrationTables(t, ctx, h.admin)
		if _, err := h.admin.Exec(ctx, `TRUNCATE river.river_job`); err != nil {
			t.Fatal(err)
		}
		backstopRoutes(t, ctx, h.admin)
		var generation int64
		if err := h.admin.QueryRow(ctx, `SELECT generation FROM public.sync_dispatch_transport_routes WHERE kind = 'post_sync'`).Scan(&generation); err != nil {
			t.Fatal(err)
		}
		seedRun(t, ctx, h.admin, materializerFinalize, "success", now.Add(-10*24*time.Hour))
		inserted, err := h.river.Insert(ctx, syncdispatchruntime.PostSyncArgs{TransportArgs: syncdispatchruntime.TransportArgs{
			Version: 1, OrgID: "00000000-0000-4000-8000-000000000001", RunID: materializerFinalize,
			DispatchOutbox: backstopOutboxID, DeliveryAttempt: 1, RouteGeneration: generation,
		}}, &river.InsertOpts{Queue: "sync"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.admin.Exec(ctx, `INSERT INTO public.sync_dispatch_outbox
			(id,org_id,sync_run_id,kind,status,available_at,attempts,dispatched_at,dispatched_transport,dispatched_route_generation,transport_job_id,created_at,updated_at)
			VALUES ($1,'org-materializer',$2,'post_sync','dispatched',$3,1,$3,'river',$5,$4,$3,$3)`,
			backstopOutboxID, materializerFinalize, createdAt, strconv.FormatInt(inserted.Job.ID, 10), generation); err != nil {
			t.Fatal(err)
		}
		discardRiverJob(t, ctx, h.admin, inserted.Job.ID, now, 5, 5, "post_sync fanout unavailable")
		return inserted.Job.ID, generation
	}
	read := func(t *testing.T) (string, *string, *string, *int64) {
		t.Helper()
		var status string
		var lastError, job *string
		var generation *int64
		if err := h.admin.QueryRow(ctx,
			`SELECT status, last_error, transport_job_id, dispatched_route_generation FROM public.sync_dispatch_outbox WHERE id = $1`,
			backstopOutboxID).Scan(&status, &lastError, &job, &generation); err != nil {
			t.Fatal(err)
		}
		return status, lastError, job, generation
	}

	t.Run("inside the window the row is re-armed and unlinked", func(t *testing.T) {
		seed(t, now.Add(-24*time.Hour))
		result, err := h.repair.Step(ctx, now, 10)
		if err != nil || result.PostSyncRearmed != 1 {
			t.Fatalf("repair = %#v, %v; want one post_sync re-arm", result, err)
		}
		status, lastError, job, generation := read(t)
		if status != "pending" || lastError == nil || *lastError != riverDeliveryExhaustedEvidence || job != nil || generation != nil {
			t.Fatalf("row status=%s last_error=%v job=%v gen=%v, want pending/%s/unlinked", status, lastError, job, generation, riverDeliveryExhaustedEvidence)
		}
	})

	t.Run("outside the window the row is marked and keeps its delivery", func(t *testing.T) {
		jobID, wantGeneration := seed(t, now.Add(-8*24*time.Hour))
		result, err := h.repair.Step(ctx, now, 10)
		if err != nil || result.PostSyncSkippedWindow != 1 || result.Recovered != 0 {
			t.Fatalf("repair = %#v, %v; want one skipped_window", result, err)
		}
		status, lastError, job, generation := read(t)
		if status != "dispatched" || lastError == nil || *lastError != riverDeliveryExhaustedOutsideWindowEvidence ||
			job == nil || *job != strconv.FormatInt(jobID, 10) || generation == nil || *generation != wantGeneration {
			t.Fatalf("row status=%s last_error=%v job=%v gen=%v, want dispatched/%s/%d/%d",
				status, lastError, job, generation, riverDeliveryExhaustedOutsideWindowEvidence, jobID, wantGeneration)
		}
		if again, err := h.repair.Step(ctx, now.Add(time.Minute), 10); err != nil || again.PostSyncSkippedWindow != 0 {
			t.Fatalf("second pass = %#v, %v; want the marked row counted once", again, err)
		}
	})
}

// TestPostSyncRedriveIsDrivenFromRiverAgainstRetainedHistory: nothing closes a post_sync row, so the outbox holds one
// 'dispatched' post_sync row per terminal run ever made. Both post_sync statements must start from River's exhausted
// post_sync jobs and reach the outbox row by its (sync_run_id, kind) unique key, never by scanning that history.
func TestPostSyncRedriveIsDrivenFromRiverAgainstRetainedHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	rig := startPostSyncRepairRig(t, ctx)
	now := time.Date(2026, time.November, 18, 12, 0, 0, 0, time.UTC)
	jobID := rig.seedPostSyncDelivery(t, ctx, now, "success", now.Add(-24*time.Hour))
	discardRiverJob(t, ctx, rig.admin, jobID, now, 5, 5, "post_sync fanout unavailable")
	if _, err := rig.admin.Exec(ctx, `
		INSERT INTO public.sync_runs (id, status)
		SELECT (lpad(to_hex(series), 8, '0') || '-0000-4000-8000-' || lpad(to_hex(series), 12, '0'))::uuid, 'success'
		FROM generate_series(1, 50000) AS series`); err != nil {
		t.Fatal(err)
	}
	if _, err := rig.admin.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts, dispatched_at, dispatched_transport,
			dispatched_route_generation, transport_job_id, created_at, updated_at)
		SELECT gen_random_uuid(), 'org-history', run.id, 'post_sync', 'dispatched', $1, 1, $1, 'river', 7, '1', $1, $1
		FROM public.sync_runs AS run WHERE run.id <> $2`, now.Add(-2*time.Hour), exhaustionRunID); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{"ANALYZE public.sync_runs", "ANALYZE public.sync_dispatch_outbox", "ANALYZE river.river_job"} {
		if _, err := rig.admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	cutoff := now.Add(-postSyncRedriveWindow)
	for name, statement := range map[string]struct {
		query    string
		evidence string
	}{
		"rearm": {rig.repair.postSyncRearmQuery, riverDeliveryExhaustedEvidence},
		"mark":  {rig.repair.postSyncMarkQuery, riverDeliveryExhaustedOutsideWindowEvidence},
	} {
		tx, err := rig.queue.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := tx.Query(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) "+statement.query, now, 10, cutoff, statement.evidence)
		if err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("%s: EXPLAIN: %v", name, err)
		}
		plan := ""
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			plan += line + "\n"
		}
		rows.Close()
		_ = tx.Rollback(ctx)
		if !strings.Contains(plan, "uq_sync_dispatch_outbox_run_kind") || strings.Contains(plan, "Seq Scan on sync_dispatch_outbox") {
			t.Fatalf("%s plan does not reach the outbox by its unique key:\n%s", name, plan)
		}
	}
	if result := rig.step(t, ctx, now); result.PostSyncRearmed != 1 || result.PostSyncSkippedWindow != 0 {
		t.Fatalf("repair over retained history = %#v, want the one exhausted delivery re-armed", result)
	}
}
