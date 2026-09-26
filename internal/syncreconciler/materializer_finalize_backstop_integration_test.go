//go:build integration

package syncreconciler

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

const backstopOutboxID = "00000000-0000-4000-8000-000000005456"

const backstopUnitID = "00000000-0000-4000-8000-000000004205"

type finalizeBackstopHarness struct {
	admin, coordinator, queue *pgxpool.Pool
	materializer              *Materializer
	repair                    *TerminalDeliveryRepair
	river                     *river.Client[pgx.Tx]
}

func startFinalizeBackstopHarness(t *testing.T, ctx context.Context) *finalizeBackstopHarness {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(c); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	if err := createMaterializerIntegrationFixture(ctx, t, admin); err != nil {
		t.Fatal(err)
	}
	// The failure-injection trigger is not wanted here; the real route-fence trigger
	// (trg_sync_dispatch_outbox_route_fence) and the seeded route table stay in place.
	if _, err := admin.Exec(ctx, `DROP TRIGGER materializer_failure ON public.sync_dispatch_outbox`); err != nil {
		t.Fatal(err)
	}
	backstopRoutes(t, ctx, admin)
	// rivermigrate does not create a non-default schema; without this the
	// migration fails with SQLSTATE 3F000 during fixture setup, which is a
	// harness failure and not product RED.
	if _, err := admin.Exec(ctx, `CREATE SCHEMA river`); err != nil {
		t.Fatal(err)
	}
	migrator, err := rivermigrate.New(riverpgxv5.New(admin), &rivermigrate.Config{Schema: "river"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		t.Fatal(err)
	}
	db, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	present := map[string]bool{"sync_runs": true, "sync_run_units": true, "scheduled_sync_occurrences": true,
		"sync_run_reference_discoveries": true, "sync_run_post_dispatches": true,
		"sync_dispatch_outbox": true, "sync_dispatch_transport_routes": true}
	pools := make([]*pgxpool.Pool, 0, 2)
	for _, cfg := range []struct {
		name    string
		posture postgres.RolePosture
		isQueue bool
	}{
		{"finalize_coordinator", postgres.CoordinatorPosture(), false},
		{"finalize_queue", postgres.QueuePosture(), true},
	} {
		role, err := containers.RoleName(cfg.name, instance)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
		statements := []string{
			"CREATE ROLE " + role + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + splitCoordinatorPass + "'",
			"GRANT CONNECT ON DATABASE " + db + " TO " + role,
			"GRANT USAGE ON SCHEMA public TO " + role,
		}
		statements = append(statements, splitGrantStatements(role, cfg.posture, present)...)
		if cfg.isQueue {
			statements = append(statements, "GRANT USAGE ON SCHEMA river TO "+role, "GRANT SELECT,UPDATE ON ALL TABLES IN SCHEMA river TO "+role)
		}
		for _, sql := range statements {
			if _, err := admin.Exec(ctx, sql); err != nil {
				t.Fatal(err)
			}
		}
		pool, err := pgxpool.New(ctx, kernelRoleURI(t, instance.URI, role, splitCoordinatorPass))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		pools = append(pools, pool)
	}
	materializer, err := NewMaterializer(pools[0])
	if err != nil {
		t.Fatal(err)
	}
	repair, err := NewTerminalDeliveryRepair(pools[1], pools[0], "river")
	if err != nil {
		t.Fatal(err)
	}
	client, err := river.NewClient(riverpgxv5.New(admin), &river.Config{Schema: "river"})
	if err != nil {
		t.Fatal(err)
	}
	return &finalizeBackstopHarness{admin: admin, coordinator: pools[0], queue: pools[1], materializer: materializer, repair: repair, river: client}
}

func (h *finalizeBackstopHarness) seed(t *testing.T, ctx context.Context, now time.Time, state string) int64 {
	t.Helper()
	resetMaterializerIntegrationTables(t, ctx, h.admin)
	if _, err := h.admin.Exec(ctx, `TRUNCATE river.river_job`); err != nil {
		t.Fatal(err)
	}
	generation := backstopRoutes(t, ctx, h.admin)
	seedRun(t, ctx, h.admin, materializerFinalize, "dispatching", now.Add(-48*time.Hour))
	seedUnit(t, ctx, h.admin, backstopUnitID, materializerFinalize, "success", nil, now.Add(-24*time.Hour))
	if _, err := h.admin.Exec(ctx, `UPDATE public.sync_runs SET triggered_by='schedule' WHERE id=$1`, materializerFinalize); err != nil {
		t.Fatal(err)
	}
	seedCompletedOccurrence(t, ctx, h.admin, "backstop-occurrence", materializerFinalize, "00000000-0000-4000-8000-000000005457")
	args := syncdispatchruntime.FinalizeSyncRunArgs{TransportArgs: syncdispatchruntime.TransportArgs{
		Version: 1, OrgID: "00000000-0000-4000-8000-000000000001", RunID: materializerFinalize,
		DispatchOutbox: backstopOutboxID, DeliveryAttempt: 38, RouteGeneration: generation}}
	inserted, err := h.river.Insert(ctx, args, &river.InsertOpts{Queue: "sync"})
	if err != nil {
		t.Fatal(err)
	}
	jobID := inserted.Job.ID
	if state == "missing" {
		// Model River's cleaner removing a completed delivery, not an
		// invented ID that was never produced by River.
		if _, err := h.admin.Exec(ctx, `UPDATE river.river_job SET state='completed',finalized_at=$2 WHERE id=$1`, jobID, now.Add(-30*time.Hour)); err != nil {
			t.Fatal(err)
		}
		if _, err := h.admin.Exec(ctx, `DELETE FROM river.river_job WHERE id=$1`, jobID); err != nil {
			t.Fatal(err)
		}
	} else if state == "completed" || state == "cancelled" {
		if _, err := h.admin.Exec(ctx, `UPDATE river.river_job SET state=$2,finalized_at=$3 WHERE id=$1`, jobID, state, now.Add(-30*time.Hour)); err != nil {
			t.Fatal(err)
		}
	} else if _, err := h.admin.Exec(ctx, `UPDATE river.river_job SET state=$2 WHERE id=$1`, jobID, state); err != nil {
		t.Fatal(err)
	}
	if _, err := h.admin.Exec(ctx, `INSERT INTO public.sync_dispatch_outbox
        (id,org_id,sync_run_id,kind,status,available_at,attempts,dispatched_at,dispatched_transport,dispatched_route_generation,transport_job_id,created_at,updated_at)
        VALUES ($1,'org-materializer',$2,'finalize_sync_run','dispatched',$3,38,$3,'river',$5,$4,$3,$3)`,
		backstopOutboxID, materializerFinalize, now.Add(-30*time.Hour), strconv.FormatInt(jobID, 10), generation); err != nil {
		t.Fatal(err)
	}
	return jobID
}

func (h *finalizeBackstopHarness) status(t *testing.T, ctx context.Context) string {
	t.Helper()
	var status string
	if err := h.admin.QueryRow(ctx, `SELECT status FROM public.sync_dispatch_outbox WHERE id=$1`, backstopOutboxID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

func TestMaterializerRearmsReadyRunAfterStaleRiverFinalize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	h := startFinalizeBackstopHarness(t, ctx)
	now := time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC)
	for _, state := range []string{"completed", "missing"} {
		t.Run(state, func(t *testing.T) {
			h.seed(t, ctx, now, state)
			t.Logf("%s queue recovery attached; delivery=%s", time.Now().UTC().Format(time.RFC3339Nano), state)
			recovered, err := h.repair.Step(ctx, now, 20)
			if err != nil {
				t.Fatal(err)
			}
			result, err := h.materializer.Step(ctx, now, now.Add(-15*time.Minute), 20)
			t.Logf("%s pipeline returned recovery=%+v materializer=%+v error=%v", time.Now().UTC().Format(time.RFC3339Nano), recovered, result, err)
			if err != nil {
				t.Fatalf("materializer must execute through real route fence: %v", err)
			}
			if status := h.status(t, ctx); status != "pending" {
				t.Fatalf("ready run has no completion wakeup: outbox=%s recovered=%d; want pending", status, recovered.Recovered)
			}
			var evidence string
			var attempts int
			if err := h.admin.QueryRow(ctx, `SELECT last_error,attempts FROM public.sync_dispatch_outbox WHERE id=$1`, backstopOutboxID).Scan(&evidence, &attempts); err != nil {
				t.Fatal(err)
			}
			if evidence != "ready_finalize_delivery_"+state || attempts != 38 {
				t.Fatalf("evidence=%s attempts=%d", evidence, attempts)
			}
			// The backstop's own count is reported separately from the three
			// River-terminal branches, and is also included in the total.
			if recovered.ReadyFinalizersRecovered != 1 || recovered.Recovered != 1 ||
				recovered.ExhaustedRecovered != 0 || recovered.RescueOnlyCancelsRecovered != 0 {
				t.Fatalf("recovery attribution=%+v", recovered)
			}
			// The re-armed row must carry no residue of the dead delivery: a
			// surviving transport_job_id would let the next pass read a
			// reaped job id as live evidence.
			var residue int
			if err := h.admin.QueryRow(ctx, `SELECT count(*) FROM public.sync_dispatch_outbox
			    WHERE id=$1 AND (dispatched_at IS NOT NULL OR dispatched_transport IS NOT NULL
			        OR dispatched_route_generation IS NOT NULL OR transport_job_id IS NOT NULL
			        OR claim_token IS NOT NULL OR claim_expires_at IS NOT NULL
			        OR claim_transport IS NOT NULL OR claim_route_generation IS NOT NULL)`,
				backstopOutboxID).Scan(&residue); err != nil {
				t.Fatal(err)
			}
			if residue != 0 {
				t.Fatal("re-armed row still carries delivery columns")
			}
			second, err := h.repair.Step(ctx, now, 20)
			if err != nil || second.Recovered != 0 {
				t.Fatalf("repeated pass=%+v err=%v", second, err)
			}
		})
	}
}

func TestReadyFinalizeRepairPreservesDeliveryAndDomainFences(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	h := startFinalizeBackstopHarness(t, ctx)
	now := time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name, state, sql string
		args             []any
	}{
		{name: "available delivery", state: "available"},
		{name: "running delivery", state: "running"},
		{name: "scheduled delivery", state: "scheduled"},
		{name: "retryable delivery", state: "retryable"},
		{name: "operator cancellation", state: "cancelled"},
		{name: "recent delivery", state: "missing", sql: `UPDATE sync_dispatch_outbox SET dispatched_at=$1 WHERE id='` + backstopOutboxID + `'`, args: []any{now}},
		{name: "live claim", state: "completed", sql: `UPDATE sync_dispatch_outbox SET claim_token='live',claim_expires_at=$1,claim_transport='river',claim_route_generation=(SELECT generation FROM sync_dispatch_transport_routes WHERE kind='finalize_sync_run') WHERE id='` + backstopOutboxID + `'`, args: []any{now.Add(time.Minute)}},
		{name: "feature disabled", state: "missing", sql: `UPDATE sync_dispatch_outbox SET last_error='feature_disabled'`},
		{name: "paused route", state: "missing", sql: `UPDATE sync_dispatch_transport_routes SET paused=true,paused_at=now(),generation=generation+1`},
		{name: "other route", state: "missing", sql: `UPDATE sync_dispatch_transport_routes SET transport='celery',generation=generation+1`},
		{name: "stale generation", state: "missing", sql: `UPDATE sync_dispatch_transport_routes SET generation=generation+1`},
		{name: "terminal run", state: "missing", sql: `UPDATE sync_runs SET status='success'`},
		{name: "unfinished unit", state: "missing", sql: `UPDATE sync_run_units SET status='running'`},
		{name: "pending discovery", state: "missing", sql: `INSERT INTO sync_run_reference_discoveries(id,org_id,sync_run_id,status,attempts,available_at,created_at,updated_at) VALUES (gen_random_uuid(),'org-materializer','` + materializerFinalize + `','running',0,now(),now(),now())`},
		{name: "unlinked occurrence", state: "missing", sql: `DELETE FROM scheduled_sync_occurrences`},
		{name: "malformed job identity", state: "missing", sql: `UPDATE sync_dispatch_outbox SET transport_job_id='not-an-id'`},
		// CHAOS-5456 guard matrix: added after a mutation pass proved the
		// clauses below had no red test behind them. Each row leaves every
		// other predicate satisfiable, so exactly one clause refuses it.
		{name: "other outbox kind", state: "missing", sql: `UPDATE sync_dispatch_outbox SET kind='post_sync'`},
		{name: "celery dispatched transport", state: "missing", sql: `UPDATE sync_dispatch_outbox SET dispatched_transport='celery'`},
		{name: "completed delivery of another kind", state: "completed", sql: `UPDATE river.river_job SET kind='dispatch_sync_run'`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h.seed(t, ctx, now, tc.state)
			if tc.sql != "" {
				if _, err := h.admin.Exec(ctx, tc.sql, tc.args...); err != nil {
					t.Fatal(err)
				}
			}
			result, err := h.repair.Step(ctx, now, 20)
			if err != nil {
				t.Fatal(err)
			}
			if result.Recovered != 0 || h.status(t, ctx) != "dispatched" {
				t.Fatalf("guard failed: recovery=%+v status=%s", result, h.status(t, ctx))
			}
		})
	}
	// The liveness predicate's `job.finalized_at IS NOT NULL` clause has no
	// behavioural guard row above because River's OWN schema forbids the shape
	// it excludes: river_job carries a CHECK constraint tying finalized_at to
	// the finalized states, so `state='completed' AND finalized_at IS NULL`
	// cannot be seeded (SQLSTATE 23514). Rather than leave the clause with no
	// red test at all, this pins the constraint that makes it structural -- a
	// future River migration that drops it turns the clause back into the only
	// thing standing between a half-written job row and a re-arm, and this is
	// where that change gets noticed.
	t.Run("river forbids a completed job with no finalized_at", func(t *testing.T) {
		h.seed(t, ctx, now, "completed")
		if _, err := h.admin.Exec(ctx, `UPDATE river.river_job SET finalized_at=NULL`); err == nil {
			t.Fatal("river_job accepted a completed row with a NULL finalized_at")
		}
	})
	t.Run("queue cannot read coordinator ledgers", func(t *testing.T) {
		if _, err := h.queue.Exec(ctx, `SELECT 1 FROM sync_run_reference_discoveries`); err == nil {
			t.Fatal("queue gained coordinator ledger access")
		}
		if _, err := h.coordinator.Exec(ctx, `SELECT 1 FROM river.river_job`); err == nil {
			t.Fatal("coordinator gained River access")
		}
	})
}

func TestReadyFinalizeRepairReplicasAndLockedDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	h := startFinalizeBackstopHarness(t, ctx)
	now := time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC)
	for _, locked := range []string{"outbox", "river"} {
		t.Run("locked "+locked, func(t *testing.T) {
			jobID := h.seed(t, ctx, now, "completed")
			tx, err := h.admin.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			if locked == "outbox" {
				_, err = tx.Exec(ctx, `SELECT 1 FROM sync_dispatch_outbox WHERE id=$1 FOR UPDATE`, backstopOutboxID)
			} else {
				_, err = tx.Exec(ctx, `SELECT 1 FROM river.river_job WHERE id=$1 FOR UPDATE`, jobID)
			}
			if err != nil {
				t.Fatal(err)
			}
			result, err := h.repair.Step(ctx, now, 20)
			if err != nil || result.Recovered != 0 {
				t.Fatalf("locked delivery result=%+v err=%v", result, err)
			}
			if err := tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			result, err = h.repair.Step(ctx, now, 20)
			if err != nil || result.Recovered != 1 {
				t.Fatalf("released delivery result=%+v err=%v", result, err)
			}
		})
	}
	t.Run("two replicas recover once", func(t *testing.T) {
		h.seed(t, ctx, now, "missing")
		var wg sync.WaitGroup
		start := make(chan struct{})
		results := make(chan TerminalDeliveryRepairResult, 2)
		errs := make(chan error, 2)
		for range 2 {
			wg.Add(1)
			go func() { defer wg.Done(); <-start; r, err := h.repair.Step(ctx, now, 20); results <- r; errs <- err }()
		}
		close(start)
		wg.Wait()
		close(results)
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		total := 0
		for r := range results {
			total += r.Recovered
		}
		if total != 1 {
			t.Fatalf("replicas recovered=%d want1", total)
		}
	})
}

// backstopRoutes puts both dispatch kinds on the active River route and returns the finalize
// route's generation. The migrations seed each kind on celery at generation 2 and the real trigger
// only ever lets a generation rise, so the value moves across cases; callers read it back and never
// assume a number.
func backstopRoutes(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int64 {
	t.Helper()
	var finalize int64
	for _, kind := range []string{"finalize_sync_run", "post_sync"} {
		got := pgseed.SyncTransportRoute(ctx, t, pool, kind, "river", 3, false, "none")
		if kind == "finalize_sync_run" {
			finalize = got
		}
	}
	return finalize
}

// TestReadyFinalizeRepairGuardsHoldWithoutTheRouteFence pins the three
// candidate clauses that migration 0049's route-fence trigger ALSO enforces.
//
// The trigger nulls dispatched_transport / dispatched_route_generation /
// transport_job_id on any row whose status is not 'dispatched', and on any
// 'dispatched' row whose last_error is 'feature_disabled'. With the trigger
// installed those two shapes cannot exist, so a guard row seeded through it
// passes whether or not the repair's own SQL still names the clause -- a
// mutation pass proved exactly that: dropping `outbox.status='dispatched'`,
// dropping the feature_disabled exclusion, and dropping transport_job_id from
// the re-arm all survived the whole matrix, pinned by the trigger rather than
// by the code under test.
//
// This test removes the trigger for the duration, seeds the shape directly,
// and measures the repair's own refusal. It is not asserting that production
// can reach these states -- it is asserting the repair does not DEPEND on a
// database trigger to be safe, which is what "each guard predicate has a red
// test behind it" has to mean.
func TestReadyFinalizeRepairGuardsHoldWithoutTheRouteFence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	h := startFinalizeBackstopHarness(t, ctx)
	now := time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC)

	withoutFence := func(t *testing.T, body func()) {
		t.Helper()
		// Drift simulation: the shapes below (a pending or feature-disabled row that still carries its
		// dead delivery columns) are exactly what the migrated schema forbids twice over -- the fence
		// trigger strips them and ck_sync_dispatch_outbox_dispatched_route_coherence rejects them.
		// The test asserts the repair does not DEPEND on either, so both are removed for its duration
		// (the constraint stays dropped for the rest of this test's database; seed() rebuilds the rows).
		if _, err := h.admin.Exec(ctx,
			`DROP TRIGGER trg_sync_dispatch_outbox_route_fence ON public.sync_dispatch_outbox;
			 ALTER TABLE public.sync_dispatch_outbox DROP CONSTRAINT IF EXISTS ck_sync_dispatch_outbox_dispatched_route_coherence`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := h.admin.Exec(ctx, `CREATE TRIGGER trg_sync_dispatch_outbox_route_fence
			    BEFORE INSERT OR UPDATE ON public.sync_dispatch_outbox FOR EACH ROW
			    EXECUTE FUNCTION enforce_sync_dispatch_outbox_route_fence()`); err != nil {
				t.Fatal(err)
			}
		}()
		body()
	}

	for _, tc := range []struct {
		name, sql string
	}{
		{
			name: "already pending row keeps its dead delivery columns",
			sql:  `UPDATE sync_dispatch_outbox SET status='pending' WHERE id='` + backstopOutboxID + `'`,
		},
		{
			name: "feature-disabled row keeps its dead delivery columns",
			sql:  `UPDATE sync_dispatch_outbox SET last_error='feature_disabled' WHERE id='` + backstopOutboxID + `'`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h.seed(t, ctx, now, "completed")
			withoutFence(t, func() {
				if _, err := h.admin.Exec(ctx, tc.sql); err != nil {
					t.Fatal(err)
				}
				var transport, jobID *string
				if err := h.admin.QueryRow(ctx,
					`SELECT dispatched_transport,transport_job_id FROM public.sync_dispatch_outbox WHERE id=$1`,
					backstopOutboxID).Scan(&transport, &jobID); err != nil {
					t.Fatal(err)
				}
				if transport == nil || jobID == nil {
					t.Fatal("fence still stripped the delivery columns; the shape under test was never created")
				}
				result, err := h.repair.Step(ctx, now, 20)
				if err != nil {
					t.Fatal(err)
				}
				if result.Recovered != 0 || result.ReadyFinalizersRecovered != 0 {
					t.Fatalf("repair recovered a row its own SQL must refuse: %+v", result)
				}
			})
		})
	}

	t.Run("re-arm clears the delivery columns itself", func(t *testing.T) {
		h.seed(t, ctx, now, "completed")
		withoutFence(t, func() {
			result, err := h.repair.Step(ctx, now, 20)
			if err != nil || result.ReadyFinalizersRecovered != 1 {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			var residue int
			if err := h.admin.QueryRow(ctx, `SELECT count(*) FROM public.sync_dispatch_outbox
			    WHERE id=$1 AND (dispatched_at IS NOT NULL OR dispatched_transport IS NOT NULL
			        OR dispatched_route_generation IS NOT NULL OR transport_job_id IS NOT NULL
			        OR claim_token IS NOT NULL OR claim_expires_at IS NOT NULL
			        OR claim_transport IS NOT NULL OR claim_route_generation IS NOT NULL)`,
				backstopOutboxID).Scan(&residue); err != nil {
				t.Fatal(err)
			}
			if residue != 0 {
				t.Fatal("re-arm left delivery columns behind when the fence was not there to clear them")
			}
		})
	})
}
