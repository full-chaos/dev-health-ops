//go:build integration

package syncreconciler

import (
	"context"
	"encoding/json"
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
			t.Fatalf(
				"result = %#v: a permanent discard was reclaimed, relitigating a terminal decision the worker already made",
				result,
			)
		}
		// last_error must remain unset: any stamped code proves a recovery
		// branch ran, which is the failure here regardless of which one.
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

	// The safety argument for including exhaustion is that the pre-existing
	// predicates already prove the delivery is the live one. These two controls
	// hold that argument to a test instead of leaving it as prose in a comment:
	// both seed an exhausted job whose work the domain has ALREADY moved past,
	// and the repair must decline both. Without them, a refactor that loosened
	// the outbox-to-job linkage would produce a double delivery while every
	// other subtest here still passed -- verified by mutation: dropping
	// `job.id::text = outbox.transport_job_id` from the join fails only the
	// superseded case.
	//
	// Both controls are killed by the LINKAGE predicate, not by the
	// `status = 'dispatched'` fence: a real re-arm nulls transport_job_id too
	// (Python's upsert and this repair both do), so no reachable state has a
	// pending row still pointing at a job. Mutating the status fence alone
	// therefore changes nothing observable here. That is redundancy in the SQL,
	// not a gap in these tests, and it is recorded so the next reader does not
	// mistake one for the other.
	t.Run("delivery whose body already re-armed the row is never reclaimed", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, adminPool)
		jobID := seedExhaustionDelivery(t, ctx, adminPool, riverClient, now, "running")
		discardRiverJob(t, ctx, adminPool, jobID, now, 5, 5, exhaustionBridgeError)
		// The compatibility body ran and stamped its own wakeup, returning the
		// row to 'pending'. The run is already progressing; a reclaim here
		// would publish a second delivery of work that is under way.
		if _, err := adminPool.Exec(
			ctx,
			`UPDATE public.sync_dispatch_outbox
			 SET status = 'pending', available_at = $2, dispatched_at = NULL,
				 dispatched_transport = NULL, dispatched_route_generation = NULL,
				 transport_job_id = NULL, updated_at = $2
			 WHERE id = $1`,
			exhaustionRunID,
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
		var status string
		var lastError *string
		if err := adminPool.QueryRow(
			ctx,
			`SELECT status, last_error FROM public.sync_dispatch_outbox WHERE id = $1`,
			exhaustionRunID,
		).Scan(&status, &lastError); err != nil {
			t.Fatal(err)
		}
		if status != "pending" || lastError != nil {
			t.Fatalf("re-armed row was mutated: status=%q last_error=%v", status, lastError)
		}
	})

	t.Run("superseded delivery is never reclaimed under a newer one", func(t *testing.T) {
		resetKernelIntegrationTables(t, ctx, adminPool)
		retired := seedExhaustionDelivery(t, ctx, adminPool, riverClient, now, "running")
		discardRiverJob(t, ctx, adminPool, retired, now, 5, 5, exhaustionBridgeError)
		// A newer delivery already replaced the exhausted one and is in flight.
		// The exhausted job still sits in river_job, and only the
		// transport_job_id linkage distinguishes it from the live delivery.
		replacement, err := riverClient.Insert(ctx, kernelRiverArgs{OutboxID: exhaustionRunID}, &river.InsertOpts{
			Queue: "sync",
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := adminPool.Exec(
			ctx,
			`UPDATE public.sync_dispatch_outbox
			 SET transport_job_id = $2, dispatched_at = $3, updated_at = $3
			 WHERE id = $1`,
			exhaustionRunID,
			strconv.FormatInt(replacement.Job.ID, 10),
			now,
		); err != nil {
			t.Fatal(err)
		}

		result, err := repair.Step(ctx, now, 10)
		if err != nil {
			t.Fatalf("repair step: %v", err)
		}
		if result != (TerminalDeliveryRepairResult{}) {
			t.Fatalf("result = %#v, want no recovery of a superseded delivery", result)
		}
		var status, transportID string
		if err := adminPool.QueryRow(
			ctx,
			`SELECT status, transport_job_id FROM public.sync_dispatch_outbox WHERE id = $1`,
			exhaustionRunID,
		).Scan(&status, &transportID); err != nil {
			t.Fatal(err)
		}
		if status != "dispatched" || transportID != strconv.FormatInt(replacement.Job.ID, 10) {
			t.Fatalf(
				"live delivery was disturbed: status=%q transport_job_id=%s want dispatched/%d",
				status, transportID, replacement.Job.ID,
			)
		}
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

// TestTerminalDeliveryRepairJoinUsesJobPrimaryKey pins CHAOS-4092: the job
// join must resolve through river_job_pkey, never by scanning every row of
// the kind. `job.id::text = outbox.transport_job_id` is not sargable against
// the primary key (a cast on the indexed column), so Postgres fell back to
// the river_job_kind index and filtered every row of that kind per
// candidate -- the exact O(candidates x jobs-of-kind) blowup that produced
// prod's 9.5h crash loop (EXPLAIN ANALYZE there: 97 loops x 18,730 rows
// filtered, ~1.8M rows touched, 140k buffers).
//
// This seeds one legitimate candidate alongside thousands of decoy
// river_job rows of the SAME kind, none of which the candidate's
// transport_job_id points at, and reads a real EXPLAIN ANALYZE plan for the
// production repair SQL. The bound below is a row-touch bound, not a string
// match on an index name, so it survives a planner/version change: it fails
// against the old `job.id::text = outbox.transport_job_id` shape (which
// scales with decoyCount) and passes against the guarded
// `job.id = <cast outbox.transport_job_id>` shape (which does not). Verified
// by hand against the pre-fix join before this test was committed.
func TestTerminalDeliveryRepairJoinUsesJobPrimaryKey(t *testing.T) {
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
	now := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)

	resetKernelIntegrationTables(t, ctx, adminPool)
	jobID := seedExhaustionDelivery(t, ctx, adminPool, riverClient, now, "running")
	discardRiverJob(t, ctx, adminPool, jobID, now, 5, 5, exhaustionBridgeError)

	// Decoys match every OTHER predicate the candidates CTE evaluates --
	// same kind, discarded, finalized, an exhausted attempt budget, a
	// non-empty error history -- so neither river_job_kind nor
	// river_job_state_and_finalized_at_index can shortcut the plan on its
	// own. The only predicate that excludes a decoy is the job.id linkage
	// itself, which is exactly the predicate this test exists to pin.
	const decoyCount = 20000
	if _, err := adminPool.Exec(
		ctx,
		`INSERT INTO river.river_job (kind, max_attempts, state, attempt, finalized_at, errors, args)
		 SELECT $1, 5, 'discarded', 5, now(),
			ARRAY[jsonb_build_object('error', 'decoy', 'attempt', 5)], '{}'::jsonb
		 FROM generate_series(1, $2)`,
		syncdispatchcontract.KindDispatchSyncRun,
		decoyCount,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := adminPool.Exec(ctx, `ANALYZE river.river_job`); err != nil {
		t.Fatal(err)
	}

	tx, err := queuePool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var explainJSON string
	if err := tx.QueryRow(
		ctx,
		"EXPLAIN (ANALYZE, VERBOSE, BUFFERS, FORMAT JSON) "+repair.query,
		now.UTC(),
		10,
		riverUnhandledRescueError,
		riverUnhandledRescueEvidence,
		riverDeliveryExhaustedEvidence,
	).Scan(&explainJSON); err != nil {
		t.Fatalf("EXPLAIN the repair statement: %v", err)
	}

	var document []map[string]any
	if err := json.Unmarshal([]byte(explainJSON), &document); err != nil {
		t.Fatalf("parse EXPLAIN JSON: %v\n%s", err, explainJSON)
	}
	if len(document) != 1 {
		t.Fatalf("EXPLAIN returned %d plans, want 1:\n%s", len(document), explainJSON)
	}
	plan, _ := document[0]["Plan"].(map[string]any)
	jobNode := findExplainNodeByRelation(plan, "river_job")
	if jobNode == nil {
		t.Fatalf("no plan node scanned river_job:\n%s", explainJSON)
	}
	nodeType, _ := jobNode["Node Type"].(string)
	indexName, _ := jobNode["Index Name"].(string)
	rowsRemoved, _ := jobNode["Rows Removed by Filter"].(float64)
	actualRows, _ := jobNode["Actual Rows"].(float64)
	actualLoops, _ := jobNode["Actual Loops"].(float64)
	if actualLoops == 0 {
		actualLoops = 1
	}
	rowsTouched := actualRows * actualLoops
	t.Logf(
		"river_job scan: node=%q index=%q actual-rows=%.0f actual-loops=%.0f "+
			"rows-touched=%.0f rows-removed-by-filter=%.0f decoys=%d",
		nodeType, indexName, actualRows, actualLoops, rowsTouched, rowsRemoved, decoyCount,
	)

	// The row-touch bound is the authoritative assertion: it holds regardless
	// of which plan shape the optimizer picks (Nested Loop + Index Scan,
	// Hash Join + Seq Scan, ...), whereas "Rows Removed by Filter" is only
	// meaningful for a Filter evaluated at THIS node -- a Hash Join can push
	// the id equality into its Hash Cond instead, which reports 0 rows
	// removed at the scan node while still having built or probed the hash
	// table from every one of the 20,000 decoys. Decoys satisfy every other
	// predicate (kind, state, finalized_at, exhausted attempts, a non-empty
	// error history), so only the job.id linkage distinguishes them; the
	// fixed join resolves through the primary key and touches exactly the 1
	// matching row. This bound is far below decoyCount but leaves headroom
	// for planner/version variance without masking an O(N) scan of 20,000
	// decoys.
	const maxRowsTouched = 5
	if rowsTouched > maxRowsTouched {
		t.Fatalf(
			"job scan touched %.0f rows with %d same-kind, same-state decoys present: "+
				"join is not resolving through the primary key (node type=%q index=%q "+
				"rows-removed-by-filter=%.0f); full plan:\n%s",
			rowsTouched, decoyCount, nodeType, indexName, rowsRemoved, explainJSON,
		)
	}
	if strings.Contains(indexName, "kind") {
		t.Fatalf(
			"job scan used index %q, a kind-only index that still visits every row of the "+
				"kind; want a primary-key lookup",
			indexName,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	assertOutboxDelivery(t, ctx, adminPool, "pending", riverDeliveryExhaustedEvidence, false)
}

// findExplainNodeByRelation searches a parsed EXPLAIN (FORMAT JSON) plan tree
// depth-first for the first node scanning the named relation.
func findExplainNodeByRelation(node map[string]any, relation string) map[string]any {
	if node == nil {
		return nil
	}
	if name, _ := node["Relation Name"].(string); name == relation {
		return node
	}
	children, _ := node["Plans"].([]any)
	for _, child := range children {
		childNode, ok := child.(map[string]any)
		if !ok {
			continue
		}
		if found := findExplainNodeByRelation(childNode, relation); found != nil {
			return found
		}
	}
	return nil
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

// TestReclaimedCoordinatorDeliveryRepublishesAsANewRiverJob pins the fact the
// whole reclaim silently depends on.
//
// The coordinator publisher inserts with UniqueOpts{ByArgs, ByState:
// JobStates()}, and JobStates() includes 'discarded'. A byte-identical
// re-publish after a reclaim would therefore be swallowed as a duplicate of the
// job River just retired, and verifyInsert does not inspect
// UniqueSkippedAsDuplicate -- so Publish would return the RETIRED job's id and
// report success. The outbox would cycle pending -> dispatched -> pending
// forever while no worker ever ran, which is a worse failure than the wedge
// this repair exists to fix.
//
// It works only because TransportArgs carries DeliveryAttempt and the Kernel's
// claim increments outbox.attempts, giving every redelivery distinct args and
// so a distinct unique key. Nothing else enforces that. This test fails if
// DeliveryAttempt is dropped from the args shape, or if the claim stops
// advancing the attempt counter.
func TestReclaimedCoordinatorDeliveryRepublishesAsANewRiverJob(t *testing.T) {
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
	riverClient, err := river.NewClient(riverpgxv5.New(adminPool), &river.Config{Schema: "river"})
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := syncdispatchruntime.NewPublisher(riverClient, syncdispatchruntime.PublisherOptions{
		Queue:       syncdispatchcontract.RiverQueue,
		MaxAttempts: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	reference := syncdispatchruntime.DomainReference{
		OrganizationID: "00000000-0000-4000-8000-0000000044f0",
		SyncRunID:      exhaustionRunID,
	}
	publish := func(attempt int64) (string, error) {
		tx, txErr := adminPool.Begin(ctx)
		if txErr != nil {
			return "", txErr
		}
		defer func() { _ = tx.Rollback(ctx) }()
		id, publishErr := publisher.Publish(ctx, tx, syncdispatchruntime.Claim{
			OutboxID:        exhaustionRunID,
			Kind:            syncdispatchcontract.KindDispatchSyncRun,
			RouteGeneration: 7,
			DeliveryAttempt: attempt,
		}, reference)
		if publishErr != nil {
			return "", publishErr
		}
		return id, tx.Commit(ctx)
	}

	resetKernelIntegrationTables(t, ctx, adminPool)
	first, err := publish(1)
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}

	// Re-publishing the SAME delivery attempt must NOT mint a second job: that
	// is the deduplication the unique key is there to provide, and it is what
	// makes the assertion below meaningful rather than accidental.
	same, err := publish(1)
	if err != nil {
		t.Fatalf("republish at the same attempt: %v", err)
	}
	if same != first {
		t.Fatalf("republish at attempt 1 minted job %s, want the existing %s", same, first)
	}

	// Retire the delivery exactly as exhaustion does, then reclaim it.
	firstID, err := strconv.ParseInt(first, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adminPool.Exec(
		ctx,
		`INSERT INTO public.sync_runs (id, status) VALUES ($1, 'running')
		 ON CONFLICT (id) DO UPDATE SET status = 'running'`,
		exhaustionRunID,
	); err != nil {
		t.Fatal(err)
	}
	discardRiverJob(t, ctx, adminPool, firstID, time.Now().UTC(), 5, 5, exhaustionBridgeError)

	// The next claim advances outbox.attempts, so the redelivery carries a
	// different attempt -- and must therefore reach River as a NEW job rather
	// than resolving to the discarded one.
	second, err := publish(2)
	if err != nil {
		t.Fatalf("republish after reclaim: %v", err)
	}
	if second == first {
		t.Fatalf(
			"redelivery was deduplicated into the retired job %s: the reclaim would loop forever without ever running a worker",
			first,
		)
	}
	var state string
	if err := adminPool.QueryRow(
		ctx,
		`SELECT state::text FROM river.river_job WHERE id = $1`,
		mustParseJobID(t, second),
	).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "available" {
		t.Fatalf("redelivered job state = %q, want an executable job", state)
	}
}

func mustParseJobID(t *testing.T, id string) int64 {
	t.Helper()
	parsed, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
