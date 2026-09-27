//go:build integration

package syncreconciler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// terminalOutboxCloseCoveringIndex is the 0118 migration's covering index:
// (kind, status, available_at) on public.sync_dispatch_outbox.
const terminalOutboxCloseCoveringIndex = "ix_sync_dispatch_outbox_kind_status_available_at"

// explainNode is the subset of Postgres's EXPLAIN (FORMAT JSON) plan node
// shape this test needs to identify which index (if any) served a scan, and
// whether a predicate was pushed into that index's own condition or left as
// a post-scan Filter.
type explainNode struct {
	NodeType     string        `json:"Node Type"`
	RelationName string        `json:"Relation Name"`
	IndexName    string        `json:"Index Name"`
	IndexCond    string        `json:"Index Cond"`
	Filter       string        `json:"Filter"`
	Plans        []explainNode `json:"Plans"`
}

type explainPlanRoot struct {
	Plan explainNode `json:"Plan"`
}

// findOutboxScan walks the plan tree for the scan node touching
// sync_dispatch_outbox -- the CTE's own candidate scan, wherever the planner
// places it (it may sit under a Nested Loop / Hash Join against sync_runs or
// sync_run_reference_discoveries; CTE inlining means its exact position in
// the tree isn't fixed).
func findOutboxScan(n explainNode) *explainNode {
	if n.RelationName == "sync_dispatch_outbox" {
		switch n.NodeType {
		case "Index Scan", "Index Only Scan", "Seq Scan", "Bitmap Heap Scan":
			found := n
			return &found
		}
	}
	for _, child := range n.Plans {
		if found := findOutboxScan(child); found != nil {
			return found
		}
	}
	return nil
}

// explainOutboxClose runs the exact production candidate-close statement
// inside a transaction that is always rolled back (EXPLAIN ANALYZE still
// executes an UPDATE's side effects, so this must never commit), and returns
// the scan node touching sync_dispatch_outbox.
func explainOutboxClose(
	ctx context.Context, t *testing.T, pool *pgxpool.Pool, sql string, now time.Time, limit int,
) explainNode {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Matches Step's own CHAOS-4262 JIT guard so the plan this test inspects
	// is the same one production actually runs.
	if _, err := tx.Exec(ctx, "SET LOCAL jit = off"); err != nil {
		t.Fatal(err)
	}
	var raw string
	if err := tx.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+sql, now, limit).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var rows []explainPlanRoot
	if err := json.Unmarshal([]byte(raw), &rows); err != nil {
		t.Fatalf("parse EXPLAIN JSON: %v\n%s", err, raw)
	}
	if len(rows) != 1 {
		t.Fatalf("EXPLAIN returned %d plan(s), want 1:\n%s", len(rows), raw)
	}
	scan := findOutboxScan(rows[0].Plan)
	if scan == nil {
		t.Fatalf("no scan node touching sync_dispatch_outbox found in plan:\n%s", raw)
	}
	return *scan
}

// TestTerminalOutboxCloseUsesCoveringIndexForCandidateScan is CHAOS-6971's
// permanent regression guard. Per D2671 (timing-based tests are flaky by
// construction and are never a committed regression guard), this asserts the
// MECHANISM, not elapsed time: each of the three CTE candidate statements in
// terminal_outbox_close.go must scan sync_dispatch_outbox via
// ix_sync_dispatch_outbox_kind_status_available_at (the 0118 migration's
// covering index) with `kind` and `status` pushed into the index's own
// Index Cond -- never left for a post-scan Filter to evaluate over an
// unbounded number of rows.
//
// On origin/main before this fix (`ORDER BY outbox.id`), the planner cannot
// use the covering index to also satisfy the id-ordered, FOR UPDATE SKIP
// LOCKED candidate scan, so it falls back to sync_dispatch_outbox_pkey (id
// order) with kind/status pushed into Filter instead of Index Cond -- the
// CHAOS-6971 defect itself: a scan whose cost is bounded only by total table
// size, not by kind/status selectivity.
//
// This is checked for all three kinds, INCLUDING reference_discovery. Its
// query shape never reproduced a measurable *timing* gap at any scale/data
// shape tried (a real, distinct investigation result, documented in the PR
// body); the mechanism check is what makes honest coverage possible here
// without a fabricated red-on-ms test: reference_discovery's own join target
// (public.sync_run_reference_discoveries) is a ledger table, not sync_runs,
// and when that ledger is SMALL relative to the outbox table the planner
// reasonably drives the join from the ledger side instead (a Nested Loop
// using uq_sync_dispatch_outbox_run_kind's (sync_run_id, kind) index for a
// point lookup per ledger row) -- a plan this ORDER BY change does not touch
// at all, since the outbox side is never scanned by kind/status in that
// shape. That plan choice is itself data-shape-dependent, not a property of
// the query text: seeding the ledger at FULL size (one row per sync_run,
// matching this fixture's own sync_runs cardinality, as below) makes
// filtering by kind/status on the outbox side cheaper again, and the planner
// picks exactly the same covering-index plan it picks for dispatch/finalize.
// So the fixture below deliberately seeds a full-size ledger -- not because
// production ledgers are always full-size, but because it is the shape that
// puts reference_discovery's OWN candidate scan (identical WHERE/ORDER
// BY/FOR UPDATE SKIP LOCKED shape to the other two kinds) under the same
// mechanism this test exists to pin, rather than exercising a join-order
// choice this ticket's fix does not touch either way.
//
// Executed evidence (this ticket): reverting locally to `ORDER BY outbox.id`
// flips all three kinds' Index Name from the covering index to
// sync_dispatch_outbox_pkey and moves `kind`/`status` from Index Cond into
// Filter -- reproduced and restored by sha256 digest match between runs, see
// the PR body. With a SMALL (eligible-backlog-only) ledger instead, all three
// kinds pass even pre-fix for the wrong reason (dispatch/finalize still fail
// as expected, but reference_discovery's plan uses the small-ledger-driven
// Nested Loop regardless of ORDER BY) -- confirmed by hand, which is why the
// full-size ledger below is load-bearing for this test's reference_discovery
// case, not an arbitrary fixture choice.
func TestTerminalOutboxCloseUsesCoveringIndexForCandidateScan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := createMaterializerIntegrationFixture(ctx, t, pool); err != nil {
		t.Fatal(err)
	}
	resetMaterializerIntegrationTables(t, ctx, pool)

	const (
		eligiblePerKind = 500    // 'dispatched' rows per kind whose owner is already terminal -- real candidates
		noiseClosed     = 40000  // already-closed rows, cycling kind, each its own run id
		noisePostSync   = 150000 // permanently-'dispatched' post_sync noise (excluded from this stage by design)
		terminalMix     = 0.97   // 97% terminal / 3% still-running -- the exact rate does not matter to the
		// mechanism; a nontrivial non-terminal slice just gives the query real filtering work to do
		totalRuns = eligiblePerKind*3 + noiseClosed + noisePostSync
		// This test asserts plan SHAPE, not elapsed time (D2671: timing-based
		// tests are flaky by construction and are never a committed
		// regression guard), so it needs far fewer rows than a timing
		// reproduction would -- but the 'dispatched'-status noise
		// (noisePostSync) must still be large enough relative to
		// eligiblePerKind that the planner's own cost estimate clearly favors
		// an index condition on (kind, status) over one on (status,
		// available_at) alone: at small scale, both ix_sync_dispatch_outbox_due
		// and the 0118 covering index look similarly cheap to the planner,
		// which defeats this test's purpose (confirmed by hand: at
		// noisePostSync=20000 the planner chose ix_sync_dispatch_outbox_due
		// for all three kinds even post-fix). R451: still well under the 1M
		// cap.
	)

	// sync_runs: one row per outbox row across the id space.
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO public.sync_runs (id, org_id, integration_id, triggered_by, mode, status,
			total_units, completed_units, failed_units, created_at)
		SELECT
			('00000000-6971-4001-8000-' || lpad(to_hex(n), 12, '0'))::uuid,
			'org-6971-plan', '%s'::uuid, 'manual', 'incremental',
			CASE WHEN n <= %d AND random() >= %f THEN 'running'
			     ELSE (ARRAY['success','partial_failed','failed'])[1 + floor(random()*3)::int] END,
			0, 0, 0, now() - (n || ' seconds')::interval
		FROM generate_series(1, %d) AS n`,
		pgseed.DefaultSyncIntegrationID, eligiblePerKind*3, terminalMix, totalRuns)); err != nil {
		t.Fatal(err)
	}
	// sync_run_reference_discoveries: FULL-SIZE ledger, one row per seeded
	// sync_run (not just the eligible-backlog range) -- see this test's own
	// doc comment for why that size is load-bearing for reference_discovery's
	// case specifically. terminalMix-weighted so the ledger's own terminal
	// ratio matches sync_runs', for a realistic-shaped join on both sides.
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO public.sync_run_reference_discoveries (
			id, sync_run_id, org_id, status, attempts, available_at, created_at, updated_at
		)
		SELECT
			gen_random_uuid(),
			('00000000-6971-4001-8000-' || lpad(to_hex(n), 12, '0'))::uuid,
			'org-6971-plan',
			CASE WHEN random() >= %f THEN (ARRAY['planned','retrying','running'])[1 + floor(random()*3)::int]
			     ELSE (ARRAY['success','failed'])[1 + floor(random()*2)::int] END,
			1, now() - (n || ' seconds')::interval,
			now() - (n || ' seconds')::interval, now() - (n || ' seconds')::interval
		FROM generate_series(1, %d) AS n`,
		terminalMix, totalRuns)); err != nil {
		t.Fatal(err)
	}
	// Noise: rows already closed across all three kinds, and a permanently-
	// 'dispatched' post_sync set -- both with fully random outbox ids, each
	// on its own run id (uq_sync_dispatch_outbox_run_kind is (sync_run_id,
	// kind), so no collision regardless of which kind a noise row gets).
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts, created_at, updated_at
		)
		SELECT
			gen_random_uuid(), 'org-6971-plan',
			('00000000-6971-4001-8000-' || lpad(to_hex(%d + n), 12, '0'))::uuid,
			(ARRAY['dispatch_sync_run','finalize_sync_run','reference_discovery'])[1 + (n %% 3)],
			'closed', now() - (n || ' seconds')::interval, 1,
			now() - (n || ' seconds')::interval, now() - (n || ' seconds')::interval
		FROM generate_series(1, %d) AS n`,
		eligiblePerKind*3, noiseClosed)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			dispatched_at, dispatched_transport, dispatched_route_generation,
			transport_job_id, created_at, updated_at
		)
		SELECT
			gen_random_uuid(), 'org-6971-plan',
			('00000000-6971-4001-8000-' || lpad(to_hex(%d + n), 12, '0'))::uuid,
			'post_sync', 'dispatched',
			now() - (n || ' seconds')::interval, 1,
			now() - (n || ' seconds')::interval, 'river', 1, 'river-job-postsync-plan-' || n,
			now() - (n || ' seconds')::interval, now() - (n || ' seconds')::interval
		FROM generate_series(1, %d) AS n`,
		eligiblePerKind*3+noiseClosed, noisePostSync)); err != nil {
		t.Fatal(err)
	}
	// The real eligible backlog for each of the three kinds, sharing the
	// SAME (terminal) sync_runs / ledger rows seeded above.
	for _, kind := range []string{"dispatch_sync_run", "finalize_sync_run", "reference_discovery"} {
		if _, err := pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO public.sync_dispatch_outbox (
				id, org_id, sync_run_id, kind, status, available_at, attempts,
				dispatched_at, dispatched_transport, dispatched_route_generation,
				transport_job_id, created_at, updated_at
			)
			SELECT
				gen_random_uuid(), 'org-6971-plan',
				('00000000-6971-4001-8000-' || lpad(to_hex(n), 12, '0'))::uuid,
				'%s', 'dispatched',
				now() - (n || ' seconds')::interval, 1,
				now() - (n || ' seconds')::interval, 'river', 1, 'river-job-%s-' || n,
				now() - (n || ' seconds')::interval, now() - (n || ' seconds')::interval
			FROM generate_series(1, %d) AS n`,
			kind, kind, eligiblePerKind)); err != nil {
			t.Fatalf("seed %s eligible backlog: %v", kind, err)
		}
	}
	if _, err := pool.Exec(ctx,
		"ANALYZE public.sync_runs, public.sync_run_reference_discoveries, public.sync_dispatch_outbox"); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	const limit = 100
	statements := map[string]string{
		"dispatch_sync_run":   fmt.Sprintf(closeDispatchSyncRunSQLTemplate, terminalOutboxCloseStatus),
		"finalize_sync_run":   fmt.Sprintf(closeFinalizeSyncRunSQLTemplate, terminalOutboxCloseStatus),
		"reference_discovery": fmt.Sprintf(closeReferenceDiscoverySQLTemplate, terminalOutboxCloseStatus),
	}
	for _, kind := range []string{"dispatch_sync_run", "finalize_sync_run", "reference_discovery"} {
		t.Run(kind, func(t *testing.T) {
			scan := explainOutboxClose(ctx, t, pool, statements[kind], now, limit)
			if scan.NodeType != "Index Scan" {
				t.Fatalf("%s: candidate scan is a %q, want \"Index Scan\" on %s (plan node: %+v)",
					kind, scan.NodeType, terminalOutboxCloseCoveringIndex, scan)
			}
			if scan.IndexName != terminalOutboxCloseCoveringIndex {
				t.Fatalf("%s: candidate scan used index %q, want the 0118 covering index %q -- "+
					"the candidate scan's cost now scales with total table size again, not with "+
					"kind/status selectivity (CHAOS-6971 regression)",
					kind, scan.IndexName, terminalOutboxCloseCoveringIndex)
			}
			if !strings.Contains(scan.IndexCond, "kind") || !strings.Contains(scan.IndexCond, "status") {
				t.Fatalf("%s: Index Cond %q does not push both kind and status into the index condition -- "+
					"want both bounded by the index, not left as a post-scan Filter", kind, scan.IndexCond)
			}
			if strings.Contains(scan.Filter, "kind") || strings.Contains(scan.Filter, "status") {
				t.Fatalf("%s: kind/status leaked into Filter %q instead of Index Cond %q -- "+
					"an unbounded predicate on the candidate scan (CHAOS-6971 regression)",
					kind, scan.Filter, scan.IndexCond)
			}
		})
	}
}
