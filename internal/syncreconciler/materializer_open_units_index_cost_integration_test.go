//go:build integration

package syncreconciler

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

const openUnitsIndexDDL = `CREATE INDEX IF NOT EXISTS ix_sync_run_units_open_run ON public.sync_run_units (sync_run_id) WHERE status NOT IN ('success', 'failed')`

func setOpenUnitsIndex(t *testing.T, ctx context.Context, pool *pgxpool.Pool, present bool) {
	t.Helper()
	statement := openUnitsIndexDDL
	if !present {
		statement = "DROP INDEX IF EXISTS public.ix_sync_run_units_open_run"
	}
	if _, err := pool.Exec(ctx, statement); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "VACUUM (ANALYZE) public.sync_run_units"); err != nil {
		t.Fatal(err)
	}
}

func timeExec(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) time.Duration {
	t.Helper()
	started := time.Now()
	if _, err := pool.Exec(ctx, sql, args...); err != nil {
		t.Fatal(err)
	}
	return time.Since(started)
}

func median(values []time.Duration) time.Duration {
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values[len(values)/2]
}

// TestOpenUnitsIndexWriteCostAndOtherPlans measures, at the backfill-scale seed, what the open-units partial index costs
// unit writes (100k-row INSERT, status UPDATE into the index's predicate, status UPDATE out of it; median of 3 rounds,
// index present vs dropped) and its size, and prints the plan of the other hot sync_run_units reads with and without it.
// It asserts nothing about speed: the numbers are the evidence.
func TestOpenUnitsIndexWriteCostAndOtherPlans(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	pool := startCandidateWindowPool(t, ctx)
	seedBackfillBudgetPopulation(t, ctx, pool)

	var indexSize, tableSize int64
	setOpenUnitsIndex(t, ctx, pool, false)
	t.Logf("build, plain CREATE INDEX (what a plain 0160 would do under a SHARE lock): %v",
		timeExec(t, ctx, pool, openUnitsIndexDDL))
	setOpenUnitsIndex(t, ctx, pool, false)
	t.Logf("build, CREATE INDEX CONCURRENTLY (what the hook does; writes do not wait): %v",
		timeExec(t, ctx, pool, strings.Replace(openUnitsIndexDDL, "CREATE INDEX", "CREATE INDEX CONCURRENTLY", 1)))
	setOpenUnitsIndex(t, ctx, pool, true)
	if err := pool.QueryRow(ctx, `SELECT pg_relation_size('public.ix_sync_run_units_open_run'), pg_total_relation_size('public.sync_run_units')`).Scan(&indexSize, &tableSize); err != nil {
		t.Fatal(err)
	}
	t.Logf("open-units index size %d MiB; sync_run_units total size %d MiB (rows: ~%d)", indexSize>>20, tableSize>>20, 200000+29960+600000)

	insertSQL := `INSERT INTO public.sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, attempts, created_at, status, available_at, updated_at)
		SELECT md5($1 || g)::uuid, 'org-materializer', md5('a' || (1 + g % 40))::uuid, '` + pgseed.DefaultSyncIntegrationID + `'::uuid, '` + pgseed.DefaultSyncSourceID + `'::uuid,
			'github', $1, 'rest_core', 'backfill', 0, now(), 'planned', NULL, now()
		FROM generate_series(1, 100000) AS g`
	results := map[bool]map[string][]time.Duration{true: {}, false: {}}
	for round := 0; round < 3; round++ {
		for _, present := range []bool{true, false} {
			setOpenUnitsIndex(t, ctx, pool, present)
			tag := fmt.Sprintf("w%d-%t", round, present)
			results[present]["insert 100k planned"] = append(results[present]["insert 100k planned"], timeExec(t, ctx, pool, insertSQL, tag))
			results[present]["update 100k planned->dispatching"] = append(results[present]["update 100k planned->dispatching"],
				timeExec(t, ctx, pool, `UPDATE public.sync_run_units SET status = 'dispatching', updated_at = now() WHERE dataset_key = $1`, tag))
			results[present]["update 100k dispatching->success"] = append(results[present]["update 100k dispatching->success"],
				timeExec(t, ctx, pool, `UPDATE public.sync_run_units SET status = 'success', updated_at = now() WHERE dataset_key = $1`, tag))
		}
	}
	for _, name := range []string{"insert 100k planned", "update 100k planned->dispatching", "update 100k dispatching->success"} {
		with, without := median(results[true][name]), median(results[false][name])
		t.Logf("%s: with index %v, without %v, change %+.1f%%", name, with, without, (float64(with)/float64(without)-1)*100)
	}

	runID := "00000000-0000-4000-8000-0000000000aa"
	big := "(SELECT md5('a' || 1)::uuid)"
	statements := []struct{ name, sql string }{
		{"claim planned (UPDATE predicate as SELECT)", `SELECT id FROM public.sync_run_units WHERE sync_run_id = ` + big + ` AND status = 'planned'`},
		{"claim due retrying", `SELECT id FROM public.sync_run_units WHERE sync_run_id = ` + big + ` AND status = 'retrying' AND available_at IS NOT NULL AND available_at <= now()`},
		{"stale dispatching reclaim", `SELECT id FROM public.sync_run_units WHERE sync_run_id = ` + big + ` AND status = 'dispatching' AND updated_at <= now() - interval '1 minute'`},
		{"claimable outside snapshot", `SELECT id::text FROM public.sync_run_units WHERE sync_run_id = ` + big + ` AND (status = 'planned' OR (status = 'retrying' AND available_at IS NOT NULL AND available_at <= now()) OR (status = 'dispatching' AND updated_at <= now())) ORDER BY id`},
		{"coverage scan", `SELECT unit.source_id, unit.dataset_key, unit.since_at, unit.before_at, unit.status, unit.id FROM public.sync_run_units AS unit JOIN public.sync_runs AS run ON run.id = unit.sync_run_id WHERE unit.org_id = 'org-materializer' AND unit.integration_id = '` + pgseed.DefaultSyncIntegrationID + `' AND unit.source_id = ANY(ARRAY['` + pgseed.DefaultSyncSourceID + `']::uuid[]) AND unit.dataset_key = ANY(ARRAY['commits']::text[]) AND unit.status = ANY(ARRAY['success']::text[]) AND unit.since_at IS NOT NULL AND unit.before_at IS NOT NULL AND unit.before_at >= now() - interval '30 days' AND run.org_id = 'org-materializer' ORDER BY COALESCE(run.completed_at, run.started_at, run.created_at), unit.id`},
		{"failure streak read", `SELECT s.id::text, recent.failed, recent.updated_at FROM public.integration_sources AS s CROSS JOIN LATERAL (SELECT u.id, u.status = 'failed' AS failed, u.updated_at FROM public.sync_run_units AS u WHERE u.org_id = 'org-materializer' AND u.integration_id = '` + pgseed.DefaultSyncIntegrationID + `'::uuid AND u.source_id = s.id AND u.dataset_key = 'commits' AND u.status IN ('success', 'failed') ORDER BY u.updated_at DESC, u.id DESC LIMIT 8) AS recent WHERE s.org_id = 'org-materializer' AND s.integration_id = '` + pgseed.DefaultSyncIntegrationID + `'::uuid ORDER BY s.id, recent.updated_at DESC, recent.id DESC`},
	}
	_ = runID
	shape := func(name, sql string) string {
		rows, err := pool.Query(ctx, "EXPLAIN (COSTS OFF) "+sql)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		defer rows.Close()
		var lines []string
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			lines = append(lines, strings.TrimRight(line, " "))
		}
		return strings.Join(lines, "\n")
	}
	for _, statement := range statements {
		setOpenUnitsIndex(t, ctx, pool, true)
		with := shape(statement.name, statement.sql)
		setOpenUnitsIndex(t, ctx, pool, false)
		without := shape(statement.name, statement.sql)
		if with == without {
			t.Logf("PLAN %s: unchanged by the index\n%s", statement.name, with)
		} else {
			t.Logf("PLAN %s: CHANGED\n-- with the index:\n%s\n-- without:\n%s", statement.name, with, without)
		}
	}
}
