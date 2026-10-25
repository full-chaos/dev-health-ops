//go:build integration

package syncreconciler

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

const legacyDispatchCandidatesSQL = `
SELECT DISTINCT run.id::text
FROM public.sync_runs AS run
JOIN public.sync_run_units AS unit ON unit.sync_run_id = run.id
WHERE run.status NOT IN ('success', 'partial_failed', 'failed')
	AND (
		run.triggered_by <> 'schedule'
		OR EXISTS (SELECT 1 FROM public.scheduled_sync_occurrences AS occurrence WHERE occurrence.sync_run_id = run.id)
	)
	AND (
		unit.status = 'planned'
		OR (unit.status = 'dispatching' AND unit.updated_at <= $2)
		OR (unit.status = 'retrying' AND unit.available_at IS NOT NULL AND unit.available_at <= $1)
	)
ORDER BY 1`

const legacyPostSyncCandidatesSQL = `
SELECT ledger.sync_run_id::text
FROM public.sync_run_post_dispatches AS ledger
JOIN public.sync_runs AS run ON run.id = ledger.sync_run_id
LEFT JOIN public.sync_dispatch_outbox AS outbox
	ON outbox.sync_run_id = ledger.sync_run_id AND outbox.kind = 'post_sync'
WHERE ledger.kind = 'post_sync' AND outbox.id IS NULL
ORDER BY 1`

func startCandidateWindowPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := createMaterializerIntegrationFixture(ctx, t, pool); err != nil {
		t.Fatal(err)
	}
	return pool
}

func candidateIDs(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) []string {
	t.Helper()
	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(ids)
	return ids
}

func uuidN(prefix, n int) string { return fmt.Sprintf("00000000-0000-4000-8%03d-%012d", prefix, n) }

// The dispatch candidate set keeps its old result: the EXISTS reshape selects the same runs as the old JOIN + DISTINCT.
func TestMaterializeDispatchSQLSelectsTheSameRunsAsTheJoinForm(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pool := startCandidateWindowPool(t, ctx)
	now := time.Now().UTC()
	cutoff := now.Add(-time.Minute)
	past, future := now.Add(-time.Hour), now.Add(time.Hour)
	type unit struct {
		status    string
		available *time.Time
		updated   time.Time
	}
	cases := []struct {
		status string
		units  []unit
	}{
		{"running", []unit{{"planned", nil, now}}},
		{"running", []unit{{"planned", nil, now}, {"planned", nil, now}, {"success", nil, now}}},
		{"running", []unit{{"dispatching", nil, past}}},
		{"running", []unit{{"dispatching", nil, now}}},
		{"running", []unit{{"retrying", &past, now}}},
		{"running", []unit{{"retrying", &future, now}}},
		{"running", []unit{{"retrying", nil, now}}},
		{"running", []unit{{"success", nil, now}, {"failed", nil, now}}},
		{"running", nil},
		{"success", []unit{{"planned", nil, now}}},
		{"failed", []unit{{"dispatching", nil, past}}},
		{"partial_failed", []unit{{"retrying", &past, now}}},
	}
	n := 0
	for i, c := range cases {
		run := uuidN(1, i)
		seedRun(t, ctx, pool, run, c.status, now.Add(-time.Duration(len(cases)-i)*time.Minute))
		for _, u := range c.units {
			n++
			seedUnit(t, ctx, pool, uuidN(2, n), run, u.status, u.available, u.updated)
		}
	}
	want := candidateIDs(t, ctx, pool, legacyDispatchCandidatesSQL, now, cutoff)
	if len(want) != 4 {
		t.Fatalf("legacy candidate set = %v, want 4 runs (the fixture changed)", want)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, materializeDispatchSQL, now, cutoff, maximumStepLimit); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(ctx, `SELECT sync_run_id::text FROM public.sync_dispatch_outbox WHERE kind = 'dispatch_sync_run' ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		got = append(got, id)
	}
	rows.Close()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("new dispatch SQL materialized %v, want the legacy set %v", got, want)
	}
}

func TestPostSyncLedgerRecordedAtIsTheDatabaseClock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pool := startCandidateWindowPool(t, ctx)
	run := uuidN(3, 1)
	seedRun(t, ctx, pool, run, "success", time.Now().Add(-time.Hour))
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_run_post_dispatches (id, org_id, sync_run_id, kind, dispatched_at)
		VALUES (gen_random_uuid(), 'org-materializer', $1, 'post_sync', now() - interval '30 days')`, run); err != nil {
		t.Fatal(err)
	}
	var skewSeconds float64
	if err := pool.QueryRow(ctx, `
		SELECT abs(extract(epoch FROM (clock_timestamp() - recorded_at)))
		FROM public.sync_run_post_dispatches WHERE sync_run_id = $1`, run).Scan(&skewSeconds); err != nil {
		t.Fatal(err)
	}
	if skewSeconds > 30 {
		t.Fatalf("recorded_at is %.0f s from the database clock, want the insert-time now() default (not dispatched_at)", skewSeconds)
	}
}

func TestMaterializePostSyncSQLRebuildsOnlyInsideTheWindow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pool := startCandidateWindowPool(t, ctx)
	now := time.Now().UTC()
	rows := []struct {
		name       string
		recordedAt string
		outbox     bool
		rebuilt    bool
	}{
		{"recent-missing", "now() - interval '1 day'", false, true},
		{"just-inside-missing", "now() - interval '6 days 23 hours'", false, true},
		{"just-outside-missing", "now() - interval '7 days 1 hour'", false, false},
		{"old-missing", "now() - interval '40 days'", false, false},
		{"pre-0160-missing", "'-infinity'::timestamptz", false, false},
		{"recent-has-outbox", "now() - interval '1 day'", true, false},
	}
	wantRebuilt := map[string]bool{}
	for i, row := range rows {
		run := uuidN(4, i)
		seedRun(t, ctx, pool, run, "success", now.Add(-time.Hour))
		if _, err := pool.Exec(ctx, `
			INSERT INTO public.sync_run_post_dispatches (id, org_id, sync_run_id, kind, dispatched_at, recorded_at)
			VALUES (gen_random_uuid(), 'org-materializer', $1, 'post_sync', now() - interval '40 days', `+row.recordedAt+`)`, run); err != nil {
			t.Fatal(err)
		}
		if row.outbox {
			if _, err := pool.Exec(ctx, `
				INSERT INTO public.sync_dispatch_outbox (id, org_id, sync_run_id, kind, status, available_at, attempts, created_at, updated_at)
				VALUES (gen_random_uuid(), 'org-materializer', $1, 'post_sync', 'pending', now(), 0, now(), now())`, run); err != nil {
				t.Fatal(err)
			}
		}
		if row.rebuilt {
			wantRebuilt[run] = true
		}
	}
	legacy := candidateIDs(t, ctx, pool, legacyPostSyncCandidatesSQL)
	if len(legacy) != 5 {
		t.Fatalf("legacy candidates = %v, want the 5 rows without an outbox row", legacy)
	}
	if _, err := pool.Exec(ctx, materializePostSyncSQL, now, maximumStepLimit); err != nil {
		t.Fatal(err)
	}
	got := candidateIDs(t, ctx, pool, `SELECT sync_run_id::text FROM public.sync_dispatch_outbox WHERE kind = 'post_sync' AND attempts = 0 AND created_at >= now() - interval '1 minute' AND updated_at = $1`, now)
	var want []string
	for run := range wantRebuilt {
		want = append(want, run)
	}
	sort.Strings(want)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("rebuilt %v, want exactly the inside-window rows without an outbox row %v", got, want)
	}
}
