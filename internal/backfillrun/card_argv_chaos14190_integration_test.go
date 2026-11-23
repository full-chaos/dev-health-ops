//go:build integration

package backfillrun

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

// DO NOT MERGE. CHAOS-14190 backfill card, executed argv: the verb runs with the card's flags (no positional) on a
// synthetic Linear configuration of the shape the card names (one planner-managed canonical configuration, one active
// integration, one enabled selected source, the work-items dataset on), and the real NativeMaterializer and
// OccurrenceReconciler plan the occurrence it writes, so the verb's own wait reads the plan. The cell wants the unit
// set: the count, and each unit's provider, dataset, mode, cost class, source and window.
const (
	card14190Org = "c6a38355-1419-4000-8000-0000000000c1"
	card14190N   = 14190
)

// card14190Seed writes the configuration of the card's shape into a fresh database and returns its id and the
// source's id.
func card14190Seed(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (string, string) {
	t.Helper()
	integration, config, source := uuidN(0x1a, card14190N), uuidN(0xc0, card14190N), uuidN(0x5a, card14190N)
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO organizations (id, slug, name, tier, is_active, created_at, updated_at)
VALUES ($1::uuid, 'org-fx-14190', 'org fx 14190', 'enterprise', true, now(), now())`, []any{card14190Org}},
		{`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1::uuid, $2, 'linear', 'linear-fx', '{}'::json, true, now(), now())`, []any{integration, card14190Org}},
		{`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, integration_id, created_at, updated_at)
VALUES ($1::uuid, $2, 'linear-fx', 'linear', '["work-items"]'::json, '{}'::json, true, true, $3::uuid, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`, []any{config, card14190Org, integration}},
		{`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1::uuid, $2, $3::uuid, 'linear', 'team', 'FX', 'FX', 'FX', $4::json, true, now(), now())`, []any{source, card14190Org, integration, fmt.Sprintf(`{"planner_managed_sync_config_id": %q}`, config)}},
		{`INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options)
VALUES (gen_random_uuid(), $1, $2::uuid, 'work-items', true, '{}'::json)`, []any{card14190Org, integration}},
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("seed: %s: %v", statement.sql, err)
		}
	}
	return config, source
}

// card14190Run runs the verb with argv at the fixed clock now, while the real reconciler plans due occurrences.
func card14190Run(t *testing.T, ctx context.Context, uri string, pool *pgxpool.Pool, now time.Time, argv []string) (int, string, string) {
	t.Helper()
	materializer, err := schedsync.NewNativeMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}
	reconciler, err := schedsync.NewOccurrenceReconciler(pool, materializer)
	if err != nil {
		t.Fatal(err)
	}
	loopCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for loopCtx.Err() == nil {
			if _, err := reconciler.Reconcile(loopCtx, now.Add(time.Minute), 50); err != nil && loopCtx.Err() == nil {
				t.Logf("reconcile: %v", err)
			}
			select {
			case <-loopCtx.Done():
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
	defer func() { stop(); <-done }()
	saved := clock
	clock = func() time.Time { return now }
	defer func() { clock = saved }()
	env := map[string]string{"MIGRATION_DATABASE_URI": uri}
	lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
	var stdout, stderr bytes.Buffer
	code := runVerb(ctx, cli.Env{Args: argv, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	t.Logf("argv %v: exit %d\nstdout: %s\nstderr: %s", argv, code, stdout.String(), stderr.String())
	return code, stdout.String(), stderr.String()
}

type card14190Unit struct {
	provider, dataset, mode, cost, source string
	since, before                         time.Time
}

// card14190Units reads the units of the one sync run the verb reported.
func card14190Units(t *testing.T, ctx context.Context, pool *pgxpool.Pool, stdout string) []card14190Unit {
	t.Helper()
	const prefix = "planned for sync_run_id="
	index := strings.Index(stdout, prefix)
	if index < 0 {
		t.Fatalf("stdout %q names no sync run", stdout)
	}
	runID := strings.TrimSpace(stdout[index+len(prefix):])
	var total int
	if err := pool.QueryRow(ctx, `SELECT COALESCE(total_units, 0) FROM sync_runs WHERE id = $1::uuid`, runID).Scan(&total); err != nil {
		t.Fatalf("sync run %s: %v", runID, err)
	}
	rows, err := pool.Query(ctx, `SELECT provider::text, dataset_key::text, mode::text, cost_class::text, source_id::text, since_at, before_at
FROM sync_run_units WHERE sync_run_id = $1::uuid ORDER BY since_at`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var units []card14190Unit
	for rows.Next() {
		var unit card14190Unit
		if err := rows.Scan(&unit.provider, &unit.dataset, &unit.mode, &unit.cost, &unit.source, &unit.since, &unit.before); err != nil {
			t.Fatal(err)
		}
		units = append(units, unit)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if total != len(units) {
		t.Fatalf("sync_runs.total_units %d, sync_run_units rows %d", total, len(units))
	}
	return units
}

// card14190CheckUnits wants count units of the source, linear work-items backfill at cost class medium, whose windows
// tile [since, before]: 14-day chunks (the Linear work-item family width) and a shorter last one.
func card14190CheckUnits(t *testing.T, label string, units []card14190Unit, source string, count int, since, before time.Time) {
	t.Helper()
	if len(units) != count {
		t.Fatalf("%s: %d units, want %d", label, len(units), count)
	}
	for index, unit := range units {
		t.Logf("%s unit %d: %s %s %s %s source=%s [%s, %s)", label, index, unit.provider, unit.dataset, unit.mode, unit.cost,
			unit.source, unit.since.UTC().Format(time.RFC3339Nano), unit.before.UTC().Format(time.RFC3339Nano))
		if unit.provider != "linear" || unit.dataset != "work-items" || unit.mode != "backfill" || unit.cost != "medium" || unit.source != source {
			t.Errorf("%s unit %d: %s %s %s %s source %s, want linear work-items backfill medium source %s", label, index,
				unit.provider, unit.dataset, unit.mode, unit.cost, unit.source, source)
		}
		wantSince := since.AddDate(0, 0, 14*index)
		if !unit.since.Equal(wantSince) {
			t.Errorf("%s unit %d: since %s, want %s", label, index, unit.since.UTC(), wantSince)
		}
		wantBefore := wantSince.AddDate(0, 0, 14)
		if index == len(units)-1 {
			wantBefore = before
		}
		if !unit.before.Equal(wantBefore) {
			t.Errorf("%s unit %d: before %s, want %s", label, index, unit.before.UTC(), wantBefore)
		}
	}
}

// TestCardArgvChaos14190PlansTheLinearWorkItemsUnits executes the card's argv: --config-id, --dataset-keys work-items,
// --since 2026-06-01, no --before (the window ends with the run's UTC day). Run on 2026-11-24 it plans 13 units:
// 2026-06-01 .. 2026-11-24 is 177 days, 12 chunks of 14 days and one of 9. The same verb with the earlier Linear
// backfill's window (--since 2026-02-01 --before 2026-11-21, run 2026-11-20) plans 21 units, the count measured on
// that run. With the work-items dataset row off, the card's argv is refused and starts nothing.
func TestCardArgvChaos14190PlansTheLinearWorkItemsUnits(t *testing.T) {
	ctx := context.Background()
	instance, admin := startDatabase(t)
	open := func() (string, *pgxpool.Pool) {
		uri := freshDatabase(t, instance, admin)
		pool, err := pgxpool.New(ctx, uri)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		return uri, pool
	}
	day := func(text string) time.Time { return mustDay(t, text) }

	uri, pool := open()
	config, source := card14190Seed(t, ctx, pool)
	cardArgv := []string{"--config-id", config, "--dataset-keys", "work-items", "--since", "2026-06-01"}
	runAt := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	code, stdout, _ := card14190Run(t, ctx, uri, pool, runAt, cardArgv)
	if code != cli.ExitOK || !strings.HasPrefix(stdout, "Backfill materialized: 13 unit(s) planned for sync_run_id=") {
		t.Fatalf("card argv: exit %d, stdout %q, want exit 0 and 13 units materialized", code, stdout)
	}
	card14190CheckUnits(t, "card", card14190Units(t, ctx, pool, stdout), source, 13,
		day("2026-06-01"), day("2026-11-25").Add(-time.Microsecond))
	var keys []string
	if err := pool.QueryRow(ctx, `SELECT dataset_keys FROM sync_manual_triggers`).Scan(&keys); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(keys) != "[work-items]" {
		t.Errorf("trigger dataset_keys %v, want [work-items]", keys)
	}

	uri, pool = open()
	config, source = card14190Seed(t, ctx, pool)
	calibration := []string{"--config-id", config, "--dataset-keys", "work-items", "--since", "2026-02-01", "--before", "2026-11-21"}
	code, stdout, _ = card14190Run(t, ctx, uri, pool, time.Date(2026, 11, 20, 0, 17, 35, 0, time.UTC), calibration)
	if code != cli.ExitOK || !strings.HasPrefix(stdout, "Backfill materialized: 21 unit(s) planned for sync_run_id=") {
		t.Fatalf("calibration argv: exit %d, stdout %q, want exit 0 and 21 units materialized", code, stdout)
	}
	card14190CheckUnits(t, "calibration", card14190Units(t, ctx, pool, stdout), source, 21,
		day("2026-02-01"), day("2026-11-21").Add(-time.Microsecond))

	if _, err := pool.Exec(ctx, `UPDATE integration_datasets SET is_enabled = false WHERE dataset_key = 'work-items'`); err != nil {
		t.Fatal(err)
	}
	var before int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM scheduled_sync_occurrences`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr := card14190Run(t, ctx, uri, pool, runAt, cardArgv)
	if code != cli.ExitRefused || stdout != "" || !strings.Contains(stderr, "work-items") || !strings.Contains(stderr, "is not enabled for integration") {
		t.Fatalf("work-items row off: exit %d, stdout %q, stderr %s, want exit %d and the not-enabled refusal", code, stdout, stderr, cli.ExitRefused)
	}
	var after int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM scheduled_sync_occurrences`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("a refused run wrote %d occurrence(s)", after-before)
	}
}
