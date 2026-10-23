//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// The writers store every row under the org and the scope view and write a row
// of zeros over the legacy key of each key they computed. A reader that keeps the
// legacy rows and one view must count the day once at every step, including the
// step where a partition has written and the run-level retraction has not run.

func runWorkItemFamiliesOnly(t *testing.T, ctx context.Context, conn driver.Conn, clock time.Time) Run {
	t.Helper()
	run := Run{ID: uuid.NewString(), OrganizationID: staleKeyAcceptanceOrg, TargetDay: staleKeyAcceptanceDay}
	partition := Partition{ID: uuid.NewString(), RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(uuid.Nil.String())}}
	now := func() time.Time { return clock }
	attribution, err := NewWorkItemAttributionExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	attribution.nowUTC = now
	metrics, err := NewWorkItemExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	metrics.nowUTC = now
	state, err := NewWorkItemStateExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	state.nowUTC = now
	for _, executor := range []NativeFamilyExecutor{attribution, metrics, state} {
		if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
			t.Fatalf("compute at %s: %v", clock.Format(time.RFC3339), err)
		}
	}
	run.DiscoveredRepoIDs = partition.RepoIDs
	return run
}

func viewSum(t *testing.T, ctx context.Context, conn driver.Conn, table, measure, predicate string) float64 {
	t.Helper()
	var sum float64
	if err := conn.QueryRow(ctx, `SELECT toFloat64(sum(`+measure+`)) FROM `+table+` FINAL WHERE org_id = ? AND day = ? AND `+predicate,
		staleKeyAcceptanceOrg, staleKeyAcceptanceDay).Scan(&sum); err != nil {
		t.Fatalf("sum %s over %s: %v", measure, predicate, err)
	}
	return sum
}

func TestWorkItemWritersGiveEveryViewTheSameDayAndRetractTheLegacyRows(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := staleKeyAcceptanceDay
	seedStaleKeyAcceptanceTeams(t, ctx, conn, day.Add(-72*time.Hour),
		staleKeyAcceptanceTeam{id: "ENG", nativeKey: "ENG", active: true},
		staleKeyAcceptanceTeam{id: "OPS", nativeKey: "OPS", active: true},
	)
	seedStaleKeyAcceptanceItems(t, ctx, conn)

	// Legacy rows written before the views existed: one for a key the run
	// computes (proj-eng, ENG), one for a key it never computes (proj-gone).
	legacyAt := day.Add(20 * time.Hour)
	for _, scope := range []struct{ id, team string }{{"proj-eng", "ENG"}, {"proj-gone", "ENG"}} {
		if err := conn.Exec(ctx, `INSERT INTO work_item_metrics_daily (day, provider, work_scope_id, team_id, items_completed, computed_at, org_id)
VALUES (?, 'linear', ?, ?, 99, ?, ?)`, day, scope.id, scope.team, legacyAt, staleKeyAcceptanceOrg); err != nil {
			t.Fatalf("seed the legacy row of %s: %v", scope.id, err)
		}
	}
	for _, status := range []string{"done", "in_progress", "blocked", "never-computed"} {
		if err := conn.Exec(ctx, `INSERT INTO work_item_state_durations_daily (day, provider, work_scope_id, team_id, status, items_touched, duration_hours, computed_at, org_id)
VALUES (?, 'linear', 'proj-eng', 'ENG', ?, 50, 5, ?, ?)`, day, status, legacyAt, staleKeyAcceptanceOrg); err != nil {
			t.Fatalf("seed the legacy state row of %s: %v", status, err)
		}
	}
	if err := conn.Exec(ctx, `INSERT INTO work_item_user_metrics_daily (day, provider, work_scope_id, user_identity, team_id, items_completed, computed_at, org_id)
VALUES (?, 'linear', 'proj-eng', 'ghost', 'ENG', 7, ?, ?)`, day, legacyAt, staleKeyAcceptanceOrg); err != nil {
		t.Fatalf("seed the legacy user row: %v", err)
	}
	const legacyAndOrg = `row_view IN ('', 'org')`
	const legacyAndScope = `row_view IN ('', 'scope')`

	// Step 1, a partition wrote and the run-level retraction has not run (a
	// finalize that failed): the computed key's legacy row is already retracted;
	// the key nothing computed still holds its legacy 99.
	first := day.Add(30 * time.Hour)
	run := runWorkItemFamiliesOnly(t, ctx, conn, first)
	org := viewSum(t, ctx, conn, "work_item_metrics_daily", "items_completed", `row_view = 'org'`)
	if org != 2 {
		t.Fatalf("the org view of the computed day = %v, want 2", org)
	}
	if got := viewSum(t, ctx, conn, "work_item_metrics_daily", "items_completed", `row_view = 'scope'`); got != org {
		t.Fatalf("the scope view = %v, want the org view %v", got, org)
	}
	if got := viewSum(t, ctx, conn, "work_item_metrics_daily", "items_completed", legacyAndOrg); got != org+99 {
		t.Fatalf("legacy + org = %v, want %v: only the legacy row of the key nothing computed (99) may be left", got, org+99)
	}
	var computedKeyLegacy float64
	if err := conn.QueryRow(ctx, `SELECT toFloat64(argMax(items_completed, computed_at)) FROM work_item_metrics_daily
WHERE org_id = ? AND day = ? AND row_view = '' AND work_scope_id = 'proj-eng'`, staleKeyAcceptanceOrg, day).Scan(&computedKeyLegacy); err != nil {
		t.Fatal(err)
	}
	if computedKeyLegacy != 0 {
		t.Fatalf("the legacy row of a computed key still holds %v after the partition wrote", computedKeyLegacy)
	}

	var liveLegacyBesideOrg uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM work_item_state_durations_daily FINAL
WHERE org_id = ? AND day = ? AND row_view = '' AND items_touched != 0
  AND status IN (SELECT status FROM work_item_state_durations_daily FINAL WHERE org_id = ? AND day = ? AND row_view = 'org')`,
		staleKeyAcceptanceOrg, day, staleKeyAcceptanceOrg, day).Scan(&liveLegacyBesideOrg); err != nil {
		t.Fatal(err)
	}
	if liveLegacyBesideOrg != 0 {
		t.Fatalf("%d live legacy state rows sit beside an org row of the same status after the partition wrote", liveLegacyBesideOrg)
	}

	// Step 2, the run ends: the legacy row of the key nothing computed goes too.
	endStaleKeyRun(t, ctx, conn, run, first.Add(time.Minute))
	for _, predicate := range []string{legacyAndOrg, legacyAndScope} {
		if got := viewSum(t, ctx, conn, "work_item_metrics_daily", "items_completed", predicate); got != org {
			t.Fatalf("%s after the run ended = %v, want %v", predicate, got, org)
		}
	}
	if got := viewSum(t, ctx, conn, "work_item_user_metrics_daily", "items_completed", `row_view IN ('', 'org') AND retracted = 0`); got != 0 {
		t.Fatalf("the legacy user row of a person nothing computed still counts %v after the run ended", got)
	}
	for _, table := range []string{"work_item_state_durations_daily"} {
		orgState := viewSum(t, ctx, conn, table, "items_touched", `row_view = 'org'`)
		if got := viewSum(t, ctx, conn, table, "items_touched", legacyAndOrg); got != orgState {
			t.Fatalf("%s legacy + org = %v, want the org view %v", table, got, orgState)
		}
		if got := viewSum(t, ctx, conn, table, "items_touched", legacyAndScope); got != orgState {
			t.Fatalf("%s legacy + scope = %v, want %v", table, got, orgState)
		}
	}

	// Step 3, the day is computed again: nothing a reader sees changes, and no
	// row of zeros is written over a legacy key that is already retracted.
	var zerosBefore uint64
	countLegacy := `SELECT count() FROM work_item_metrics_daily WHERE org_id = ? AND day = ? AND row_view = ''`
	if err := conn.QueryRow(ctx, countLegacy, staleKeyAcceptanceOrg, day).Scan(&zerosBefore); err != nil {
		t.Fatal(err)
	}
	second := first.Add(10 * time.Hour)
	run = runWorkItemFamiliesOnly(t, ctx, conn, second)
	endStaleKeyRun(t, ctx, conn, run, second.Add(time.Minute))
	var zerosAfter uint64
	if err := conn.QueryRow(ctx, countLegacy, staleKeyAcceptanceOrg, day).Scan(&zerosAfter); err != nil {
		t.Fatal(err)
	}
	if zerosAfter != zerosBefore {
		t.Fatalf("legacy row versions %d -> %d: a recompute must not write over a legacy key that is already retracted", zerosBefore, zerosAfter)
	}
	if got := viewSum(t, ctx, conn, "work_item_metrics_daily", "items_completed", legacyAndOrg); got != org {
		t.Fatalf("legacy + org after the recompute = %v, want %v", got, org)
	}
}
