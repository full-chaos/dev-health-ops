//go:build integration

package sync

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

const (
	streakOrgID         = "00000000-0000-4000-8000-0000000000aa"
	streakIntegrationID = "00000000-0000-4000-8000-000000001002"
	streakSourceA       = "00000000-0000-4000-8000-000000001003"
	streakSourceB       = "00000000-0000-4000-8000-0000000b1003"
	streakSourceEmpty   = "00000000-0000-4000-8000-0000000c1003"
	streakRunID         = "00000000-0000-4000-8000-0000000d1003"
)

// legacyRecentTerminalUnitsSQL is RecentTerminalUnitsSQL as it was before
// CHAOS-11391: a window over every terminal unit of the integration.
const legacyRecentTerminalUnitsSQL = `
SELECT source_id::text, status = 'failed', COALESCE(result::jsonb ->> 'cause_class', ''), updated_at
FROM (
  SELECT source_id, status, result, updated_at,
         row_number() OVER (PARTITION BY source_id ORDER BY updated_at DESC, id DESC) AS position
  FROM public.sync_run_units
  WHERE org_id = $1 AND integration_id = $2::uuid AND dataset_key = $3
    AND status IN ('success', 'failed')
) AS recent
WHERE position <= $4
ORDER BY source_id, position`

type streakRow struct {
	Source string
	Failed bool
	Cause  string
	At     time.Time
}

func seedStreakUnits(t *testing.T, pool *pgxpool.Pool, perSource int) {
	t.Helper()
	ctx := context.Background()
	for _, source := range []string{streakSourceB, streakSourceEmpty} {
		if _, err := pool.Exec(ctx, `INSERT INTO integration_sources (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at) VALUES ($1::uuid,$2,$3::uuid,'github','repository',$1,$1,$1,TRUE,'{}'::json,now(),now())`,
			source, streakOrgID, streakIntegrationID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO sync_runs (id,org_id,integration_id,triggered_by,mode,status,total_units,completed_units,failed_units,created_at) VALUES ($1::uuid,$2,$3::uuid,'schedule','incremental','running',0,0,0,now())`,
		streakRunID, streakOrgID, streakIntegrationID); err != nil {
		t.Fatal(err)
	}
	// Three units share each updated_at, so the id tie-break decides the order.
	// A third of the failures carry a cause class; some units are not terminal
	// and some belong to another dataset.
	for _, source := range []string{streakSourceA, streakSourceB} {
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,integration_id,source_id,provider,dataset_key,cost_class,mode,status,attempts,result,created_at,updated_at)
SELECT gen_random_uuid(), $1, $2::uuid, $3::uuid, $4::uuid, 'github',
       CASE WHEN g % 11 = 0 THEN 'commits' ELSE 'work-items' END, 'medium', 'incremental',
       CASE WHEN g % 7 = 0 THEN 'running' WHEN g % 3 = 0 THEN 'success' ELSE 'failed' END, 0,
       CASE WHEN g % 3 = 1 THEN '{"cause_class":"rate_limited"}'::json ELSE '{}'::json END,
       now(), timestamptz '2026-08-01 00:00:00+00' + ((g / 3) * interval '1 minute')
FROM generate_series(1, $5) AS g`,
			streakOrgID, streakRunID, streakIntegrationID, source, perSource); err != nil {
			t.Fatal(err)
		}
	}
}

func readStreakRows(t *testing.T, pool *pgxpool.Pool, sql string) []streakRow {
	t.Helper()
	rows, err := pool.Query(context.Background(), sql, streakOrgID, streakIntegrationID, canonicalWorkItemsDataset, providersync.RecentTerminalUnitsLimit)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []streakRow
	for rows.Next() {
		var row streakRow
		if err := rows.Scan(&row.Source, &row.Failed, &row.Cause, &row.At); err != nil {
			t.Fatal(err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// CHAOS-11391 (acp C3): with more than eight units per source and ties on
// updated_at, the per-source top-N returns the same rows in the same order as
// the window it replaces, and the materializer's streaks equal the streaks
// computed from the old rows.
func TestFailureStreakReadMatchesTheLegacyWindowQuery(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	seedStreakUnits(t, fixture.pool, 60)
	legacy := readStreakRows(t, fixture.pool, legacyRecentTerminalUnitsSQL)
	current := readStreakRows(t, fixture.pool, providersync.RecentTerminalUnitsSQL)
	if len(legacy) != 2*providersync.RecentTerminalUnitsLimit {
		t.Fatalf("legacy rows = %d, want %d (two sources with eight rows each)", len(legacy), 2*providersync.RecentTerminalUnitsLimit)
	}
	if !reflect.DeepEqual(legacy, current) {
		t.Fatalf("rows differ\nlegacy:  %+v\ncurrent: %+v", legacy, current)
	}

	want := map[string]providersync.FailureStreak{}
	bySource := map[string][]providersync.UnitOutcome{}
	for _, row := range legacy {
		bySource[row.Source] = append(bySource[row.Source], providersync.UnitOutcome{Failed: row.Failed, CauseClass: row.Cause, At: row.At.UTC()})
	}
	for source, outcomes := range bySource {
		if streak := providersync.FailureStreakOf(outcomes); streak.Count > 0 {
			want[source] = streak
		}
	}
	got := loadFailureStreaks(context.Background(), fixture.pool, streakOrgID, streakIntegrationID, "github",
		[]PlanDataset{{Key: workitemcontract.FamilyDatasets()[0]}})
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(want)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("streaks = %s, want %s", gotJSON, wantJSON)
	}
}

// CHAOS-11391 + CHAOS-11427: a failure-history read that cannot finish must
// end at its own bound and leave the materialize (coordinator) transaction
// untouched: the read runs on its own connection, so the coordinator
// transaction stays usable and its statement_timeout never changes. A table
// lock held by another session stands in for the slow read.
func TestFailureStreakReadThatCannotFinishLeavesTheTransactionUsable(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	seedStreakUnits(t, fixture.pool, 20)
	background := context.Background()

	locker, err := fixture.pool.Acquire(background)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Release()
	lockTx, err := locker.Begin(background)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(background) }()
	if _, err := lockTx.Exec(background, "LOCK TABLE public.sync_run_units IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}

	tx, err := fixture.pool.Begin(background)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(background) }()
	var original string
	if err := tx.QueryRow(background, "SHOW statement_timeout").Scan(&original); err != nil {
		t.Fatal(err)
	}

	stepCtx, cancel := context.WithTimeout(background, 15*time.Second)
	defer cancel()
	started := time.Now()
	got := loadFailureStreaks(stepCtx, fixture.pool, streakOrgID, streakIntegrationID, "github",
		[]PlanDataset{{Key: workitemcontract.FamilyDatasets()[0]}})
	if got != nil {
		t.Fatalf("streaks = %v, want nil", got)
	}
	if elapsed := time.Since(started); elapsed > failureStreakReadCap+failureStreakClientMargin+300*time.Millisecond {
		t.Fatalf("the read held the step for %s: want it ended near the %s bound", elapsed, failureStreakReadCap)
	}

	var one int
	if err := tx.QueryRow(background, "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("the coordinator transaction is unusable after a failed streak read: %v", err)
	}
	var after string
	if err := tx.QueryRow(background, "SHOW statement_timeout").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != original {
		t.Fatalf("statement_timeout after the read = %q, want the original %q", after, original)
	}
}
