//go:build integration

package analytics

import (
	"errors"
	"fmt"
	"testing"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/chclient"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopspooled"
)

// CHAOS-14595 granule edges: a qualifying repository Q (a legacy day) and a non-qualifying N (every day stored) share ONE
// merged part of test_suite_results, with neighbouring repo_ids. The pooled suite read of a stored-only window reads at most
// 1,000 rows; with Q's legacy day it reads at most Q's rows plus one granule at each edge of Q's key range (2 x 8,192) plus
// 1,000. Then the row bound is set just at that read: the measure is served; at half of Q's rows it is refused (code 158).

const (
	granuleOrg   = "f0f0f0f0-0000-4000-8000-0000001459e0"
	granuleRepoQ = "f0f0f0f0-0000-4000-8000-0000001459a1"
	granuleRepoN = "f0f0f0f0-0000-4000-8000-0000001459a2"
	granuleRows  = 8_192
)

func TestTestopsPooledSuiteReadIsBoundedAtGranuleEdges(t *testing.T) {
	e := startOwnersEnv(t)
	var version string
	if err := e.admin.QueryRow(e.ctx, `SELECT version()`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	const history = 100_000
	for _, repo := range []string{granuleRepoQ, granuleRepoN} {
		name := "acme/" + repo[len(repo)-2:]
		e.repo(granuleOrg, repo, name)
		e.owner(granuleOrg, "team-g", repo, name, 1, openFrom(), nil)
		e.exec(fmt.Sprintf(`INSERT INTO test_suite_results (repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count,
  skipped_count, duration_seconds, started_at, finished_at, org_id, last_synced, report_slot, run_attempt)
SELECT toUUID('%s'), concat('h-', toString(intDiv(number, 10))), concat('hs-', toString(number)), 'pkg', 1, 1, 0, 0,
  toFloat64(number %% 600), toDateTime64('2025-01-01 00:00:00', 3, 'UTC') + intDiv(number, 3), NULL, '%s',
  toDateTime64('2025-12-31 00:00:00', 3, 'UTC'), '', 1
FROM numbers(%d)`, repo, granuleOrg, history))
		e.durations(granuleOrg, repo, day1, 10, 20, 30)
	}
	e.write(granuleOrg, day1, granuleRepoQ, granuleRepoN)
	e.exec(`OPTIMIZE TABLE test_suite_results FINAL`)
	var parts, rowsQ uint64
	if err := e.admin.QueryRow(e.ctx, `SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'test_suite_results' AND active`).Scan(&parts); err != nil {
		t.Fatal(err)
	}
	if err := e.admin.QueryRow(e.ctx, `SELECT count() FROM test_suite_results WHERE org_id = ? AND repo_id = ?`, granuleOrg, granuleRepoQ).Scan(&rowsQ); err != nil {
		t.Fatal(err)
	}
	if parts != 1 {
		t.Fatalf("control: test_suite_results holds %d active parts after OPTIMIZE FINAL, want 1 (Q and N in one merged part)", parts)
	}

	read := func(phase string) uint64 {
		t.Helper()
		id := "c14595-granule-" + phase + "-" + uuid.NewString()[:8]
		ctx := stdclickhouse.Context(e.ctx, stdclickhouse.WithQueryID(id))
		var p95 *float64
		var samples uint64
		if err := e.admin.QueryRow(ctx, "SELECT "+testopspooled.P95+", count() FROM "+testopspooled.SuiteRunsSource+" SETTINGS max_rows_to_read = 0",
			stdclickhouse.Named("org_id", granuleOrg), stdclickhouse.Named("start_date", "2026-01-01"), stdclickhouse.Named("end_date", "2026-01-07")).
			Scan(&p95, &samples); err != nil {
			t.Fatalf("%s: %v", phase, err)
		}
		e.exec(`SYSTEM FLUSH LOGS`)
		var rows uint64
		if err := e.admin.QueryRow(e.ctx, `SELECT read_rows FROM system.query_log WHERE query_id = ? AND type = 'QueryFinish'`, id).Scan(&rows); err != nil {
			t.Fatalf("%s query_log: %v", phase, err)
		}
		t.Logf("CELL14595 granule %s %s: read_rows %d (Q %d rows, N %d rows, 1 part), p95 %v over %d samples", version, phase, rows, rowsQ, history, str(p95), samples)
		return rows
	}

	if rows := read("stored-only"); rows > 1_000 {
		t.Errorf("%s stored-only: the read reads %d rows, want at most 1,000", version, rows)
	}
	e.exec(`INSERT INTO testops_test_metrics_daily (repo_id, day, org_id, computed_at) VALUES (?, ?, ?, now() + INTERVAL 1 HOUR)`, granuleRepoQ, day1, granuleOrg)
	bound := rowsQ + 2*granuleRows + 1_000
	if rows := read("Q-legacy"); rows > bound {
		t.Errorf("%s Q-legacy: the read reads %d rows, want at most %d (Q's %d rows + 2 granules + 1,000): N's rows in the shared part are read",
			version, rows, bound, rowsQ)
	}

	prod, err := dhclickhouse.NewClickHouseQueryClientWithOptions(chclient.Options(e.uri))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = prod.Close() })
	previous := TestopsPooledMaxRowsToRead
	t.Cleanup(func() { TestopsPooledMaxRowsToRead = previous })
	serve := func(limit uint64) error {
		TestopsPooledMaxRowsToRead = limit
		_, err := resolveOneTimeseries(e.ctx, prod, granuleOrg, tsInput(model.DimensionInputTeam, model.MeasureInputTestSuiteDurationP95), false, nil, true)
		return err
	}
	if err := serve(bound); err != nil {
		t.Errorf("%s: with the row bound at %d (Q + 2 granules + 1,000) TEST_SUITE_DURATION_P95 is not served: %v", version, bound, err)
	}
	var refused *PooledReadBoundError
	if err := serve(rowsQ / 2); !errors.As(err, &refused) || refused.Code != 158 {
		t.Errorf("%s: with the row bound at %d (half of Q's rows) the read gives %v, want a refusal with code 158", version, rowsQ/2, err)
	}
}
