//go:build integration

package analytics

// CHAOS-14591 pooled TEST_SUITE_DURATION_P95 at 3,000,000 synthetic suite rows of one organization (repository A 2,700,000,
// repository B 300,000), on the test ClickHouse (its version is logged). The statement's read
// (system.query_log read_rows) must be: <= 1,000 rows when every window day holds stored samples; <= 1.2 x B's rows when
// only B has a day without stored samples (the repository filter prunes A: without it the read is A + B); <= 1.2 x the
// organization's rows and <= TestopsPooledMaxRowsToRead when both have such a day. A mutant without the repository filter
// reads the whole organization even when every day is stored. The measure is served on the production query client
// (chclient.Options) in every phase, and the statement's memory stays under 1 GiB (query-api sends no max_memory_usage).
// The source text of the query-api build before the retired-slot rule (ops 84d5d1da, verbatim below) is read once as the
// logged reference.

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/chclient"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopspooled"
)

// suiteRunsSourceLive is testopspooled.SuiteRunsSource at ops 84d5d1da (the source of the live query-api build), rendered by
// a named test at that commit.
const suiteRunsSourceLive = `(
    SELECT org_id, repo_id, service_id, day, duration_sample
    FROM (
        SELECT
            org_id,
            repo_id,
            stored_service_id AS service_id,
            day,
            stored_duration AS duration_sample
        FROM (
            SELECT org_id, repo_id, day, pooled_samples_stored, pooled_service_ids, pooled_durations
            FROM testops_test_metrics_daily
            WHERE org_id = {org_id:String} AND day >= {start_date:Date} AND day < {end_date:Date}
            ORDER BY computed_at DESC, cityHash64(tuple(*)) DESC
            LIMIT 1 BY org_id, repo_id, day
        )
        ARRAY JOIN pooled_service_ids AS stored_service_id, pooled_durations AS stored_duration
        WHERE pooled_samples_stored = 1
        UNION ALL
        SELECT
            org_id,
            repo_id,
            service_id,
            toDate(coalesce(started_at, finished_at)) AS day,
            if(duration_seconds >= 0, duration_seconds, NULL) AS duration_sample
        FROM test_suite_results FINAL
        WHERE org_id = {org_id:String}
          AND toDate(coalesce(started_at, finished_at)) >= {start_date:Date} AND toDate(coalesce(started_at, finished_at)) < {end_date:Date}
          AND ((report_slot != '' AND (repo_id, run_id, report_slot, run_attempt) IN (SELECT repo_id, run_id, report_slot, max(run_attempt) FROM test_suite_results FINAL WHERE org_id = {org_id:String} AND report_slot != '' GROUP BY repo_id, run_id, report_slot)) OR (report_slot = '' AND (repo_id, run_id, suite_id) NOT IN (SELECT repo_id, run_id, arrayJoin(legacy_suite_ids) FROM test_suite_results FINAL WHERE org_id = {org_id:String} AND report_slot != '' AND (repo_id, run_id, report_slot, run_attempt) IN (SELECT repo_id, run_id, report_slot, max(run_attempt) FROM test_suite_results FINAL WHERE org_id = {org_id:String} AND report_slot != '' GROUP BY repo_id, run_id, report_slot))))
          AND (repo_id, toDate(coalesce(started_at, finished_at))) IN (SELECT repo_id, day FROM (
            SELECT org_id, repo_id, day, pooled_samples_stored
            FROM testops_test_metrics_daily
            WHERE org_id = {org_id:String} AND day >= {start_date:Date} AND day < {end_date:Date}
            ORDER BY computed_at DESC, cityHash64(tuple(*)) DESC
            LIMIT 1 BY org_id, repo_id, day
        ) WHERE pooled_samples_stored != 1)
    )
    WHERE (repo_id, day) IN (
        SELECT repo_id, day FROM testops_test_metrics_daily
        WHERE org_id = {org_id:String} AND day >= {start_date:Date} AND day < {end_date:Date}
        GROUP BY repo_id, day
        HAVING (argMax(testops_test_metrics_daily.retracted, testops_test_metrics_daily.computed_at) = 0))
) AS testops_suite_runs`

const suiteRunsSourceLiveSHA256 = "2558cbe4b0db75bc01c064cb123d3d9ee58677035495f5d3df4f765cd7c9d86e"

func TestTestopsPooledSuiteP95ReadStaysUnderTheRowBoundAtThreeTimesProductionSize(t *testing.T) {
	if sum := sha256.Sum256([]byte(suiteRunsSourceLive)); hex.EncodeToString(sum[:]) != suiteRunsSourceLiveSHA256 {
		t.Fatalf("the live source text changed: sha256 %x", sum)
	}
	repoFilter := "AND repo_id IN (SELECT repo_id FROM (" + testopspooled.LegacyRepoDays("testops_test_metrics_daily") + "))"
	if got := strings.Count(testopspooled.SuiteRunsSource, repoFilter); got != 1 {
		t.Fatalf("SuiteRunsSource holds the repository filter %d times, want 1", got)
	}
	mutant := strings.Replace(testopspooled.SuiteRunsSource, repoFilter, "", 1)
	e := startOwnersEnv(t)
	const org = "f0f0f0f0-0000-4000-8000-000000014591"
	const repoA, repoB = "b0b0b0b0-0000-4000-8000-00000014591a", "b0b0b0b0-0000-4000-8000-00000014591b"
	const rowsA, rowsB = 2_700_000, 300_000
	var version string
	if err := e.admin.QueryRow(e.ctx, `SELECT version()`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	for _, r := range []struct {
		id   string
		rows int
	}{{repoA, rowsA}, {repoB, rowsB}} {
		e.repo(org, r.id, "acme/"+r.id[len(r.id)-1:])
		e.owner(org, "team-a", r.id, "acme/"+r.id[len(r.id)-1:], 1, openFrom(), nil)
		e.durations(org, r.id, day1, 10, 20, 30)
		e.exec(fmt.Sprintf(`INSERT INTO test_suite_results (repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count,
  skipped_count, duration_seconds, started_at, finished_at, org_id, last_synced, report_slot, run_attempt)
SELECT toUUID('%s'), concat('h-', toString(intDiv(number, 10))), concat('hs-', toString(number)), 'pkg', 1, 1, 0, 0,
  toFloat64(number %% 600), toDateTime64('2025-01-01 00:00:00', 3, 'UTC') + intDiv(number, 3), NULL, '%s',
  toDateTime64('2025-12-31 00:00:00', 3, 'UTC'), if(number %% 2 = 0, '', concat('slot-', toString(number %% 5))), 1
FROM numbers(%d)`, r.id, org, r.rows))
	}
	e.write(org, day1, repoA, repoB)
	e.exec(`OPTIMIZE TABLE test_suite_results FINAL`)
	suiteRows := func(repo string) uint64 {
		t.Helper()
		var n uint64
		if err := e.admin.QueryRow(e.ctx, `SELECT count() FROM test_suite_results WHERE org_id = ? AND repo_id = ?`, org, repo).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	inA, inB := suiteRows(repoA), suiteRows(repoB)
	all := inA + inB

	read := func(phase, label, source string) (rows, memory uint64) {
		t.Helper()
		id := "c14591-" + phase + "-" + label + "-" + uuid.NewString()[:8]
		ctx := stdclickhouse.Context(e.ctx, stdclickhouse.WithQueryID(id))
		var p95 *float64
		var samples uint64
		if err := e.admin.QueryRow(ctx, "SELECT "+testopspooled.P95+", count() FROM "+source+" SETTINGS max_rows_to_read = 0",
			stdclickhouse.Named("org_id", org), stdclickhouse.Named("start_date", "2026-01-01"), stdclickhouse.Named("end_date", "2026-01-07")).
			Scan(&p95, &samples); err != nil {
			t.Fatalf("%s %s: %v", phase, label, err)
		}
		e.exec(`SYSTEM FLUSH LOGS`)
		if err := e.admin.QueryRow(e.ctx, `SELECT read_rows, memory_usage FROM system.query_log WHERE query_id = ? AND type = 'QueryFinish'`, id).
			Scan(&rows, &memory); err != nil {
			t.Fatalf("%s %s query_log: %v", phase, label, err)
		}
		t.Logf("CELL14591 %s %s %s: read_rows %d = %.3f x the %d suite rows (A %d, B %d), memory %d bytes, p95 %v over %d samples",
			version, phase, label, rows, float64(rows)/float64(all), all, inA, inB, memory, p95, samples)
		return rows, memory
	}
	prod, err := dhclickhouse.NewClickHouseQueryClientWithOptions(chclient.Options(e.uri))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = prod.Close() })
	served := func(phase string) {
		t.Helper()
		_, err := resolveOneTimeseries(e.ctx, prod, org, tsInput(model.DimensionInputTeam, model.MeasureInputTestSuiteDurationP95), false, nil, true)
		var refused *PooledReadBoundError
		if errors.As(err, &refused) || err != nil {
			t.Errorf("%s %s: TEST_SUITE_DURATION_P95 under the %d-row bound: %v; want served", version, phase, TestopsPooledMaxRowsToRead, err)
		}
	}
	const memoryCap = 1 << 30
	check := func(phase string, want uint64, wantText string) {
		t.Helper()
		rows, memory := read(phase, "head", testopspooled.SuiteRunsSource)
		if rows > want || rows > TestopsPooledMaxRowsToRead {
			t.Errorf("%s %s: the statement reads %d rows, want at most %d (%s) and the %d-row bound", version, phase, rows, want, wantText, TestopsPooledMaxRowsToRead)
		}
		if memory > memoryCap {
			t.Errorf("%s %s: the statement uses %d bytes of memory, want at most %d", version, phase, memory, memoryCap)
		}
		served(phase)
	}
	legacy := func(repo string) {
		// A newest daily row without stored samples (written before the writer stored them): the raw arm reads that day.
		e.exec(`INSERT INTO testops_test_metrics_daily (repo_id, day, org_id, computed_at) VALUES (?, ?, ?, now() + INTERVAL 1 HOUR)`, repo, day1, org)
	}

	check("stored", 1_000, "every window day holds stored samples")
	if rows, _ := read("stored", "mutant-without-repository-filter", mutant); rows < all {
		t.Errorf("%s stored: the mutant without the repository filter reads %d rows, want at least the %d suite rows (the filter is what prunes)", version, rows, all)
	}
	legacy(repoB)
	check("legacy-B", inB+inB/5+1_000, "1.2 x repository B's rows: A is pruned")
	legacy(repoA)
	check("legacy-A-and-B", all+all/5+1_000, "1.2 x the organization's rows")
	read("legacy-A-and-B", "reference-84d5d1da", suiteRunsSourceLive)
}
