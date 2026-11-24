//go:build integration

package analytics

// CHAOS-14259 pooled TEST_SUITE_DURATION_P95 at three times the production size (2,700,000 synthetic suite rows of one
// organization): the measure's statement reads at most TestopsPooledMaxRowsToRead rows (system.query_log read_rows) and is
// served on the production query client, both when the window's repository-days hold stored samples and when one does not
// (the raw arm reads). The table has no day key, so an organization-wide inner select reads the organization's whole history:
// the raw arm's newest-attempt rule reads only the runs of the repository-days without stored samples. The source text of the
// query-api build before the retired-slot rule (ops 84d5d1da, rendered verbatim below) is read too, as the logged reference.

import (
	"crypto/sha256"
	"encoding/hex"
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
	e := startOwnersEnv(t)
	const org = "f0f0f0f0-0000-4000-8000-000000014259"
	const repo = "b0b0b0b0-0000-4000-8000-000000014259"
	const history = 2_700_000
	e.repo(org, repo, "acme/history")
	e.owner(org, "team-a", repo, "acme/history", 1, openFrom(), nil)
	e.durations(org, repo, day1, 10, 20, 30)
	e.write(org, day1, repo)
	e.exec(fmt.Sprintf(`INSERT INTO test_suite_results (repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count,
  skipped_count, duration_seconds, started_at, finished_at, org_id, last_synced, report_slot, run_attempt)
SELECT toUUID('%s'), concat('h-', toString(intDiv(number, 10))), concat('hs-', toString(number)), 'pkg', 1, 1, 0, 0,
  toFloat64(number %% 600), toDateTime64('2025-01-01 00:00:00', 3, 'UTC') + intDiv(number, 3), NULL, '%s',
  toDateTime64('2025-12-31 00:00:00', 3, 'UTC'), if(number %% 2 = 0, '', concat('slot-', toString(number %% 5))), 1
FROM numbers(%d)`, repo, org, history))
	e.exec(`OPTIMIZE TABLE test_suite_results FINAL`)
	var suiteRows uint64
	if err := e.admin.QueryRow(e.ctx, `SELECT count() FROM test_suite_results WHERE org_id = ?`, org).Scan(&suiteRows); err != nil {
		t.Fatal(err)
	}

	read := func(phase, label, source string) uint64 {
		t.Helper()
		id := "c14259-" + phase + "-" + label + "-" + uuid.NewString()[:8]
		ctx := stdclickhouse.Context(e.ctx, stdclickhouse.WithQueryID(id))
		var p95 *float64
		var samples uint64
		if err := e.admin.QueryRow(ctx, "SELECT "+testopspooled.P95+", count() FROM "+source+" SETTINGS max_rows_to_read = 0",
			stdclickhouse.Named("org_id", org), stdclickhouse.Named("start_date", "2026-01-01"), stdclickhouse.Named("end_date", "2026-01-07")).
			Scan(&p95, &samples); err != nil {
			t.Fatalf("%s %s: %v", phase, label, err)
		}
		e.exec(`SYSTEM FLUSH LOGS`)
		var rows uint64
		if err := e.admin.QueryRow(e.ctx, `SELECT read_rows FROM system.query_log WHERE query_id = ? AND type = 'QueryFinish'`, id).Scan(&rows); err != nil {
			t.Fatalf("%s %s query_log: %v", phase, label, err)
		}
		t.Logf("CELL14259 %s %s: read_rows %d = %.2f x the %d suite rows of the organization (p95 %v over %d samples)",
			phase, label, rows, float64(rows)/float64(suiteRows), suiteRows, p95, samples)
		return rows
	}
	// The served path runs on the production query client (chclient.Options: no byte bound, CHAOS-4651), so only the pooled
	// row bound can refuse the read.
	prod, err := dhclickhouse.NewClickHouseQueryClientWithOptions(chclient.Options(e.uri))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = prod.Close() })
	served := func(phase string) error {
		t.Helper()
		_, err := resolveOneTimeseries(e.ctx, prod, org, tsInput(model.DimensionInputTeam, model.MeasureInputTestSuiteDurationP95), false, nil, true)
		var refused *PooledReadBoundError
		if errors.As(err, &refused) {
			t.Logf("CELL14259 %s served TEST_SUITE_DURATION_P95 under the %d-row bound: refused, code %d (%v)", phase, TestopsPooledMaxRowsToRead, refused.Code, err)
		} else {
			t.Logf("CELL14259 %s served TEST_SUITE_DURATION_P95 under the %d-row bound: err %v", phase, TestopsPooledMaxRowsToRead, err)
		}
		return err
	}

	for _, phase := range []string{"stored", "legacy"} {
		if phase == "legacy" {
			// A newest daily row without stored samples (written before the writer stored them): the raw arm reads the day too.
			e.exec(`INSERT INTO testops_test_metrics_daily (repo_id, day, org_id, computed_at) VALUES (?, ?, ?, now() + INTERVAL 1 HOUR)`, repo, day1, org)
		}
		live := read(phase, "live-84d5d1da", suiteRunsSourceLive)
		head := read(phase, "head", testopspooled.SuiteRunsSource)
		err := served(phase)
		if head > TestopsPooledMaxRowsToRead || err != nil {
			t.Errorf("%s: the statement reads %d rows (the 84d5d1da text %d) for %d suite rows; the measure is served with err %v; want at most the %d-row bound and served",
				phase, head, live, suiteRows, err, TestopsPooledMaxRowsToRead)
		}
	}
}
