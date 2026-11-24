//go:build integration

package chmigrate_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// THROWAWAY exec proof of a prod read form (FORM-14591-1, CHAOS-14591): never merged.
// form14591 is FORM-14591-1.sql with ONE swap: every "c6a38355" -> "f0f0f0f0" (public repo, synthetic org prefix).
// The cell pins the sha256 of this copy and of the copy swapped back (= the file pm runs).
const (
	form14591SHA256     = "e0f02f6bf63a933e44cd1f517d51196c137dfad033ea81bd382259afae91a760"
	form14591RealSHA256 = "5dbac2c1dbeea2fa8614005e9cc89cecaaff0b38ce11d9d8ff5d63f831be2fe8"
	form14591RowsCap    = "> 9990000"
	form14591BytesCap   = "> 4294967296"
	form14591GateTables = "table IN ('test_suite_results', 'testops_test_metrics_daily')"
)

const form14591 = `-- FORM-14591-1: legacy suite repo-days of the pooled TEST_SUITE_DURATION_P95 arm (CHAOS-14591), lane l14574, 2026-11-24.
-- ONE statement, ClickHouse, prod read. Org prefix f0f0f0f0. No SET / SETTINGS / EXPLAIN / FORMAT / writes.
-- The rule read (internal/queryapi/testopspooled/testopspooled.go at origin/main 9c3e2a07 and at 84d5d1da, same text):
--   NewestStoredRows :52-64  newest row of each (org_id, repo_id, day) of testops_test_metrics_daily:
--                            ORDER BY computed_at DESC, cityHash64(tuple(*)) DESC LIMIT 1 BY org_id, repo_id, day.
--   LegacyRepoDays   :68-70  that newest row has pooled_samples_stored != 1 (0 = written before chmigrate 161, 2 = over
--                            the writers' cap). Only these repo-days read test_suite_results FINAL (the raw arm).
--   LiveRepoDays     :77-83  both arms keep only (repo_id, day) whose newest row by computed_at has retracted = 0
--                            (GROUP BY repo_id, day HAVING argMax(retracted, computed_at) = 0; liverow.NewestPredicate).
--   SuiteRunsSource  :117-148 raw suite day = toDate(coalesce(started_at, finished_at)).
-- Columns: pooled_samples_stored (chmigrate 161), retracted on the daily table (chmigrate 165); the live query-api
-- build reads both on prod. test_suite_results: org_id, repo_id, started_at, finished_at (baseline). Not read:
-- test_suite_results.retracted (chmigrate 171).
-- Windows: day >= today() - 30, day >= today() - 90, and all days (today() of the CH server, UTC on prod).
-- Run rule: once. A SQL error (STOP-COST-GATE included) = STOP. A timeout (30 s) = STOP, no re-run.

-- stmt1 ONE statement. Gate tables: testops_test_metrics_daily (read 2 times: newest rows + live keys),
-- test_suite_results (read 1 time, FINAL). Budget: the gate sums system.parts rows weighted by reads,
-- 2 x R_daily + R_suite <= 9,990,000, so the FINAL work bound 2 x (2 x R_daily + R_suite) + 2 x P + 1 < 20,000,000.
-- WANT daily_orgs = 1 and suite_orgs = 1 (ORG-1). WANT days_all > 0 (control).
-- ANSWER legacy_live_* = repo-days the raw arm reads (newest row pooled_samples_stored != 1 AND live);
-- legacy_live_suite_* = those with raw suite rows. INFO the rest.
WITH (SELECT throwIf(
    (SELECT sum(rows * multiIf(table = 'testops_test_metrics_daily', 2, 1)) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('test_suite_results', 'testops_test_metrics_daily')) > 9990000
    OR (SELECT sum(bytes_on_disk * multiIf(table = 'testops_test_metrics_daily', 2, 1)) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('test_suite_results', 'testops_test_metrics_daily')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT
  uniqExactIf(k_org, in_d = 1) AS daily_orgs,
  uniqExactIf(k_org, in_s = 1) AS suite_orgs,
  countIf(in_d = 1 AND k_day >= today() - 30) AS days_30,
  countIf(in_d = 1 AND live = 1 AND k_day >= today() - 30) AS live_30,
  countIf(in_d = 1 AND live = 1 AND ps != 1 AND k_day >= today() - 30) AS legacy_live_30,
  countIf(in_d = 1 AND live = 1 AND ps != 1 AND in_s = 1 AND k_day >= today() - 30) AS legacy_live_suite_30,
  countIf(in_d = 1 AND live = 1 AND ps = 0 AND k_day >= today() - 30) AS ps0_live_30,
  countIf(in_d = 1 AND live = 1 AND ps = 2 AND k_day >= today() - 30) AS ps2_live_30,
  countIf(in_d = 0 AND in_s = 1 AND k_day >= today() - 30) AS suite_only_30,
  countIf(in_d = 1 AND k_day >= today() - 90) AS days_90,
  countIf(in_d = 1 AND live = 1 AND k_day >= today() - 90) AS live_90,
  countIf(in_d = 1 AND live = 1 AND ps != 1 AND k_day >= today() - 90) AS legacy_live_90,
  countIf(in_d = 1 AND live = 1 AND ps != 1 AND in_s = 1 AND k_day >= today() - 90) AS legacy_live_suite_90,
  countIf(in_d = 0 AND in_s = 1 AND k_day >= today() - 90) AS suite_only_90,
  countIf(in_d = 1) AS days_all,
  countIf(in_d = 1 AND live = 1) AS live_all,
  countIf(in_d = 1 AND live = 1 AND ps != 1) AS legacy_live_all,
  countIf(in_d = 1 AND live = 1 AND ps != 1 AND in_s = 1) AS legacy_live_suite_all,
  countIf(in_d = 1 AND live = 0 AND ps != 1) AS legacy_retired_all,
  countIf(in_d = 0 AND in_s = 1) AS suite_only_all,
  minIf(k_day, in_d = 1 AND live = 1 AND ps != 1) AS legacy_first_day,
  maxIf(k_day, in_d = 1 AND live = 1 AND ps != 1) AS legacy_last_day,
  minIf(k_day, in_d = 1 AND live = 1 AND ps = 1) AS stored_first_day
FROM (
  SELECT
    if(d.in_d = 1, d.org_id, s.org_id) AS k_org,
    if(d.in_d = 1, d.day, s.day) AS k_day,
    d.in_d AS in_d, s.in_s AS in_s, d.ps AS ps, d.live AS live
  FROM (
    SELECT n.org_id AS org_id, n.repo_id AS repo_id, n.day AS day, n.pooled_samples_stored AS ps,
           l.live AS live, toUInt8(1) AS in_d
    FROM (
      SELECT org_id, repo_id, day, pooled_samples_stored
      FROM testops_test_metrics_daily
      WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0
      ORDER BY computed_at DESC, cityHash64(tuple(*)) DESC
      LIMIT 1 BY org_id, repo_id, day
    ) AS n
    INNER JOIN (
      SELECT org_id, repo_id, day, toUInt8(argMax(retracted, computed_at) = 0) AS live
      FROM testops_test_metrics_daily
      WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0
      GROUP BY org_id, repo_id, day
    ) AS l ON l.org_id = n.org_id AND l.repo_id = n.repo_id AND l.day = n.day
  ) AS d
  FULL OUTER JOIN (
    SELECT DISTINCT org_id, repo_id, toDate(assumeNotNull(coalesce(started_at, finished_at))) AS day, toUInt8(1) AS in_s
    FROM test_suite_results FINAL
    WHERE startsWith(org_id, 'f0f0f0f0') AND coalesce(started_at, finished_at) IS NOT NULL AND cost_gate = 0
  ) AS s ON s.org_id = d.org_id AND s.repo_id = d.repo_id AND s.day = d.day
);
`

const (
	f14591Org   = "f0f0f0f0-0000-4000-8000-000000000000"
	f14591Other = "b0b0b0b0-0000-4000-8000-000000000000"
	f14591Repo1 = "00000000-0000-4000-8000-000000000001"
	f14591Repo2 = "00000000-0000-4000-8000-000000000002"
	f14591Repo3 = "00000000-0000-4000-8000-000000000003"
)

// f14591Fixture builds the rows around the server's today() (t). Org f0f0f0f0, newest daily row per key:
//
//	k1 repo1 t-1   ps 1 live            suite rows yes  (stored)
//	k2 repo1 t-2   ps 1 then ps 0 live  suite row: started t-2 23:00, finished t-1 01:00 (coalesce day t-2)
//	k3 repo2 t-5   ps 2 live            no suite rows   (over the cap)
//	k4 repo2 t-40  ps 1 then ps 0 retracted 1 (retired) suite rows yes
//	k5 repo3 t-100 ps 0 live            suite rows yes
//	suite only: repo3 t-3 (no daily row); a suite row with no start and no finish (no day).
//
// Org b0b0b0b0: one live legacy daily row and a suite row (the org mutant counts them).
func f14591Fixture(t time.Time) []string {
	day := func(n int) string { return t.AddDate(0, 0, -n).Format("2006-01-02") }
	at := func(n int, clock string) string { return day(n) + " " + clock }
	return []string{
		`INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, pooled_samples_stored, retracted, computed_at) VALUES
('` + f14591Org + `', '` + f14591Repo1 + `', '` + day(1) + `', 1, 0, '` + at(0, "01:00:00") + `'),
('` + f14591Org + `', '` + f14591Repo1 + `', '` + day(2) + `', 1, 0, '` + at(1, "01:00:00") + `'),
('` + f14591Org + `', '` + f14591Repo2 + `', '` + day(5) + `', 2, 0, '` + at(4, "01:00:00") + `'),
('` + f14591Org + `', '` + f14591Repo2 + `', '` + day(40) + `', 1, 0, '` + at(39, "01:00:00") + `'),
('` + f14591Org + `', '` + f14591Repo3 + `', '` + day(100) + `', 0, 0, '` + at(99, "01:00:00") + `'),
('` + f14591Other + `', '` + f14591Repo1 + `', '` + day(1) + `', 0, 0, '` + at(0, "01:00:00") + `')`,
		`INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, pooled_samples_stored, retracted, computed_at) VALUES
('` + f14591Org + `', '` + f14591Repo1 + `', '` + day(2) + `', 0, 0, '` + at(0, "02:00:00") + `'),
('` + f14591Org + `', '` + f14591Repo2 + `', '` + day(40) + `', 0, 1, '` + at(0, "02:00:00") + `')`,
		`INSERT INTO test_suite_results (org_id, repo_id, run_id, suite_id, suite_name, started_at, finished_at, last_synced) VALUES
('` + f14591Org + `', '` + f14591Repo1 + `', 'run-1', 's-1', 'a', '` + at(1, "10:00:00") + `', '` + at(1, "10:05:00") + `', '` + at(0, "01:00:00") + `'),
('` + f14591Org + `', '` + f14591Repo1 + `', 'run-2', 's-1', 'a', '` + at(2, "23:00:00") + `', '` + at(1, "01:00:00") + `', '` + at(0, "01:00:00") + `'),
('` + f14591Org + `', '` + f14591Repo2 + `', 'run-3', 's-1', 'a', '` + at(40, "10:00:00") + `', '` + at(40, "10:05:00") + `', '` + at(0, "01:00:00") + `'),
('` + f14591Org + `', '` + f14591Repo3 + `', 'run-4', 's-1', 'a', '` + at(100, "10:00:00") + `', '` + at(100, "10:05:00") + `', '` + at(0, "01:00:00") + `'),
('` + f14591Org + `', '` + f14591Repo3 + `', 'run-5', 's-1', 'a', '` + at(3, "10:00:00") + `', '` + at(3, "10:05:00") + `', '` + at(0, "01:00:00") + `'),
('` + f14591Org + `', '` + f14591Repo3 + `', 'run-6', 's-1', 'a', NULL, NULL, '` + at(0, "01:00:00") + `'),
('` + f14591Other + `', '` + f14591Repo1 + `', 'run-7', 's-1', 'a', '` + at(1, "10:00:00") + `', '` + at(1, "10:05:00") + `', '` + at(0, "01:00:00") + `')`,
		// A newer version of run-1 (FINAL keeps one).
		`INSERT INTO test_suite_results (org_id, repo_id, run_id, suite_id, suite_name, started_at, finished_at, last_synced) VALUES
('` + f14591Org + `', '` + f14591Repo1 + `', 'run-1', 's-1', 'a', '` + at(1, "10:00:00") + `', '` + at(1, "10:06:00") + `', '` + at(0, "02:00:00") + `')`,
	}
}

func f14591Render(v any) string {
	value := reflect.ValueOf(v)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return "NULL"
		}
		value = value.Elem()
	}
	if ts, ok := value.Interface().(time.Time); ok {
		return ts.UTC().Format("2006-01-02")
	}
	return fmt.Sprint(value.Interface())
}

// f14591Run runs one statement and returns its single row, the client read_rows and the error.
func f14591Run(ctx context.Context, conn driver.Conn, sql, comment string) (map[string]string, []string, uint64, error) {
	var readRows uint64
	qctx := clickhouse.Context(ctx,
		clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}),
		clickhouse.WithProgress(func(p *clickhouse.Progress) { readRows += p.Rows }))
	rows, err := conn.Query(qctx, sql)
	if err != nil {
		return nil, nil, readRows, err
	}
	defer rows.Close()
	columns := rows.Columns()
	types := rows.ColumnTypes()
	var got map[string]string
	n := 0
	for rows.Next() {
		cells := make([]any, len(types))
		for i, ct := range types {
			cells[i] = reflect.New(ct.ScanType()).Interface()
		}
		if err := rows.Scan(cells...); err != nil {
			return nil, columns, readRows, err
		}
		got = map[string]string{}
		for i, name := range columns {
			got[name] = f14591Render(cells[i])
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return nil, columns, readRows, err
	}
	if n != 1 {
		return nil, columns, readRows, fmt.Errorf("%d rows, want 1", n)
	}
	return got, columns, readRows, nil
}

func f14591QueryLog(t *testing.T, ctx context.Context, conn driver.Conn, comment string) ([]string, uint64) {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT toString(type), read_rows FROM system.query_log
		WHERE log_comment = ? AND type != 'QueryStart' ORDER BY event_time_microseconds`, comment)
	if err != nil {
		t.Fatalf("query log: %v", err)
	}
	defer rows.Close()
	var types []string
	var total uint64
	for rows.Next() {
		var typ string
		var r uint64
		if err := rows.Scan(&typ, &r); err != nil {
			t.Fatalf("query log scan: %v", err)
		}
		types = append(types, typ)
		total += r
	}
	return types, total
}

func TestForm14591ExecutesWithItsMutantsAndGateFires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	sum := sha256.Sum256([]byte(form14591))
	if got := hex.EncodeToString(sum[:]); got != form14591SHA256 {
		t.Fatalf("form copy sha256 = %s, want %s", got, form14591SHA256)
	}
	real := strings.ReplaceAll(form14591, "f0f0f0f0", "c6a38355")
	realSum := sha256.Sum256([]byte(real))
	if got := hex.EncodeToString(realSum[:]); got != form14591RealSHA256 {
		t.Fatalf("swapped-back form sha256 = %s, want %s (FORM-14591-1.sql)", got, form14591RealSHA256)
	}
	if strings.Contains(form14591, "c6a38355") || strings.Count(form14591, "f0f0f0f0") != 4 {
		t.Fatalf("the copy must hold the synthetic prefix 4 times and no real prefix")
	}
	code := chmigrate.StripLineComments(form14591)
	statements := chmigrate.SplitStatements(form14591)
	if len(statements) != 1 {
		t.Fatalf("%d statements, want ONE", len(statements))
	}
	for _, banned := range []string{"SETTINGS", "EXPLAIN", "FORMAT", "INSERT", "ALTER", "SET "} {
		if strings.Contains(strings.ToUpper(code), banned) {
			t.Fatalf("the form code holds %q", banned)
		}
	}
	for literal, want := range map[string]int{form14591RowsCap: 1, form14591BytesCap: 1, "'STOP-COST-GATE'": 1,
		form14591GateTables: 2, "table IN (": 2, "AND cost_gate = 0": 3, "startsWith(org_id, 'f0f0f0f0')": 3} {
		if n := strings.Count(code, literal); n != want {
			t.Fatalf("form code holds %q %d times, want %d", literal, n, want)
		}
	}

	scratch := scratchDatabase(t)
	conn := openDatabase(t, scratch.URI, "")
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil {
		t.Fatalf("real chain: %v", err)
	}
	var version string
	var today time.Time
	if err := conn.QueryRow(ctx, "SELECT version(), today()").Scan(&version, &today); err != nil {
		t.Fatal(err)
	}
	t.Logf("schema head %s, clickhouse %s, today %s", result.Head, version, today.Format("2006-01-02"))
	// The fixture keeps two versions of a key apart: a background merge would fold them (ReplacingMergeTree).
	for _, table := range []string{"testops_test_metrics_daily", "test_suite_results"} {
		if err := conn.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
			t.Fatalf("stop merges %s: %v", table, err)
		}
	}
	for _, statement := range f14591Fixture(today) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("fixture: %v\n%s", err, statement)
		}
	}
	day := func(n int) string { return today.AddDate(0, 0, -n).Format("2006-01-02") }

	want := map[string]string{
		"daily_orgs": "1", "suite_orgs": "1",
		"days_30": "3", "live_30": "3", "legacy_live_30": "2", "legacy_live_suite_30": "1", "ps0_live_30": "1",
		"ps2_live_30": "1", "suite_only_30": "1",
		"days_90": "4", "live_90": "3", "legacy_live_90": "2", "legacy_live_suite_90": "1", "suite_only_90": "1",
		"days_all": "5", "live_all": "4", "legacy_live_all": "3", "legacy_live_suite_all": "2", "legacy_retired_all": "1",
		"suite_only_all":   "1",
		"legacy_first_day": day(100), "legacy_last_day": day(2), "stored_first_day": day(1),
	}

	// PASS path, caps at the real values.
	passComment := fmt.Sprintf("form14591-pass-%d", time.Now().UnixNano())
	got, columns, clientRead, err := f14591Run(ctx, conn, statements[0], passComment)
	if err != nil {
		t.Fatalf("PASS: SQL error: %v", err)
	}
	parts := make([]string, 0, len(columns))
	for _, name := range columns {
		parts = append(parts, name+"="+got[name])
		if _, ok := want[name]; !ok {
			t.Errorf("PASS: column %s has no WANT", name)
		}
	}
	t.Logf("PASS ROW %s", strings.Join(parts, " "))
	t.Logf("PASS client read_rows=%d", clientRead)
	for name, value := range want {
		if got[name] != value {
			t.Errorf("PASS: %s = %s, want %s", name, got[name], value)
		}
	}

	// Mutants: one rule changed each; WANT the named column to move off its PASS value.
	mutants := []struct {
		name, from, to string
		count          int
		column         string
	}{
		{"org prefix dropped", "startsWith(org_id, 'f0f0f0f0') AND ", "", 3, "daily_orgs"},
		{"legacy = ps 0 only (drops the over-cap 2)", "ps != 1", "ps = 0", 9, "legacy_live_30"},
		{"no live gate", "toUInt8(argMax(retracted, computed_at) = 0)", "toUInt8(1)", 1, "legacy_live_90"},
		{"oldest row instead of newest", "ORDER BY computed_at DESC,", "ORDER BY computed_at ASC,", 1, "legacy_live_30"},
		{"suite day = finished day", "toDate(assumeNotNull(coalesce(started_at, finished_at)))", "toDate(assumeNotNull(finished_at))", 1, "legacy_live_suite_30"},
	}
	for i, m := range mutants {
		if n := strings.Count(statements[0], m.from); n != m.count {
			t.Fatalf("mutant %q: %d matches of %q, want %d", m.name, n, m.from, m.count)
		}
		mutated := strings.ReplaceAll(statements[0], m.from, m.to)
		mgot, _, _, err := f14591Run(ctx, conn, mutated, fmt.Sprintf("form14591-mutant-%d-%d", i, time.Now().UnixNano()))
		if err != nil {
			t.Errorf("mutant %q: SQL error %v (want a value change)", m.name, err)
			continue
		}
		t.Logf("MUTANT %q %s = %s (pass %s)", m.name, m.column, mgot[m.column], want[m.column])
		if mgot[m.column] == want[m.column] {
			t.Errorf("mutant %q survived: %s = %s", m.name, m.column, mgot[m.column])
		}
	}

	// Gate fires: ONE cap lowered to 0. WANT code 395 STOP-COST-GATE, query_log [ExceptionBeforeStart], read_rows <= 2P+1.
	var p uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM system.parts WHERE active AND database = currentDatabase()
		AND table IN ('test_suite_results', 'testops_test_metrics_daily')`).Scan(&p); err != nil {
		t.Fatal(err)
	}
	var g uint64
	if err := conn.QueryRow(ctx, `SELECT sum(rows * multiIf(table = 'testops_test_metrics_daily', 2, 1)) FROM system.parts
		WHERE active AND database = currentDatabase() AND table IN ('test_suite_results', 'testops_test_metrics_daily')`).Scan(&g); err != nil {
		t.Fatal(err)
	}
	fires := map[string]string{"rows": form14591RowsCap, "bytes": form14591BytesCap}
	fireComments := map[string]string{}
	for name, literal := range fires {
		fired := strings.Replace(statements[0], literal, "> 0", 1)
		fireComments[name] = fmt.Sprintf("form14591-fire-%s-%d", name, time.Now().UnixNano())
		_, _, clientFire, err := f14591Run(ctx, conn, fired, fireComments[name])
		if err == nil || !strings.Contains(err.Error(), "STOP-COST-GATE") || !strings.Contains(err.Error(), "395") {
			t.Errorf("GATE %s: err %v, want code 395 STOP-COST-GATE", name, err)
		}
		t.Logf("GATE %s fired client read_rows=%d err=%v", name, clientFire, err)
	}
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	types, logRead := f14591QueryLog(t, ctx, conn, passComment)
	t.Logf("PASS query_log types=%v read_rows=%d G=%d P=%d band=[%d, %d]", types, logRead, g, p, g, 2*g+2*p+1)
	if len(types) != 1 || types[0] != "QueryFinish" || logRead < g || logRead > 2*g+2*p+1 {
		t.Errorf("PASS query_log types %v read_rows %d, want [QueryFinish] in [%d, %d]", types, logRead, g, 2*g+2*p+1)
	}
	for name := range fires {
		types, logRead := f14591QueryLog(t, ctx, conn, fireComments[name])
		t.Logf("GATE %s query_log types=%v read_rows=%d bound=%d", name, types, logRead, 2*p+1)
		if len(types) != 1 || types[0] != "ExceptionBeforeStart" || logRead > 2*p+1 {
			t.Errorf("GATE %s: query_log types %v read_rows %d, want [ExceptionBeforeStart] and <= %d", name, types, logRead, 2*p+1)
		}
	}
}
