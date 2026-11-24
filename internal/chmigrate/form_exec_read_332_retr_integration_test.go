//go:build integration

package chmigrate_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// DO NOT MERGE. Throwaway exec proof of a prod read form (FORM-332-RETR-3). retr332Form is the form file with ONE swap
// (the app org prefix c6a38355 -> the synthetic f0f0f0f0, public repo); its sha256 is pinned. The {{KP}} placeholder is
// filled with retr332KP as pm's one sed does. Synthetic rows only.
const (
	retr332RowsCap  = "> 20000000"
	retr332BytesCap = "> 4000000000"
	retr332KP       = "2026-11-24 10:00:00"
	retr332Org      = "f0f0f0f0-0000-4000-8000-000000000000"
	retr332Other    = "b0000000-0000-4000-8000-000000000000"
)

const retr332FormSHA256 = "a49dde14dd980dcaa9f60f43b55947be54a4154d3d4491fd6d0951f422d38ad9"

const retr332Form = `-- FORM-332-RETR-3 (ClickHouse, prod, read-only). acp-train34, 2026-11-24. STEP 332 READBACK R-RETR (acp Q4 named limit scope).
-- -3 = FORM-332-RETR-2 09903734a555 with acp F2: stmt1 / stmt2 have NO KP bound (all retracted = 1 rows); stmt3 keeps KP.
--   Premise (md, census at the pin 3207ac62): retracted = 1 on these 2 tables is written only by the 332 workers (github_tests_run_report_rows.go
--   :488 / :660 / :692 through the 2 INSERTs); CH 171 adds the column only (DEFAULT 0). last_synced is the stream sync clock, so a KP
--   bound would miss a stream that started before KP and resumed after it.
-- Form: lane-scratch/pm-guard/FORM-TEMPLATE-3.md 6b9e73a3c45a (CH shape, one statement per call, self-gating). Replaces FORM-332-RETR-2.sql 09903734a555.
-- What: the retraction rows (retracted = 1, CH 171 column) that the 332 workers wrote (all of them), per table.
--   Each one is a report slot that query-api 155276ecf30e (no #6906 / #6938 reader) shows as an EMPTY suite until STEP 333.
--   Writer: ops #6906 d4b4c5a38ab2 / #6938 e46d7611fd3a internal/providersync/github_tests_effects_clickhouse.go:158 / :195 (the two INSERTs; a retraction row has retracted = 1). Cites at the pin 3207ac62.
-- Tables at the chain head (baseline head.json + chain 171): test_suite_results / test_case_results, ReplacingMergeTree(last_synced),
--   ORDER BY (repo_id, run_id, suite_id[, case_id]), no PARTITION BY, org_id LowCardinality(String), last_synced DateTime64(3, 'UTC'),
--   retracted UInt8 DEFAULT 0 (171).
-- Placeholder: {{KP}} (stmt3 only) = the RUNLOG STEP-KIT-PASS time, UTC, 'YYYY-MM-DD hh:mm:ss'. pm makes the run copy with ONE sed:
--   sed "s/{{KP}}/<KP>/g" FORM-332-RETR-3.sql > FORM-332-RETR-3-run.sql ; WANT grep -c '{{' FORM-332-RETR-3-run.sql = 0.
--   The file sha12 acp names is of THIS file (with {{KP}}).
-- WORK MATH (fill from S0 before the run): stmt0, stmt1, stmt3 each read test_suite_results x 1 = R_s rows; stmt2 reads
--   test_case_results x 1 = R_c rows. No FINAL, no CTE, no self-join, no scalar subquery on data. Each cap = 20,000,000 rows
--   / 4,000,000,000 bytes on that statement's table. The gate itself reads system.parts twice (2P) + system.one (1).
--   If S0 shows R_s or R_c over 20,000,000, that statement STOPs (code 395) by design: report, acp rules a narrower form.
-- Counts are raw rows (no FINAL): a re-sync that writes the same key again counts again. INFO for the limit scope.
-- Output: counts and days (UTC) only. No ids, no times, no payload text.

-- S0 (system.parts only). WANT 2 rows (test_case_results, test_suite_results), each rows > 0 and active_parts > 0 (a missing row = wrong name: STOP).
SELECT table AS t, sum(rows) AS rows, sum(bytes_on_disk) AS bytes, count() AS active_parts
FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_suite_results', 'test_case_results')
GROUP BY table ORDER BY table;

-- stmt0 ORG-1 on test_suite_results. WANT orgs = 1, org_rows > 0.
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_suite_results')) > 20000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_suite_results')) > 4000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT uniqExact(org_id) AS orgs, count() AS org_rows
FROM test_suite_results
WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0;

-- stmt1 suite retraction rows (all; no KP bound). WANT a count (0 = NOT EXERCISED, allowed); first_day / last_day = INFO.
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_suite_results')) > 20000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_suite_results')) > 4000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT count() AS retracted_suite_rows, uniqExact(repo_id) AS repos, toDate(min(last_synced)) AS first_day, toDate(max(last_synced)) AS last_day
FROM test_suite_results
WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0
  AND retracted = 1;

-- stmt2 case retraction rows (all; no KP bound). WANT a count (0 = NOT EXERCISED, allowed); first_day / last_day = INFO.
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_case_results')) > 20000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_case_results')) > 4000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT count() AS retracted_case_rows, uniqExact(repo_id) AS repos, toDate(min(last_synced)) AS first_day, toDate(max(last_synced)) AS last_day
FROM test_case_results
WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0
  AND retracted = 1;

-- stmt3 control: plain (retracted = 0) suite rows written at or after KP. WANT > 0 once a wave-2 row or a scheduled sync wrote
--   a tests suite after KP (0 with stmt1 > 0 = report: the window or the filter is wrong).
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_suite_results')) > 20000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_suite_results')) > 4000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT count() AS plain_suite_rows
FROM test_suite_results
WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0
  AND retracted = 0
  AND last_synced >= toDateTime64('{{KP}}', 3, 'UTC');
`

// test_suite_results rows. KP = 2026-11-24 10:00:00 (inclusive). O = the org, X = another org.
// s1-s3 O retracted at/after KP (repos r1, r1, r2); s4 O retracted 1 ms before KP; s5-s8 O plain after KP; s9-s10 O plain
// before KP; s11 O retracted on 2026-11-20 (repo r4, long before KP); x1-x2 X retracted after KP; x3 X plain after KP;
// filler 2000 O plain on 2026-11-01. Cases: c6 O retracted 1 h before KP; c11 O retracted on 2026-11-20 (repo r4).
const retr332Fixture = `
SYSTEM STOP MERGES test_suite_results;
SYSTEM STOP MERGES test_case_results;
INSERT INTO test_suite_results (repo_id, run_id, suite_id, suite_name, org_id, last_synced, retracted) VALUES
('00000000-0000-4000-8000-0000000000a1', 'run-s1', 'suite-1', 'a', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-24 10:00:00.000', 1),
('00000000-0000-4000-8000-0000000000a1', 'run-s2', 'suite-1', 'a', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-24 11:00:00.000', 1),
('00000000-0000-4000-8000-0000000000a2', 'run-s3', 'suite-1', 'a', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-24 12:30:00.000', 1),
('00000000-0000-4000-8000-0000000000a2', 'run-s4', 'suite-1', 'a', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-24 09:59:59.999', 1),
('00000000-0000-4000-8000-0000000000a3', 'run-s5', 'suite-1', 'a', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-24 10:30:00.000', 0),
('00000000-0000-4000-8000-0000000000a3', 'run-s6', 'suite-1', 'a', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-24 11:30:00.000', 0),
('00000000-0000-4000-8000-0000000000a3', 'run-s7', 'suite-1', 'a', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-24 12:00:00.000', 0),
('00000000-0000-4000-8000-0000000000a3', 'run-s8', 'suite-1', 'a', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-24 13:00:00.000', 0),
('00000000-0000-4000-8000-0000000000a3', 'run-s9', 'suite-1', 'a', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-24 08:00:00.000', 0),
('00000000-0000-4000-8000-0000000000a3', 'run-s10', 'suite-1', 'a', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-24 08:00:00.000', 0),
('00000000-0000-4000-8000-0000000000a4', 'run-s11', 'suite-1', 'a', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-20 08:00:00.000', 1),
('00000000-0000-4000-8000-0000000000b9', 'run-x1', 'suite-1', 'a', 'b0000000-0000-4000-8000-000000000000', '2026-11-24 11:00:00.000', 1),
('00000000-0000-4000-8000-0000000000b9', 'run-x2', 'suite-1', 'a', 'b0000000-0000-4000-8000-000000000000', '2026-11-24 11:00:00.000', 1),
('00000000-0000-4000-8000-0000000000b9', 'run-x3', 'suite-1', 'a', 'b0000000-0000-4000-8000-000000000000', '2026-11-24 11:00:00.000', 0);
INSERT INTO test_suite_results (repo_id, run_id, suite_id, suite_name, org_id, last_synced, retracted)
SELECT generateUUIDv4(), concat('run-f', toString(number)), 'suite-1', 'a', 'f0f0f0f0-0000-4000-8000-000000000000', toDateTime64('2026-11-01 12:00:00', 3, 'UTC'), 0
FROM numbers(2000);
INSERT INTO test_case_results (repo_id, run_id, suite_id, case_id, case_name, status, org_id, last_synced, retracted) VALUES
('00000000-0000-4000-8000-0000000000a1', 'run-c1', 'suite-1', 'case-1', 'c', 'passed', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-24 10:00:00.000', 1),
('00000000-0000-4000-8000-0000000000a1', 'run-c2', 'suite-1', 'case-1', 'c', 'passed', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-24 10:10:00.000', 1),
('00000000-0000-4000-8000-0000000000a2', 'run-c3', 'suite-1', 'case-1', 'c', 'passed', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-24 10:20:00.000', 1),
('00000000-0000-4000-8000-0000000000a3', 'run-c4', 'suite-1', 'case-1', 'c', 'passed', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-24 10:30:00.000', 1),
('00000000-0000-4000-8000-0000000000a3', 'run-c5', 'suite-1', 'case-1', 'c', 'passed', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-24 14:00:00.000', 1),
('00000000-0000-4000-8000-0000000000a3', 'run-c6', 'suite-1', 'case-1', 'c', 'passed', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-24 09:00:00.000', 1),
('00000000-0000-4000-8000-0000000000a3', 'run-c7', 'suite-1', 'case-1', 'c', 'passed', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-24 11:00:00.000', 0),
('00000000-0000-4000-8000-0000000000a3', 'run-c8', 'suite-1', 'case-1', 'c', 'passed', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-24 11:00:00.000', 0),
('00000000-0000-4000-8000-0000000000a3', 'run-c9', 'suite-1', 'case-1', 'c', 'passed', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-24 11:00:00.000', 0),
('00000000-0000-4000-8000-0000000000a4', 'run-c11', 'suite-1', 'case-1', 'c', 'passed', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-20 08:00:00.000', 1),
('00000000-0000-4000-8000-0000000000b9', 'run-cx1', 'suite-1', 'case-1', 'c', 'passed', 'b0000000-0000-4000-8000-000000000000', '2026-11-24 11:00:00.000', 1),
('00000000-0000-4000-8000-0000000000b9', 'run-cx2', 'suite-1', 'case-1', 'c', 'passed', 'b0000000-0000-4000-8000-000000000000', '2026-11-24 11:00:00.000', 1);
INSERT INTO test_case_results (repo_id, run_id, suite_id, case_id, case_name, status, org_id, last_synced, retracted)
SELECT generateUUIDv4(), concat('run-cf', toString(number)), 'suite-1', 'case-1', 'c', 'passed', 'f0f0f0f0-0000-4000-8000-000000000000', toDateTime64('2026-11-01 12:00:00', 3, 'UTC'), 0
FROM numbers(1000);
`

type retr332Statement struct{ label, text string }

func retr332Split(body string) []retr332Statement {
	var out []retr332Statement
	var chunk []string
	for _, line := range strings.Split(body, "\n") {
		chunk = append(chunk, line)
		if !strings.HasSuffix(strings.TrimSpace(chmigrate.StripLineComments(line)), ";") {
			continue
		}
		text := strings.TrimSuffix(strings.TrimRight(strings.Join(chunk, "\n"), " \t\n"), ";")
		label := ""
		for _, l := range chunk {
			if t := strings.TrimSpace(l); strings.HasPrefix(t, "-- S0") || strings.HasPrefix(t, "-- stmt") {
				label = strings.Fields(strings.TrimPrefix(t, "-- "))[0]
				break
			}
		}
		out = append(out, retr332Statement{label: label, text: text})
		chunk = nil
	}
	return out
}

func retr332Render(v any, dbType string) string {
	value := reflect.ValueOf(v)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return "NULL"
		}
		value = value.Elem()
	}
	if ts, ok := value.Interface().(time.Time); ok {
		if strings.HasPrefix(dbType, "DateTime") {
			return ts.UTC().Format("2006-01-02 15:04:05.000")
		}
		return ts.UTC().Format("2006-01-02")
	}
	return fmt.Sprint(value.Interface())
}

func retr332Run(ctx context.Context, conn driver.Conn, text, comment string) ([]map[string]string, error) {
	qctx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}))
	rows, err := conn.Query(qctx, text)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns := rows.Columns()
	types := rows.ColumnTypes()
	var got []map[string]string
	for rows.Next() {
		cells := make([]any, len(types))
		for i, ct := range types {
			cells[i] = reflect.New(ct.ScanType()).Interface()
		}
		if err := rows.Scan(cells...); err != nil {
			return nil, err
		}
		row := map[string]string{}
		for i, name := range columns {
			row[name] = retr332Render(cells[i], types[i].DatabaseTypeName())
		}
		got = append(got, row)
	}
	return got, rows.Err()
}

func retr332Log(ctx context.Context, t *testing.T, conn driver.Conn, comment string) ([]string, uint64) {
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
		var read uint64
		if err := rows.Scan(&typ, &read); err != nil {
			t.Fatalf("query log scan: %v", err)
		}
		types = append(types, typ)
		total += read
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return types, total
}

func TestForm332RetrExecutesAtTheChainHead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	sum := sha256.Sum256([]byte(retr332Form))
	if got := hex.EncodeToString(sum[:]); got != retr332FormSHA256 {
		t.Fatalf("form sha256 = %s, want %s", got, retr332FormSHA256)
	}
	if n := strings.Count(retr332Form, "{{KP}}"); n != 4 {
		t.Fatalf("{{KP}} found %d times, want 4 (stmt3 + 3 header lines)", n)
	}
	run := strings.ReplaceAll(retr332Form, "{{KP}}", retr332KP)
	statements := retr332Split(run)
	byLabel := map[string]string{}
	labels := make([]string, 0, len(statements))
	for _, s := range statements {
		labels = append(labels, s.label)
		byLabel[s.label] = s.text
	}
	if strings.Join(labels, ",") != "S0,stmt0,stmt1,stmt2,stmt3" {
		t.Fatalf("form statements = %v", labels)
	}
	gateTable := map[string]string{"stmt0": "test_suite_results", "stmt1": "test_suite_results", "stmt2": "test_case_results", "stmt3": "test_suite_results"}
	for _, s := range statements {
		want := 1
		if s.label == "S0" {
			want = 0
		}
		for _, lit := range []string{retr332RowsCap, retr332BytesCap, "'STOP-COST-GATE'"} {
			if n := strings.Count(s.text, lit); n != want {
				t.Fatalf("%s: %q found %d times, want %d", s.label, lit, n, want)
			}
		}
		if s.label != "S0" {
			if n := strings.Count(s.text, "table IN ('"+gateTable[s.label]+"')"); n != 2 {
				t.Fatalf("%s: gate table list found %d times, want 2", s.label, n)
			}
			if !strings.Contains(s.text, "FROM "+gateTable[s.label]+"\n") || !strings.Contains(s.text, "startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0") {
				t.Fatalf("%s: FROM / org rule / cost_gate shape", s.label)
			}
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
	if result.Head < "171_" {
		t.Fatalf("schema head %q, want 171 or later", result.Head)
	}
	var version string
	if err := conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("schema head %s, clickhouse %s", result.Head, version)

	for _, statement := range chmigrate.SplitStatements(retr332Fixture) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("fixture: %v\n%s", err, statement)
		}
	}
	size := map[string][2]uint64{}
	for _, table := range []string{"test_suite_results", "test_case_results"} {
		var rowsN, partsN uint64
		if err := conn.QueryRow(ctx, `SELECT sum(rows), count() FROM system.parts
			WHERE active AND database = currentDatabase() AND table = ?`, table).Scan(&rowsN, &partsN); err != nil {
			t.Fatal(err)
		}
		size[table] = [2]uint64{rowsN, partsN}
		t.Logf("fixture: %s rows=%d active_parts=%d", table, rowsN, partsN)
	}
	if size["test_suite_results"][0] != 2014 || size["test_case_results"][0] != 1012 {
		t.Fatalf("fixture rows %v, want suite 2014 / case 1012", size)
	}

	one := func(phase, label, text string) map[string]string {
		t.Helper()
		got, err := retr332Run(ctx, conn, text, fmt.Sprintf("retr332-%s-%s-%d", phase, label, time.Now().UnixNano()))
		if err != nil {
			t.Fatalf("%s %s: SQL error: %v", phase, label, err)
		}
		if len(got) != 1 {
			t.Fatalf("%s %s: %d rows, want 1", phase, label, len(got))
		}
		t.Logf("%s %s ROW %v", phase, label, got[0])
		return got[0]
	}
	expect := func(phase, label string, got, want map[string]string) {
		t.Helper()
		for name, value := range want {
			if got[name] != value {
				t.Errorf("%s %s: %s = %s, want %s", phase, label, name, got[name], value)
			}
		}
	}

	// S0: 2 rows, one per table.
	s0, err := retr332Run(ctx, conn, byLabel["S0"], fmt.Sprintf("retr332-pass-S0-%d", time.Now().UnixNano()))
	if err != nil || len(s0) != 2 {
		t.Fatalf("PASS S0: err=%v rows=%d, want 2", err, len(s0))
	}
	t.Logf("PASS S0 ROWS %v", s0)
	if s0[0]["t"] != "test_case_results" || s0[0]["rows"] != "1012" || s0[1]["t"] != "test_suite_results" || s0[1]["rows"] != "2014" {
		t.Errorf("PASS S0: %v", s0)
	}

	cleanWant := map[string]map[string]string{
		"stmt0": {"orgs": "1", "org_rows": "2011"},
		"stmt1": {"retracted_suite_rows": "5", "repos": "3", "first_day": "2026-11-20", "last_day": "2026-11-24"},
		"stmt2": {"retracted_case_rows": "7", "repos": "4", "first_day": "2026-11-20", "last_day": "2026-11-24"},
		"stmt3": {"plain_suite_rows": "4"},
	}
	passComments := map[string]string{}
	for _, label := range []string{"stmt0", "stmt1", "stmt2", "stmt3"} {
		passComments[label] = fmt.Sprintf("retr332-pass-%s-%d", label, time.Now().UnixNano())
		got, err := retr332Run(ctx, conn, byLabel[label], passComments[label])
		if err != nil || len(got) != 1 {
			t.Fatalf("PASS %s: err=%v rows=%d", label, err, len(got))
		}
		t.Logf("PASS %s ROW %v", label, got[0])
		expect("PASS", label, got[0], cleanWant[label])
	}
	// Output cell (acp F1): every output column of stmt0-stmt3 is a count or a Date; no DateTime / String column.
	for _, label := range []string{"stmt0", "stmt1", "stmt2", "stmt3"} {
		rows, err := conn.Query(ctx, byLabel[label])
		if err != nil {
			t.Fatalf("OUTPUT %s: %v", label, err)
		}
		for i, ct := range rows.ColumnTypes() {
			typ := ct.DatabaseTypeName()
			if typ != "UInt64" && typ != "Date" {
				t.Errorf("OUTPUT %s: column %s type %s, want UInt64 or Date", label, rows.Columns()[i], typ)
			}
		}
		t.Logf("OUTPUT %s columns %v ok", label, rows.Columns())
		rows.Close()
	}
	// Control: stmt3 > 0 on the clean fixture.
	if cleanWant["stmt3"]["plain_suite_rows"] == "0" {
		t.Fatal("control stmt3 WANT must be > 0")
	}

	// Mutants. Each is a test copy of one statement with ONE change; each must change the output.
	orgSwap := func(text string) string { return strings.ReplaceAll(text, "'f0f0f0f0'", "'b0000000'") }
	type mutant struct {
		label, name string
		from, to    string
		want        map[string]string
	}
	mutants := []mutant{
		{"S0", "rule: table list without test_case_results", "'test_suite_results', 'test_case_results'", "'test_suite_results'", nil},
		{"stmt0", "org: prefix of another org", "", "", map[string]string{"orgs": "1", "org_rows": "3"}},
		{"stmt0", "rule: no org filter", "startsWith(org_id, 'f0f0f0f0') AND ", "", map[string]string{"orgs": "2", "org_rows": "2014"}},
		{"stmt1", "org: prefix of another org", "", "", map[string]string{"retracted_suite_rows": "2", "repos": "1"}},
		{"stmt1", "rule: retracted = 0", "AND retracted = 1", "AND retracted = 0", map[string]string{"retracted_suite_rows": "2006", "repos": "2001"}},
		{"stmt1", "rule: no org filter", "startsWith(org_id, 'f0f0f0f0') AND ", "", map[string]string{"retracted_suite_rows": "7", "repos": "4"}},
		{"stmt2", "org: prefix of another org", "", "", map[string]string{"retracted_case_rows": "2", "repos": "1"}},
		{"stmt2", "rule: retracted = 0", "AND retracted = 1", "AND retracted = 0", map[string]string{"retracted_case_rows": "1003", "repos": "1001"}},
		{"stmt2", "rule: no org filter", "startsWith(org_id, 'f0f0f0f0') AND ", "", map[string]string{"retracted_case_rows": "9", "repos": "5"}},
		{"stmt3", "org: prefix of another org", "", "", map[string]string{"plain_suite_rows": "1"}},
		{"stmt3", "rule: retracted = 1", "AND retracted = 0", "AND retracted = 1", map[string]string{"plain_suite_rows": "3"}},
		{"stmt3", "rule: window after every row", retr332KP, "2030-01-01 00:00:00", map[string]string{"plain_suite_rows": "0"}},
		{"stmt3", "rule: KP bound flipped (>= -> <)", "last_synced >= ", "last_synced < ", map[string]string{"plain_suite_rows": "2002"}},
	}
	for _, m := range mutants {
		text := byLabel[m.label]
		var mutated string
		if m.from == "" {
			mutated = orgSwap(text)
		} else {
			mutated = strings.Replace(text, m.from, m.to, 1)
		}
		if mutated == text {
			t.Fatalf("MUTANT %s %s: nothing to mutate", m.label, m.name)
		}
		if m.label == "S0" {
			got, err := retr332Run(ctx, conn, mutated, fmt.Sprintf("retr332-mut-S0-%d", time.Now().UnixNano()))
			if err != nil || len(got) != 1 {
				t.Errorf("MUTANT S0 %s: err=%v rows=%d, want 1 (the WANT 2 rows fails)", m.name, err, len(got))
			}
			continue
		}
		got := one("MUTANT", m.label+" "+m.name, mutated)
		expect("MUTANT", m.label+" "+m.name, got, m.want)
		changed := false
		for k, v := range cleanWant[m.label] {
			if got[k] != v {
				changed = true
			}
		}
		if !changed {
			t.Errorf("MUTANT %s %s: output equals the clean output", m.label, m.name)
		}
	}
	t.Logf("mutants run: %d", len(mutants))

	// PASS-path read_rows: one scan of the statement table T + the gate 2 x P (system.parts twice) + 1 (system.one).
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	for _, label := range []string{"stmt0", "stmt1", "stmt2", "stmt3"} {
		types, read := retr332Log(ctx, t, conn, passComments[label])
		sz := size[gateTable[label]]
		t.Logf("PASS %s query_log types=%v read_rows=%d (T=%d P=%d)", label, types, read, sz[0], sz[1])
		if len(types) != 1 || types[0] != "QueryFinish" {
			t.Errorf("PASS %s: query_log types %v, want [QueryFinish]", label, types)
		}
		if want := sz[0] + 2*sz[1] + 1; read != want {
			t.Errorf("PASS %s: query_log read_rows %d, want T + 2P + 1 = %d", label, read, want)
		}
	}

	// Gate fires: each cap literal lowered to 0 in a test copy. WANT code 395 STOP-COST-GATE before any table read.
	type fired struct{ label, cap, comment string }
	var fires []fired
	for _, label := range []string{"stmt0", "stmt1", "stmt2", "stmt3"} {
		for _, literal := range []string{retr332RowsCap, retr332BytesCap} {
			lowered := strings.Replace(byLabel[label], literal, "> 0", 1)
			comment := fmt.Sprintf("retr332-gate-%s-%s-%d", label, strings.TrimPrefix(literal, "> "), time.Now().UnixNano())
			_, err := retr332Run(ctx, conn, lowered, comment)
			var ex *clickhouse.Exception
			if err == nil || !errors.As(err, &ex) || ex.Code != 395 || !strings.Contains(ex.Message, "STOP-COST-GATE") {
				t.Errorf("GATE %s %s lowered to 0: err = %v, want code 395 STOP-COST-GATE", label, literal, err)
				continue
			}
			t.Logf("GATE %s %s lowered to 0: fired code=%d", label, literal, ex.Code)
			fires = append(fires, fired{label, literal, comment})
		}
	}
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	for _, f := range fires {
		types, read := retr332Log(ctx, t, conn, f.comment)
		p := size[gateTable[f.label]][1]
		t.Logf("GATE %s %s query_log types=%v read_rows=%d bound=%d", f.label, f.cap, types, read, 2*p+1)
		if len(types) != 1 || types[0] != "ExceptionBeforeStart" {
			t.Errorf("GATE %s %s: query_log types %v, want [ExceptionBeforeStart]", f.label, f.cap, types)
		}
		if read > 2*p+1 {
			t.Errorf("GATE %s %s: read_rows %d over 2 x P + 1 = %d", f.label, f.cap, read, 2*p+1)
		}
	}
	if len(fires) != 8 {
		t.Errorf("gates fired %d times, want 8 (4 statements x 2 caps)", len(fires))
	}

	// acp F2 (d): a retracted = 1 row with last_synced BEFORE KP (a stream that started before the roll) is counted by
	// stmt1 / stmt2 (no KP bound) and NOT by stmt3 (KP-bounded, retracted = 0). One more suite + one more case row.
	for _, statement := range chmigrate.SplitStatements(`
INSERT INTO test_suite_results (repo_id, run_id, suite_id, suite_name, org_id, last_synced, retracted) VALUES
('00000000-0000-4000-8000-0000000000a5', 'run-s12', 'suite-1', 'a', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-24 09:00:00.000', 1);
INSERT INTO test_case_results (repo_id, run_id, suite_id, case_id, case_name, status, org_id, last_synced, retracted) VALUES
('00000000-0000-4000-8000-0000000000a5', 'run-c12', 'suite-1', 'case-1', 'c', 'passed', 'f0f0f0f0-0000-4000-8000-000000000000', '2026-11-24 09:00:00.000', 1);
`) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("pre-KP rows: %v\n%s", err, statement)
		}
	}
	preKP := map[string]map[string]string{
		"stmt1": {"retracted_suite_rows": "6", "repos": "4"},
		"stmt2": {"retracted_case_rows": "8", "repos": "5"},
		"stmt3": {"plain_suite_rows": "4"},
	}
	for _, label := range []string{"stmt1", "stmt2", "stmt3"} {
		got := one("PREKP", label, byLabel[label])
		expect("PREKP", label, got, preKP[label])
	}
}
