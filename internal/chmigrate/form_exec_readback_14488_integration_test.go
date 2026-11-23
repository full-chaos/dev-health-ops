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

// Throwaway exec proof of a prod read form: not for merge. rb14488Form is the form file byte for byte.
const (
	rb14488RowsCap  = "> 20000000"
	rb14488BytesCap = "> 4000000000"
)

const rb14488FormSHA256 = "916cca0cb5d14d146a8883a62aa7ee1afb8cf7e0ad4d211884bb427cb9efb94f"

const rb14488Form = `-- NOT EXECUTED. READBACK-14488-1 (ClickHouse, prod, read-only). acr-mcp, 2026-11-23. acp FORM read first; then pm runs it once.
-- Form: lane-scratch/pm-guard/FORM-TEMPLATE-3.md 6b9e73a3c45a (CH shape, one statement per call, self-gating) + PM-RULES MAC §4 (8e04e2613b6f).
-- Not executed: no local CH at the prod schema and no CI exec leg yet. Per the template pm does not run it until an exec proof exists
--   (each gate fired once with cap 0 = code 395 STOP-COST-GATE; a pass-path cell on a fixture).
-- What: CHAOS-14488, ops #6819 squash 2c77674879f9, live since STEP 325 (workers image c550fafac95f = ops a7febdf50002;
--   acp RECORD-325-sw6-a-1.md 7f7f2d07be64: STEP-KIT-PASS 2026-11-23T06:24:03Z). Window = computed_at >= that time.
-- Writer at a7febdf50002, internal/jobs/metrics/daily/testops_risk_native_clickhouse.go:
--   :714 queueUnknown := pipe != nil && pipe.PipelinesCount > 0 && pipe.AvgQueueSeconds == nil
--   :736 factors_json "pipelines_count"; :737 "avg_queue_seconds" = JSON null when queueUnknown (ffRate :487-492, isNull)
--   :748 queue_wait_hours = NULL when queueUnknown (queueHoursOrUnknown :436-441); :751 computed_at; :1012 INSERT INTO testops_quality_drag.
-- Table at the live CH head (internal/chmigrate/baseline/head.json:679-681; nullable since 091): testops_quality_drag(repo_id UUID, day Date,
--   drag_hours/failure_rework_hours/flake_investigation_hours/queue_wait_hours/retry_overhead_hours Nullable(Float64), factors_json String,
--   team_id, service_id, org_id LowCardinality(String), computed_at DateTime('UTC')), ReplacingMergeTree(computed_at), ORDER BY (org_id, repo_id, day).
--   Column ` + "`" + `retracted` + "`" + ` (163) is NOT read here (its roll state on prod is not checked; the pipelines_count > 0 filter drops '{}' marker rows).
-- WORK MATH (fill from S0 before the run): each of stmt1..stmt4 reads testops_quality_drag x 1 = R rows <= 20,000,000 (cap below).
--   No CTE, no self-join, no scalar subquery on data. The gate reads system.parts only.
-- Output: counts only. No ids, no payload text.
-- Note (not SQL): when stmt3 n_null >= 1, the Risk page shows "· partial" for that day; a person checks it by eye.

-- S0 (gated on system.one): the table exists and has rows. WANT 1 row, rows > 0, active_parts > 0 (0 = wrong name: STOP).
SELECT 'testops_quality_drag' AS t, sum(rows) AS rows, sum(bytes_on_disk) AS bytes, count() AS active_parts
FROM system.parts WHERE active AND database = currentDatabase() AND table = 'testops_quality_drag';

-- stmt0 ORG-1. WANT orgs = 1.
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('testops_quality_drag')) > 20000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('testops_quality_drag')) > 4000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT uniqExact(org_id) AS orgs
FROM testops_quality_drag
WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0;

-- stmt1 control: rows written after the roll. WANT rows_after > 0 and groups_after > 0.
--   rows_after = 0 = no recompute since the roll = WAIT (re-run later), not FAIL.
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('testops_quality_drag')) > 20000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('testops_quality_drag')) > 4000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT count() AS rows_after,
       countIf(JSONExtractInt(factors_json, 'pipelines_count') > 0) AS groups_after,
       min(computed_at) AS first_after, max(computed_at) AS last_after
FROM testops_quality_drag
WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0
  AND computed_at >= toDateTime('2026-11-23 06:24:03', 'UTC');

-- stmt2 control that can fail: groups WITH a queue sample still store a value.
--   Rows after the roll with pipelines_count > 0 and avg_queue_seconds a number.
--   WANT n_sampled > 0 (0 = the filter is wrong or no data: STOP), n_sampled_null = 0 (> 0 = the fix nulls too much: report).
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('testops_quality_drag')) > 20000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('testops_quality_drag')) > 4000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT countIf(JSONType(factors_json, 'avg_queue_seconds') IN ('Double', 'Int64', 'UInt64')) AS n_sampled,
       countIf(JSONType(factors_json, 'avg_queue_seconds') IN ('Double', 'Int64', 'UInt64') AND queue_wait_hours IS NULL) AS n_sampled_null
FROM testops_quality_drag
WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0
  AND computed_at >= toDateTime('2026-11-23 06:24:03', 'UTC')
  AND JSONExtractInt(factors_json, 'pipelines_count') > 0;

-- stmt3 the fix: groups with runs but NO queue sample (avg_queue_seconds JSON null).
--   WANT n_zero = 0 and n_other = 0. n_null >= 1 = EXERCISED PASS. All three 0 = NOT EXERCISED (no such group since the roll): report, re-run later.
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('testops_quality_drag')) > 20000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('testops_quality_drag')) > 4000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT countIf(queue_wait_hours IS NULL) AS n_null,
       countIf(queue_wait_hours = 0) AS n_zero,
       countIf(queue_wait_hours IS NOT NULL AND queue_wait_hours != 0) AS n_other,
       uniqExact(day) AS days
FROM testops_quality_drag
WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0
  AND computed_at >= toDateTime('2026-11-23 06:24:03', 'UTC')
  AND JSONExtractInt(factors_json, 'pipelines_count') > 0
  AND JSONType(factors_json, 'avg_queue_seconds') = 'Null';

-- stmt4 converse: after the roll, a NULL queue_wait_hours with pipelines_count > 0 comes only from the unknown queue.
--   WANT n_null_with_number = 0 (> 0 = NULL from another cause, e.g. a non-finite value in nullablePyRound: report).
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('testops_quality_drag')) > 20000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('testops_quality_drag')) > 4000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT countIf(JSONType(factors_json, 'avg_queue_seconds') != 'Null') AS n_null_with_number,
       count() AS n_null_total
FROM testops_quality_drag
WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0
  AND computed_at >= toDateTime('2026-11-23 06:24:03', 'UTC')
  AND JSONExtractInt(factors_json, 'pipelines_count') > 0
  AND queue_wait_hours IS NULL;
`

const rb14488Org = "c6a38355-0000-4000-8000-000000000000"
const rb14488OtherOrg = "b0000000-0000-4000-8000-000000000000"

// Rows of testops_quality_drag. Window start 2026-11-23 06:24:03 (inclusive).
// A, B, H: groups with a queue sample (H computed exactly at the window start). C, D: groups with runs but no sample.
// E: marker row ('{}'). F: unknown queue one second before the window. G: other org. Filler: 2000 rows of the org
// before the window (scan size for the read_rows cell).
const rb14488Fixture = `
SYSTEM STOP MERGES testops_quality_drag;
INSERT INTO testops_quality_drag (repo_id, day, queue_wait_hours, factors_json, org_id, computed_at) VALUES
('00000000-0000-4000-8000-000000000001', '2026-11-20', 0.0104, '{"failure_count": 1, "pipelines_count": 3, "avg_queue_seconds": 12.5, "rerun_rate": 0.0}', 'c6a38355-0000-4000-8000-000000000000', '2026-11-23 07:00:00'),
('00000000-0000-4000-8000-000000000002', '2026-11-21', 0.5, '{"failure_count": 0, "pipelines_count": 5, "avg_queue_seconds": 60, "rerun_rate": 0.0}', 'c6a38355-0000-4000-8000-000000000000', '2026-11-23 08:00:00'),
('00000000-0000-4000-8000-000000000003', '2026-11-22', NULL, '{"failure_count": 0, "pipelines_count": 4, "avg_queue_seconds": null, "rerun_rate": 0.0}', 'c6a38355-0000-4000-8000-000000000000', '2026-11-23 09:00:00'),
('00000000-0000-4000-8000-000000000004', '2026-11-19', NULL, '{"failure_count": 0, "pipelines_count": 2, "avg_queue_seconds": null, "rerun_rate": 0.0}', 'c6a38355-0000-4000-8000-000000000000', '2026-11-23 10:00:00'),
('00000000-0000-4000-8000-000000000005', '2026-11-22', NULL, '{}', 'c6a38355-0000-4000-8000-000000000000', '2026-11-23 11:00:00'),
('00000000-0000-4000-8000-000000000006', '2026-11-22', NULL, '{"failure_count": 0, "pipelines_count": 2, "avg_queue_seconds": null, "rerun_rate": 0.0}', 'c6a38355-0000-4000-8000-000000000000', '2026-11-23 06:24:02'),
('00000000-0000-4000-8000-000000000007', '2026-11-22', NULL, '{"failure_count": 0, "pipelines_count": 2, "avg_queue_seconds": null, "rerun_rate": 0.0}', 'b0000000-0000-4000-8000-000000000000', '2026-11-23 12:00:00'),
('00000000-0000-4000-8000-000000000008', '2026-11-18', 0.25, '{"failure_count": 0, "pipelines_count": 1, "avg_queue_seconds": 900, "rerun_rate": 0.0}', 'c6a38355-0000-4000-8000-000000000000', '2026-11-23 06:24:03');
INSERT INTO testops_quality_drag (repo_id, day, queue_wait_hours, factors_json, org_id, computed_at)
SELECT generateUUIDv4(), toDate('2026-11-01'), NULL, '{"failure_count": 0, "pipelines_count": 1, "avg_queue_seconds": null, "rerun_rate": 0.0}', 'c6a38355-0000-4000-8000-000000000000', toDateTime('2026-11-01 12:00:00', 'UTC')
FROM numbers(2000);
`

// Defect rows: each makes one control or WANT of the form fail, so each statement is shown able to fail.
const rb14488Defects = `
INSERT INTO testops_quality_drag (repo_id, day, queue_wait_hours, factors_json, org_id, computed_at) VALUES
('00000000-0000-4000-8000-000000000011', '2026-11-22', NULL, '{"failure_count": 0, "pipelines_count": 1, "avg_queue_seconds": 30, "rerun_rate": 0.0}', 'c6a38355-0000-4000-8000-000000000000', '2026-11-23 13:00:00'),
('00000000-0000-4000-8000-000000000012', '2026-11-22', 0.0, '{"failure_count": 0, "pipelines_count": 1, "avg_queue_seconds": null, "rerun_rate": 0.0}', 'c6a38355-0000-4000-8000-000000000000', '2026-11-23 13:00:00'),
('00000000-0000-4000-8000-000000000013', '2026-11-22', 0.5, '{"failure_count": 0, "pipelines_count": 1, "avg_queue_seconds": null, "rerun_rate": 0.0}', 'c6a38355-0000-4000-8000-000000000000', '2026-11-23 13:00:00');
`

type rb14488Statement struct{ label, text string }

func rb14488Split(body string) []rb14488Statement {
	var out []rb14488Statement
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
		out = append(out, rb14488Statement{label: label, text: text})
		chunk = nil
	}
	return out
}

func rb14488Render(v any, dbType string) string {
	value := reflect.ValueOf(v)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return "NULL"
		}
		value = value.Elem()
	}
	if ts, ok := value.Interface().(time.Time); ok {
		if strings.HasPrefix(dbType, "DateTime") {
			return ts.UTC().Format("2006-01-02 15:04:05")
		}
		return ts.UTC().Format("2006-01-02")
	}
	return fmt.Sprint(value.Interface())
}

func rb14488Run(ctx context.Context, conn driver.Conn, text, comment string) ([]map[string]string, error) {
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
			row[name] = rb14488Render(cells[i], types[i].DatabaseTypeName())
		}
		got = append(got, row)
	}
	return got, rows.Err()
}

func rb14488Log(ctx context.Context, t *testing.T, conn driver.Conn, comment string) ([]string, uint64) {
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

func TestReadback14488FormExecutesAtTheChainHead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	sum := sha256.Sum256([]byte(rb14488Form))
	if got := hex.EncodeToString(sum[:]); got != rb14488FormSHA256 {
		t.Fatalf("form sha256 = %s, want %s", got, rb14488FormSHA256)
	}
	statements := rb14488Split(rb14488Form)
	byLabel := map[string]string{}
	labels := make([]string, 0, len(statements))
	for _, s := range statements {
		labels = append(labels, s.label)
		byLabel[s.label] = s.text
	}
	if strings.Join(labels, ",") != "S0,stmt0,stmt1,stmt2,stmt3,stmt4" {
		t.Fatalf("form statements = %v", labels)
	}
	// stmt0..stmt4 each hold exactly one gate over exactly testops_quality_drag; S0 holds none.
	for _, s := range statements {
		want := 1
		if s.label == "S0" {
			want = 0
		}
		if n := strings.Count(s.text, rb14488RowsCap); n != want {
			t.Fatalf("%s: %d rows caps, want %d", s.label, n, want)
		}
		if n := strings.Count(s.text, rb14488BytesCap); n != want {
			t.Fatalf("%s: %d bytes caps, want %d", s.label, n, want)
		}
		if n := strings.Count(s.text, "'STOP-COST-GATE'"); n != want {
			t.Fatalf("%s: %d STOP-COST-GATE, want %d", s.label, n, want)
		}
		if n := strings.Count(s.text, "table IN ('testops_quality_drag')"); n != 2*want && s.label != "S0" {
			t.Fatalf("%s: gate table list found %d times, want 2", s.label, n)
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
	if result.Head < "170_" {
		t.Fatalf("schema head %q, want 170 or later", result.Head)
	}
	var version string
	if err := conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("schema head %s, clickhouse %s", result.Head, version)

	for _, statement := range chmigrate.SplitStatements(rb14488Fixture) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("fixture: %v\n%s", err, statement)
		}
	}

	var tableRows, parts uint64
	if err := conn.QueryRow(ctx, `SELECT sum(rows), count() FROM system.parts
		WHERE active AND database = currentDatabase() AND table = 'testops_quality_drag'`).Scan(&tableRows, &parts); err != nil {
		t.Fatal(err)
	}
	t.Logf("fixture: table_rows=%d active_parts=%d", tableRows, parts)
	if tableRows != 2008 {
		t.Fatalf("table rows %d, want 2008", tableRows)
	}

	// WANT per statement on the fixture (clean state).
	cleanWant := map[string]map[string]string{
		"S0":    {"t": "testops_quality_drag", "rows": fmt.Sprint(tableRows)},
		"stmt0": {"orgs": "1"},
		"stmt1": {"rows_after": "6", "groups_after": "5", "first_after": "2026-11-23 06:24:03", "last_after": "2026-11-23 11:00:00"},
		"stmt2": {"n_sampled": "3", "n_sampled_null": "0"},
		"stmt3": {"n_null": "2", "n_zero": "0", "n_other": "0", "days": "2"},
		"stmt4": {"n_null_with_number": "0", "n_null_total": "2"},
	}
	check := func(phase string, want map[string]map[string]string, only ...string) {
		for _, s := range statements {
			if len(only) > 0 {
				skip := true
				for _, o := range only {
					if o == s.label {
						skip = false
					}
				}
				if skip {
					continue
				}
			}
			comment := fmt.Sprintf("rb14488-%s-%s-%d", phase, s.label, time.Now().UnixNano())
			got, err := rb14488Run(ctx, conn, s.text, comment)
			if err != nil {
				t.Errorf("%s %s: SQL error: %v", phase, s.label, err)
				continue
			}
			if len(got) != 1 {
				t.Errorf("%s %s: %d rows, want 1", phase, s.label, len(got))
				continue
			}
			t.Logf("%s %s ROW %v", phase, s.label, got[0])
			for name, value := range want[s.label] {
				if got[0][name] != value {
					t.Errorf("%s %s: %s = %s, want %s", phase, s.label, name, got[0][name], value)
				}
			}
			if s.label == "S0" {
				if got[0]["active_parts"] == "0" {
					t.Errorf("S0: active_parts 0")
				}
			}
		}
	}
	passComments := map[string]string{}
	for _, s := range statements {
		passComments[s.label] = fmt.Sprintf("rb14488-pass-%s-%d", s.label, time.Now().UnixNano())
	}
	for _, s := range statements {
		got, err := rb14488Run(ctx, conn, s.text, passComments[s.label])
		if err != nil {
			t.Errorf("PASS %s: SQL error: %v", s.label, err)
			continue
		}
		if len(got) != 1 {
			t.Errorf("PASS %s: %d rows, want 1", s.label, len(got))
			continue
		}
		t.Logf("PASS %s ROW %v", s.label, got[0])
		for name, value := range cleanWant[s.label] {
			if got[0][name] != value {
				t.Errorf("PASS %s: %s = %s, want %s", s.label, name, got[0][name], value)
			}
		}
		if s.label == "S0" && (got[0]["active_parts"] == "0" || got[0]["rows"] == "0") {
			t.Errorf("S0: rows or active_parts 0")
		}
	}

	// Defect phase: each of stmt2, stmt3, stmt4 must be able to fail.
	for _, statement := range chmigrate.SplitStatements(rb14488Defects) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("defects: %v\n%s", err, statement)
		}
	}
	defectWant := map[string]map[string]string{
		"stmt2": {"n_sampled": "4", "n_sampled_null": "1"},
		"stmt3": {"n_null": "2", "n_zero": "1", "n_other": "1", "days": "2"},
		"stmt4": {"n_null_with_number": "1", "n_null_total": "3"},
	}
	check("DEFECT", defectWant, "stmt2", "stmt3", "stmt4")

	// PASS-path cells: query_log read_rows of each statement on the clean fixture. Model (TEMPLATE-3, measured on
	// fcc 14531): one scan of the table T + the gate 2 x P (system.parts twice) + 1 (system.one).
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	for _, s := range statements {
		types, read := rb14488Log(ctx, t, conn, passComments[s.label])
		t.Logf("PASS %s query_log types=%v read_rows=%d (T=%d P=%d)", s.label, types, read, tableRows, parts)
		if len(types) != 1 || types[0] != "QueryFinish" {
			t.Errorf("PASS %s: query_log types %v, want [QueryFinish]", s.label, types)
		}
		if s.label == "S0" {
			continue
		}
		low, high := tableRows, tableRows+2*parts+1
		if read < low || read > high {
			t.Errorf("PASS %s: query_log read_rows %d outside [T, T + 2P + 1] = [%d, %d]", s.label, read, low, high)
		}
	}

	// Gate fires: each cap literal lowered to 0 in a test copy. WANT code 395 STOP-COST-GATE before any table read.
	type fired struct{ label, cap, comment string }
	var fires []fired
	for _, s := range statements {
		if s.label == "S0" {
			continue
		}
		for _, literal := range []string{rb14488RowsCap, rb14488BytesCap} {
			lowered := strings.Replace(s.text, literal, "> 0", 1)
			comment := fmt.Sprintf("rb14488-gate-%s-%s-%d", s.label, strings.TrimPrefix(literal, "> "), time.Now().UnixNano())
			_, err := rb14488Run(ctx, conn, lowered, comment)
			var ex *clickhouse.Exception
			if err == nil || !errors.As(err, &ex) || ex.Code != 395 || !strings.Contains(ex.Message, "STOP-COST-GATE") {
				t.Errorf("GATE %s %s lowered to 0: err = %v, want code 395 STOP-COST-GATE", s.label, literal, err)
				continue
			}
			t.Logf("GATE %s %s lowered to 0: fired code=%d", s.label, literal, ex.Code)
			fires = append(fires, fired{s.label, literal, comment})
		}
	}
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	for _, f := range fires {
		types, read := rb14488Log(ctx, t, conn, f.comment)
		t.Logf("GATE %s %s query_log types=%v read_rows=%d bound=%d", f.label, f.cap, types, read, 2*parts+1)
		if len(types) != 1 || types[0] != "ExceptionBeforeStart" {
			t.Errorf("GATE %s %s: query_log types %v, want [ExceptionBeforeStart]", f.label, f.cap, types)
		}
		if read > 2*parts+1 {
			t.Errorf("GATE %s %s: read_rows %d over 2 x P + 1 = %d", f.label, f.cap, read, 2*parts+1)
		}
	}
	if len(fires) != 10 {
		t.Errorf("gates fired %d times, want 10 (5 statements x 2 caps)", len(fires))
	}
}
