//go:build integration

package analytics

// DO NOT MERGE. Throwaway exec proof of a prod read form (FORM-3NN-RBPOOL-1: system.query_log, the served pooled suite reads).
// rbpoolForm is the form file as it is (it holds no org prefix); its sha256 is pinned. Synthetic rows only. The ClickHouse server
// can be shared with other tests, so a server-wide count is a lower bound here; the exact checks run a test copy of stmt0 with
// one more conjunct, query_id IN (the ids this test sent).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/chclient"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopspooled"
)

const rbpoolFormSHA256 = "832d1e2fab6973e06e80325cf004ccb9ccca3566f1db239752109837e5a55c90"

const rbpoolForm = `-- FORM-3NN-RBPOOL-2 (ClickHouse, prod, read-only). acp-train39, 2026-11-24. = FORM-3NN-RBPOOL-1.sql cb716948220d + the acp ruling (1):
--   stmt0 also prints run_at = now() and first_matched = min(event_time) of the matched rows (INFO). pm checks that run_at is inside
--   KP + 60 .. KP + 65 min and writes it down; outside that span = STOP, no re-run. A statement in KP .. KP + 5 min may fall out of
--   the window: accepted (acp). Ruling (2): failures = type IN (ExceptionBeforeStart, ExceptionWhileProcessing), correct for code 158.
-- (was -1:) query-api roll READBACK cell R-POOLQL (pm ruling: the
--   prod CH 26.6 cost proof of the CHAOS-14591 fix; CI asserts only at CH 26.7). Point-in-time SELECT on system.query_log; no created
--   traffic (D6310): it reads what web / acr already sent. acp FORM read before the roll.
-- WHEN: pm runs it at KP + 60 .. KP + 65 min (KP = the RUNLOG STEP-KIT-PASS time). The window is the LAST 60 MINUTES of the server
--   clock, so it starts at KP .. KP + 5 min: only the new query-api build (no pre-KP old-build statement). No placeholder in this file.
-- WHAT it selects (the served pooled suite reads of query-api; cites at the #7008 head 05eee711f1db, product text = 557a2ad42bde):
--   every query-api pooled statement ends with the SETTINGS clause of analytics.statementSettings (orgpooled.go:87-93):
--   settingsMaxExecutionTime (cost.go:48-49 "SETTINGS max_execution_time = %d", queryTimeoutSecs 30, cost.go:17) + ", max_rows_to_read =
--   5000000, read_overflow_mode = 'throw'" (orgpooled.go:90, TestopsPooledMaxRowsToRead = testopspooled.MaxRowsToRead 5_000_000).
--   Marker = the text ", max_rows_to_read = 5000000, read_overflow_mode = 'throw'" (the leading comma: the workers report reader
--   jobs/report/pooled.go:200 writes "SETTINGS max_rows_to_read = ..." with no comma; pooledmttr.go:138 the same, and it reads no suite
--   table) AND the text ") AS testops_suite_runs" (SuiteRunsSource ends with it, testopspooled.go:138; alias SuiteRunsAlias :28;
--   the pipeline source is "AS testops_pipeline_runs", :109). A text marker, not tables: an ExceptionBeforeStart row (a refusal at
--   index analysis) may hold no tables list, and it must be counted. Callers: timeseries / breakdown / flowmatrix /
--   sankey statements of TEST_SUITE_DURATION_P95 (all use statementSettings).
-- ORG RULE = N/A: system.query_log has no org column, and the output holds no org data (counts / max / day only). The org of every
--   matched statement is a bound server parameter ({org_id:String}); prod holds ORG-1 only (FORM-3NN-POOL-2 stmt1 orgs = 1).
-- WANT (stmt0): failed_158 = 0 (no pooled refusal, ClickHouse TOO_MANY_ROWS); max_read_rows < 5,000,000 with finished > 0; INFO:
--   max_memory_usage < 1,073,741,824 (1 GiB; LINE-5: query-api sends no max_memory_usage; the #7008 cost cell measured ~625 MB at
--   3M rows on CH 26.7). finished = 0 = NOT EXERCISED (allowed). failed > failed_158 = report (another error class).
-- Form: lane-scratch/pm-guard/FORM-TEMPLATE-3.md 6b9e73a3c45a (CH shape, one statement per call, self-gating); form of
--   acp-train39 q39/FORM-3NN-POOL-2.sql 55858f43a45d.
-- Table: system.query_log (system log engine: PARTITION BY toYYYYMM(event_date), ORDER BY (event_date, event_time)); S0 shows its parts.
-- WORK MATH: S0 reads system.parts only. stmt0 reads system.query_log once, pruned by the key to event_date >= yesterday() and the
--   last 60 minutes; columns type, query_kind, query, read_rows, memory_usage, exception_code, event_date, event_time.
--   Cost gate inside stmt0 (2 parts reads + 1 window count): the query_log parts that hold a day >= yesterday() <= 200,000,000 rows /
--   100,000,000,000 bytes, AND the window rows <= 20,000,000 (a key-pruned count of event_date / event_time only).
-- Output: counts, max values, the day and the 2 INFO times (run_at, first_matched). No query text, no parameters, no ids, no user names.

-- S0 (system.parts only). WANT >= 1 row (one per active query_log partition with a day >= yesterday()); rows > 0.
SELECT partition, sum(rows) AS rows, sum(bytes_on_disk) AS bytes, count() AS active_parts
FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= yesterday()
GROUP BY partition ORDER BY partition;

-- stmt0 served pooled TEST_SUITE_DURATION_P95 statements of query-api in the last 60 minutes. WANT 1 row; failed_158 = 0; max_read_rows < 5000000 when finished > 0.
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= yesterday()) > 200000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= yesterday()) > 100000000000
  OR (SELECT count() FROM system.query_log WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 60 MINUTE) > 20000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT toString(today()) AS day,
  toString(now()) AS run_at,
  toString(min(event_time)) AS first_matched,
  countIf(type = 'QueryFinish') AS finished,
  maxIf(read_rows, type = 'QueryFinish') AS max_read_rows,
  maxIf(memory_usage, type = 'QueryFinish') AS max_memory_usage,
  countIf(type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')) AS failed,
  countIf(type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing') AND exception_code = 158) AS failed_158
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 60 MINUTE
  AND type != 'QueryStart' AND query_kind = 'Select'
  AND position(query, ') AS testops_suite_runs') > 0
  AND position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0
  AND cost_gate = 0;
`

type rbpoolStatement struct{ label, text string }

// rbpoolSplit splits the form on the lines that end a statement (";" at the end of a non-comment line).
func rbpoolSplit(body string) []rbpoolStatement {
	var out []rbpoolStatement
	var chunk []string
	for _, line := range strings.Split(body, "\n") {
		chunk = append(chunk, line)
		t := strings.TrimSpace(line)
		if strings.HasPrefix(t, "--") || !strings.HasSuffix(t, ";") {
			continue
		}
		label := ""
		for _, l := range chunk {
			if s := strings.TrimSpace(l); strings.HasPrefix(s, "-- S0") || strings.HasPrefix(s, "-- stmt") {
				label = strings.Fields(strings.TrimPrefix(s, "-- "))[0]
				break
			}
		}
		out = append(out, rbpoolStatement{label: label, text: strings.TrimSuffix(strings.TrimRight(strings.Join(chunk, "\n"), " \t\n"), ";")})
		chunk = nil
	}
	return out
}

func rbpoolRows(ctx context.Context, conn stdclickhouse.Conn, text string) ([]map[string]string, []string, error) {
	rows, err := conn.Query(ctx, text)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	types := rows.ColumnTypes()
	var names []string
	for _, ct := range types {
		names = append(names, ct.DatabaseTypeName())
	}
	var got []map[string]string
	for rows.Next() {
		cells := make([]any, len(types))
		for i, ct := range types {
			cells[i] = reflect.New(ct.ScanType()).Interface()
		}
		if err := rows.Scan(cells...); err != nil {
			return nil, nil, err
		}
		row := map[string]string{}
		for i, name := range rows.Columns() {
			row[name] = fmt.Sprint(reflect.ValueOf(cells[i]).Elem().Interface())
		}
		got = append(got, row)
	}
	return got, names, rows.Err()
}

func TestForm3nnRbpoolExecutesOnTheServedStatements(t *testing.T) {
	sum := sha256.Sum256([]byte(rbpoolForm))
	if got := hex.EncodeToString(sum[:]); got != rbpoolFormSHA256 {
		t.Fatalf("form sha256 = %s, want %s", got, rbpoolFormSHA256)
	}
	statements := rbpoolSplit(rbpoolForm)
	byLabel := map[string]string{}
	var labels []string
	for _, s := range statements {
		labels = append(labels, s.label)
		byLabel[s.label] = s.text
	}
	if strings.Join(labels, ",") != "S0,stmt0" {
		t.Fatalf("form statements = %v", labels)
	}
	marker := ", max_rows_to_read = 5000000, read_overflow_mode = ''throw''"
	caps := []string{"> 200000000\n", "> 100000000000\n", "> 20000000,"}
	for lit, n := range map[string]int{caps[0]: 1, caps[1]: 1, caps[2]: 1, "'STOP-COST-GATE'": 1, "cost_gate = 0": 1, marker: 1,
		"') AS testops_suite_runs'": 1, "FROM system.query_log": 2} {
		if got := strings.Count(byLabel["stmt0"], lit); got != n {
			t.Fatalf("stmt0: %q found %d times, want %d", lit, got, n)
		}
	}
	if strings.Contains(byLabel["S0"], "STOP-COST-GATE") || strings.Contains(byLabel["S0"], "FROM system.query_log") {
		t.Fatal("S0 reads system.parts only")
	}
	// The form's marker is the text the served path writes: the settings clause of a pooled suite statement.
	served := statementSettings(pooledSuiteRunsSource, queryTimeoutSecs)
	if !strings.Contains(served, strings.ReplaceAll(marker, "''", "'")) || !strings.HasSuffix(testopspooled.SuiteRunsSource, ") AS testops_suite_runs") {
		t.Fatalf("the served settings %q or the source alias do not hold the form marker", served)
	}
	if TestopsPooledMaxRowsToRead != 5_000_000 {
		t.Fatalf("TestopsPooledMaxRowsToRead = %d, the form marker names 5000000", TestopsPooledMaxRowsToRead)
	}

	e := startOwnersEnv(t)
	var version string
	if err := e.admin.QueryRow(e.ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("clickhouse %s", version)
	var testStart string
	if err := e.admin.QueryRow(e.ctx, "SELECT toString(now())").Scan(&testStart); err != nil {
		t.Fatal(err)
	}
	const org = "f0f0f0f0-0000-4000-8000-0000000003b1"
	const small = "b0b0b0b0-0000-4000-8000-0000000003b1"
	const big = "b0b0b0b0-0000-4000-8000-0000000003b2"
	e.repo(org, small, "acme/small")
	e.repo(org, big, "acme/big")
	e.owner(org, "team-a", small, "acme/small", 1, openFrom(), nil)
	e.owner(org, "team-a", big, "acme/big", 1, openFrom(), nil)
	e.durations(org, small, day1, 10, 20, 30)
	e.durations(org, big, day1, 40)
	e.write(org, day1, small, big)
	const history = 5_200_000
	e.exec(fmt.Sprintf(`INSERT INTO test_suite_results (repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count,
  skipped_count, duration_seconds, started_at, finished_at, org_id, last_synced, report_slot, run_attempt)
SELECT toUUID('%s'), concat('h-', toString(intDiv(number, 10))), concat('hs-', toString(number)), 'pkg', 1, 1, 0, 0,
  toFloat64(number %% 600), toDateTime64('2025-01-01 00:00:00', 3, 'UTC') + intDiv(number, 3), NULL, '%s',
  toDateTime64('2025-12-31 00:00:00', 3, 'UTC'), '', 1
FROM numbers(%d)`, big, org, history))

	prod, err := dhclickhouse.NewClickHouseQueryClientWithOptions(chclient.Options(e.uri))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = prod.Close() })
	ids := map[string]string{}
	call := func(name string, measure model.MeasureInput) error {
		t.Helper()
		ids[name] = "rbpool-" + name + "-" + uuid.NewString()[:8]
		ctx := stdclickhouse.Context(e.ctx, stdclickhouse.WithQueryID(ids[name]))
		_, err := resolveOneTimeseries(ctx, prod, org, tsInput(model.DimensionInputTeam, measure), false, nil, true)
		t.Logf("SERVED %s %s: err %v", name, measure, err)
		return err
	}
	// 1. stored days only: the raw arm reads no suite row; served.
	if err := call("stored", model.MeasureInputTestSuiteDurationP95); err != nil {
		t.Fatalf("stored phase: %v", err)
	}
	// 2. a legacy day of the big repository: the one raw read is its 5.2M history rows: refused, code 158.
	e.exec(`INSERT INTO testops_test_metrics_daily (repo_id, day, org_id, computed_at) VALUES (?, ?, ?, now() + INTERVAL 1 HOUR)`, big, day1, org)
	var refused *PooledReadBoundError
	if err := call("legacy", model.MeasureInputTestSuiteDurationP95); !errors.As(err, &refused) || refused.Code != 158 {
		t.Fatalf("legacy phase: err %v, want a PooledReadBoundError code 158", err)
	}
	// 3. the pooled PIPELINE source (same settings clause, other alias): never counted.
	_ = call("pipeline", model.MeasureInputPipelineDurationP95)
	// 4. the workers report form of the same source ("SETTINGS max_rows_to_read", no comma): never counted.
	ids["workers"] = "rbpool-workers-" + uuid.NewString()[:8]
	wctx := stdclickhouse.Context(e.ctx, stdclickhouse.WithQueryID(ids["workers"]))
	werr := e.admin.QueryRow(wctx, "SELECT count() FROM "+testopspooled.SuiteRunsSource+
		fmt.Sprintf(" SETTINGS max_rows_to_read = %d, read_overflow_mode = 'throw'", testopspooled.MaxRowsToRead),
		stdclickhouse.Named("org_id", org), stdclickhouse.Named("start_date", "2026-01-01"), stdclickhouse.Named("end_date", "2026-01-07")).Scan(new(uint64))
	t.Logf("WORKERS-FORM statement: err %v", werr)
	e.exec("SYSTEM FLUSH LOGS")

	// Independent recompute per query id (Go reads every query_log row of the id and applies the marker in Go).
	type want struct {
		finished, failed, failed158 int
		maxRead                     uint64
	}
	recompute := func(name string) want {
		t.Helper()
		rows, err := e.admin.Query(e.ctx, `SELECT toString(type), query, read_rows, exception_code FROM system.query_log
			WHERE query_id = ? AND type != 'QueryStart'`, ids[name])
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var w want
		for rows.Next() {
			var typ, query string
			var read uint64
			var code int32
			if err := rows.Scan(&typ, &query, &read, &code); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(query, ", max_rows_to_read = 5000000, read_overflow_mode = 'throw'") || !strings.Contains(query, ") AS testops_suite_runs") {
				continue
			}
			switch typ {
			case "QueryFinish":
				w.finished++
				if read > w.maxRead {
					w.maxRead = read
				}
			case "ExceptionBeforeStart", "ExceptionWhileProcessing":
				w.failed++
				if code == 158 {
					w.failed158++
				}
			}
		}
		return w
	}
	only := func(text string, names ...string) string {
		var quoted []string
		for _, n := range names {
			quoted = append(quoted, "'"+ids[n]+"'")
		}
		out := strings.Replace(text, "AND cost_gate = 0", "AND cost_gate = 0 AND query_id IN ("+strings.Join(quoted, ", ")+")", 1)
		if out == text {
			t.Fatal("no cost_gate conjunct to extend")
		}
		return out
	}
	one := func(label, text string) map[string]string {
		t.Helper()
		got, types, err := rbpoolRows(e.ctx, e.admin, text)
		if err != nil || len(got) != 1 {
			t.Fatalf("%s: err %v rows %d", label, err, len(got))
		}
		if strings.Join(types, ",") != "String,String,String,UInt64,UInt64,UInt64,UInt64,UInt64" {
			t.Errorf("%s: column types %v", label, types)
		}
		t.Logf("%s ROW %v", label, got[0])
		return got[0]
	}
	check := func(label string, got map[string]string, w want) {
		t.Helper()
		for k, v := range map[string]string{"finished": fmt.Sprint(w.finished), "failed": fmt.Sprint(w.failed), "failed_158": fmt.Sprint(w.failed158)} {
			if got[k] != v {
				t.Errorf("%s: %s = %s, want %s (Go recompute)", label, k, got[k], v)
			}
		}
		if w.finished > 0 && got["max_read_rows"] != fmt.Sprint(w.maxRead) {
			t.Errorf("%s: max_read_rows = %s, want %d", label, got["max_read_rows"], w.maxRead)
		}
	}

	s0, _, err := rbpoolRows(e.ctx, e.admin, byLabel["S0"])
	if err != nil || len(s0) < 1 {
		t.Fatalf("S0: err %v rows %d, want >= 1", err, len(s0))
	}
	t.Logf("PASS S0 ROWS %v", s0)
	clean := one("PASS stmt0 (server-wide)", byLabel["stmt0"])
	stored, legacy := recompute("stored"), recompute("legacy")
	if stored.finished < 1 || stored.maxRead >= 5_000_000 || legacy.failed158 < 1 {
		t.Fatalf("the served path did not give the cases: stored %+v legacy %+v", stored, legacy)
	}
	for k, lo := range map[string]int{"finished": stored.finished, "failed_158": legacy.failed158} {
		var n int
		if _, err := fmt.Sscan(clean[k], &n); err != nil || n < lo {
			t.Errorf("PASS stmt0 server-wide %s = %s, want >= %d (this test's own statements)", k, clean[k], lo)
		}
	}
	exactStored := one("EXACT stored", only(byLabel["stmt0"], "stored"))
	check("EXACT stored", exactStored, stored)
	if exactStored["first_matched"] < testStart || exactStored["run_at"] < exactStored["first_matched"] {
		t.Errorf("INFO times: run_at %s, first_matched %s, test start %s: want test start <= first_matched <= run_at", exactStored["run_at"], exactStored["first_matched"], testStart)
	}
	check("EXACT legacy", one("EXACT legacy", only(byLabel["stmt0"], "legacy")), legacy)
	check("EXACT pipeline", one("EXACT pipeline", only(byLabel["stmt0"], "pipeline")), want{})
	check("EXACT workers", one("EXACT workers", only(byLabel["stmt0"], "workers")), want{})
	t.Logf("WANT stored %+v legacy %+v (Go recompute)", stored, legacy)

	// Mutants: each test copy changes ONE rule and must count a statement the clean form leaves out.
	noComma := strings.Replace(byLabel["stmt0"], "', max_rows_to_read", "'max_rows_to_read", 1)
	if got := one("MUTANT rule: marker without the comma (workers)", only(noComma, "workers")); got["finished"] == "0" && got["failed"] == "0" {
		t.Errorf("MUTANT marker without the comma: the workers statement is still not counted")
	}
	pipeAlias := strings.Replace(byLabel["stmt0"], "') AS testops_suite_runs'", "') AS testops_pipeline_runs'", 1)
	if got := one("MUTANT rule: the pipeline alias", only(pipeAlias, "pipeline")); got["finished"] == "0" && got["failed"] == "0" {
		t.Errorf("MUTANT pipeline alias: the pipeline statements are still not counted")
	}

	// Gate fires: each cap lowered to 0 in a test copy. WANT code 395 STOP-COST-GATE.
	fires := 0
	for _, c := range caps {
		_, _, err := rbpoolRows(e.ctx, e.admin, strings.Replace(byLabel["stmt0"], c, "> 0"+c[len(c)-1:], 1))
		var ex *stdclickhouse.Exception
		if err == nil || !errors.As(err, &ex) || ex.Code != 395 || !strings.Contains(ex.Message, "STOP-COST-GATE") {
			t.Errorf("GATE %q lowered to 0: err = %v, want code 395 STOP-COST-GATE", c, err)
			continue
		}
		t.Logf("GATE %q lowered to 0: fired code=%d", c, ex.Code)
		fires++
	}
	if fires != 3 {
		t.Errorf("gates fired %d times, want 3", fires)
	}
}
