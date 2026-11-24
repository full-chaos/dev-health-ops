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

const rbpoolFormSHA256 = "89e27d22ae88370a515001df422916f116704decbac0f9b2d41cef15394cbc5b"

const rbpoolForm = `-- FORM-3NN-RBPOOL-4 (ClickHouse, prod, read-only). acp-train39, 2026-11-24. query-api roll READBACK cell R-POOLQL: the served cost
--   proof of the CHAOS-14591 fix (CI asserts only on its own ClickHouse version). Point-in-time SELECT on system.query_log; no created
--   traffic (D6310): it reads what web / acr already sent. acp FORM read before the roll.
-- = FORM-3NN-RBPOOL-3.sql 1da6d7368575 + the acp delta read READ-FORM-POOL3-RBPOOL3-1 1b5274a8ad84 lines: P2-7 the control = query-api analytics
--   timeseries / breakdown statements only (the sync worker writes the same settings text); P3-11 the fail-path read is stated.
-- (was -3: P1-2 control, P2-1 cap 9,000,000, P3-1 org text, P3-2 the 4,000,000 line, P3-3 stmt1 sorting key, P3-4 KP = 0 old pods, P3-8 no
--   server version fact, P3-9 UTC, P3-10 the 1970 rule; no marker literal in any comment line: a CI cell sends a comment with the statement.)
-- WHEN: pm runs it at KP + 60 .. KP + 65 min. KP = the RUNLOG STEP-KIT-PASS time AND 0 old-ReplicaSet query-api pods (the R1CARD DRAIN line
--   = 0 pods with a deletionTimestamp; a Terminating old pod in its grace time still sends the SAME marker from the old build). The window
--   is the LAST 60 MINUTES of the server clock, so it starts at KP .. KP + 5 min; a statement in KP .. KP + 5 may fall out: accepted (acp).
--   stmt0 prints run_at = toString(now(), 'UTC'); pm checks that run_at is inside KP + 60 .. KP + 65 (UTC) and writes it down; outside that
--   span = STOP, no re-run. No placeholder in this file.
-- WHAT it selects (cites at the pin ba2d85c5; internal/queryapi = the #7008 head text). The WHERE admits a row that is a CONTROL row OR holds
--   the pooled settings tail; every count is a countIf of its own shape (each shape is admitted by the WHERE).
--   CONTROL rows = query-api analytics timeseries / breakdown statements of ANY measure: the text " AS dimension" + "_value," (timeseries.go:286,
--   breakdown.go:160; analytics is imported only inside internal/queryapi at ba2d85c5, so only query-api writes it) AND the settings text
--   "SETTINGS max_execution" + "_time = 30" (settingsMaxExecutionTime, cost.go:48-49; queryTimeoutSecs 30, cost.go:17). The settings text
--   alone is NOT query-api only: the sync worker writes "... = 30, max_rows_to_read = 50000000" (providersync/github_tests_run_listing.go:330,
--   :360) and teamscope (also imported by internal/jobs/report and internal/jobs/metrics/daily/benchmarking) writes it (emptyreason.go:122,
--   :158, heldrepos.go:38); none of them writes the dimension text (git grep -F at ba2d85c5: 2 writers, both in analytics). Both literals are
--   built by concat() in the SQL, so this form's own text (in query_log after an earlier call) cannot count as a control row.
--   POOLED SUITE rows = rows that hold the statementSettings tail ", max_rows_to_read = 5000000" + ", read_overflow_mode = 'throw'"
--   (orgpooled.go:87-93, :90; TestopsPooledMaxRowsToRead 5_000_000, :80) AND ")" + " AS testops_suite_runs" (testopspooled.go:138; alias :28),
--   from every served shape (timeseries, rate, breakdown, flowmatrix, sankey; the control needs only timeseries / breakdown). The leading comma
--   keeps out the workers report reader (jobs/report/pooled.go:200 and pooledmttr.go:138 write "SETTINGS max_rows_to_read" with no
--   max_execution_time) and the sync worker ("= 50000000"); the alias keeps out the pipeline source ("AS testops_pipeline_runs", :109). The
--   pooled marker holds doubled quotes (''throw''), so this form's own text does not match it. A text marker, not the tables list: an
--   ExceptionBeforeStart refusal row may hold no tables list, and it must be counted.
-- ORG RULE = N/A: system.query_log has no org column, and the output holds no org data. The org of a statement is a bound server parameter
--   ({org_id:String}); the counts cover the pooled suite statements of EVERY org (stricter than one org).
-- WANT (stmt0): analytics_finished > 0 (the control: query-api analytics statements ran in the window; 0 = query_log off, a text change, or
--   no traffic: report, and the pooled WANTs below prove nothing). failed_158 = 0 (no pooled refusal, TOO_MANY_ROWS). When finished > 0:
--   max_read_rows < 5,000,000, and max_read_rows >= 4,000,000 = report (INFO, 80 % of the bound, as FORM-3NN-POOL-3). INFO: max_memory_usage
--   < 1,073,741,824 (1 GiB; LINE-5). finished = 0 and failed = 0 with analytics_finished > 0 = NOT EXERCISED (allowed). failed > failed_158 =
--   report (another error class). first_matched = toString(min(event_time) of the POOLED SUITE rows, 'UTC'); it is 1970-01-01 00:00:00 when
--   no pooled suite row matched, and it counts as INFO only when finished + failed > 0.
-- WANT (stmt1, INFO): sorting_key starts with "event_date, event_time" (then the window bounds are key-pruned); another key = report.
-- Form: lane-scratch/pm-guard/FORM-TEMPLATE-3.md 6b9e73a3c45a (CH shape, one statement per call, self-gating); form of
--   acp-train39 q39/FORM-3NN-POOL-3.sql c63d50cf3991.
-- Table: system.query_log (system log engine; partition by month of event_date); S0 shows its parts, stmt1 its keys.
-- WORK MATH: S0 and stmt1 read system tables (parts, tables) only. stmt0 reads system.query_log TWICE: the window count of its cost gate
--   (event_date / event_time only) and the scan; each is bounded by W = the window rows: W <= 9,000,000 (the gate), so 2 x W + 2P + 1 <=
--   18,000,000 + 2P + 1 (P = the query_log parts of the parts gate). The parts gate (the query_log parts that hold a day >= yesterday()
--   <= 200,000,000 rows / 100,000,000,000 bytes) is the upper bound of both reads when the key prunes nothing.
--   FAIL PATH (P3-11): the gate's 3 scalar subqueries run before throwIf, so a fire can read the window rows once (up to W) before it
--   throws: a fire reads <= W + 2P + 1. W can be over 9,000,000 only on that path, and then the statement stops before the scan. The pass
--   path holds the 2 x W bound (CI: read_rows 4,926 <= 2Q + 2P + 1 = 9,905, #7023 job 138649935509; fires read 6).
-- Output: counts, max values, the day and 2 INFO times (UTC). No query text, no parameters, no ids, no user names.

-- S0 (system.parts only). WANT >= 1 row (one per active query_log partition with a day >= yesterday()), rows > 0.
SELECT partition, sum(rows) AS rows, sum(bytes_on_disk) AS bytes, count() AS active_parts
FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= yesterday()
GROUP BY partition ORDER BY partition;

-- stmt0 served pooled TEST_SUITE_DURATION_P95 statements of query-api in the last 60 minutes, and the control. WANT 1 row, analytics_finished > 0, failed_158 = 0.
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= yesterday()) > 200000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= yesterday()) > 100000000000
  OR (SELECT count() FROM system.query_log WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 60 MINUTE) > 9000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT toString(today()) AS day,
  toString(now(), 'UTC') AS run_at,
  toString(minIf(event_time, position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0 AND position(query, ') AS testops_suite_runs') > 0), 'UTC') AS first_matched,
  countIf(type = 'QueryFinish' AND position(query, concat(' AS dimension', '_value,')) > 0 AND position(query, concat('SETTINGS max_execution', '_time = 30')) > 0) AS analytics_finished,
  countIf(type = 'QueryFinish' AND position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0 AND position(query, ') AS testops_suite_runs') > 0) AS finished,
  maxIf(read_rows, type = 'QueryFinish' AND position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0 AND position(query, ') AS testops_suite_runs') > 0) AS max_read_rows,
  maxIf(memory_usage, type = 'QueryFinish' AND position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0 AND position(query, ') AS testops_suite_runs') > 0) AS max_memory_usage,
  countIf(type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing') AND position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0 AND position(query, ') AS testops_suite_runs') > 0) AS failed,
  countIf(type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing') AND exception_code = 158 AND position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0 AND position(query, ') AS testops_suite_runs') > 0) AS failed_158
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 60 MINUTE
  AND type != 'QueryStart' AND query_kind = 'Select'
  AND ((position(query, concat(' AS dimension', '_value,')) > 0 AND position(query, concat('SETTINGS max_execution', '_time = 30')) > 0)
    OR position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0)
  AND cost_gate = 0;

-- stmt1 sorting key of system.query_log (system.tables only, INFO). WANT 1 row; sorting_key starts with "event_date, event_time".
SELECT sorting_key, partition_key FROM system.tables WHERE database = 'system' AND name = 'query_log';
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
	if strings.Join(labels, ",") != "S0,stmt0,stmt1" {
		t.Fatalf("form statements = %v", labels)
	}
	marker := ", max_rows_to_read = 5000000, read_overflow_mode = ''throw''"
	caps := []string{"> 200000000\n", "> 100000000000\n", "> 9000000,"}
	control := "concat('SETTINGS max_execution', '_time = 30')"
	dimension := "position(query, concat(' AS dimension', '_value,')) > 0 AND "
	for lit, n := range map[string]int{caps[0]: 1, caps[1]: 1, caps[2]: 1, "'STOP-COST-GATE'": 1, "cost_gate = 0": 1, marker: 7,
		"') AS testops_suite_runs'": 6, "FROM system.query_log": 2, control: 2, dimension: 2, "SETTINGS max_execution_time = 30": 0, " AS dimension_value,": 0} {
		if got := strings.Count(byLabel["stmt0"], lit); got != n {
			t.Fatalf("stmt0: %q found %d times, want %d", lit, got, n)
		}
	}
	if strings.Contains(byLabel["S0"], "STOP-COST-GATE") || strings.Contains(byLabel["S0"], "FROM system.query_log") {
		t.Fatal("S0 reads system.parts only")
	}
	// The form's marker is the text the served path writes: the settings clause of a pooled suite statement.
	served := statementSettings(pooledSuiteRunsSource, queryTimeoutSecs)
	if !strings.Contains(served, "SETTINGS max_execution_time = 30"+strings.ReplaceAll(marker, "''", "'")) || !strings.HasSuffix(testopspooled.SuiteRunsSource, ") AS testops_suite_runs") {
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
	// 5. the sync worker shape (providersync github_tests_run_listing.go:330): the same settings text, no analytics dimension: never counted.
	ids["sync"] = "rbpool-sync-" + uuid.NewString()[:8]
	sctx := stdclickhouse.Context(e.ctx, stdclickhouse.WithQueryID(ids["sync"]))
	serr := e.admin.QueryRow(sctx, "SELECT count() FROM test_suite_results WHERE org_id = {org_id:String} SETTINGS max_execution_time = 30, max_rows_to_read = 50000000",
		stdclickhouse.Named("org_id", org)).Scan(new(uint64))
	if serr != nil {
		t.Fatalf("SYNC-SHAPE statement: %v", serr)
	}
	e.exec("SYSTEM FLUSH LOGS")

	// Independent recompute per query id (Go reads every query_log row of the id and applies the marker in Go).
	type want struct {
		analytics, finished, failed, failed158 int
		maxRead                                uint64
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
			if typ == "QueryFinish" && strings.Contains(query, " AS dimension_value,") && strings.Contains(query, "SETTINGS max_execution_time = 30") {
				w.analytics++
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
	one := func(label, text string, comment ...string) map[string]string {
		t.Helper()
		ctx := e.ctx
		if len(comment) > 0 {
			ctx = stdclickhouse.Context(e.ctx, stdclickhouse.WithSettings(stdclickhouse.Settings{"log_comment": comment[0]}))
		}
		got, types, err := rbpoolRows(ctx, e.admin, text)
		if err != nil || len(got) != 1 {
			t.Fatalf("%s: err %v rows %d", label, err, len(got))
		}
		if strings.Join(types, ",") != "String,String,String,UInt64,UInt64,UInt64,UInt64,UInt64,UInt64" {
			t.Errorf("%s: column types %v", label, types)
		}
		t.Logf("%s ROW %v", label, got[0])
		return got[0]
	}
	check := func(label string, got map[string]string, w want) {
		t.Helper()
		for k, v := range map[string]string{"analytics_finished": fmt.Sprint(w.analytics), "finished": fmt.Sprint(w.finished), "failed": fmt.Sprint(w.failed), "failed_158": fmt.Sprint(w.failed158)} {
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
	passComment := "rbpool-pass-" + uuid.NewString()[:8]
	clean := one("PASS stmt0 (server-wide)", byLabel["stmt0"], passComment)
	if got, _, err := rbpoolRows(e.ctx, e.admin, byLabel["stmt1"]); err != nil || len(got) != 1 || !strings.HasPrefix(got[0]["sorting_key"], "event_date, event_time") {
		t.Errorf("stmt1: err %v rows %v, want 1 row with sorting_key from event_date, event_time", err, got)
	} else {
		t.Logf("PASS stmt1 ROW %v", got[0])
	}
	stored, legacy := recompute("stored"), recompute("legacy")
	if stored.finished < 1 || stored.maxRead >= 5_000_000 || legacy.failed158 < 1 {
		t.Fatalf("the served path did not give the cases: stored %+v legacy %+v", stored, legacy)
	}
	pipeline := recompute("pipeline")
	if pipeline.analytics < 1 || pipeline.finished != 0 {
		t.Fatalf("the pipeline call gave no control row or a suite row: %+v", pipeline)
	}
	for k, lo := range map[string]int{"analytics_finished": stored.analytics + pipeline.analytics, "finished": stored.finished, "failed_158": legacy.failed158} {
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
	check("EXACT pipeline", one("EXACT pipeline", only(byLabel["stmt0"], "pipeline")), pipeline)
	check("EXACT workers", one("EXACT workers", only(byLabel["stmt0"], "workers")), want{})
	check("EXACT sync", one("EXACT sync", only(byLabel["stmt0"], "sync")), want{})
	if w := recompute("sync"); w != (want{}) {
		t.Errorf("the sync shape recompute = %+v, want zero", w)
	}
	t.Logf("WANT stored %+v legacy %+v (Go recompute)", stored, legacy)

	// Mutants: each test copy changes ONE rule and must count a statement the clean form leaves out.
	noControl := strings.ReplaceAll(byLabel["stmt0"], control, "concat('SETTINGS max_execution', '_time = 31')")
	if got := one("MUTANT rule: another control settings text", only(noControl, "stored")); got["analytics_finished"] != "0" || got["finished"] != fmt.Sprint(stored.finished) {
		t.Errorf("MUTANT control text: analytics_finished %s finished %s, want 0 / %d (the pooled rows stay admitted by their own tail)", got["analytics_finished"], got["finished"], stored.finished)
	}
	noDimension := strings.ReplaceAll(byLabel["stmt0"], dimension, "")
	if got := one("MUTANT rule: the control without the analytics dimension text (sync)", only(noDimension, "sync")); got["analytics_finished"] == "0" {
		t.Errorf("MUTANT no dimension term: the sync statement is still not counted as a control row")
	}
	pipeAlias := strings.ReplaceAll(byLabel["stmt0"], "') AS testops_suite_runs'", "') AS testops_pipeline_runs'")
	if got := one("MUTANT rule: the pipeline alias", only(pipeAlias, "pipeline")); got["finished"] == "0" && got["failed"] == "0" {
		t.Errorf("MUTANT pipeline alias: the pipeline statements are still not counted")
	}

	// Gate fires: each cap lowered to 0 in a test copy. WANT code 395 STOP-COST-GATE, then the query_log type and read_rows.
	fires := 0
	var fireComments []string
	for _, c := range caps {
		comment := "rbpool-gate-" + uuid.NewString()[:8]
		fctx := stdclickhouse.Context(e.ctx, stdclickhouse.WithSettings(stdclickhouse.Settings{"log_comment": comment}))
		_, _, err := rbpoolRows(fctx, e.admin, strings.Replace(byLabel["stmt0"], c, "> 0"+c[len(c)-1:], 1))
		var ex *stdclickhouse.Exception
		if err == nil || !errors.As(err, &ex) || ex.Code != 395 || !strings.Contains(ex.Message, "STOP-COST-GATE") {
			t.Errorf("GATE %q lowered to 0: err = %v, want code 395 STOP-COST-GATE", c, err)
			continue
		}
		t.Logf("GATE %q lowered to 0: fired code=%d", c, ex.Code)
		fires++
		fireComments = append(fireComments, comment)
	}
	if fires != 3 {
		t.Errorf("gates fired %d times, want 3", fires)
	}
	// PASS-path and gate read_rows from query_log. The bound: the rows and parts of the query_log parts of the parts gate (Q, P):
	// a pass reads the window twice (<= 2 Q) + the parts gate (2 P) + 1; a gate fire reads at most the window count once (<= Q) + 2 P + 1.
	e.exec("SYSTEM FLUSH LOGS")
	var q, p uint64
	if err := e.admin.QueryRow(e.ctx, `SELECT sum(rows), count() FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= yesterday()`).Scan(&q, &p); err != nil {
		t.Fatal(err)
	}
	logOf := func(comment string) ([]string, uint64) {
		t.Helper()
		rows, err := e.admin.Query(e.ctx, `SELECT toString(type), read_rows FROM system.query_log WHERE log_comment = ? AND type != 'QueryStart' ORDER BY event_time_microseconds`, comment)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var types []string
		var total uint64
		for rows.Next() {
			var typ string
			var read uint64
			if err := rows.Scan(&typ, &read); err != nil {
				t.Fatal(err)
			}
			types = append(types, typ)
			total += read
		}
		return types, total
	}
	if types, read := logOf(passComment); len(types) != 1 || types[0] != "QueryFinish" || read < 1 || read > 2*q+2*p+1 {
		t.Errorf("PASS stmt0 query_log types %v read_rows %d, want [QueryFinish] and 1..%d (2Q + 2P + 1, Q %d P %d)", types, read, 2*q+2*p+1, q, p)
	} else {
		t.Logf("PASS stmt0 query_log types %v read_rows %d (2Q + 2P + 1 = %d)", types, read, 2*q+2*p+1)
	}
	for _, c := range fireComments {
		if types, read := logOf(c); len(types) != 1 || types[0] != "ExceptionBeforeStart" || read > q+2*p+1 {
			t.Errorf("GATE %s query_log types %v read_rows %d, want [ExceptionBeforeStart] and <= Q + 2P + 1 = %d", c, types, read, q+2*p+1)
		} else {
			t.Logf("GATE %s query_log types %v read_rows %d (Q + 2P + 1 = %d)", c, types, read, q+2*p+1)
		}
	}
}
