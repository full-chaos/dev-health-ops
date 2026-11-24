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

const rbpoolFormSHA256 = "5f782b78b08aa8ffecdb97a8550b7da42d78c5cf433e194cc6d0ec2f59867a04"

const rbpoolForm = `-- FORM-3NN-RBPOOL-6 (ClickHouse, prod, read-only). acp-train39, 2026-11-24. query-api roll READBACK R-POOLQL, LATER LOOK (acp word): it
--   reads the facts of FORM-3NN-RBPOOL-4 89e27d22ae88 over a FIXED window from KP, so a later run sees every served statement since KP (the
--   why and the run facts: READBACK-RBPOOL5-39-1.md, a lane file). Point-in-time SELECT on system.query_log; no created traffic (D6310).
--   acp FORM read before pm runs it.
-- = FORM-3NN-RBPOOL-5.sql f5d65342e9d6 + the acp read READ-FORM-RBPOOL5-1 27741d599f7e: NEW stmtR (the replica count, before stmt0; WANT 1,
--   else STOP: P2-1), S0 min_day (P3-1), the START..KP gap line (P3-2), stmt0h 0 = UNSPLIT (P3-4), the granule slack line (P3-6).
-- RBPOOL-5 = FORM-3NN-RBPOOL-4.sql 89e27d22ae88 with: stmt0 window = the FIXED literal KP .. now() (KP = the roll's STEP-KIT-PASS time),
--   event_date >= the KP day, its gate counts that window (cap 9,000,000); NEW stmt0h = the control only over the 24 h before the roll
--   START, with its own gate; no stmt1. run_at = INFO (no run-time span rule: the window is fixed). The window literals are in the SQL lines.
-- WHAT it selects: the -4 markers, unchanged (cites at the pin ba2d85c5; internal/queryapi = the #7008 head text):
--   CONTROL rows = query-api analytics timeseries / breakdown statements of ANY measure: the text " AS dimension" + "_value," (timeseries.go:286,
--   breakdown.go:160) AND "SETTINGS max_execution" + "_time = 30" (settingsMaxExecutionTime, cost.go:48-49; queryTimeoutSecs 30, cost.go:17).
--   Only query-api writes the dimension text (analytics has 0 importers outside internal/queryapi). Both literals are built by concat() in
--   the SQL, so this form's own text cannot count as a control row.
--   POOLED SUITE rows = rows that hold ", max_rows_to_read = 5000000" + ", read_overflow_mode = 'throw'" (orgpooled.go:87-93, :90) AND
--   ")" + " AS testops_suite_runs" (testopspooled.go:138). The leading comma keeps out the workers report reader and the sync worker; the
--   alias keeps out the pipeline source. The pooled marker holds doubled quotes (''throw''), so this form's own text does not match it.
-- THE CONTROL TEXT OF stmt0h AT THE OLD BUILD (acp word: prove it from source): the old pods ran the query-api build of ops 84d5d1da. At 84d5d1da
--   and at ba2d85c5 the writers are byte-equal: git diff --quiet 84d5d1da ba2d85c5 -- internal/queryapi/analytics/{cost,timeseries,breakdown,
--   orgpooled,resolve}.go rc 0 (sha12 cost.go 93e1a5997d65, timeseries.go 9ca3c9eacf98, breakdown.go ecaab9c9cf2f at both); the dimension
--   text has the same 2 writers (timeseries.go:286, breakdown.go:160) and analytics the same 0 outside importers at both; resolve.go:356 /
--   :407 pass queryTimeoutSecs (30). So the old build wrote the same control text, and a 24 h count of it is a valid baseline.
-- REPLICAS (P2-1): system.query_log and system.parts are local to each replica of the server; the connector session reads ONE replica.
--   stmtR counts the replicas of cluster 'default' (one row of system.one per replica). WANT replicas = 1, else STOP: no stmt0 / stmt0h run
--   (with >= 2 replicas a 158 on another replica is not in this replica's query_log = a false pass). A stmtR SQL error = STOP too.
-- START..KP GAP (INFO, by design): the stmt0 window starts at KP (the kit pass = 0 old pods left); the minutes from START to KP (old pods
--   draining, new pods starting) are outside the stmt0 window and outside the stmt0h window. The old build writes the same pooled text, so the
--   text cannot split pods; traffic in both roll windows was 0 (a lane fact), so this gap holds no row that stmt0 needs.
-- RETENTION (P3-1): S0 min_day = the oldest day of the active query_log parts with a day in the windows (the TTL floor). WANT min_day <=
--   the stmt0h first day (its first toDate literal), else stmt0h = NOT EXERCISED (its window head is gone; state it, no stmt0h WANT).
-- ORG RULE = N/A: system.query_log has no org column, and the output holds no org data (counts / max / day / an INFO time).
-- WANT (stmt0): analytics_finished > 0 (the control: query-api analytics statements ran since KP; 0 = still NOT EXERCISED, report, the pooled
--   WANTs below prove nothing). failed_158 = 0. When finished > 0: max_read_rows < 5,000,000, and >= 4,000,000 = report (INFO). INFO:
--   max_memory_usage < 1,073,741,824 (1 GiB; LINE-5). finished = 0 and failed = 0 with analytics_finished > 0 = NOT EXERCISED (allowed).
--   failed > failed_158 = report. first_matched = toString(min(event_time) of the POOLED SUITE rows, 'UTC'); 1970-01-01 00:00:00 when no pooled
--   suite row matched; INFO only when finished + failed > 0. run_at = toString(now(), 'UTC') = INFO.
-- WANT (stmt0h, INFO): analytics_finished_24h > 0 = the control text matches the served statements (the old build wrote them) and the hour
--   after the roll simply had no caller; 0 = UNSPLIT (no caller in 24 h, a lost window head, or a text change cannot be split): report to
--   acp, no ruling on the text.
-- Form: lane-scratch/pm-guard/FORM-TEMPLATE-3.md 6b9e73a3c45a (CH shape, one statement per call, self-gating).
-- Table: system.query_log (system log engine; partition by month of event_date; sorting key event_date, event_time: the -4 stmt1 read in CI).
-- WORK MATH: S0 reads system.parts only. stmt0 and stmt0h each read system.query_log TWICE: the window count of the cost gate (event_date /
--   event_time only) and the scan; each bounded by W = that statement's window rows: W <= 9,000,000 (the gate), so 2 x W + 2P + 1 <=
--   18,000,000 + 2P + 1 < 20,000,000 (P = the query_log parts of the parts gate). The parts gate (the query_log parts that hold a day of the
--   window <= 200,000,000 rows / 100,000,000,000 bytes) is the upper bound when the key prunes nothing. FAIL PATH: the gate's scalar
--   subqueries run before throwIf, so a fire can read the window once (<= W + 2P + 1) and stops before the scan. stmtR reads 1 row per
--   replica (system.one), no gate. GRANULE SLACK (P3-6, INFO): reads are whole granules (8192 rows), so a pass can read up to 2 x 8192 x P
--   rows more than 2W + 2P + 1; at P <= 10 that is < 170,000 rows: inside the 20,000,000 budget. No change.
-- Output: counts, max values, the day and INFO times (UTC). No query text, no parameters, no ids, no user names.

-- S0 (system.parts only). WANT >= 1 row (one per active query_log partition with a day in the windows), rows > 0; min(min_day) <= the stmt0h first day.
SELECT partition, sum(rows) AS rows, sum(bytes_on_disk) AS bytes, count() AS active_parts, toString(min(min_date)) AS min_day
FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= toDate('2026-01-01')
GROUP BY partition ORDER BY partition;

-- stmtR (the replicas of the server, 1 row per replica). WANT replicas = 1, else STOP: no stmt0 / stmt0h.
SELECT count() AS replicas FROM clusterAllReplicas('default', system.one);

-- stmt0 served pooled TEST_SUITE_DURATION_P95 statements of query-api from KP to now, and the control. WANT 1 row, analytics_finished > 0, failed_158 = 0.
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= toDate('2026-01-01')) > 200000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= toDate('2026-01-01')) > 100000000000
  OR (SELECT count() FROM system.query_log WHERE event_date >= toDate('2026-01-01') AND event_time >= toDateTime('2026-01-01 00:00:00', 'UTC') AND event_time < now()) > 9000000,
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
WHERE event_date >= toDate('2026-01-01') AND event_time >= toDateTime('2026-01-01 00:00:00', 'UTC') AND event_time < now()
  AND type != 'QueryStart' AND query_kind = 'Select'
  AND ((position(query, concat(' AS dimension', '_value,')) > 0 AND position(query, concat('SETTINGS max_execution', '_time = 30')) > 0)
    OR position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0)
  AND cost_gate = 0;

-- stmt0h the control only over the 24 h before the roll START (old pods). WANT 1 row; analytics_finished_24h > 0 = INFO (the control text matches).
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= toDate('2026-01-01') AND min_date <= toDate('2099-01-01')) > 200000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= toDate('2026-01-01') AND min_date <= toDate('2099-01-01')) > 100000000000
  OR (SELECT count() FROM system.query_log WHERE event_date >= toDate('2026-01-01') AND event_date <= toDate('2099-01-01') AND event_time >= toDateTime('2026-01-01 00:00:01', 'UTC') AND event_time < toDateTime('2099-01-01 00:00:00', 'UTC')) > 9000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT toString(today()) AS day,
  countIf(type = 'QueryFinish' AND position(query, concat(' AS dimension', '_value,')) > 0 AND position(query, concat('SETTINGS max_execution', '_time = 30')) > 0) AS analytics_finished_24h
FROM system.query_log
WHERE event_date >= toDate('2026-01-01') AND event_date <= toDate('2099-01-01') AND event_time >= toDateTime('2026-01-01 00:00:01', 'UTC') AND event_time < toDateTime('2099-01-01 00:00:00', 'UTC')
  AND type != 'QueryStart' AND query_kind = 'Select'
  AND position(query, concat(' AS dimension', '_value,')) > 0 AND position(query, concat('SETTINGS max_execution', '_time = 30')) > 0
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
	if strings.Join(labels, ",") != "S0,stmtR,stmt0,stmt0h" {
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
	for lit, n := range map[string]int{caps[0]: 1, caps[1]: 1, caps[2]: 1, "'STOP-COST-GATE'": 1, "cost_gate = 0": 1, marker: 0,
		"') AS testops_suite_runs'": 0, "FROM system.query_log": 2, control: 2, dimension: 2, "SETTINGS max_execution_time = 30": 0, " AS dimension_value,": 0,
		"toDateTime('2026-01-01 00:00:01', 'UTC')": 2, "toDateTime('2099-01-01 00:00:00', 'UTC')": 2} {
		if got := strings.Count(byLabel["stmt0h"], lit); got != n {
			t.Fatalf("stmt0h: %q found %d times, want %d", lit, got, n)
		}
	}
	if got := strings.Count(byLabel["stmt0"], "toDateTime('2026-01-01 00:00:00', 'UTC')"); got != 2 {
		t.Fatalf("stmt0: the KP literal found %d times, want 2 (the gate count and the scan)", got)
	}
	if r := strings.TrimSpace(byLabel["stmtR"]); strings.Count(r, "\n") != 1 || !strings.HasSuffix(r, "\nSELECT count() AS replicas FROM clusterAllReplicas('default', system.one)") {
		t.Fatalf("stmtR: text %q, want the marker line + the one replica-count line", r)
	}
	if !strings.Contains(byLabel["S0"], "toString(min(min_date)) AS min_day") {
		t.Fatalf("S0: no min_day column")
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
	var minDay string
	if err := e.admin.QueryRow(e.ctx, `SELECT toString(min(min_date)) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= toDate('2026-01-01')`).Scan(&minDay); err != nil {
		t.Fatal(err)
	}
	lowDay := ""
	for _, r := range s0 {
		if lowDay == "" || r["min_day"] < lowDay {
			lowDay = r["min_day"]
		}
	}
	if lowDay != minDay {
		t.Errorf("S0 min(min_day) %q, want %q (a direct min(min_date) read)", lowDay, minDay)
	} else {
		t.Logf("PASS S0 min_day %s (= a direct min(min_date) read)", lowDay)
	}

	// stmtR: count() FROM clusterAllReplicas(<cluster>, system.one) = one row per replica host of <cluster>. The arms come from the CI
	// server's own system.clusters (one row per host): a 1-host cluster = the PASS value 1, a >= 2-host cluster = a STOP value.
	hosts := map[string]uint64{}
	var clusterNames []string
	if rows, err := e.admin.Query(e.ctx, `SELECT cluster, count() FROM system.clusters GROUP BY cluster ORDER BY cluster`); err != nil {
		t.Fatal(err)
	} else {
		for rows.Next() {
			var name string
			var n uint64
			if err := rows.Scan(&name, &n); err != nil {
				t.Fatal(err)
			}
			hosts[name] = n
			clusterNames = append(clusterNames, name)
		}
		rows.Close()
	}
	t.Logf("CLUSTERS on the CI server (hosts per cluster): %v", hosts)
	replicasOf := func(label, text string) (string, error) {
		t.Helper()
		got, types, err := rbpoolRows(e.ctx, e.admin, text)
		if err != nil {
			return "", err
		}
		if len(got) != 1 || strings.Join(types, ",") != "UInt64" {
			t.Errorf("%s: rows %v types %v, want 1 row of UInt64", label, got, types)
			return "", nil
		}
		return got[0]["replicas"], nil
	}
	firstLine := func(err error) string {
		var ex *stdclickhouse.Exception
		if errors.As(err, &ex) {
			return fmt.Sprintf("code %d %s", ex.Code, strings.SplitN(ex.Message, "\n", 2)[0])
		}
		return strings.SplitN(err.Error(), "\n", 2)[0]
	}
	if n, ok := hosts["default"]; ok {
		got, err := replicasOf("stmtR", byLabel["stmtR"])
		if err != nil || got != fmt.Sprint(n) {
			t.Errorf("ARM stmtR clean ('default' has %d hosts): replicas %q err %v, want %d", n, got, err, n)
		} else {
			t.Logf("ARM stmtR clean: cluster 'default' present, replicas %s = its system.clusters hosts %d (WANT 1: %v)", got, n, got == "1")
		}
	} else {
		_, err := replicasOf("stmtR", byLabel["stmtR"])
		var ex *stdclickhouse.Exception
		if err == nil || !errors.As(err, &ex) {
			t.Errorf("ARM stmtR clean (no 'default' cluster on the CI server): err %v, want a server exception (= STOP)", err)
		} else {
			t.Logf("ARM stmtR clean: no 'default' cluster on the CI server: %s (= STOP)", firstLine(err))
		}
	}
	proved := 0
	if _, ok := hosts["default"]; ok {
		proved++
	}
	for _, name := range clusterNames {
		if name == "default" {
			continue
		}
		got, err := replicasOf("stmtR cluster "+name, strings.Replace(byLabel["stmtR"], "'default'", "'"+name+"'", 1))
		if err != nil {
			t.Logf("ARM stmtR cluster %s (%d hosts): NOT PROVEN, %s", name, hosts[name], firstLine(err))
			continue
		}
		if got != fmt.Sprint(hosts[name]) {
			t.Errorf("ARM stmtR cluster %s: replicas %s, want %d (its system.clusters hosts)", name, got, hosts[name])
			continue
		}
		proved++
		t.Logf("ARM stmtR cluster %s: replicas %s = its system.clusters hosts %d (WANT 1: %v)", name, got, hosts[name], got == "1")
	}
	if len(hosts) > 0 && proved == 0 {
		t.Errorf("stmtR: no CI cluster gave replicas = its system.clusters hosts")
	}
	if len(hosts) == 0 {
		t.Logf("stmtR: NOT PROVEN on a cluster: the CI server has no cluster in system.clusters")
	}
	if _, err := replicasOf("MUTANT stmtR rule: an unknown cluster", strings.Replace(byLabel["stmtR"], "'default'", "'rbpool_no_such_cluster'", 1)); err == nil {
		t.Errorf("MUTANT stmtR unknown cluster: no error, want a server exception (= STOP)")
	} else {
		t.Logf("MUTANT stmtR unknown cluster: %s (= STOP)", firstLine(err))
	}
	passComment := "rbpool-pass-" + uuid.NewString()[:8]
	clean := one("PASS stmt0 (server-wide)", byLabel["stmt0"], passComment)
	// stmt0h: one row of (day String, analytics_finished_24h UInt64).
	oneH := func(label, text string, comment ...string) map[string]string {
		t.Helper()
		ctx := e.ctx
		if len(comment) > 0 {
			ctx = stdclickhouse.Context(e.ctx, stdclickhouse.WithSettings(stdclickhouse.Settings{"log_comment": comment[0]}))
		}
		got, types, err := rbpoolRows(ctx, e.admin, text)
		if err != nil || len(got) != 1 {
			t.Fatalf("%s: err %v rows %d", label, err, len(got))
		}
		if strings.Join(types, ",") != "String,UInt64" {
			t.Errorf("%s: column types %v", label, types)
		}
		t.Logf("%s ROW %v", label, got[0])
		return got[0]
	}
	passCommentH := "rbpool-passh-" + uuid.NewString()[:8]
	cleanH := oneH("PASS stmt0h (synthetic window holds the CI statements)", byLabel["stmt0h"], passCommentH)
	nowH := byLabel["stmt0h"]
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
	lateKP := strings.ReplaceAll(byLabel["stmt0"], "toDateTime('2026-01-01 00:00:00', 'UTC')", "toDateTime('2098-01-01 00:00:00', 'UTC')")
	for _, name := range []string{"stored", "legacy", "pipeline"} {
		check("WINDOW stmt0 KP after the CI rows "+name, one("WINDOW stmt0 KP after the CI rows "+name, only(lateKP, name)), want{})
	}
	check("EXACT workers", one("EXACT workers", only(byLabel["stmt0"], "workers")), want{})
	check("EXACT sync", one("EXACT sync", only(byLabel["stmt0"], "sync")), want{})
	if w := recompute("sync"); w != (want{}) {
		t.Errorf("the sync shape recompute = %+v, want zero", w)
	}
	t.Logf("WANT stored %+v legacy %+v (Go recompute)", stored, legacy)
	// stmt0h: the control count per query id on the now-window copy = the Go recompute; the clean fixed past window holds none of them.
	for _, name := range []string{"stored", "pipeline", "legacy", "workers", "sync"} {
		w := recompute(name)
		if got := oneH("EXACT stmt0h now-window "+name, only(nowH, name)); got["analytics_finished_24h"] != fmt.Sprint(w.analytics) {
			t.Errorf("EXACT stmt0h %s: analytics_finished_24h %s, want %d (Go recompute)", name, got["analytics_finished_24h"], w.analytics)
		}
		pastH := strings.ReplaceAll(byLabel["stmt0h"], "toDateTime('2026-01-01 00:00:01', 'UTC')", "toDateTime('2098-01-01 00:00:00', 'UTC')")
		if got := oneH("WINDOW stmt0h window start after the CI statements "+name, only(pastH, name)); got["analytics_finished_24h"] != "0" {
			t.Errorf("WINDOW stmt0h %s: a window that starts after this test's rows counts %s, want 0", name, got["analytics_finished_24h"])
		}
	}
	if n, _ := fmt.Sscan(cleanH["analytics_finished_24h"], new(int)); n != 1 || cleanH["analytics_finished_24h"] == "0" {
		t.Errorf("PASS stmt0h server-wide analytics_finished_24h %s, want > 0 (this test's own control rows)", cleanH["analytics_finished_24h"])
	}
	if got := oneH("MUTANT stmt0h rule: another control settings text", only(strings.ReplaceAll(nowH, control, "concat('SETTINGS max_execution', '_time = 31')"), "stored")); got["analytics_finished_24h"] != "0" {
		t.Errorf("MUTANT stmt0h control text: %s, want 0", got["analytics_finished_24h"])
	}
	if got := oneH("MUTANT stmt0h rule: the control without the dimension text (sync)", only(strings.ReplaceAll(nowH, dimension, ""), "sync")); got["analytics_finished_24h"] == "0" {
		t.Errorf("MUTANT stmt0h no dimension term: the sync statement is still not counted")
	}

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
	for _, fc := range []struct{ label, c string }{{"stmt0", caps[0]}, {"stmt0", caps[1]}, {"stmt0", caps[2]}, {"stmt0h", caps[0]}, {"stmt0h", caps[1]}, {"stmt0h", caps[2]}} {
		c := fc.c
		comment := "rbpool-gate-" + fc.label + "-" + uuid.NewString()[:8]
		fctx := stdclickhouse.Context(e.ctx, stdclickhouse.WithSettings(stdclickhouse.Settings{"log_comment": comment}))
		_, _, err := rbpoolRows(fctx, e.admin, strings.Replace(byLabel[fc.label], c, "> 0"+c[len(c)-1:], 1))
		var ex *stdclickhouse.Exception
		if err == nil || !errors.As(err, &ex) || ex.Code != 395 || !strings.Contains(ex.Message, "STOP-COST-GATE") {
			t.Errorf("GATE %q lowered to 0: err = %v, want code 395 STOP-COST-GATE", c, err)
			continue
		}
		t.Logf("GATE %q lowered to 0: fired code=%d", c, ex.Code)
		fires++
		fireComments = append(fireComments, comment)
	}
	if fires != 6 {
		t.Errorf("gates fired %d times, want 6 (2 statements x 3 caps)", fires)
	}
	// PASS-path and gate read_rows from query_log. The bound: the rows and parts of the query_log parts of the parts gate (Q, P):
	// a pass reads the window twice (<= 2 Q) + the parts gate (2 P) + 1; a gate fire reads at most the window count once (<= Q) + 2 P + 1.
	e.exec("SYSTEM FLUSH LOGS")
	var q, p uint64
	if err := e.admin.QueryRow(e.ctx, `SELECT sum(rows), count() FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= toDate('2026-01-01')`).Scan(&q, &p); err != nil {
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
	if types, read := logOf(passCommentH); len(types) != 1 || types[0] != "QueryFinish" || read > 2*q+2*p+1 {
		t.Errorf("PASS stmt0h query_log types %v read_rows %d, want [QueryFinish] and <= %d (2Q + 2P + 1)", types, read, 2*q+2*p+1)
	} else {
		t.Logf("PASS stmt0h query_log types %v read_rows %d (2Q + 2P + 1 = %d)", types, read, 2*q+2*p+1)
	}
	for _, c := range fireComments {
		if types, read := logOf(c); len(types) != 1 || types[0] != "ExceptionBeforeStart" || read > q+2*p+1 {
			t.Errorf("GATE %s query_log types %v read_rows %d, want [ExceptionBeforeStart] and <= Q + 2P + 1 = %d", c, types, read, q+2*p+1)
		} else {
			t.Logf("GATE %s query_log types %v read_rows %d (Q + 2P + 1 = %d)", c, types, read, q+2*p+1)
		}
	}
}
