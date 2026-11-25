//go:build integration

package analytics

// DO NOT MERGE. Throwaway exec proof of a prod read form (FORM-45-QL-1: system.query_log, the served pooled suite statements).
// ql45Form is the form file in its token form (no org prefix, no prod literal); its sha256 and the sha256 of each awk cut are
// pinned. The window tokens are filled at test time by pm's sed rule, with literals from the ClickHouse server clock, so the
// windows hold the rows this test makes. Synthetic rows only.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/chclient"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopspooled"
)

const ql45FormSHA256 = "e414f803c74950d83dcb879e2f96872b56c86f541cf3718603e2ff138bfddb0f"

// ql45CutSHA256 is the sha256 of: /usr/bin/awk '/^-- <label> /{f=1;next} f{print} f&&/;$/{exit}' FORM-45-QL-1.sql (pm's cut).
var ql45CutSHA256 = map[string]string{
	"S0":     "8b7a7a7ed68cd3b487f0543449d47ad1487cb79e0a6fa42fe341c7a095560ed8",
	"stmtR":  "268721d0140090cea906a8ef46d53fba2590bc3ffa9a34459615c99afdcdc08e",
	"stmt0h": "0987a8acaa2d747aa445369ff64402e1b57db43b62bac4c90209926f32f4e228",
	"stmt0":  "0acedc3d30a14a320d6a13626be7db60ad76e55cffbd0b2d3e9a6d7e00d02d24",
}

const ql45Form = `-- FORM-45-QL-1 (ClickHouse, prod, read-only). acp-train45, 2026-11-25. DRAFT, NOT EXECUTED, NOT READ BY ACP. query-api roll sq45-a READBACK R-POOLQL45 for CHAOS-14595.
--   Form = lane-scratch/acp-train39/q39/FORM-3NN-RBPOOL-6.sql (stmtR replica gate, S0 min_day, fixed literal windows, concat() markers so this text cannot match itself).
-- OPEN FILL TOKENS (filled at FINAL; every window edge is a FIXED literal, never now() as a start): <D_H0> = the day of the history start; <T_H0> = START minus 7 days (UTC, second);
--   <T_P10> = the P10 run time (UTC, a fixed literal written by pm at the run, within 60 min before START); <D_KP> = the day of KP; <T_KP> = KP (STEP-KIT-PASS AND 0 old pods); <D_NOW> = the day of the read. Run stmt0h BEFORE START (pre-START history arm).
-- WHAT it selects (markers, cites at cd2fbc1f543b):
--   CONTROL = query-api analytics timeseries / breakdown statements: ' AS dimension' + '_value,' AND 'SETTINGS max_execution' + '_time = 30' (as RBPOOL-6).
--   POOLED SUITE = ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''' AND ') AS testops_suite_runs' (as RBPOOL-6; present in the old and the new build).
--   NEW TEXT = POOLED SUITE AND 'HAVING tupleElement(max((computed_at, 1 - ret' + 'racted)), 2) = 0' (LiveLegacyRepoDays, testopspooled.go:62-70; only the fixed build writes it).
-- PRE-START ARM (stmt0h, 7 days before START, fixed literal): pooled_finished_7d = the old build's served pooled suite statements; control_7d = analytics statements.
--   WANT (decided BEFORE START): pooled_finished_7d = 0 -> the post arm for CHAOS-14595 is NOT EXERCISED by traffic (no caller in 7 days); the post read still runs, it can only fail on > 0 refusals.
--   control_7d = 0 -> UNSPLIT (the control text cannot be shown to match any served statement); report to acp, no ruling on text.
-- POST ARM (stmt0, KP .. KP + 65 min, fixed literal): WANT failed_158 = 0; new_text_finished > 0 = EXERCISED (max_read_rows < 5,000,000); new_text_finished = 0 with control > 0 and pooled_finished_7d = 0 = NOT EXERCISED.
--   Quiet prod: no created traffic (D6310), no soak (D6723).
-- Org rule N/A (system.query_log has no org column; output = counts / max / days only). No SET / SETTINGS / EXPLAIN.
-- WORK MATH: each of stmt0 / stmt0h reads system.query_log twice (the gate count + the scan), each <= 9,000,000 rows: 2 x 9,000,000 + 2P + 1 < 20,000,000.

-- S0 (system.parts only). WANT >= 1 row; min(min_day) <= <D_H0>, else stmt0h = NOT EXERCISED (retention).
SELECT partition, sum(rows) AS rows, sum(bytes_on_disk) AS bytes, count() AS active_parts, toString(min(min_date)) AS min_day
FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= toDate('<D_H0>')
GROUP BY partition ORDER BY partition;

-- stmtR replicas (1 row per replica). WANT replicas = 1, else STOP: no stmt0h / stmt0.
SELECT count() AS replicas FROM clusterAllReplicas('default', system.one);

-- stmt0h pre-START history arm. WANT 1 row; INFO: control_7d, pooled_finished_7d, pooled_failed_7d, max_read_rows_7d.
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= toDate('<D_H0>')) > 200000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= toDate('<D_H0>')) > 100000000000
  OR (SELECT count() FROM system.query_log WHERE event_date >= toDate('<D_H0>') AND event_time >= toDateTime('<T_H0>', 'UTC') AND event_time < toDateTime('<T_P10>', 'UTC')) > 9000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT toString(today()) AS day,
  countIf(type = 'QueryFinish' AND position(query, concat(' AS dimension', '_value,')) > 0 AND position(query, concat('SETTINGS max_execution', '_time = 30')) > 0) AS control_7d,
  countIf(type = 'QueryFinish' AND position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0 AND position(query, ') AS testops_suite_runs') > 0) AS pooled_finished_7d,
  countIf(type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing') AND position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0 AND position(query, ') AS testops_suite_runs') > 0) AS pooled_failed_7d,
  maxIf(read_rows, type = 'QueryFinish' AND position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0 AND position(query, ') AS testops_suite_runs') > 0) AS max_read_rows_7d
FROM system.query_log
WHERE event_date >= toDate('<D_H0>') AND event_time >= toDateTime('<T_H0>', 'UTC') AND event_time < toDateTime('<T_P10>', 'UTC')
  AND type != 'QueryStart' AND query_kind = 'Select'
  AND ((position(query, concat(' AS dimension', '_value,')) > 0 AND position(query, concat('SETTINGS max_execution', '_time = 30')) > 0)
    OR position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0)
  AND cost_gate = 0;

-- stmt0 post arm, KP .. <T_NOW>. WANT 1 row; analytics_finished > 0 = control; failed_158 = 0; new_text_finished > 0 = EXERCISED, max_read_rows < 5,000,000.
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= toDate('<D_KP>')) > 200000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= toDate('<D_KP>')) > 100000000000
  OR (SELECT count() FROM system.query_log WHERE event_date >= toDate('<D_KP>') AND event_time >= toDateTime('<T_KP>', 'UTC') AND event_time < toDateTime('<T_NOW>', 'UTC')) > 9000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT toString(now(), 'UTC') AS run_at,
  countIf(type = 'QueryFinish' AND position(query, concat(' AS dimension', '_value,')) > 0 AND position(query, concat('SETTINGS max_execution', '_time = 30')) > 0) AS analytics_finished,
  countIf(type = 'QueryFinish' AND position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0 AND position(query, ') AS testops_suite_runs') > 0) AS finished,
  countIf(type = 'QueryFinish' AND position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0 AND position(query, ') AS testops_suite_runs') > 0 AND position(query, concat('HAVING tupleElement(max((computed_at, 1 - ret', 'racted)), 2) = 0')) > 0) AS new_text_finished,
  maxIf(read_rows, type = 'QueryFinish' AND position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0 AND position(query, ') AS testops_suite_runs') > 0) AS max_read_rows,
  countIf(type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing') AND position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0 AND position(query, ') AS testops_suite_runs') > 0) AS failed,
  countIf(type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing') AND exception_code = 158 AND position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0 AND position(query, ') AS testops_suite_runs') > 0) AS failed_158
FROM system.query_log
WHERE event_date >= toDate('<D_KP>') AND event_time >= toDateTime('<T_KP>', 'UTC') AND event_time < toDateTime('<T_NOW>', 'UTC')
  AND type != 'QueryStart' AND query_kind = 'Select'
  AND ((position(query, concat(' AS dimension', '_value,')) > 0 AND position(query, concat('SETTINGS max_execution', '_time = 30')) > 0)
    OR position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0)
  AND cost_gate = 0;
`

// ql45Cut is pm's awk cut: the lines after the first line that starts with "-- <label> ", up to and including the first
// line that ends in ";", each line with its "\n".
func ql45Cut(body, label string) string {
	lines := strings.Split(body, "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	var b strings.Builder
	on := false
	for _, line := range lines {
		if !on {
			on = strings.HasPrefix(line, "-- "+label+" ")
			continue
		}
		b.WriteString(line + "\n")
		if strings.HasSuffix(line, ";") {
			break
		}
	}
	return b.String()
}

// ql45TS is the fill format of a window token: UTC 'YYYY-MM-DD HH:MM:SS'.
func ql45TS(at time.Time) string { return at.UTC().Format("2006-01-02 15:04:05") }

func ql45Rows(ctx context.Context, conn stdclickhouse.Conn, text string) ([]map[string]string, []string, []string, error) {
	rows, err := conn.Query(ctx, text)
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()
	types := rows.ColumnTypes()
	var names, typeNames []string
	for _, ct := range types {
		names = append(names, ct.Name())
		typeNames = append(typeNames, ct.DatabaseTypeName())
	}
	var got []map[string]string
	for rows.Next() {
		cells := make([]any, len(types))
		for i, ct := range types {
			cells[i] = reflect.New(ct.ScanType()).Interface()
		}
		if err := rows.Scan(cells...); err != nil {
			return nil, nil, nil, err
		}
		row := map[string]string{}
		for i, name := range names {
			row[name] = fmt.Sprint(reflect.ValueOf(cells[i]).Elem().Interface())
		}
		got = append(got, row)
	}
	return got, names, typeNames, rows.Err()
}

// ql45Want is the Go recompute of one query id: the form's column rules applied in Go to every query_log row of the id.
type ql45Want struct {
	analytics, finished, newText, failed, failed158 int
	maxRead                                         uint64
}

func ql45FirstLine(err error) string {
	var ex *stdclickhouse.Exception
	if errors.As(err, &ex) {
		return fmt.Sprintf("code %d %s", ex.Code, strings.SplitN(ex.Message, "\n", 2)[0])
	}
	return strings.SplitN(err.Error(), "\n", 2)[0]
}

func TestForm45QLExecutesOnTheServedStatements(t *testing.T) {
	// The texts the served path writes (the form builds the same texts with concat() or '' so its own text cannot match).
	const (
		dimMarker    = " AS dimension_value,"
		ctlMarker    = "SETTINGS max_execution_time = 30"
		pooledMarker = ", max_rows_to_read = 5000000, read_overflow_mode = 'throw'"
		aliasMarker  = ") AS testops_suite_runs"
		newMarker    = "HAVING tupleElement(max((computed_at, 1 - retracted)), 2) = 0"
	)
	labels := []string{"S0", "stmtR", "stmt0h", "stmt0"}
	cuts := map[string]string{}
	for _, l := range labels {
		cuts[l] = ql45Cut(ql45Form, l)
	}
	cell := func(name string, fn func(t *testing.T)) bool {
		return t.Run(name, func(t *testing.T) {
			fn(t)
			if !t.Failed() {
				t.Logf("CELL45QL %s PASS", name)
			}
		})
	}
	tokenRE := regexp.MustCompile(`<[A-Z_0-9]*>`)

	cell("pin", func(t *testing.T) {
		sum := sha256.Sum256([]byte(ql45Form))
		if got := hex.EncodeToString(sum[:]); got != ql45FormSHA256 {
			t.Fatalf("form sha256 = %s, want %s", got, ql45FormSHA256)
		}
		for _, l := range labels {
			sum := sha256.Sum256([]byte(cuts[l]))
			if got := hex.EncodeToString(sum[:]); got != ql45CutSHA256[l] {
				t.Errorf("cut %s sha256 = %s, want %s (the awk cut)", l, got, ql45CutSHA256[l])
			} else {
				t.Logf("CUT %s sha12 %s", l, got[:12])
			}
		}
	})

	cell("cuts", func(t *testing.T) {
		literalRE := regexp.MustCompile(`'(?:[^']|'')*'`)
		wordRE := regexp.MustCompile(`(?i)\b(SET|SETTINGS|EXPLAIN)\b`)
		wantTokens := map[string]map[string]int{
			"S0":     {"<D_H0>": 1},
			"stmtR":  {},
			"stmt0h": {"<D_H0>": 4, "<T_H0>": 2, "<T_P10>": 2},
			"stmt0":  {"<D_KP>": 4, "<T_KP>": 2, "<T_NOW>": 2},
		}
		for _, l := range labels {
			c := cuts[l]
			if c == "" {
				t.Fatalf("%s: empty cut", l)
			}
			lines := strings.Split(strings.TrimSuffix(c, "\n"), "\n")
			dash, semi := 0, 0
			for i, line := range lines {
				if strings.HasPrefix(strings.TrimSpace(line), "--") {
					dash++
				}
				if strings.HasSuffix(line, ";") {
					semi++
					if i != len(lines)-1 {
						t.Errorf("%s: a ';' line before the last line (%d)", l, i+1)
					}
				}
			}
			if dash != 0 || semi != 1 || strings.Contains(c, "--") {
				t.Errorf("%s: %d '--' lines, %d ';' lines, want 0 and 1", l, dash, semi)
			}
			if words := wordRE.FindAllString(literalRE.ReplaceAllString(c, "''"), -1); len(words) > 0 {
				t.Errorf("%s: SET / SETTINGS / EXPLAIN words outside string literals: %v", l, words)
			}
			got := map[string]int{}
			for _, m := range tokenRE.FindAllString(c, -1) {
				got[m]++
			}
			if !reflect.DeepEqual(got, wantTokens[l]) {
				t.Errorf("%s: tokens %v, want %v", l, got, wantTokens[l])
			}
			t.Logf("CUT %s: %d lines, tokens %v, SET/SETTINGS/EXPLAIN only inside literals: %d", l, len(lines), got, len(wordRE.FindAllString(c, -1)))
		}
	})

	live := testopspooled.LiveLegacyRepoDays("testops_test_metrics_daily")
	// oldSource is the suite source of the build before LiveLegacyRepoDays (LegacyRepoDays at both sites): the old marker text only.
	oldSource := strings.ReplaceAll(testopspooled.SuiteRunsSource, live, testopspooled.LegacyRepoDays("testops_test_metrics_daily"))
	pooledSettings := statementSettings(pooledSuiteRunsSource, queryTimeoutSecs)
	cell("markers", func(t *testing.T) {
		if n := strings.Count(testopspooled.SuiteRunsSource, live); n != 2 {
			t.Errorf("SuiteRunsSource holds LiveLegacyRepoDays %d times, want 2", n)
		}
		if n := strings.Count(testopspooled.SuiteRunsSource, newMarker); n != 2 {
			t.Errorf("SuiteRunsSource holds the new marker %d times, want 2", n)
		}
		if strings.Contains(oldSource, newMarker) || strings.Contains(testopspooled.PipelineRunsSource, newMarker) {
			t.Errorf("the old suite source or the pipeline source holds the new marker")
		}
		if pooledSettings != ctlMarker+pooledMarker {
			t.Errorf("served pooled settings %q, want %q", pooledSettings, ctlMarker+pooledMarker)
		}
		if !strings.HasSuffix(testopspooled.SuiteRunsSource, aliasMarker) || !strings.HasSuffix(oldSource, aliasMarker) {
			t.Errorf("a suite source does not end in %q", aliasMarker)
		}
		if TestopsPooledMaxRowsToRead != 5_000_000 {
			t.Errorf("TestopsPooledMaxRowsToRead = %d, the form marker names 5000000", TestopsPooledMaxRowsToRead)
		}
		for text, n := range map[string]int{
			"concat('HAVING tupleElement(max((computed_at, 1 - ret', 'racted)), 2) = 0')": 1,
			"', max_rows_to_read = 5000000, read_overflow_mode = ''throw'''":              6,
			"') AS testops_suite_runs'":                      5,
			"concat(' AS dimension', '_value,')":             2,
			"concat('SETTINGS max_execution', '_time = 30')": 2,
			newMarker:    0,
			pooledMarker: 0,
			dimMarker:    0,
			ctlMarker:    0,
		} {
			if got := strings.Count(cuts["stmt0"], text); got != n {
				t.Errorf("stmt0: %q found %d times, want %d", text, got, n)
			}
		}
		if "HAVING tupleElement(max((computed_at, 1 - ret"+"racted)), 2) = 0" != newMarker || " AS dimension"+"_value," != dimMarker ||
			"SETTINGS max_execution"+"_time = 30" != ctlMarker {
			t.Errorf("a concat() marker of the form is not the served text")
		}
	})
	if t.Failed() {
		t.FailNow()
	}

	e := startOwnersEnv(t)
	var version string
	if err := e.admin.QueryRow(e.ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("clickhouse %s", version)
	serverNow := func(t *testing.T) time.Time {
		t.Helper()
		var s string
		if err := e.admin.QueryRow(e.ctx, "SELECT toString(now(), 'UTC')").Scan(&s); err != nil {
			t.Fatal(err)
		}
		at, err := time.ParseInLocation("2006-01-02 15:04:05", s, time.UTC)
		if err != nil {
			t.Fatal(err)
		}
		return at
	}
	testStart := serverNow(t)

	ids := map[string]string{}
	withID := func(name string) context.Context {
		ids[name] = "ql45-" + name + "-" + uuid.NewString()[:8]
		return stdclickhouse.Context(e.ctx, stdclickhouse.WithQueryID(ids[name]))
	}
	recompute := func(t *testing.T, name string) ql45Want {
		t.Helper()
		rows, err := e.admin.Query(e.ctx, `SELECT toString(type), toString(query_kind), query, read_rows, exception_code FROM system.query_log
			WHERE query_id = ? AND type != 'QueryStart'`, ids[name])
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var w ql45Want
		for rows.Next() {
			var typ, kind, query string
			var read uint64
			var code int32
			if err := rows.Scan(&typ, &kind, &query, &read, &code); err != nil {
				t.Fatal(err)
			}
			if kind != "Select" {
				continue
			}
			if typ == "QueryFinish" && strings.Contains(query, dimMarker) && strings.Contains(query, ctlMarker) {
				w.analytics++
			}
			if !strings.Contains(query, pooledMarker) || !strings.Contains(query, aliasMarker) {
				continue
			}
			switch typ {
			case "QueryFinish":
				w.finished++
				if strings.Contains(query, newMarker) {
					w.newText++
				}
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
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return w
	}
	wants := map[string]ql45Want{}
	exactNames := []string{"stored", "old", "pipeline", "sync", "legacy", "workers"}

	const org = "f0f0f0f0-0000-4000-8000-0000000045a1"
	const small = "b0b0b0b0-0000-4000-8000-0000000045a1"
	const big = "b0b0b0b0-0000-4000-8000-0000000045a2"
	if !cell("served", func(t *testing.T) {
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
		call := func(name string, measure model.MeasureInput) error {
			t.Helper()
			_, err := resolveOneTimeseries(withID(name), prod, org, tsInput(model.DimensionInputTeam, measure), false, nil, true)
			t.Logf("SERVED %s %s: err %v", name, measure, err)
			return err
		}
		params := []any{stdclickhouse.Named("org_id", org), stdclickhouse.Named("start_date", "2026-01-01"), stdclickhouse.Named("end_date", "2026-01-07")}
		// 1. stored days only: the served pooled suite statements finish and hold the new marker.
		if err := call("stored", model.MeasureInputTestSuiteDurationP95); err != nil {
			t.Fatalf("stored phase: %v", err)
		}
		// 2. a pooled suite statement with the old marker text only (the source before LiveLegacyRepoDays, the served settings).
		if err := e.admin.QueryRow(withID("old"), "SELECT count() FROM "+oldSource+" "+pooledSettings, params...).Scan(new(uint64)); err != nil {
			t.Fatalf("old-text statement: %v", err)
		}
		// 3. the pooled PIPELINE source (same settings clause, other alias): never a pooled suite row.
		_ = call("pipeline", model.MeasureInputPipelineDurationP95)
		// 4. the sync worker shape: the settings text, no analytics dimension, no pooled marker: never counted.
		if err := e.admin.QueryRow(withID("sync"), "SELECT count() FROM test_suite_results WHERE org_id = {org_id:String} SETTINGS max_execution_time = 30, max_rows_to_read = 50000000",
			stdclickhouse.Named("org_id", org)).Scan(new(uint64)); err != nil {
			t.Fatalf("sync-shape statement: %v", err)
		}
		// 5. a legacy day of the big repository: its raw read is the 5.2M history rows: refused, code 158.
		e.exec(`INSERT INTO testops_test_metrics_daily (repo_id, day, org_id, computed_at) VALUES (?, ?, ?, now() + INTERVAL 1 HOUR)`, big, day1, org)
		var refused *PooledReadBoundError
		if err := call("legacy", model.MeasureInputTestSuiteDurationP95); !errors.As(err, &refused) || refused.Code != 158 {
			t.Fatalf("legacy phase: err %v, want a PooledReadBoundError code 158", err)
		}
		// 6. the workers report form ("SETTINGS max_rows_to_read", no leading comma): never counted, refused or not.
		werr := e.admin.QueryRow(withID("workers"), "SELECT count() FROM "+testopspooled.SuiteRunsSource+
			fmt.Sprintf(" SETTINGS max_rows_to_read = %d, read_overflow_mode = 'throw'", testopspooled.MaxRowsToRead), params...).Scan(new(uint64))
		t.Logf("WORKERS-FORM statement: err %v", werr)
		e.exec("SYSTEM FLUSH LOGS")
		for _, name := range exactNames {
			wants[name] = recompute(t, name)
			t.Logf("WANT %s (query id %s) %+v (Go recompute)", name, ids[name], wants[name])
		}
		s, o, l, p := wants["stored"], wants["old"], wants["legacy"], wants["pipeline"]
		if s.finished < 1 || s.newText != s.finished || s.analytics < 1 || s.failed != 0 || s.maxRead >= 5_000_000 {
			t.Errorf("stored %+v: want finished >= 1, newText = finished, analytics >= 1, failed 0, maxRead < 5000000", s)
		}
		if o != (ql45Want{finished: 1, maxRead: o.maxRead}) {
			t.Errorf("old %+v: want finished 1, newText 0, analytics 0, failed 0", o)
		}
		if l.failed158 < 1 || l.finished != 0 {
			t.Errorf("legacy %+v: want failed158 >= 1, finished 0", l)
		}
		if p.analytics < 1 || p.finished != 0 || p.failed != 0 {
			t.Errorf("pipeline %+v: want analytics >= 1, no pooled suite row", p)
		}
		if wants["sync"] != (ql45Want{}) || wants["workers"] != (ql45Want{}) {
			t.Errorf("sync %+v workers %+v: want zero", wants["sync"], wants["workers"])
		}
	}) {
		t.FailNow()
	}

	// Windows from the server clock: stmt0h (and S0) as pm fills them at P10 = now (P10 = the end of the setup + 1 minute, so the
	// setup rows are inside [T_H0, T_P10)); stmt0 with KP = the test start (before the first statement), T_NOW = KP + 65 minutes.
	tEnd := serverNow(t)
	tP10, tKP := tEnd.Add(time.Minute), testStart
	fillH := func(text string, p10 time.Time) string {
		h0 := ql45TS(p10.Add(-7 * 24 * time.Hour))
		out := strings.ReplaceAll(text, "<T_P10>", ql45TS(p10))
		out = strings.ReplaceAll(out, "<T_H0>", h0)
		return strings.ReplaceAll(out, "<D_H0>", strings.SplitN(h0, " ", 2)[0])
	}
	fillK := func(text string, kp time.Time) string {
		k := ql45TS(kp)
		out := strings.ReplaceAll(text, "<T_KP>", k)
		out = strings.ReplaceAll(out, "<T_NOW>", ql45TS(kp.Add(65*time.Minute)))
		return strings.ReplaceAll(out, "<D_KP>", strings.SplitN(k, " ", 2)[0])
	}
	dH0 := strings.SplitN(ql45TS(tP10.Add(-7*24*time.Hour)), " ", 2)[0]
	dKP := strings.SplitN(ql45TS(tKP), " ", 2)[0]
	sent := map[string]string{"S0": fillH(cuts["S0"], tP10), "stmtR": cuts["stmtR"], "stmt0h": fillH(cuts["stmt0h"], tP10), "stmt0": fillK(cuts["stmt0"], tKP)}
	for _, l := range labels {
		if tokenRE.MatchString(sent[l]) {
			t.Fatalf("%s: a token is left after the fill", l)
		}
	}
	t.Logf("WINDOWS stmt0h [%s, %s) D_H0 %s; stmt0 [%s, %s) D_KP %s", ql45TS(tP10.Add(-7*24*time.Hour)), ql45TS(tP10), dH0, ql45TS(tKP), ql45TS(tKP.Add(65*time.Minute)), dKP)
	// The driver gets the cut without its final ";" (as the model leg); stmtR also runs once byte for byte, as INFO.
	sqlOf := func(s string) string { return strings.TrimSuffix(strings.TrimSuffix(s, "\n"), ";") }
	run := func(name, text string) ([]map[string]string, []string, []string, error) {
		ctx := e.ctx
		if name != "" {
			ctx = withID(name)
		}
		return ql45Rows(ctx, e.admin, sqlOf(text))
	}
	one := func(t *testing.T, label, name, text, cols, types string) map[string]string {
		t.Helper()
		got, names, typeNames, err := run(name, text)
		if err != nil || len(got) != 1 {
			t.Fatalf("%s: err %v rows %d", label, err, len(got))
		}
		if strings.Join(names, ",") != cols || strings.Join(typeNames, ",") != types {
			t.Errorf("%s: columns %v types %v, want %s / %s", label, names, typeNames, cols, types)
		}
		t.Logf("%s ROW %v", label, got[0])
		return got[0]
	}
	const cols0 = "run_at,analytics_finished,finished,new_text_finished,max_read_rows,failed,failed_158"
	const cols0h = "day,control_7d,pooled_finished_7d,pooled_failed_7d,max_read_rows_7d"
	one0 := func(t *testing.T, label, name, text string) map[string]string {
		t.Helper()
		return one(t, label, name, text, cols0, "String,UInt64,UInt64,UInt64,UInt64,UInt64,UInt64")
	}
	one0h := func(t *testing.T, label, name, text string) map[string]string {
		t.Helper()
		return one(t, label, name, text, cols0h, "String,UInt64,UInt64,UInt64,UInt64")
	}
	// only is the EXACT test copy: one more conjunct, query_id IN (the ids this test sent), as other statements share the server.
	only := func(t *testing.T, text string, names ...string) string {
		t.Helper()
		var quoted []string
		for _, n := range names {
			if ids[n] == "" {
				t.Fatalf("no query id for %s", n)
			}
			quoted = append(quoted, "'"+ids[n]+"'")
		}
		if strings.Count(text, "AND cost_gate = 0") != 1 {
			t.Fatal("no single cost_gate conjunct to extend")
		}
		return strings.Replace(text, "AND cost_gate = 0", "AND cost_gate = 0 AND query_id IN ("+strings.Join(quoted, ", ")+")", 1)
	}
	mutate := func(t *testing.T, text, old, repl string) string {
		t.Helper()
		if n := strings.Count(text, old); n != 1 {
			t.Fatalf("mutant target %q found %d times, want 1", old, n)
		}
		t.Logf("MUTANT %q -> %q (1 hit)", old, repl)
		return strings.Replace(text, old, repl, 1)
	}
	expect := func(t *testing.T, label string, got map[string]string, want map[string]any) {
		t.Helper()
		for k, v := range want {
			if got[k] != fmt.Sprint(v) {
				t.Errorf("%s: %s = %s, want %v", label, k, got[k], v)
			}
		}
	}
	num := func(t *testing.T, s string) uint64 {
		t.Helper()
		var n uint64
		if _, err := fmt.Sscan(s, &n); err != nil {
			t.Fatalf("not a number: %q", s)
		}
		return n
	}
	check0 := func(t *testing.T, label string, got map[string]string, w ql45Want) {
		t.Helper()
		want := map[string]any{"analytics_finished": w.analytics, "finished": w.finished, "new_text_finished": w.newText, "failed": w.failed, "failed_158": w.failed158}
		if w.finished > 0 {
			want["max_read_rows"] = w.maxRead
		}
		expect(t, label, got, want)
	}
	check0h := func(t *testing.T, label string, got map[string]string, w ql45Want) {
		t.Helper()
		want := map[string]any{"control_7d": w.analytics, "pooled_finished_7d": w.finished, "pooled_failed_7d": w.failed}
		if w.finished > 0 {
			want["max_read_rows_7d"] = w.maxRead
		}
		expect(t, label, got, want)
	}
	maxRead := func(names ...string) uint64 {
		var m uint64
		for _, n := range names {
			if wants[n].maxRead > m {
				m = wants[n].maxRead
			}
		}
		return m
	}
	s, o, l, p := wants["stored"], wants["old"], wants["legacy"], wants["pipeline"]

	cell("S0", func(t *testing.T) {
		got, names, typeNames, err := run("form-S0", sent["S0"])
		if err != nil || len(got) < 1 {
			t.Fatalf("S0: err %v rows %d, want >= 1", err, len(got))
		}
		if strings.Join(names, ",") != "partition,rows,bytes,active_parts,min_day" || strings.Join(typeNames, ",") != "String,UInt64,UInt64,UInt64,String" {
			t.Errorf("S0: columns %v types %v", names, typeNames)
		}
		low := ""
		for _, r := range got {
			if num(t, r["rows"]) == 0 {
				t.Errorf("S0: a partition with rows 0: %v", r)
			}
			if low == "" || r["min_day"] < low {
				low = r["min_day"]
			}
		}
		var direct string
		if err := e.admin.QueryRow(e.ctx, `SELECT toString(min(min_date)) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= toDate(?)`, dH0).Scan(&direct); err != nil {
			t.Fatal(err)
		}
		if low != direct {
			t.Errorf("S0 min(min_day) %q, want %q (a direct min(min_date) read)", low, direct)
		}
		first := regexp.MustCompile(`toDate\('([0-9-]+)'\)`).FindStringSubmatch(sent["stmt0h"])
		if first == nil || first[1] != dH0 {
			t.Fatalf("stmt0h first day %v, want %s", first, dH0)
		}
		t.Logf("S0 ROWS %v; min_day %s vs the stmt0h first day %s: stmt0h EXERCISED = %v (a CI server younger than 7 days = the NOT EXERCISED arm)", got, low, first[1], low <= first[1])
		// The PASS arm: S0 filled with a first day = the oldest query_log day gives min_day <= the first day.
		alt, _, _, err := run("", strings.ReplaceAll(cuts["S0"], "<D_H0>", low))
		if err != nil || len(alt) < 1 {
			t.Fatalf("S0 at first day %s: err %v rows %d", low, err, len(alt))
		}
		for _, r := range alt {
			if r["min_day"] > low {
				t.Errorf("S0 at first day %s: min_day %s, want <= %s", low, r["min_day"], low)
			}
		}
	})

	stmtRFinished := false
	cell("stmtR", func(t *testing.T) {
		hosts := map[string]uint64{}
		rows, err := e.admin.Query(e.ctx, `SELECT cluster, count() FROM system.clusters GROUP BY cluster ORDER BY cluster`)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var name string
			var n uint64
			if err := rows.Scan(&name, &n); err != nil {
				t.Fatal(err)
			}
			hosts[name] = n
		}
		rows.Close()
		t.Logf("CLUSTERS on the CI server (hosts per cluster): %v", hosts)
		got, names, typeNames, err := run("form-stmtR", sent["stmtR"])
		if n, ok := hosts["default"]; ok {
			if err != nil || len(got) != 1 || strings.Join(names, ",") != "replicas" || strings.Join(typeNames, ",") != "UInt64" {
				t.Fatalf("stmtR: err %v rows %v columns %v types %v", err, got, names, typeNames)
			}
			if got[0]["replicas"] != fmt.Sprint(n) || got[0]["replicas"] != "1" {
				t.Errorf("stmtR: replicas %s, want %d (system.clusters hosts of 'default') and 1", got[0]["replicas"], n)
			}
			stmtRFinished = true
			t.Logf("ARM stmtR clean: cluster 'default' present, replicas %s (WANT 1)", got[0]["replicas"])
		} else {
			var ex *stdclickhouse.Exception
			if err == nil || !errors.As(err, &ex) {
				t.Errorf("stmtR (no 'default' cluster on the CI server): err %v, want a server exception (= STOP)", err)
			}
			t.Logf("ARM stmtR clean: no 'default' cluster on the CI server: %v (= STOP); replicas = 1 NOT PROVEN", err)
		}
		if _, _, _, err := run("", strings.Replace(sent["stmtR"], "'default'", "'ql45_no_such_cluster'", 1)); err == nil {
			t.Errorf("MUTANT stmtR unknown cluster: no error, want a server exception (= STOP)")
		} else {
			t.Logf("MUTANT stmtR unknown cluster: %s (= STOP)", ql45FirstLine(err))
		}
		// INFO: the cut byte for byte (its final ";" and "\n" kept).
		if r, _, _, err := ql45Rows(e.ctx, e.admin, sent["stmtR"]); err != nil {
			t.Logf("INFO stmtR byte for byte: %s", ql45FirstLine(err))
		} else {
			t.Logf("INFO stmtR byte for byte: rows %v", r)
		}
	})

	cell("stmt0h", func(t *testing.T) {
		got := one0h(t, "REAL stmt0h", "form-stmt0h", sent["stmt0h"])
		for k, lo := range map[string]uint64{
			"control_7d":         uint64(s.analytics + p.analytics),
			"pooled_finished_7d": uint64(s.finished + o.finished),
			"pooled_failed_7d":   uint64(l.failed),
			"max_read_rows_7d":   maxRead("stored", "old"),
		} {
			if num(t, got[k]) < lo {
				t.Errorf("REAL stmt0h %s = %s, want >= %d (this test's own statements)", k, got[k], lo)
			}
		}
		for _, name := range exactNames {
			check0h(t, "EXACT stmt0h "+name, one0h(t, "EXACT stmt0h "+name, "", only(t, sent["stmt0h"], name)), wants[name])
		}
	})

	cell("stmt0", func(t *testing.T) {
		got := one0(t, "REAL stmt0", "form-stmt0", sent["stmt0"])
		for k, lo := range map[string]uint64{
			"analytics_finished": uint64(s.analytics + p.analytics),
			"finished":           uint64(s.finished + o.finished),
			"new_text_finished":  uint64(s.newText),
			"failed":             uint64(l.failed),
			"failed_158":         uint64(l.failed158),
			"max_read_rows":      maxRead("stored", "old"),
		} {
			if num(t, got[k]) < lo {
				t.Errorf("REAL stmt0 %s = %s, want >= %d (this test's own statements)", k, got[k], lo)
			}
		}
		if num(t, got["max_read_rows"]) >= 5_000_000 || got["run_at"] < ql45TS(tKP) {
			t.Errorf("REAL stmt0: max_read_rows %s (want < 5000000), run_at %s (want >= KP %s)", got["max_read_rows"], got["run_at"], ql45TS(tKP))
		}
		for _, name := range exactNames {
			check0(t, "EXACT stmt0 "+name, one0(t, "EXACT stmt0 "+name, "", only(t, sent["stmt0"], name)), wants[name])
		}
		check0(t, "EXACT stmt0 all ids", one0(t, "EXACT stmt0 all ids", "", only(t, sent["stmt0"], exactNames...)), ql45Want{
			analytics: s.analytics + p.analytics, finished: s.finished + o.finished, newText: s.newText, failed: l.failed, failed158: l.failed158,
			maxRead: maxRead("stored", "old"),
		})
	})

	cell("mutant-alias", func(t *testing.T) {
		m := mutate(t, sent["stmt0"], "') AS testops_suite_runs') > 0) AS finished,", "') AS testops_pipeline_runs') > 0) AS finished,")
		expect(t, "MUTANT alias stored", one0(t, "MUTANT alias stored", "", only(t, m, "stored")), map[string]any{"finished": 0, "new_text_finished": s.newText})
		if got := one0(t, "MUTANT alias pipeline", "", only(t, m, "pipeline")); got["finished"] == "0" {
			t.Errorf("MUTANT alias: the pipeline statements are still not counted")
		}
	})
	cell("mutant-stmt0h-alias", func(t *testing.T) {
		m := mutate(t, sent["stmt0h"], "') AS testops_suite_runs') > 0) AS pooled_finished_7d,", "') AS testops_pipeline_runs') > 0) AS pooled_finished_7d,")
		expect(t, "MUTANT stmt0h alias stored", one0h(t, "MUTANT stmt0h alias stored", "", only(t, m, "stored")), map[string]any{"pooled_finished_7d": 0})
		if got := one0h(t, "MUTANT stmt0h alias pipeline", "", only(t, m, "pipeline")); got["pooled_finished_7d"] == "0" {
			t.Errorf("MUTANT stmt0h alias: the pipeline statements are still not counted")
		}
	})
	cell("mutant-max-rows", func(t *testing.T) {
		m := mutate(t, sent["stmt0"], "max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0 AND position(query, ') AS testops_suite_runs') > 0) AS finished,",
			"max_rows_to_read = 5000001, read_overflow_mode = ''throw''') > 0 AND position(query, ') AS testops_suite_runs') > 0) AS finished,")
		expect(t, "MUTANT max_rows stored", one0(t, "MUTANT max_rows stored", "", only(t, m, "stored")), map[string]any{"finished": 0, "new_text_finished": s.newText})
		expect(t, "MUTANT max_rows old", one0(t, "MUTANT max_rows old", "", only(t, m, "old")), map[string]any{"finished": 0})
	})
	cell("mutant-max-rows-where", func(t *testing.T) {
		m := mutate(t, sent["stmt0"], "OR position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0)",
			"OR position(query, ', max_rows_to_read = 5000001, read_overflow_mode = ''throw''') > 0)")
		check0(t, "MUTANT max_rows WHERE old", one0(t, "MUTANT max_rows WHERE old", "", only(t, m, "old")), ql45Want{})
	})
	cell("mutant-new-text", func(t *testing.T) {
		m := mutate(t, sent["stmt0"], "'racted)), 2) = 0')) > 0) AS new_text_finished,", "'racted)), 2) = 1')) > 0) AS new_text_finished,")
		expect(t, "MUTANT new text stored", one0(t, "MUTANT new text stored", "", only(t, m, "stored")), map[string]any{"new_text_finished": 0, "finished": s.finished})
	})
	cell("mutant-new-text-dropped", func(t *testing.T) {
		m := mutate(t, sent["stmt0"], " AND position(query, concat('HAVING tupleElement(max((computed_at, 1 - ret', 'racted)), 2) = 0')) > 0) AS new_text_finished,", ") AS new_text_finished,")
		expect(t, "MUTANT new text dropped old", one0(t, "MUTANT new text dropped old", "", only(t, m, "old")), map[string]any{"new_text_finished": o.finished, "finished": o.finished})
		expect(t, "MUTANT new text dropped stored", one0(t, "MUTANT new text dropped stored", "", only(t, m, "stored")), map[string]any{"new_text_finished": s.newText})
	})
	cell("mutant-dimension", func(t *testing.T) {
		m := mutate(t, sent["stmt0"], "'_value,')) > 0 AND position(query, concat('SETTINGS max_execution', '_time = 30')) > 0) AS analytics_finished,",
			"'_value;')) > 0 AND position(query, concat('SETTINGS max_execution', '_time = 30')) > 0) AS analytics_finished,")
		for _, name := range []string{"stored", "pipeline"} {
			expect(t, "MUTANT dimension "+name, one0(t, "MUTANT dimension "+name, "", only(t, m, name)), map[string]any{"analytics_finished": 0, "finished": wants[name].finished})
		}
	})
	cell("mutant-max-execution", func(t *testing.T) {
		m := mutate(t, sent["stmt0"], "'_time = 30')) > 0) AS analytics_finished,", "'_time = 31')) > 0) AS analytics_finished,")
		for _, name := range []string{"stored", "pipeline"} {
			expect(t, "MUTANT max_execution "+name, one0(t, "MUTANT max_execution "+name, "", only(t, m, name)), map[string]any{"analytics_finished": 0})
		}
	})
	cell("mutant-stmt0h-control", func(t *testing.T) {
		m := mutate(t, sent["stmt0h"], "'_time = 30')) > 0) AS control_7d,", "'_time = 31')) > 0) AS control_7d,")
		for _, name := range []string{"stored", "pipeline"} {
			expect(t, "MUTANT stmt0h control "+name, one0h(t, "MUTANT stmt0h control "+name, "", only(t, m, name)), map[string]any{"control_7d": 0})
		}
	})
	cell("mutant-window-stmt0", func(t *testing.T) {
		m := mutate(t, sent["stmt0"], "\nWHERE event_date >= toDate('"+dKP+"') AND event_time >= toDateTime('"+ql45TS(tKP)+"', 'UTC')",
			"\nWHERE event_date >= toDate('"+dKP+"') AND event_time >= toDateTime('"+ql45TS(tEnd.Add(time.Hour))+"', 'UTC')")
		check0(t, "MUTANT window stmt0 all ids", one0(t, "MUTANT window stmt0 all ids", "", only(t, m, exactNames...)), ql45Want{})
	})
	cell("mutant-window-stmt0h", func(t *testing.T) {
		m := mutate(t, sent["stmt0h"], "AND event_time < toDateTime('"+ql45TS(tP10)+"', 'UTC')\n  AND type",
			"AND event_time < toDateTime('"+ql45TS(testStart.Add(-time.Minute))+"', 'UTC')\n  AND type")
		check0h(t, "MUTANT window stmt0h all ids", one0h(t, "MUTANT window stmt0h all ids", "", only(t, m, exactNames...)), ql45Want{})
	})
	cell("mutant-exception-code", func(t *testing.T) {
		m := mutate(t, sent["stmt0"], "exception_code = 158", "exception_code = 0")
		expect(t, "MUTANT exception code legacy", one0(t, "MUTANT exception code legacy", "", only(t, m, "legacy")), map[string]any{"failed_158": 0, "failed": l.failed})
	})
	cell("mutant-type", func(t *testing.T) {
		m := mutate(t, sent["stmt0"], "AND type != 'QueryStart'", "AND type != 'QueryFinish'")
		expect(t, "MUTANT type stored", one0(t, "MUTANT type stored", "", only(t, m, "stored")), map[string]any{"finished": 0, "analytics_finished": 0, "new_text_finished": 0})
		expect(t, "MUTANT type legacy", one0(t, "MUTANT type legacy", "", only(t, m, "legacy")), map[string]any{"failed_158": l.failed158})
	})

	caps := []string{"> 200000000\n", "> 100000000000\n", "> 9000000,"}
	var gateNames []string
	gate := func(t *testing.T, label string, oneFn func(*testing.T, string, string, string) map[string]string) {
		oneFn(t, "CONTROL real "+label, "ctl-"+label, sent[label])
		for i, c := range caps {
			name := fmt.Sprintf("gate-%s-%d", label, i)
			_, _, _, err := run(name, mutate(t, sent[label], c, "> 0"+c[len(c)-1:]))
			var ex *stdclickhouse.Exception
			if err == nil || !errors.As(err, &ex) || ex.Code != 395 || !strings.Contains(ex.Message, "STOP-COST-GATE") {
				t.Errorf("GATE %s %q at 0: err %v, want code 395 STOP-COST-GATE", label, c, err)
				continue
			}
			gateNames = append(gateNames, name)
			t.Logf("GATE %s %q at 0: fired code %d", label, c, ex.Code)
		}
	}
	cell("gate-stmt0h", func(t *testing.T) { gate(t, "stmt0h", one0h) })
	cell("gate-stmt0", func(t *testing.T) { gate(t, "stmt0", one0) })

	formNames := []string{"form-S0", "form-stmt0h", "form-stmt0", "ctl-stmt0h", "ctl-stmt0"}
	if stmtRFinished {
		formNames = append(formNames, "form-stmtR")
	}
	cell("self-match", func(t *testing.T) {
		e.exec("SYSTEM FLUSH LOGS")
		for _, name := range formNames {
			var texts []string
			if ids[name] == "" {
				t.Fatalf("%s did not run", name)
			}
			if err := e.admin.QueryRow(e.ctx, `SELECT groupArray(query) FROM system.query_log WHERE query_id = ? AND type = 'QueryFinish'`, ids[name]).Scan(&texts); err != nil || len(texts) != 1 {
				t.Fatalf("%s: query_log texts %d err %v, want 1", name, len(texts), err)
			}
			for _, mk := range []string{dimMarker, ctlMarker, pooledMarker, newMarker} {
				if strings.Contains(texts[0], mk) {
					t.Errorf("%s: the form's own text holds %q", name, mk)
				}
			}
			if w := recompute(t, name); w != (ql45Want{}) {
				t.Errorf("%s: Go recompute %+v, want zero", name, w)
			}
			t.Logf("SELF %s: own text holds the alias text %v (INFO; the pooled marker it needs is absent)", name, strings.Contains(texts[0], aliasMarker))
		}
		check0(t, "SELF stmt0", one0(t, "SELF stmt0", "", only(t, sent["stmt0"], formNames...)), ql45Want{})
		check0h(t, "SELF stmt0h", one0h(t, "SELF stmt0h", "", only(t, fillH(cuts["stmt0h"], serverNow(t).Add(time.Minute)), formNames...)), ql45Want{})
	})

	cell("read-budget", func(t *testing.T) {
		e.exec("SYSTEM FLUSH LOGS")
		var q, parts uint64
		if err := e.admin.QueryRow(e.ctx, `SELECT sum(rows), count() FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= toDate(?)`, dH0).Scan(&q, &parts); err != nil {
			t.Fatal(err)
		}
		logOf := func(name string) ([]string, uint64) {
			t.Helper()
			rows, err := e.admin.Query(e.ctx, `SELECT toString(type), read_rows FROM system.query_log WHERE query_id = ? AND type != 'QueryStart'`, ids[name])
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
		const budget = 20_000_000
		for _, name := range formNames {
			bound := uint64(budget)
			if strings.HasSuffix(name, "stmt0") || strings.HasSuffix(name, "stmt0h") {
				bound = min(bound, 2*q+2*parts+1)
			}
			if types, read := logOf(name); len(types) != 1 || types[0] != "QueryFinish" || read >= budget || read > bound {
				t.Errorf("%s: query_log types %v read_rows %d, want [QueryFinish] and < %d and <= %d (2Q + 2P + 1, Q %d P %d)", name, types, read, budget, bound, q, parts)
			} else {
				t.Logf("BUDGET %s: read_rows %d (< %d; bound %d)", name, read, budget, bound)
			}
		}
		for _, name := range gateNames {
			if types, read := logOf(name); len(types) != 1 || types[0] != "ExceptionBeforeStart" || read > q+2*parts+1 {
				t.Errorf("%s: query_log types %v read_rows %d, want [ExceptionBeforeStart] and <= Q + 2P + 1 = %d", name, types, read, q+2*parts+1)
			} else {
				t.Logf("BUDGET %s: read_rows %d (Q + 2P + 1 = %d)", name, read, q+2*parts+1)
			}
		}
	})
}
