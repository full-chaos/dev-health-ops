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
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/chclient"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopspooled"
)

const rbpoolFormSHA256 = "48130e5162fcef766edd3d34fa62326ab33f24389f45262673f765bfe542319b"

const rbpoolForm = `-- FORM-3NN-RBPOOL-9 (ClickHouse, prod, read-only). acp-train39, written 2026-11-25. Next query-api roll (q45) READBACK R-POOLQL: the pooled
--   TEST_SUITE_DURATION_P95 suite reads of query-api after the roll, with a pre-START control history arm, over EVERY replica.
--   Point-in-time SELECTs on system.query_log / system.parts / system.one of all replicas; no created traffic (D6310). acp FORM read
--   before pm runs it; the run block is a lane file.
-- = FORM-3NN-RBPOOL-8.sql 112b17b9e128 (frozen) with the acp word 11-25: the app org prefix is the 8-hex literal f0f0f0f0 (traced to its
--   prod out column in the run block), not a token; the replica tag replica_tag = cityHash64(hostName()) % 1000000 (no host names in the outs),
--   stmtR one row per tag with one_rows (N = their sum). RBPOOL-8 = FORM-3NN-RBPOOL-7.sql c5dcf381420c (frozen) with: (1) every system-table read goes through clusterAllReplicas('default', ..) (the
--   server can run >= 2 replicas, and these tables are local to each replica); stmtR lists the replicas (WANT >= 1, N = its rows); S0 and
--   stmtT give one row per replica (hostName()); stmt0 / stmt0h are totals over all replicas plus replicas_with_rows. (2) the ORG RULE: the
--   logged query text of the served statements holds the org value (the RBPOOL-7 CI leg 3: the text holds the value, not the placeholder),
--   so stmt0 / stmt0h keep only rows whose text holds the app org prefix, and org_ids = the distinct full org ids after
--   that prefix (ORG-1). (3) the stmt0 window end is event_time <= now() (the RBPOOL-7 leg 3: < now() drops the rows of the read's own second).
-- TOKENS (pm fills them from the q45 RECORD; an unfilled token = a toDateTime parse error = STOP): <Q45_RECORD_MINUTE> = the q45
--   KP minute as the RECORD states it (stmt0 window start); <Q45_START_MINUTE> = the q45 START minute as the RECORD states it (stmt0h = the
--   6 h before it); 'YYYY-MM-DD HH:MM:SS' UTC.
-- WHAT it selects (cites at ops e3d3a5f4bcf3; the q45 PREPARE re-checks them at the q45 pin):
--   CONTROL rows = query-api analytics timeseries / breakdown statements of ANY measure: the text " AS dimension" + "_value," (timeseries.go:286,
--   breakdown.go:160, the only 2 writers) AND "SETTINGS max_execution" + "_time = 30" (settingsMaxExecutionTime, cost.go:48; queryTimeoutSecs
--   30, cost.go:17). analytics has 0 importers outside internal/queryapi. Both literals are built by concat() in the SQL, so this form's own
--   text cannot count as a control row.
--   POOLED SUITE rows = rows that hold ", max_rows_to_read = 5000000" + ", read_overflow_mode = 'throw'" (orgpooled.go:90) AND ")" +
--   " AS testops_suite_runs" (testopspooled/testopspooled.go:151). The leading comma keeps out the workers report reader and the sync worker,
--   and the alias keeps out the pipeline source. The pooled marker holds doubled quotes (''throw''), so this form's own text (and the text a
--   replica logs for this form's remote part, which writes the quote as \') does not match it.
--   The writers of both texts are byte-equal at ba2d85c5 and e3d3a5f4 (git diff --quiet rc 0 on analytics/{cost,timeseries,breakdown,
--   orgpooled,resolve}.go), so the history arm (old pods) and stmt0 (new pods) count the same text.
-- ORG RULE: stmt0 and stmt0h hold position(query, 'f0f0f0f0') > 0 in the WHERE (the startsWith rule on the logged text); org_ids /
--   org_ids_6h = uniqExact of the full org id after the prefix. WANT org_ids = 1 when a count > 0 (ORG-1), 0 with every count 0 =
--   NOT EXERCISED, >= 2 = STOP (the prefix is not one org). S0 / stmtR / stmtT read system tables with no org data: org rule N/A there.
-- REPLICAS: stmtR = one row per replica tag of cluster 'default' with one_rows (the replicas behind that tag). WANT >= 1 rows and
--   one_rows = 1 each (2 replicas with one tag = report); N = the sum of one_rows; a SQL error = STOP. Every later statement reads all N.
--   replica_tag = cityHash64(hostName()) % 1000000: a stable per-replica number, so the outs hold no host name.
-- RETENTION: the log keeps rows a short time (a lane fact, in the run block). S0 head_day_kept = 1 (per replica) = that replica's oldest
--   active part day <= the stmt0h first day. stmtT head_rows_kp / head_rows_h (per replica) > 0 = that replica still holds the rows of the
--   10 min before each window start (TTL drops the oldest rows first, so the window after it is kept). A replica with a 0, or with no
--   stmtT row, = that replica NOT EXERCISED for that window (report, no rollback).
-- START..KP GAP (INFO, by design): the minutes from START to KP (old pods draining, new pods starting) are in neither window.
-- WANT (S0): one row per stmtR replica_tag, rows > 0, head_day_kept = 1 each. min_day = INFO.
-- WANT (stmtR): >= 1 row; one_rows = 1 each. N = the sum of one_rows (INFO; the cost caps below are totals over the N replicas).
-- WANT (stmtT): one row per stmtR replica_tag with head_rows_kp > 0 and head_rows_h > 0 (else that replica NOT EXERCISED for that window).
-- WANT (stmt0): analytics_finished > 0 (the control; 0 = NOT EXERCISED, the pooled WANTs prove nothing). org_ids = 1. failed_158 = 0. When
--   finished > 0: max_read_rows < 5,000,000, >= 4,000,000 = report (INFO). INFO: max_memory_usage < 1,073,741,824. finished = 0 and
--   failed = 0 with analytics_finished > 0 = NOT EXERCISED (allowed). failed > failed_158 = report. first_matched = INFO only when
--   finished + failed > 0 (1970-01-01 00:00:00 = no pooled row). run_at, replicas_with_rows = INFO.
-- WANT (stmt0h, the control history arm): analytics_finished_6h > 0 and org_ids_6h = 1 = the control text matches the served statements of
--   the old build, and 0 = UNSPLIT (no caller in 6 h, a lost window head, or a text change cannot be split): report to acp, no ruling on the text.
-- Form: lane-scratch/pm-guard/FORM-TEMPLATE-5.md 7fb32b16d35d (CH shape: SELECT only, no SET / SETTINGS / EXPLAIN, one statement per call,
--   a cost gate in each query_log statement). Table: system.query_log (partition by month of event_date; sorting key event_date, event_time).
-- WORK MATH (totals over the N replicas): S0 reads system.parts only; stmtR reads 1 row per replica. stmtT, stmt0 and stmt0h each read
--   system.query_log of all replicas TWICE: the window count of the cost gate and the scan of the same window; W = that window's rows over
--   all replicas <= 9,000,000 (the gate, so <= 9,000,000 / N per replica on average), and 2 x W + 2P + 1 <= 18,000,000 + 2P + 1 < 20,000,000
--   (P = the query_log parts of all replicas in the parts gate). The parts gate (<= 200,000,000 rows / 100,000,000,000 bytes over all
--   replicas) bounds the count when the key prunes nothing. FAIL PATH: the gate's scalar subqueries run on the initiator before throwIf, so
--   a fire reads the window once (<= W + 2P + 1) and sends no scan to a replica. GRANULE SLACK (INFO): reads are whole granules (8192 rows),
--   up to 2 x 8192 x P rows more; inside the budget. Day bounds carry 1 day of slack (a superset for any server time zone).
-- Output: counts, max values, days, INFO times (UTC) and replica tags (6-digit hash numbers). No query text, no org id, no host or user names.

-- S0 (system.parts of every replica). WANT one row per stmtR replica_tag, rows > 0, head_day_kept = 1 each.
SELECT cityHash64(hostName()) % 1000000 AS replica_tag, sum(rows) AS rows, sum(bytes_on_disk) AS bytes, count() AS active_parts, toString(min(min_date)) AS min_day,
  toUInt8(min(min_date) <= toDate(toDateTime('<Q45_START_MINUTE>', 'UTC') - INTERVAL 6 HOUR)) AS head_day_kept
FROM clusterAllReplicas('default', system.parts) WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= toDate(toDateTime('<Q45_START_MINUTE>', 'UTC') - INTERVAL 6 HOUR) - 1 GROUP BY replica_tag ORDER BY replica_tag;

-- stmtR (the replicas of the server, 1 row per replica tag). WANT >= 1 rows, one_rows = 1 each; N = the sum of one_rows; a SQL error = STOP.
SELECT cityHash64(hostName()) % 1000000 AS replica_tag, count() AS one_rows FROM clusterAllReplicas('default', system.one) GROUP BY replica_tag ORDER BY replica_tag;

-- stmtT (the window heads, per replica). WANT one row per stmtR replica_tag, head_rows_kp > 0, head_rows_h > 0.
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM clusterAllReplicas('default', system.parts) WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= least(toDate(toDateTime('<Q45_START_MINUTE>', 'UTC') - INTERVAL 6 HOUR) - 1, toDate(toDateTime('<Q45_RECORD_MINUTE>', 'UTC')) - 1)) > 200000000
  OR (SELECT sum(bytes_on_disk) FROM clusterAllReplicas('default', system.parts) WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= least(toDate(toDateTime('<Q45_START_MINUTE>', 'UTC') - INTERVAL 6 HOUR) - 1, toDate(toDateTime('<Q45_RECORD_MINUTE>', 'UTC')) - 1)) > 100000000000
  OR (SELECT count() FROM clusterAllReplicas('default', system.query_log) WHERE event_date >= least(toDate(toDateTime('<Q45_START_MINUTE>', 'UTC') - INTERVAL 6 HOUR) - 1, toDate(toDateTime('<Q45_RECORD_MINUTE>', 'UTC')) - 1) AND ((event_time >= toDateTime('<Q45_START_MINUTE>', 'UTC') - INTERVAL 6 HOUR - INTERVAL 10 MINUTE AND event_time < toDateTime('<Q45_START_MINUTE>', 'UTC') - INTERVAL 6 HOUR) OR (event_time >= toDateTime('<Q45_RECORD_MINUTE>', 'UTC') - INTERVAL 10 MINUTE AND event_time < toDateTime('<Q45_RECORD_MINUTE>', 'UTC')))) > 9000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT cityHash64(hostName()) % 1000000 AS replica_tag,
  countIf(event_time >= toDateTime('<Q45_RECORD_MINUTE>', 'UTC') - INTERVAL 10 MINUTE AND event_time < toDateTime('<Q45_RECORD_MINUTE>', 'UTC')) AS head_rows_kp,
  countIf(event_time >= toDateTime('<Q45_START_MINUTE>', 'UTC') - INTERVAL 6 HOUR - INTERVAL 10 MINUTE AND event_time < toDateTime('<Q45_START_MINUTE>', 'UTC') - INTERVAL 6 HOUR) AS head_rows_h
FROM clusterAllReplicas('default', system.query_log)
WHERE event_date >= least(toDate(toDateTime('<Q45_START_MINUTE>', 'UTC') - INTERVAL 6 HOUR) - 1, toDate(toDateTime('<Q45_RECORD_MINUTE>', 'UTC')) - 1) AND ((event_time >= toDateTime('<Q45_START_MINUTE>', 'UTC') - INTERVAL 6 HOUR - INTERVAL 10 MINUTE AND event_time < toDateTime('<Q45_START_MINUTE>', 'UTC') - INTERVAL 6 HOUR) OR (event_time >= toDateTime('<Q45_RECORD_MINUTE>', 'UTC') - INTERVAL 10 MINUTE AND event_time < toDateTime('<Q45_RECORD_MINUTE>', 'UTC')))
  AND cost_gate = 0
GROUP BY replica_tag ORDER BY replica_tag;

-- stmt0 served pooled TEST_SUITE_DURATION_P95 statements of query-api of the app org from KP to now, and the control, over all replicas. WANT 1 row, analytics_finished > 0, org_ids = 1, failed_158 = 0.
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM clusterAllReplicas('default', system.parts) WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= toDate(toDateTime('<Q45_RECORD_MINUTE>', 'UTC')) - 1) > 200000000
  OR (SELECT sum(bytes_on_disk) FROM clusterAllReplicas('default', system.parts) WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= toDate(toDateTime('<Q45_RECORD_MINUTE>', 'UTC')) - 1) > 100000000000
  OR (SELECT count() FROM clusterAllReplicas('default', system.query_log) WHERE event_date >= toDate(toDateTime('<Q45_RECORD_MINUTE>', 'UTC')) - 1 AND event_time >= toDateTime('<Q45_RECORD_MINUTE>', 'UTC') AND event_time <= now()) > 9000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT toString(today()) AS day,
  toString(now(), 'UTC') AS run_at,
  toString(minIf(event_time, position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0 AND position(query, ') AS testops_suite_runs') > 0), 'UTC') AS first_matched,
  countIf(type = 'QueryFinish' AND position(query, concat(' AS dimension', '_value,')) > 0 AND position(query, concat('SETTINGS max_execution', '_time = 30')) > 0) AS analytics_finished,
  countIf(type = 'QueryFinish' AND position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0 AND position(query, ') AS testops_suite_runs') > 0) AS finished,
  maxIf(read_rows, type = 'QueryFinish' AND position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0 AND position(query, ') AS testops_suite_runs') > 0) AS max_read_rows,
  maxIf(memory_usage, type = 'QueryFinish' AND position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0 AND position(query, ') AS testops_suite_runs') > 0) AS max_memory_usage,
  countIf(type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing') AND position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0 AND position(query, ') AS testops_suite_runs') > 0) AS failed,
  countIf(type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing') AND exception_code = 158 AND position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0 AND position(query, ') AS testops_suite_runs') > 0) AS failed_158,
  uniqExact(extract(query, 'f0f0f0f0-[0-9a-f-]+')) AS org_ids,
  uniqExact(cityHash64(hostName()) % 1000000) AS replicas_with_rows
FROM clusterAllReplicas('default', system.query_log)
WHERE event_date >= toDate(toDateTime('<Q45_RECORD_MINUTE>', 'UTC')) - 1 AND event_time >= toDateTime('<Q45_RECORD_MINUTE>', 'UTC') AND event_time <= now()
  AND type != 'QueryStart' AND query_kind = 'Select'
  AND ((position(query, concat(' AS dimension', '_value,')) > 0 AND position(query, concat('SETTINGS max_execution', '_time = 30')) > 0)
    OR position(query, ', max_rows_to_read = 5000000, read_overflow_mode = ''throw''') > 0)
  AND position(query, 'f0f0f0f0') > 0
  AND cost_gate = 0;

-- stmt0h the control of the app org over the 6 h before the roll START (old pods), over all replicas. WANT 1 row; analytics_finished_6h > 0, org_ids_6h = 1.
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM clusterAllReplicas('default', system.parts) WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= toDate(toDateTime('<Q45_START_MINUTE>', 'UTC') - INTERVAL 6 HOUR) - 1 AND min_date <= toDate(toDateTime('<Q45_START_MINUTE>', 'UTC')) + 1) > 200000000
  OR (SELECT sum(bytes_on_disk) FROM clusterAllReplicas('default', system.parts) WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= toDate(toDateTime('<Q45_START_MINUTE>', 'UTC') - INTERVAL 6 HOUR) - 1 AND min_date <= toDate(toDateTime('<Q45_START_MINUTE>', 'UTC')) + 1) > 100000000000
  OR (SELECT count() FROM clusterAllReplicas('default', system.query_log) WHERE event_date >= toDate(toDateTime('<Q45_START_MINUTE>', 'UTC') - INTERVAL 6 HOUR) - 1 AND event_date <= toDate(toDateTime('<Q45_START_MINUTE>', 'UTC')) + 1 AND event_time >= toDateTime('<Q45_START_MINUTE>', 'UTC') - INTERVAL 6 HOUR AND event_time < toDateTime('<Q45_START_MINUTE>', 'UTC')) > 9000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT toString(today()) AS day,
  countIf(type = 'QueryFinish' AND position(query, concat(' AS dimension', '_value,')) > 0 AND position(query, concat('SETTINGS max_execution', '_time = 30')) > 0) AS analytics_finished_6h,
  uniqExact(extract(query, 'f0f0f0f0-[0-9a-f-]+')) AS org_ids_6h,
  uniqExact(cityHash64(hostName()) % 1000000) AS replicas_with_rows
FROM clusterAllReplicas('default', system.query_log)
WHERE event_date >= toDate(toDateTime('<Q45_START_MINUTE>', 'UTC') - INTERVAL 6 HOUR) - 1 AND event_date <= toDate(toDateTime('<Q45_START_MINUTE>', 'UTC')) + 1 AND event_time >= toDateTime('<Q45_START_MINUTE>', 'UTC') - INTERVAL 6 HOUR AND event_time < toDateTime('<Q45_START_MINUTE>', 'UTC')
  AND type != 'QueryStart' AND query_kind = 'Select'
  AND position(query, concat(' AS dimension', '_value,')) > 0 AND position(query, concat('SETTINGS max_execution', '_time = 30')) > 0
  AND position(query, 'f0f0f0f0') > 0
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
	if strings.Join(labels, ",") != "S0,stmtR,stmtT,stmt0,stmt0h" {
		t.Fatalf("form statements = %v", labels)
	}
	marker := ", max_rows_to_read = 5000000, read_overflow_mode = ''throw''"
	caps := []string{"> 200000000\n", "> 100000000000\n", "> 9000000,"}
	control := "concat('SETTINGS max_execution', '_time = 30')"
	dimension := "position(query, concat(' AS dimension', '_value,')) > 0 AND "
	qlAll, partsAll := "clusterAllReplicas('default', system.query_log)", "clusterAllReplicas('default', system.parts)"
	// Every system-table read of every statement goes through clusterAllReplicas (the tables are local to each replica).
	for _, l := range labels {
		sql := ""
		for _, line := range strings.Split(byLabel[l], "\n") {
			if !strings.HasPrefix(strings.TrimSpace(line), "--") {
				sql += line + "\n"
			}
		}
		if strings.Contains(sql, "FROM system.") {
			t.Fatalf("%s: a local system-table read (FROM system.)", l)
		}
	}
	for lit, n := range map[string]int{caps[0]: 1, caps[1]: 1, caps[2]: 1, "'STOP-COST-GATE'": 1, "cost_gate = 0": 1, marker: 7,
		"') AS testops_suite_runs'": 6, qlAll: 2, partsAll: 2, control: 2, dimension: 2, "SETTINGS max_execution_time = 30": 0, " AS dimension_value,": 0, "f0f0f0f0": 2} {
		if got := strings.Count(byLabel["stmt0"], lit); got != n {
			t.Fatalf("stmt0: %q found %d times, want %d", lit, got, n)
		}
	}
	for lit, n := range map[string]int{caps[0]: 1, caps[1]: 1, caps[2]: 1, "'STOP-COST-GATE'": 1, "cost_gate = 0": 1, marker: 0,
		"') AS testops_suite_runs'": 0, qlAll: 2, partsAll: 2, control: 2, dimension: 2, "SETTINGS max_execution_time = 30": 0, " AS dimension_value,": 0,
		"<Q45_RECORD_MINUTE>": 0, "f0f0f0f0": 2} {
		if got := strings.Count(byLabel["stmt0h"], lit); got != n {
			t.Fatalf("stmt0h: %q found %d times, want %d", lit, got, n)
		}
	}
	for lit, n := range map[string]int{caps[0]: 1, caps[1]: 1, caps[2]: 1, "'STOP-COST-GATE'": 1, "cost_gate = 0": 1, marker: 0,
		qlAll: 2, partsAll: 2, control: 0, "<Q45_RECORD_MINUTE>": 10, "f0f0f0f0": 0} {
		if got := strings.Count(byLabel["stmtT"], lit); got != n {
			t.Fatalf("stmtT: %q found %d times, want %d", lit, got, n)
		}
	}
	if got := strings.Count(byLabel["stmt0"], "toDateTime('<Q45_RECORD_MINUTE>', 'UTC')"); got != 6 || strings.Contains(byLabel["stmt0"], "<Q45_START_MINUTE>") {
		t.Fatalf("stmt0: the KP token found %d times, want 6 (2 in the parts gate, 2 in the window count, 2 in the scan)", got)
	}
	// Every date of every statement is a token: no other toDateTime / toDate literal.
	for _, l := range labels {
		if a, b := strings.Count(byLabel[l], "toDateTime('"), strings.Count(byLabel[l], "toDateTime('<Q45_"); a != b || strings.Contains(byLabel[l], "toDate('") {
			t.Fatalf("%s: %d toDateTime literals, %d of them tokens; or a toDate literal", l, a, b)
		}
	}
	if r := strings.TrimSpace(byLabel["stmtR"]); strings.Count(r, "\n") != 1 || !strings.HasSuffix(r, "\nSELECT cityHash64(hostName()) % 1000000 AS replica_tag, count() AS one_rows FROM clusterAllReplicas('default', system.one) GROUP BY replica_tag ORDER BY replica_tag") {
		t.Fatalf("stmtR: text %q, want the marker line + the one replica-count line", r)
	}
	if !strings.Contains(byLabel["S0"], "toString(min(min_date)) AS min_day") {
		t.Fatalf("S0: no min_day column")
	}
	if strings.Contains(byLabel["S0"], "STOP-COST-GATE") || !strings.Contains(byLabel["S0"], "FROM "+partsAll+" WHERE") {
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
	var testStart, tokStart string
	if err := e.admin.QueryRow(e.ctx, "SELECT toString(now(), 'UTC'), toString(now() + INTERVAL 6 HOUR, 'UTC')").Scan(&testStart, &tokStart); err != nil {
		t.Fatal(err)
	}
	// The fill pm does from the RECORD: <Q45_RECORD_MINUTE> = KP (here the test start), <Q45_START_MINUTE> = START (here test start + 6 h,
	// so the 6 h history window starts at the test start and holds this test's statements).
	const orgPrefix = "f0f0f0f0"
	fillOrg := func(text, record, start, prefix string) string {
		return strings.NewReplacer("<Q45_RECORD_MINUTE>", record, "<Q45_START_MINUTE>", start, "f0f0f0f0", prefix).Replace(text)
	}
	fill := func(text, record, start string) string { return fillOrg(text, record, start, orgPrefix) }
	raw := byLabel
	byLabel = map[string]string{}
	for _, l := range labels {
		byLabel[l] = fill(raw[l], testStart, tokStart)
		if strings.Contains(byLabel[l], "<Q45_") {
			t.Fatalf("%s: a token is left after the fill", l)
		}
	}
	t.Logf("FILL <Q45_RECORD_MINUTE> = %s, <Q45_START_MINUTE> = %s", testStart, tokStart)
	const org = "f0f0f0f0-0000-4000-8000-0000000003b1"
	if !strings.HasPrefix(org, orgPrefix+"-") {
		t.Fatalf("the test org %s does not start with the token value %s", org, orgPrefix)
	}
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
		if strings.Join(types, ",") != "String,String,String,UInt64,UInt64,UInt64,UInt64,UInt64,UInt64,UInt64,UInt64" {
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
	if len(s0) != 1 || s0[0]["head_day_kept"] != "1" {
		t.Errorf("S0: rows %v, want 1 row with head_day_kept 1", s0)
	}
	if got, _, err := rbpoolRows(e.ctx, e.admin, fill(raw["S0"], testStart, "2020-01-01 06:00:00")); err != nil || len(got) != 1 || got[0]["head_day_kept"] != "0" {
		t.Errorf("MUTANT S0 rule: a START token older than the log: err %v rows %v, want head_day_kept 0", err, got)
	} else {
		t.Logf("MUTANT S0 rule: START token 2020-01-01 06:00:00: head_day_kept 0 (min_day %s)", got[0]["min_day"])
	}
	var minDay string
	if err := e.admin.QueryRow(e.ctx, `SELECT toString(min(min_date)) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND max_date >= toDate(toDateTime(?, 'UTC') - INTERVAL 6 HOUR) - 1`, tokStart).Scan(&minDay); err != nil {
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
		if strings.Join(types, ",") != "UInt32,UInt64" {
			t.Errorf("%s: types %v, want UInt32,UInt64", label, types)
			return "", nil
		}
		n := 0
		for _, r := range got {
			if r["one_rows"] != "1" {
				t.Errorf("%s: row %v, want one_rows 1", label, r)
			}
			n++
		}
		return fmt.Sprint(n), nil
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
	// The host names: S0 and stmtT give one row per stmtR host.
	var stmtRHosts []string
	if got, _, err := rbpoolRows(e.ctx, e.admin, byLabel["stmtR"]); err != nil {
		t.Fatal(err)
	} else {
		for _, r := range got {
			stmtRHosts = append(stmtRHosts, r["replica_tag"])
		}
	}
	var s0Hosts []string
	for _, r := range s0 {
		s0Hosts = append(s0Hosts, r["replica_tag"])
	}
	if strings.Join(s0Hosts, ",") != strings.Join(stmtRHosts, ",") || len(stmtRHosts) == 0 {
		t.Errorf("S0 replica tags %v, stmtR replica tags %v: want the same, not empty", s0Hosts, stmtRHosts)
	} else {
		t.Logf("PASS S0 replica tags = stmtR replica tags (N = %d)", len(stmtRHosts))
	}
	if _, err := replicasOf("MUTANT stmtR rule: an unknown cluster", strings.Replace(byLabel["stmtR"], "'default'", "'rbpool_no_such_cluster'", 1)); err == nil {
		t.Errorf("MUTANT stmtR unknown cluster: no error, want a server exception (= STOP)")
	} else {
		t.Logf("MUTANT stmtR unknown cluster: %s (= STOP)", firstLine(err))
	}
	// stmtT: the window heads (10 min before each window start; here both = the 10 min before the test start: the schema setup rows).
	headOf := func(label, text string, comment ...string) map[string]string {
		t.Helper()
		ctx := e.ctx
		if len(comment) > 0 {
			ctx = stdclickhouse.Context(e.ctx, stdclickhouse.WithSettings(stdclickhouse.Settings{"log_comment": comment[0]}))
		}
		got, types, err := rbpoolRows(ctx, e.admin, text)
		if err != nil || len(got) != 1 || strings.Join(types, ",") != "UInt32,UInt64,UInt64" || got[0]["replica_tag"] != strings.Join(stmtRHosts, ",") {
			t.Fatalf("%s: err %v rows %v types %v (want 1 row per stmtR replica tag %v)", label, err, got, types, stmtRHosts)
		}
		t.Logf("%s ROW %v", label, got[0])
		return got[0]
	}
	passCommentT := "rbpool-passt-" + uuid.NewString()[:8]
	if h := headOf("PASS stmtT", byLabel["stmtT"], passCommentT); h["head_rows_kp"] == "0" || h["head_rows_h"] == "0" {
		t.Errorf("PASS stmtT: %v, want head_rows_kp > 0 and head_rows_h > 0", h)
	}
	if h := headOf("MUTANT stmtT rule: KP token after the log", fill(raw["stmtT"], "2098-01-01 00:00:00", tokStart)); h["head_rows_kp"] != "0" || h["head_rows_h"] == "0" {
		t.Errorf("MUTANT stmtT KP token after the log: %v, want head_rows_kp 0 and head_rows_h > 0", h)
	}
	if h := headOf("MUTANT stmtT rule: START token after the log", fill(raw["stmtT"], testStart, "2098-01-01 06:00:00")); h["head_rows_h"] != "0" || h["head_rows_kp"] == "0" {
		t.Errorf("MUTANT stmtT START token after the log: %v, want head_rows_h 0 and head_rows_kp > 0", h)
	}
	// stmt0 ends its window at event_time <= now(); the 1 s wait also keeps the served rows out of the read's own second.
	e.exec("SELECT sleep(1.1)")
	passComment := "rbpool-pass-" + uuid.NewString()[:8]
	clean := one("PASS stmt0 (server-wide)", byLabel["stmt0"], passComment)
	// stmt0h: one row of (day String, analytics_finished_6h UInt64).
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
		if strings.Join(types, ",") != "String,UInt64,UInt64,UInt64" {
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
	for name, wantIDs := range map[string]string{"stored": "1", "legacy": "1", "pipeline": "1", "workers": "0", "sync": "0"} {
		if got := one("ORG-1 "+name, only(byLabel["stmt0"], name)); got["org_ids"] != wantIDs {
			t.Errorf("ORG-1 %s: org_ids %s, want %s", name, got["org_ids"], wantIDs)
		}
	}
	otherOrg := fillOrg(raw["stmt0"], testStart, tokStart, "e0e0e0e0")
	if strings.Contains(otherOrg, "f0f0f0f0") {
		t.Fatal("the org mutant still holds the test org prefix")
	}
	for _, name := range []string{"stored", "legacy", "pipeline"} {
		got := one("MUTANT org rule: another org prefix "+name, only(otherOrg, name))
		check("MUTANT org rule: another org prefix "+name, got, want{})
		if got["org_ids"] != "0" {
			t.Errorf("MUTANT org rule %s: org_ids %s, want 0", name, got["org_ids"])
		}
	}
	lateKP := fill(raw["stmt0"], "2098-01-01 00:00:00", tokStart)
	for _, name := range []string{"stored", "legacy", "pipeline"} {
		check("WINDOW stmt0 KP after the CI rows "+name, one("WINDOW stmt0 KP after the CI rows "+name, only(lateKP, name)), want{})
	}
	check("EXACT workers", one("EXACT workers", only(byLabel["stmt0"], "workers")), want{})
	check("EXACT sync", one("EXACT sync", only(byLabel["stmt0"], "sync")), want{})
	if w := recompute("sync"); w != (want{}) {
		t.Errorf("the sync shape recompute = %+v, want zero", w)
	}
	t.Logf("WANT stored %+v legacy %+v (Go recompute)", stored, legacy)
	// ORG RULE proof: the logged text of this test's pooled and control statements holds the org value (the RBPOOL-7 leg 3 fact), so the
	// form's position(query, <APP_ORG8>) rule can scope by org.
	for _, name := range []string{"stored", "legacy", "pipeline"} {
		var rowsN, placeholder, inText, inSettings uint64
		if err := e.admin.QueryRow(e.ctx, `SELECT count(), countIf(position(query, concat(char(123), 'org_id:String', char(125))) > 0), countIf(position(query, ?) > 0),
			countIf(position(arrayStringConcat(mapValues(Settings), ','), ?) > 0) FROM system.query_log WHERE query_id = ? AND type != 'QueryStart'`, org, org, ids[name]).Scan(&rowsN, &placeholder, &inText, &inSettings); err != nil {
			t.Fatal(err)
		}
		if rowsN == 0 || inText != rowsN {
			t.Errorf("ORG %s: rows %d, with the org value in the text %d: want rows > 0, every row with the value (the form's org rule reads it)", name, rowsN, inText)
		} else {
			t.Logf("ORG %s: rows %d, every row with the org value in the text; with the placeholder %d, in Settings %d (INFO)", name, rowsN, placeholder, inSettings)
		}
	}
	// stmt0h: the control count per query id on the now-window copy = the Go recompute; the clean fixed past window holds none of them.
	for _, name := range []string{"stored", "pipeline", "legacy", "workers", "sync"} {
		w := recompute(name)
		if got := oneH("EXACT stmt0h now-window "+name, only(nowH, name)); got["analytics_finished_6h"] != fmt.Sprint(w.analytics) {
			t.Errorf("EXACT stmt0h %s: analytics_finished_6h %s, want %d (Go recompute)", name, got["analytics_finished_6h"], w.analytics)
		}
		pastH := fill(raw["stmt0h"], testStart, "2098-01-01 06:00:00")
		if got := oneH("WINDOW stmt0h window start after the CI statements "+name, only(pastH, name)); got["analytics_finished_6h"] != "0" {
			t.Errorf("WINDOW stmt0h %s: a window that starts after this test's rows counts %s, want 0", name, got["analytics_finished_6h"])
		}
	}
	if n, _ := fmt.Sscan(cleanH["analytics_finished_6h"], new(int)); n != 1 || cleanH["analytics_finished_6h"] == "0" {
		t.Errorf("PASS stmt0h server-wide analytics_finished_6h %s, want > 0 (this test's own control rows)", cleanH["analytics_finished_6h"])
	}
	if got := oneH("MUTANT stmt0h org rule: another org prefix", only(fillOrg(raw["stmt0h"], testStart, tokStart, "e0e0e0e0"), "stored")); got["analytics_finished_6h"] != "0" || got["org_ids_6h"] != "0" {
		t.Errorf("MUTANT stmt0h org rule: %v, want analytics_finished_6h 0, org_ids_6h 0", got)
	}
	if got := oneH("ORG-1 stmt0h stored", only(nowH, "stored")); got["org_ids_6h"] != "1" {
		t.Errorf("ORG-1 stmt0h stored: org_ids_6h %s, want 1", got["org_ids_6h"])
	}
	if got := oneH("MUTANT stmt0h rule: another control settings text", only(strings.ReplaceAll(nowH, control, "concat('SETTINGS max_execution', '_time = 31')"), "stored")); got["analytics_finished_6h"] != "0" {
		t.Errorf("MUTANT stmt0h control text: %s, want 0", got["analytics_finished_6h"])
	}
	if got := oneH("MUTANT stmt0h rule: the control without the dimension text (sync)", only(strings.ReplaceAll(nowH, dimension, ""), "sync")); got["analytics_finished_6h"] == "0" {
		t.Errorf("MUTANT stmt0h no dimension term: the sync statement is still not counted")
	}

	// 2-REPLICA ARM: 2 ClickHouse servers chr1 / chr2 on one docker network, each with cluster 'default' = {chr1, chr2}. The form text runs
	// as is on chr1 (clusterAllReplicas reaches chr2 over the network): it proves N = 2, one replica_tag per server, the totals over both
	// replicas, remote routing, the org rule on both, and the gate's cost scaling (the window count is the total over both replicas).
	r1, r2 := rbpoolStartReplicas(t, e.ctx)
	conns := map[string]stdclickhouse.Conn{"r1": r1, "r2": r2}
	tags := map[string]string{}
	for name, c := range conns {
		var tag uint32
		if err := c.QueryRow(e.ctx, "SELECT cityHash64(hostName()) % 1000000").Scan(&tag); err != nil {
			t.Fatal(err)
		}
		tags[name] = fmt.Sprint(tag)
		for i := 0; i < 3; i++ {
			if err := c.Exec(e.ctx, "SELECT 1"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if tags["r1"] == tags["r2"] {
		t.Fatalf("2-REPLICA: both servers have replica_tag %s", tags["r1"])
	}
	if err := r1.Exec(e.ctx, "SELECT sleep(1.1)"); err != nil {
		t.Fatal(err)
	}
	var rStart, rTok string
	if err := r1.QueryRow(e.ctx, "SELECT toString(now(), 'UTC'), toString(now() + INTERVAL 6 HOUR, 'UTC')").Scan(&rStart, &rTok); err != nil {
		t.Fatal(err)
	}
	// The served-statement shapes, written straight to each server's query_log (no schema there): a pooled suite read that finishes, one
	// refused with 158 (6M rows > 5M), and an analytics control statement; each holds the test org value in its text.
	shapes := map[string]string{
		"pass": "SELECT count() FROM (SELECT number FROM numbers(10)) AS testops_suite_runs WHERE '" + org + "' != '' SETTINGS max_execution_time = 30, max_rows_to_read = 5000000, read_overflow_mode = 'throw'",
		"fail": "SELECT count() FROM (SELECT number FROM numbers(6000000)) AS testops_suite_runs WHERE '" + org + "' != '' SETTINGS max_execution_time = 30, max_rows_to_read = 5000000, read_overflow_mode = 'throw'",
		"ctrl": "SELECT 1 AS dimension_value, '" + org + "' AS org SETTINGS max_execution_time = 30",
	}
	for name, c := range conns {
		for shape, q := range shapes {
			id := "rbpool-2r-" + name + "-" + shape + "-" + uuid.NewString()[:8]
			ids[name+shape] = id
			err := c.Exec(stdclickhouse.Context(e.ctx, stdclickhouse.WithQueryID(id)), q)
			var ex *stdclickhouse.Exception
			if shape == "fail" {
				if !errors.As(err, &ex) || ex.Code != 158 {
					t.Fatalf("2-REPLICA %s %s: err %v, want code 158", name, shape, err)
				}
			} else if err != nil {
				t.Fatalf("2-REPLICA %s %s: %v", name, shape, err)
			}
		}
	}
	for _, c := range conns {
		if err := c.Exec(e.ctx, "SYSTEM FLUSH LOGS"); err != nil {
			t.Fatal(err)
		}
	}
	if err := r1.Exec(e.ctx, "SELECT sleep(1.1)"); err != nil {
		t.Fatal(err)
	}
	rRows := func(label, text string) []map[string]string {
		t.Helper()
		got, _, err := rbpoolRows(e.ctx, r1, text)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		t.Logf("%s ROWS %v", label, got)
		return got
	}
	byTag := func(rows []map[string]string) map[string]map[string]string {
		m := map[string]map[string]string{}
		for _, r := range rows {
			m[r["replica_tag"]] = r
		}
		return m
	}
	f2 := func(l string) string { return fill(raw[l], rStart, rTok) }
	if got := byTag(rRows("2-REPLICA stmtR", f2("stmtR"))); len(got) != 2 || got[tags["r1"]]["one_rows"] != "1" || got[tags["r2"]]["one_rows"] != "1" {
		t.Errorf("2-REPLICA stmtR: %v, want 2 rows (the 2 server tags %v), one_rows 1 each (N = 2)", got, tags)
	}
	if got := byTag(rRows("2-REPLICA S0", f2("S0"))); len(got) != 2 || got[tags["r1"]]["head_day_kept"] != "1" || got[tags["r2"]]["head_day_kept"] != "1" {
		t.Errorf("2-REPLICA S0: %v, want 1 row per server tag, head_day_kept 1", got)
	}
	headWant := map[string]uint64{}
	var wTotal uint64
	for name, c := range conns {
		var n uint64
		if err := c.QueryRow(e.ctx, "SELECT count() FROM system.query_log WHERE event_date >= toDate(toDateTime(?, 'UTC')) - 1 AND event_time >= toDateTime(?, 'UTC') - INTERVAL 10 MINUTE AND event_time < toDateTime(?, 'UTC')", rStart, rStart, rStart).Scan(&n); err != nil {
			t.Fatal(err)
		}
		headWant[tags[name]] = n
		wTotal += n
	}
	if got := byTag(rRows("2-REPLICA stmtT", f2("stmtT"))); len(got) != 2 {
		t.Errorf("2-REPLICA stmtT: %v, want 1 row per server tag", got)
	} else {
		for tag, n := range headWant {
			if n == 0 || got[tag]["head_rows_kp"] != fmt.Sprint(n) || got[tag]["head_rows_h"] != fmt.Sprint(n) {
				t.Errorf("2-REPLICA stmtT tag %s: %v, want both heads = that server's own count %d (> 0)", tag, got[tag], n)
			}
		}
	}
	two := rRows("2-REPLICA EXACT stmt0", only(f2("stmt0"), "r1pass", "r1fail", "r1ctrl", "r2pass", "r2fail", "r2ctrl"))
	if len(two) != 1 || two[0]["finished"] != "2" || two[0]["failed"] != "2" || two[0]["failed_158"] != "2" || two[0]["analytics_finished"] != "2" ||
		two[0]["org_ids"] != "1" || two[0]["replicas_with_rows"] != "2" {
		t.Errorf("2-REPLICA EXACT stmt0: %v, want finished 2, failed 2, failed_158 2, analytics_finished 2, org_ids 1, replicas_with_rows 2", two)
	}
	if got := rRows("2-REPLICA MUTANT org rule stmt0", only(fillOrg(raw["stmt0"], rStart, rTok, "e0e0e0e0"), "r1pass", "r1fail", "r1ctrl", "r2pass", "r2fail", "r2ctrl")); len(got) != 1 || got[0]["finished"] != "0" || got[0]["failed"] != "0" || got[0]["analytics_finished"] != "0" || got[0]["org_ids"] != "0" {
		t.Errorf("2-REPLICA MUTANT org rule stmt0: %v, want all 0", got)
	}
	twoH := rRows("2-REPLICA EXACT stmt0h", only(f2("stmt0h"), "r1ctrl", "r2ctrl"))
	if len(twoH) != 1 || twoH[0]["analytics_finished_6h"] != "2" || twoH[0]["org_ids_6h"] != "1" || twoH[0]["replicas_with_rows"] != "2" {
		t.Errorf("2-REPLICA EXACT stmt0h: %v, want analytics_finished_6h 2, org_ids_6h 1, replicas_with_rows 2", twoH)
	}
	// The gate counts the window over BOTH replicas: cap = the total W passes, cap = W - 1 fires, cap = the larger single-server count fires.
	var wMax uint64
	for _, n := range headWant {
		if n > wMax {
			wMax = n
		}
	}
	for _, cc := range []struct {
		label string
		capN  uint64
		fire  bool
	}{{"total W", wTotal, false}, {"total W - 1", wTotal - 1, true}, {"one server's W", wMax, true}} {
		_, _, err := rbpoolRows(e.ctx, r1, strings.Replace(f2("stmtT"), "> 9000000,", fmt.Sprintf("> %d,", cc.capN), 1))
		var ex *stdclickhouse.Exception
		fired := err != nil && errors.As(err, &ex) && ex.Code == 395
		if fired != cc.fire || (err != nil && !fired) {
			t.Errorf("2-REPLICA CAP %s (%d of total %d): err %v, want fired = %v", cc.label, cc.capN, wTotal, err, cc.fire)
		} else {
			t.Logf("2-REPLICA CAP %s (%d of total %d): fired = %v", cc.label, cc.capN, wTotal, fired)
		}
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
	for _, fc := range []struct{ label, c string }{{"stmtT", caps[0]}, {"stmtT", caps[1]}, {"stmtT", caps[2]}, {"stmt0", caps[0]}, {"stmt0", caps[1]}, {"stmt0", caps[2]}, {"stmt0h", caps[0]}, {"stmt0h", caps[1]}, {"stmt0h", caps[2]}} {
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
	if fires != 9 {
		t.Errorf("gates fired %d times, want 9 (3 statements x 3 caps)", fires)
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
		rows, err := e.admin.Query(e.ctx, `SELECT toString(type), read_rows FROM system.query_log WHERE log_comment = ? AND is_initial_query AND type != 'QueryStart' ORDER BY event_time_microseconds`, comment)
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
	if types, read := logOf(passCommentT); len(types) != 1 || types[0] != "QueryFinish" || read > 2*q+2*p+1 {
		t.Errorf("PASS stmtT query_log types %v read_rows %d, want [QueryFinish] and <= %d (2Q + 2P + 1)", types, read, 2*q+2*p+1)
	} else {
		t.Logf("PASS stmtT query_log types %v read_rows %d (2Q + 2P + 1 = %d)", types, read, 2*q+2*p+1)
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

// rbpoolClusterConfig lists both servers as the 2 replicas of cluster 'default' (the lane-form-14624 probe config form).
const rbpoolClusterConfig = `<clickhouse>
    <remote_servers>
        <default replace="replace">
            <shard>
                <replica><host>chr1</host><port>9000</port><user>rbpool</user><password>rbpool-test-only-pw</password></replica>
                <replica><host>chr2</host><port>9000</port><user>rbpool</user><password>rbpool-test-only-pw</password></replica>
            </shard>
        </default>
    </remote_servers>
</clickhouse>
`

// rbpoolStartReplicas starts 2 ClickHouse servers on one docker network, each with the 2-replica cluster 'default', and returns a
// connection to each.
func rbpoolStartReplicas(t *testing.T, ctx context.Context) (stdclickhouse.Conn, stdclickhouse.Conn) {
	t.Helper()
	nw, err := network.New(ctx)
	if err != nil {
		t.Fatalf("create docker network: %v", err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })
	configPath := filepath.Join(t.TempDir(), "zz-rbpool-cluster.xml")
	if err := os.WriteFile(configPath, []byte(rbpoolClusterConfig), 0o644); err != nil {
		t.Fatal(err)
	}
	var conns []stdclickhouse.Conn
	for _, alias := range []string{"chr1", "chr2"} {
		c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
			Image:          containers.ClickHouseImage,
			ExposedPorts:   []string{"9000/tcp"},
			Env:            map[string]string{"CLICKHOUSE_USER": "rbpool", "CLICKHOUSE_PASSWORD": "rbpool-test-only-pw", "CLICKHOUSE_DB": "default", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1"},
			Networks:       []string{nw.Name},
			NetworkAliases: map[string][]string{nw.Name: {alias}},
			Files:          []testcontainers.ContainerFile{{HostFilePath: configPath, ContainerFilePath: "/etc/clickhouse-server/config.d/zz-rbpool-cluster.xml", FileMode: 0o644}},
			WaitingFor:     wait.ForListeningPort("9000/tcp").WithStartupTimeout(2 * time.Minute),
		}, Started: true})
		if err != nil {
			t.Fatalf("start %s: %v", alias, err)
		}
		t.Cleanup(func() { _ = c.Terminate(context.Background()) })
		host, err := c.Host(ctx)
		if err != nil {
			t.Fatal(err)
		}
		port, err := c.MappedPort(ctx, "9000/tcp")
		if err != nil {
			t.Fatal(err)
		}
		conn, err := stdclickhouse.Open(&stdclickhouse.Options{Addr: []string{host + ":" + port.Port()}, Auth: stdclickhouse.Auth{Database: "default", Username: "rbpool", Password: "rbpool-test-only-pw"}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		deadline := time.Now().Add(90 * time.Second)
		for {
			if err = conn.Ping(ctx); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s not ready: %v", alias, err)
			}
			time.Sleep(time.Second)
		}
		conns = append(conns, conn)
	}
	// Both servers see the cluster: 2 replicas on each.
	for i, c := range conns {
		var n uint64
		if err := c.QueryRow(ctx, "SELECT count() FROM system.clusters WHERE cluster = 'default'").Scan(&n); err != nil || n != 2 {
			t.Fatalf("server %d: cluster 'default' has %d replicas (err %v), want 2", i+1, n, err)
		}
	}
	return conns[0], conns[1]
}
