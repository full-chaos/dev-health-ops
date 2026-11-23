//go:build integration

package chmigrate_test

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

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// Throwaway exec proof of a prod read form (READ-14548-PLAN-1): not for merge. pl14548Form is the form file byte for byte.
const pl14548FormSHA256 = "84a106be84f354a0edcb7759446b5f1b5b2670d938b1fd0f7bdbdd8fc382668c"

const pl14548Form = `-- NOT EXECUTED. READ-14548-PLAN-1 (system tables only; numbers, names and setting values only). acr-mcp, 2026-11-23. acp FORM read first; then pm runs it once.
-- Exec leg: a throwaway CI leg runs each statement (FORM-TEMPLATE-3); its run id and job id are in READ-14548-PLAN-1.md only.
--   Until that leg is green and acp reads it, pm does not run this file.
-- Why (CHAOS-14548): READ-14548-PARTS-3 S4b showed that the projector statement 7cbf3538fee3 = membershipEdgeCountSQL(ingest=true)
--   (acr internal/contextfabric/devhealthsource/edge_counts.go:49-56) read about 46.5k more rows per run from the first run after
--   the acr roll at 09:01Z. Inserts do not explain it. A CI cell on CH 26.7 (acr draft #2196, closed) showed that a repos rewrite moves the
--   read by under 250 rows and does not change the plan. This read asks which read step grew, and whether the server, its version
--   or the run settings changed at 09:01Z.
-- S0: server version and start time. S1: the last run before 09:01Z and the first run after it (query_log, one row): revision, tables,
--   views, used_* counts, ProfileEvents names present on one side only, and the settings that differ (key and both values).
-- S2a: is processors_profile_log on and does it cover the window. S2b: the read steps (MergeTreeSelect processors) of the same two runs,
--   per step: table name, processors, input and output rows and bytes. The query_id join is inside S2b and never output.
-- Output: no query text, no query_id, no hash of text, no org id. Setting keys starting with param_ and log_comment are left out of
--   settings_changed (a query parameter can hold an org id; log_comment is a free-text tag).
-- No org scope: system tables hold no org rows, so ORG-1 and the org prefix do not apply. No data table is read. No FINAL.
-- Cost gates are scoped to the day: they sum the active parts of the log table whose min_date..max_date holds 2026-11-23
--   (the parts the event_date filter can read). One log table per gate; each gated read reads its log table ONCE. WORK MATH: READ-14548-PLAN-1.md.
-- Named limits: (a) the log tables are per node; S1 runs_before = 0 or runs_after = 0 = the runs are on another node or out of TTL: STOP,
--   do not read S2b. (b) S2a/S2b code 60 UNKNOWN_TABLE = processors_profile_log not enabled. S2a read_processors_window = 0, or S2b
--   with fewer than 2 run_side values = the runs did not log processors (log_processors_profiles off). (c) S0 is the node that answers;
--   S1 before_revision / after_revision are the revisions the runs ran on.
-- Match key: startsWith(query, 'SELECT source, uniqExact(subject_kind') + type = 'QueryFinish' (edge_counts.go:50), as in PARTS-3.
--   The query column is read to match, never output.
-- SELECT only; no SET / SETTINGS.

-- S0 (no table): WANT 1 row, uptime_ok = 1. server_started after 2026-11-23 08:56 = the node restarted between the last run before 09:01Z and the first after.
SELECT version() AS ch_version, uptime() AS uptime_s, now() - uptime() AS server_started,
       uptime() > 0 AND now() - uptime() <= now() AS uptime_ok;

-- S1: one row. WANT runs_before >= 1 and runs_after >= 1 (else STOP, limit a). Read: before_* = the last run before 09:01Z, after_* = the first
--   run at or after 09:01Z. A changed revision, settings_changed not empty, a changed tables/views list, or events present on one side only
--   name what changed. Control that can fail: runs_before and runs_after count the matched runs (0 = wrong day, prefix or type).
--   The gate caps the day's query_log parts (9 M rows, 4 GB).
SELECT runs_before, runs_after, before_minute, after_minute, before_revision, after_revision,
       before_read_rows, after_read_rows, before_memory, after_memory,
       before_tables, after_tables, before_views, after_views,
       before_functions_n, after_functions_n, before_aggregates_n, after_aggregates_n, before_storages_n, after_storages_n,
       arrayFilter(e -> NOT mapContains(after_events, e), mapKeys(before_events)) AS events_only_before,
       arrayFilter(e -> NOT mapContains(before_events, e), mapKeys(after_events)) AS events_only_after,
       arrayMap(k -> tuple(k, mapContains(before_settings, k), before_settings[k], mapContains(after_settings, k), after_settings[k]),
                arrayFilter(k -> NOT startsWith(k, 'param_') AND k != 'log_comment'
                                 AND (mapContains(before_settings, k) != mapContains(after_settings, k) OR before_settings[k] != after_settings[k]),
                            arraySort(arrayDistinct(arrayConcat(mapKeys(before_settings), mapKeys(after_settings)))))) AS settings_changed
FROM (
  WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND min_date <= toDate('2026-11-23') AND max_date >= toDate('2026-11-23')) > 9000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND min_date <= toDate('2026-11-23') AND max_date >= toDate('2026-11-23')) > 4000000000,
    'STOP-COST-GATE')) AS cost_gate
  SELECT countIf(event_time < toDateTime('2026-11-23 09:01:00', 'UTC')) AS runs_before,
         countIf(event_time >= toDateTime('2026-11-23 09:01:00', 'UTC')) AS runs_after,
         toStartOfMinute(maxIf(event_time, event_time < toDateTime('2026-11-23 09:01:00', 'UTC'))) AS before_minute,
         toStartOfMinute(minIf(event_time, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC'))) AS after_minute,
         argMaxIf(revision, event_time_microseconds, event_time < toDateTime('2026-11-23 09:01:00', 'UTC')) AS before_revision,
         argMinIf(revision, event_time_microseconds, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC')) AS after_revision,
         argMaxIf(read_rows, event_time_microseconds, event_time < toDateTime('2026-11-23 09:01:00', 'UTC')) AS before_read_rows,
         argMinIf(read_rows, event_time_microseconds, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC')) AS after_read_rows,
         argMaxIf(memory_usage, event_time_microseconds, event_time < toDateTime('2026-11-23 09:01:00', 'UTC')) AS before_memory,
         argMinIf(memory_usage, event_time_microseconds, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC')) AS after_memory,
         argMaxIf(tables, event_time_microseconds, event_time < toDateTime('2026-11-23 09:01:00', 'UTC')) AS before_tables,
         argMinIf(tables, event_time_microseconds, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC')) AS after_tables,
         argMaxIf(views, event_time_microseconds, event_time < toDateTime('2026-11-23 09:01:00', 'UTC')) AS before_views,
         argMinIf(views, event_time_microseconds, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC')) AS after_views,
         argMaxIf(length(used_functions), event_time_microseconds, event_time < toDateTime('2026-11-23 09:01:00', 'UTC')) AS before_functions_n,
         argMinIf(length(used_functions), event_time_microseconds, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC')) AS after_functions_n,
         argMaxIf(length(used_aggregate_functions), event_time_microseconds, event_time < toDateTime('2026-11-23 09:01:00', 'UTC')) AS before_aggregates_n,
         argMinIf(length(used_aggregate_functions), event_time_microseconds, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC')) AS after_aggregates_n,
         argMaxIf(length(used_storages), event_time_microseconds, event_time < toDateTime('2026-11-23 09:01:00', 'UTC')) AS before_storages_n,
         argMinIf(length(used_storages), event_time_microseconds, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC')) AS after_storages_n,
         argMaxIf(ProfileEvents, event_time_microseconds, event_time < toDateTime('2026-11-23 09:01:00', 'UTC')) AS before_events,
         argMinIf(ProfileEvents, event_time_microseconds, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC')) AS after_events,
         argMaxIf(Settings, event_time_microseconds, event_time < toDateTime('2026-11-23 09:01:00', 'UTC')) AS before_settings,
         argMinIf(Settings, event_time_microseconds, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC')) AS after_settings
  FROM system.query_log
  WHERE cost_gate = 0
    AND event_date = toDate('2026-11-23')
    AND event_time >= toDateTime('2026-11-23 08:30:00', 'UTC') AND event_time < toDateTime('2026-11-23 09:30:00', 'UTC')
    AND type = 'QueryFinish'
    AND startsWith(query, 'SELECT source, uniqExact(subject_kind')
);

-- S2a control: is processors_profile_log on, and does it cover the window? WANT processors_rows_day > 0, processors_rows_window > 0 and
--   read_processors_window > 0 (0 = off, wrong day or TTL: S2b is not run). Code 60 = the table is not enabled. Counts only.
--   The gate caps the day's processors_profile_log parts (9 M rows, 4 GB).
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = 'system' AND table = 'processors_profile_log' AND min_date <= toDate('2026-11-23') AND max_date >= toDate('2026-11-23')) > 9000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = 'system' AND table = 'processors_profile_log' AND min_date <= toDate('2026-11-23') AND max_date >= toDate('2026-11-23')) > 4000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT count() AS processors_rows_day,
       countIf(event_time >= toDateTime('2026-11-23 08:30:00', 'UTC') AND event_time < toDateTime('2026-11-23 09:30:00', 'UTC')) AS processors_rows_window,
       countIf(event_time >= toDateTime('2026-11-23 08:30:00', 'UTC') AND event_time < toDateTime('2026-11-23 09:30:00', 'UTC') AND startsWith(name, 'MergeTreeSelect')) AS read_processors_window
FROM system.processors_profile_log
WHERE cost_gate = 0 AND event_date = toDate('2026-11-23');

-- S2b: the read steps of the two runs of S1. One row per run_side, run_minute and read step. WANT: report. Read: per step_table, compare
--   the steps (count and output_rows) of run_side 'before' and 'after'; a step with about 46.5k more output_rows, or an extra step, is the arm
--   that grew. Control that can fail: 2 run_side values (fewer = limit a or b). Run S2b only when S1 runs_before >= 1 and runs_after >= 1.
--   Two gates (query_log and processors_profile_log of the day), each 4.5 M rows, 2 GB; each table is read once.
SELECT r.run_side AS run_side, r.run_minute AS run_minute, p.plan_step_name AS step_name, p.plan_step_description AS step_table, p.plan_step AS step_id,
       count() AS read_processors, sum(p.input_rows) AS input_rows, sum(p.output_rows) AS output_rows, sum(p.output_bytes) AS output_bytes
FROM (
  WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = 'system' AND table = 'processors_profile_log' AND min_date <= toDate('2026-11-23') AND max_date >= toDate('2026-11-23')) > 4500000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = 'system' AND table = 'processors_profile_log' AND min_date <= toDate('2026-11-23') AND max_date >= toDate('2026-11-23')) > 2000000000,
    'STOP-COST-GATE')) AS cost_gate
  SELECT query_id, plan_step_name, plan_step_description, plan_step, input_rows, output_rows, output_bytes
  FROM system.processors_profile_log
  WHERE cost_gate = 0
    AND event_date = toDate('2026-11-23')
    AND event_time >= toDateTime('2026-11-23 08:30:00', 'UTC') AND event_time < toDateTime('2026-11-23 09:30:00', 'UTC')
    AND startsWith(name, 'MergeTreeSelect')
) AS p
INNER JOIN (
  SELECT tupleElement(run, 1) AS query_id, tupleElement(run, 2) AS run_side, tupleElement(run, 3) AS run_minute
  FROM (
    WITH (SELECT throwIf(
      (SELECT sum(rows) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND min_date <= toDate('2026-11-23') AND max_date >= toDate('2026-11-23')) > 4500000
      OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log' AND min_date <= toDate('2026-11-23') AND max_date >= toDate('2026-11-23')) > 2000000000,
      'STOP-COST-GATE')) AS cost_gate
    SELECT arrayJoin([
             tuple(argMaxIf(query_id, event_time_microseconds, event_time < toDateTime('2026-11-23 09:01:00', 'UTC')), 'before',
                   toStartOfMinute(maxIf(event_time, event_time < toDateTime('2026-11-23 09:01:00', 'UTC')))),
             tuple(argMinIf(query_id, event_time_microseconds, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC')), 'after',
                   toStartOfMinute(minIf(event_time, event_time >= toDateTime('2026-11-23 09:01:00', 'UTC'))))]) AS run
    FROM system.query_log
    WHERE cost_gate = 0
      AND event_date = toDate('2026-11-23')
      AND event_time >= toDateTime('2026-11-23 08:30:00', 'UTC') AND event_time < toDateTime('2026-11-23 09:30:00', 'UTC')
      AND type = 'QueryFinish'
      AND startsWith(query, 'SELECT source, uniqExact(subject_kind')
  )
) AS r ON p.query_id = r.query_id
GROUP BY run_side, run_minute, step_name, step_table, step_id
ORDER BY run_side DESC, step_table, step_id;
`

// Synthetic rows of one fake org. Merges stop so each INSERT stays one part: 3 parts (2+3+4 rows) before run 1, a 4th part (5 rows) before run 2.
const pl14548Fixture = `
SYSTEM STOP MERGES project_membership_transitions;
INSERT INTO project_membership_transitions (org_id, repo_id, subject_kind, subject_id, provider, from_project_id, to_project_id, occurred_at, last_synced, event_id) VALUES
('14548000-0000-4000-8000-0000000000aa', '00000000-0000-4000-8000-000000000001', 'work_item', 'w1', 'linear', '', 'p1', '2026-11-20 08:00:00.000', '2026-11-20 08:00:00.000', 'e1'),
('14548000-0000-4000-8000-0000000000aa', '00000000-0000-4000-8000-000000000001', 'work_item', 'w2', 'linear', '', 'p1', '2026-11-20 08:00:01.000', '2026-11-20 08:00:01.000', 'e2');
INSERT INTO project_membership_transitions (org_id, repo_id, subject_kind, subject_id, provider, from_project_id, to_project_id, occurred_at, last_synced, event_id) VALUES
('14548000-0000-4000-8000-0000000000aa', '00000000-0000-4000-8000-000000000001', 'work_item', 'w3', 'linear', '', 'p2', '2026-11-20 09:00:00.000', '2026-11-20 09:00:00.000', 'e3'),
('14548000-0000-4000-8000-0000000000aa', '00000000-0000-4000-8000-000000000001', 'work_item', 'w4', 'linear', '', 'p2', '2026-11-20 09:00:01.000', '2026-11-20 09:00:01.000', 'e4'),
('14548000-0000-4000-8000-0000000000aa', '00000000-0000-4000-8000-000000000001', 'work_item', 'w5', 'linear', '', 'p2', '2026-11-20 09:00:02.000', '2026-11-20 09:00:02.000', 'e5');
INSERT INTO project_membership_transitions (org_id, repo_id, subject_kind, subject_id, provider, from_project_id, to_project_id, occurred_at, last_synced, event_id) VALUES
('14548000-0000-4000-8000-0000000000aa', '00000000-0000-4000-8000-000000000002', 'work_item', 'w6', 'linear', 'p1', 'p2', '2026-11-21 08:00:00.000', '2026-11-21 08:00:00.000', 'e6'),
('14548000-0000-4000-8000-0000000000aa', '00000000-0000-4000-8000-000000000002', 'work_item', 'w7', 'linear', 'p1', 'p2', '2026-11-21 08:00:01.000', '2026-11-21 08:00:01.000', 'e7'),
('14548000-0000-4000-8000-0000000000aa', '00000000-0000-4000-8000-000000000002', 'work_item', 'w8', 'linear', 'p1', 'p2', '2026-11-21 08:00:02.000', '2026-11-21 08:00:02.000', 'e8'),
('14548000-0000-4000-8000-0000000000aa', '00000000-0000-4000-8000-000000000002', 'work_item', 'w9', 'linear', 'p1', 'p2', '2026-11-21 08:00:03.000', '2026-11-21 08:00:03.000', 'e9');
`

const pl14548Fourth = `INSERT INTO project_membership_transitions (org_id, repo_id, subject_kind, subject_id, provider, from_project_id, to_project_id, occurred_at, last_synced, event_id) VALUES
('14548000-0000-4000-8000-0000000000aa', '00000000-0000-4000-8000-000000000003', 'work_item', 'w10', 'linear', '', 'p3', '2026-11-22 08:00:00.000', '2026-11-22 08:00:00.000', 'e10'),
('14548000-0000-4000-8000-0000000000aa', '00000000-0000-4000-8000-000000000003', 'work_item', 'w11', 'linear', '', 'p3', '2026-11-22 08:00:01.000', '2026-11-22 08:00:01.000', 'e11'),
('14548000-0000-4000-8000-0000000000aa', '00000000-0000-4000-8000-000000000003', 'work_item', 'w12', 'linear', '', 'p3', '2026-11-22 08:00:02.000', '2026-11-22 08:00:02.000', 'e12'),
('14548000-0000-4000-8000-0000000000aa', '00000000-0000-4000-8000-000000000003', 'work_item', 'w13', 'linear', '', 'p3', '2026-11-22 08:00:03.000', '2026-11-22 08:00:03.000', 'e13'),
('14548000-0000-4000-8000-0000000000aa', '00000000-0000-4000-8000-000000000003', 'work_item', 'w14', 'linear', '', 'p3', '2026-11-22 08:00:04.000', '2026-11-22 08:00:04.000', 'e14')`

// The same first token as the projector statement (edge_counts.go:50); it reads the whole transitions table once.
const pl14548Like = `SELECT source, uniqExact(subject_kind, subject_id) AS edge_count
FROM (SELECT 'transition' AS source, subject_kind, subject_id FROM project_membership_transitions)
GROUP BY source`

type pl14548Statement struct{ label, text string }

func pl14548Split(body string) []pl14548Statement {
	var out []pl14548Statement
	var chunk []string
	names := []string{"S0", "S1", "S2a", "S2b"}
	for _, line := range strings.Split(body, "\n") {
		chunk = append(chunk, line)
		if !strings.HasSuffix(strings.TrimSpace(chmigrate.StripLineComments(line)), ";") {
			continue
		}
		text := strings.TrimSuffix(strings.TrimRight(strings.Join(chunk, "\n"), " \t\n"), ";")
		label := ""
		if len(out) < len(names) {
			label = names[len(out)]
		}
		out = append(out, pl14548Statement{label: label, text: text})
		chunk = nil
	}
	return out
}

func pl14548Render(v any, dbType string) string {
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

func pl14548Run(ctx context.Context, conn driver.Conn, text, comment string, extra clickhouse.Settings) ([]map[string]string, error) {
	settings := clickhouse.Settings{"log_comment": comment}
	for k, v := range extra {
		settings[k] = v
	}
	rows, err := conn.Query(clickhouse.Context(ctx, clickhouse.WithSettings(settings)), text)
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
			row[name] = pl14548Render(cells[i], types[i].DatabaseTypeName())
		}
		got = append(got, row)
	}
	return got, rows.Err()
}

func pl14548Log(ctx context.Context, t *testing.T, conn driver.Conn, comment string) ([]string, uint64) {
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

func pl14548Count(ctx context.Context, t *testing.T, conn driver.Conn, query string, args ...any) uint64 {
	t.Helper()
	var n uint64
	if err := conn.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func pl14548Uint(s string) uint64 {
	var n uint64
	fmt.Sscan(s, &n)
	return n
}

func TestReadPlanForm14548ExecutesAtTheChainHead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	sum := sha256.Sum256([]byte(pl14548Form))
	if got := hex.EncodeToString(sum[:]); got != pl14548FormSHA256 {
		t.Fatalf("form sha256 = %s, want %s", got, pl14548FormSHA256)
	}
	statements := pl14548Split(pl14548Form)
	byLabel := map[string]string{}
	labels := make([]string, 0, len(statements))
	for _, s := range statements {
		labels = append(labels, s.label)
		byLabel[s.label] = s.text
	}
	if strings.Join(labels, ",") != "S0,S1,S2a,S2b" {
		t.Fatalf("form statements = %v", labels)
	}
	code := chmigrate.StripLineComments(pl14548Form)
	for _, forbidden := range []string{`(?i)\b(FORMAT|EXPLAIN|INSERT|FINAL|LIMIT)\b`, `(?i)\bSETTINGS\s+\w+\s*=`, `(?im)^\s*SET\b`} {
		if regexp.MustCompile(forbidden).MatchString(code) {
			t.Fatalf("form holds a forbidden keyword (%s)", forbidden)
		}
	}
	// Gate literals per statement: table -> count of gates on that table, and the cap literals of the statement.
	type gateSpec struct {
		tables      map[string]int
		rows, bytes string
	}
	gates := map[string]gateSpec{
		"S1":  {map[string]int{"query_log": 1}, "> 9000000", "> 4000000000"},
		"S2a": {map[string]int{"processors_profile_log": 1}, "> 9000000", "> 4000000000"},
		"S2b": {map[string]int{"processors_profile_log": 1, "query_log": 1}, "> 4500000", "> 2000000000"},
	}
	for _, s := range statements {
		g, gated := gates[s.label]
		n := 0
		for _, c := range g.tables {
			n += c
		}
		for _, literal := range []string{"> 9000000", "> 4000000000", "> 4500000", "> 2000000000", "'STOP-COST-GATE'", "cost_gate = 0"} {
			want := 0
			if gated && (literal == g.rows || literal == g.bytes || literal == "'STOP-COST-GATE'" || literal == "cost_gate = 0") {
				want = n
			}
			if got := strings.Count(s.text, literal); got != want {
				t.Fatalf("%s: %q found %d times, want %d", s.label, literal, got, want)
			}
		}
		for table, c := range g.tables {
			scope := "database = 'system' AND table = '" + table + "' AND min_date <= toDate('2026-11-23') AND max_date >= toDate('2026-11-23')"
			if got := strings.Count(s.text, scope); got != 2*c {
				t.Fatalf("%s: day-scoped gate on %s found %d times, want %d", s.label, table, got, 2*c)
			}
			if got := strings.Count(s.text, "FROM system."+table+"\n"); got != c {
				t.Fatalf("%s: %d reads of system.%s, want %d", s.label, got, table, c)
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
	var version string
	if err := conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("schema head %s, clickhouse %s", result.Head, version)
	var ppDefault string
	if err := conn.QueryRow(ctx, "SELECT value FROM system.settings WHERE name = 'log_processors_profiles'").Scan(&ppDefault); err != nil {
		t.Fatal(err)
	}
	ppOn := pl14548Count(ctx, t, conn, "SELECT count() FROM system.tables WHERE database = 'system' AND name = 'processors_profile_log'") == 1
	t.Logf("log_processors_profiles default %s; processors_profile_log table present: %v", ppDefault, ppOn)

	for _, statement := range chmigrate.SplitStatements(pl14548Fixture) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("fixture: %v\n%s", err, statement)
		}
	}

	// Run 1 on 3 parts; the cut T one second later; a 4th part; run 2 with one more setting. The test session asks for processor profiles.
	likeComments := []string{fmt.Sprintf("pl14548-like-1-%d", time.Now().UnixNano()), fmt.Sprintf("pl14548-like-2-%d", time.Now().UnixNano())}
	profiles := clickhouse.Settings{"log_processors_profiles": 1}
	if _, err := pl14548Run(ctx, conn, pl14548Like, likeComments[0], profiles); err != nil {
		t.Fatalf("like run 1: %v", err)
	}
	time.Sleep(1100 * time.Millisecond)
	var cut, day, nextDay string
	if err := conn.QueryRow(ctx, "SELECT toString(now('UTC')), toString(toDate(now('UTC'))), toString(toDate(now('UTC')) + 1)").Scan(&cut, &day, &nextDay); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	if err := conn.Exec(ctx, pl14548Fourth); err != nil {
		t.Fatalf("fourth part: %v", err)
	}
	if _, err := pl14548Run(ctx, conn, pl14548Like, likeComments[1], clickhouse.Settings{"log_processors_profiles": 1, "max_threads": 3}); err != nil {
		t.Fatalf("like run 2: %v", err)
	}
	dated := func(text string) string {
		text = strings.ReplaceAll(text, "2026-11-23 08:30:00", day+" 00:00:00")
		text = strings.ReplaceAll(text, "2026-11-23 09:30:00", nextDay+" 00:00:00")
		text = strings.ReplaceAll(text, "2026-11-23 09:01:00", cut)
		return strings.ReplaceAll(text, "2026-11-23", day)
	}
	t.Logf("dated copies: window %s 00:00:00 .. %s 00:00:00 UTC, cut %s (the CI clock, not the prod day)", day, nextDay, cut)

	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the logs are not flushable by this user: %v", err)
	}
	type likeRun struct {
		id                                string
		readRows, memory, revision, parts uint64
	}
	var likes []likeRun
	for _, c := range likeComments {
		var r likeRun
		if err := conn.QueryRow(ctx, `SELECT query_id, read_rows, toUInt64(memory_usage), toUInt64(revision), ProfileEvents['SelectedParts']
			FROM system.query_log WHERE log_comment = ? AND type = 'QueryFinish'`, c).Scan(&r.id, &r.readRows, &r.memory, &r.revision, &r.parts); err != nil {
			t.Fatalf("like run in query_log: %v", err)
		}
		t.Logf("LIKE run: read_rows=%d memory=%d revision=%d selected_parts=%d", r.readRows, r.memory, r.revision, r.parts)
		likes = append(likes, r)
	}
	if likes[0].readRows != 9 || likes[1].readRows != 14 {
		t.Fatalf("like runs read %d and %d rows, want 9 and 14", likes[0].readRows, likes[1].readRows)
	}

	logToday := func(table string) uint64 {
		return pl14548Count(ctx, t, conn, "SELECT count() FROM system."+table+" WHERE event_date = toDate(?)", day)
	}
	partsOf := func(table string) uint64 {
		return pl14548Count(ctx, t, conn, "SELECT count() FROM system.parts WHERE database = 'system' AND table = ?", table)
	}
	q0, pq0 := logToday("query_log"), partsOf("query_log")
	var pp0, ppp0 uint64
	if ppOn {
		pp0, ppp0 = logToday("processors_profile_log"), partsOf("processors_profile_log")
	}
	t.Logf("before PASS: query_log rows today=%d parts=%d; processors_profile_log rows today=%d parts=%d", q0, pq0, pp0, ppp0)

	passComments := map[string]string{}
	passRows := map[string][]map[string]string{}
	for _, s := range statements {
		passComments[s.label] = fmt.Sprintf("pl14548-pass-%s-%d", s.label, time.Now().UnixNano())
		got, err := pl14548Run(ctx, conn, dated(s.text), passComments[s.label], nil)
		if err != nil {
			var ex *clickhouse.Exception
			if (s.label == "S2a" || s.label == "S2b") && !ppOn && errors.As(err, &ex) && ex.Code == 60 {
				t.Logf("PASS %s: processors_profile_log absent, code 60 UNKNOWN_TABLE as the form says", s.label)
				continue
			}
			t.Errorf("PASS %s: SQL error: %v", s.label, err)
			continue
		}
		passRows[s.label] = got
		t.Logf("PASS %s ROWS %v", s.label, got)
	}

	// S0: one row, uptime_ok true.
	if r := passRows["S0"]; len(r) != 1 || (r[0]["uptime_ok"] != "1" && r[0]["uptime_ok"] != "true") || r[0]["ch_version"] != version || pl14548Uint(r[0]["uptime_s"]) == 0 {
		t.Errorf("S0: %v, want 1 row, uptime_ok 1, ch_version %s, uptime_s > 0", r, version)
	}
	// S1: run 1 before the cut, run 2 after; values are the runs' own log values; only max_threads differs.
	if r := passRows["S1"]; len(r) != 1 {
		t.Errorf("S1: %d rows, want 1", len(r))
	} else {
		s1 := r[0]
		want := map[string]string{
			"runs_before": "1", "runs_after": "1",
			"before_read_rows": fmt.Sprint(likes[0].readRows), "after_read_rows": fmt.Sprint(likes[1].readRows),
			"before_memory": fmt.Sprint(likes[0].memory), "after_memory": fmt.Sprint(likes[1].memory),
			"before_revision": fmt.Sprint(likes[0].revision), "after_revision": fmt.Sprint(likes[1].revision),
		}
		for k, v := range want {
			if s1[k] != v {
				t.Errorf("S1 %s = %s, want %s", k, s1[k], v)
			}
		}
		for _, k := range []string{"before_tables", "after_tables"} {
			if !strings.Contains(s1[k], ".project_membership_transitions") {
				t.Errorf("S1 %s = %s, want the transitions table", k, s1[k])
			}
		}
		for _, k := range []string{"before_minute", "after_minute"} {
			if s1[k] == "" || strings.HasPrefix(s1[k], "1970") {
				t.Errorf("S1 %s empty: %s", k, s1[k])
			}
		}
		if c := s1["settings_changed"]; !strings.Contains(c, "max_threads") || strings.Count(c, "[") != 2 || strings.Contains(c, "log_comment") || strings.Contains(c, "log_processors_profiles") {
			t.Errorf("S1 settings_changed = %s, want exactly the max_threads entry", c)
		}
	}
	// S2a and S2b: processor profiles of the two runs.
	if ppOn {
		if r := passRows["S2a"]; len(r) != 1 || pl14548Uint(r[0]["processors_rows_day"]) == 0 || r[0]["processors_rows_window"] != r[0]["processors_rows_day"] || pl14548Uint(r[0]["read_processors_window"]) == 0 {
			t.Errorf("S2a: %v, want day > 0 = window (the window is the whole CI day), read_processors_window > 0", r)
		}
		sides := map[string]uint64{}
		steps := map[string]int{}
		for _, r := range passRows["S2b"] {
			if !strings.HasSuffix(r["step_table"], ".project_membership_transitions") {
				t.Errorf("S2b step on an unexpected table: %v", r)
				continue
			}
			sides[r["run_side"]] += pl14548Uint(r["output_rows"])
			steps[r["run_side"]]++
		}
		if sides["before"] != 9 || sides["after"] != 14 || steps["before"] != 1 || steps["after"] != 1 {
			t.Errorf("S2b: output_rows %v steps %v, want before 9 / 1 step, after 14 / 1 step", sides, steps)
		}
	}

	// Control mutations: each must change the result so its WANT fails.
	mutateN := func(label, from, to string, n int) []map[string]string {
		mutated := strings.Replace(dated(byLabel[label]), from, to, n)
		if mutated == dated(byLabel[label]) {
			t.Fatalf("MUTATE %s: %q not found", label, from)
		}
		got, err := pl14548Run(ctx, conn, mutated, fmt.Sprintf("pl14548-mutate-%s-%d", label, time.Now().UnixNano()), nil)
		if err != nil {
			t.Fatalf("MUTATE %s %q: %v", label, from, err)
		}
		t.Logf("MUTATE %s %q -> %q ROWS %v", label, from, to, got)
		return got
	}
	mutate := func(label, from, to string) []map[string]string { return mutateN(label, from, to, 1) }
	wrongDay := "toDate('2030-01-01')"
	if got := mutate("S0", "uptime() > 0", "uptime() > 4000000000"); len(got) != 1 || (got[0]["uptime_ok"] != "0" && got[0]["uptime_ok"] != "false") {
		t.Errorf("MUTATE S0 uptime: %v, want uptime_ok 0", got)
	}
	if got := mutateN("S1", "event_date = toDate('"+day+"')", "event_date = "+wrongDay, -1); len(got) != 1 || got[0]["runs_before"] != "0" || got[0]["runs_after"] != "0" {
		t.Errorf("MUTATE S1 day: %v, want runs 0 and 0", got)
	}
	if got := mutateN("S1", "'SELECT source, uniqExact(subject_kind'", "'SELECT zource, uniqExact(subject_kind'", -1); len(got) != 1 || got[0]["runs_before"] != "0" || got[0]["runs_after"] != "0" {
		t.Errorf("MUTATE S1 prefix: %v, want runs 0 and 0", got)
	}
	if got := mutate("S1", "type = 'QueryFinish'", "type = 'ExceptionWhileProcessing'"); len(got) != 1 || got[0]["runs_before"] != "0" || got[0]["runs_after"] != "0" {
		t.Errorf("MUTATE S1 type: %v, want runs 0 and 0", got)
	}
	if got := mutateN("S1", "toDateTime('"+cut+"', 'UTC')", "toDateTime('"+day+" 00:00:00', 'UTC')", -1); len(got) != 1 || got[0]["runs_before"] != "0" || got[0]["runs_after"] != "2" {
		t.Errorf("MUTATE S1 cut at the window start: %v, want runs_before 0, runs_after 2", got)
	}
	if got := mutate("S1", " AND k != 'log_comment'", ""); len(got) != 1 || !strings.Contains(got[0]["settings_changed"], "log_comment") {
		t.Errorf("MUTATE S1 log_comment kept: %v, want log_comment in settings_changed", got)
	}
	if ppOn {
		if got := mutateN("S2a", "event_date = toDate('"+day+"')", "event_date = "+wrongDay, -1); len(got) != 1 || got[0]["processors_rows_day"] != "0" || got[0]["read_processors_window"] != "0" {
			t.Errorf("MUTATE S2a day: %v, want 0 rows counted", got)
		}
		if got := mutateN("S2b", "'SELECT source, uniqExact(subject_kind'", "'SELECT zource, uniqExact(subject_kind'", -1); len(got) != 0 {
			t.Errorf("MUTATE S2b prefix: %v, want 0 rows", got)
		}
		if got := mutateN("S2b", "event_date = toDate('"+day+"')", "event_date = "+wrongDay, -1); len(got) != 0 {
			t.Errorf("MUTATE S2b day: %v, want 0 rows", got)
		}
		if got := mutate("S2b", "startsWith(name, 'MergeTreeSelect')", "startsWith(name, 'MergeTreeSelectz')"); len(got) != 0 {
			t.Errorf("MUTATE S2b processor name: %v, want 0 rows", got)
		}
	}

	// PASS-path bounds. The logs are live, so the bound is rows before <= read_rows <= rows after + 2P + 1 per log table.
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatal(err)
	}
	q1, pq1 := logToday("query_log"), partsOf("query_log")
	var pp1, ppp1 uint64
	if ppOn {
		pp1, ppp1 = logToday("processors_profile_log"), partsOf("processors_profile_log")
	}
	pq, ppp := max(pq0, pq1), max(ppp0, ppp1)
	for _, s := range statements {
		if _, ok := passRows[s.label]; !ok {
			continue
		}
		types, read := pl14548Log(ctx, t, conn, passComments[s.label])
		if len(types) != 1 || types[0] != "QueryFinish" {
			t.Errorf("PASS %s: query_log types %v, want [QueryFinish]", s.label, types)
		}
		var lo, hi uint64
		switch s.label {
		case "S0":
			lo, hi = 0, 1
		case "S1":
			lo, hi = q0, q1+2*pq+1
		case "S2a":
			lo, hi = pp0, pp1+2*ppp+1
		case "S2b":
			lo, hi = q0+pp0, q1+pp1+2*pq+2*ppp+2
		}
		t.Logf("PASS %s bound: %d <= read_rows %d <= %d", s.label, lo, read, hi)
		if read < lo || read > hi {
			t.Errorf("PASS %s: read_rows %d outside [%d, %d]", s.label, read, lo, hi)
		}
	}

	// Gate fires: each cap literal occurrence lowered to 0 in a test copy. WANT code 395 STOP-COST-GATE, ExceptionBeforeStart, no log row read.
	fires := 0
	for _, s := range statements {
		g, gated := gates[s.label]
		if !gated {
			continue
		}
		if !ppOn && g.tables["processors_profile_log"] > 0 {
			t.Logf("GATE %s: not fired, processors_profile_log absent", s.label)
			continue
		}
		for _, literal := range []string{g.rows, g.bytes} {
			for occurrence := 1; occurrence <= strings.Count(s.text, literal); occurrence++ {
				idx := -1
				for i := 0; i < occurrence; i++ {
					idx += 1 + strings.Index(s.text[idx+1:], literal)
				}
				lowered := s.text[:idx] + "> 0" + s.text[idx+len(literal):]
				comment := fmt.Sprintf("pl14548-gate-%s-%d-%d", s.label, occurrence, time.Now().UnixNano())
				_, err := pl14548Run(ctx, conn, dated(lowered), comment, nil)
				var ex *clickhouse.Exception
				if err == nil || !errors.As(err, &ex) || ex.Code != 395 || !strings.Contains(ex.Message, "STOP-COST-GATE") {
					t.Errorf("GATE %s %s #%d lowered to 0: err = %v, want code 395 STOP-COST-GATE", s.label, literal, occurrence, err)
					continue
				}
				if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
					t.Fatal(err)
				}
				types, read := pl14548Log(ctx, t, conn, comment)
				bound := 2*partsOf("query_log") + 2*max(ppp, partsOf("processors_profile_log")) + 2
				t.Logf("GATE %s %s #%d: code %d, query_log types=%v read_rows=%d bound=%d", s.label, literal, occurrence, ex.Code, types, read, bound)
				if len(types) != 1 || types[0] != "ExceptionBeforeStart" {
					t.Errorf("GATE %s %s #%d: query_log types %v, want [ExceptionBeforeStart]", s.label, literal, occurrence, types)
				}
				if read > bound {
					t.Errorf("GATE %s %s #%d: read_rows %d over %d", s.label, literal, occurrence, read, bound)
				}
				fires++
			}
		}
	}
	wantFires := 8
	if !ppOn {
		wantFires = 2
	}
	if fires != wantFires {
		t.Errorf("gates fired %d times, want %d", fires, wantFires)
	}

	// Output cell: no column is named like query text or id, and no value holds a run's query_id or the statement text.
	secrets := []string{likes[0].id, likes[1].id, "uniqExact(subject_kind", "14548000-0000-4000-8000-0000000000aa"}
	leaks := func(label, text string) []string {
		got, err := pl14548Run(ctx, conn, dated(text), fmt.Sprintf("pl14548-output-%s-%d", label, time.Now().UnixNano()), nil)
		if err != nil {
			t.Fatalf("OUTPUT %s: %v", label, err)
		}
		var bad []string
		for _, row := range got {
			for name, value := range row {
				if name == "query" || name == "query_id" || name == "initial_query_id" || name == "formatted_query" || name == "normalized_query_hash" {
					bad = append(bad, "column "+name)
				}
				for _, secret := range secrets {
					if strings.Contains(value, secret) {
						bad = append(bad, name+" holds "+secret)
					}
				}
			}
		}
		return bad
	}
	for _, s := range statements {
		if _, ok := passRows[s.label]; !ok {
			continue
		}
		if bad := leaks(s.label, s.text); len(bad) != 0 {
			t.Errorf("OUTPUT %s: %v", s.label, bad)
		} else {
			t.Logf("OUTPUT %s: no query text, query_id or org id", s.label)
		}
	}
	if ppOn {
		mutant := strings.Replace(byLabel["S2b"], "SELECT r.run_side AS run_side,", "SELECT r.run_side AS run_side, p.query_id AS leaked,", 1)
		mutant = strings.Replace(mutant, "GROUP BY run_side,", "GROUP BY leaked, run_side,", 1)
		if bad := leaks("S2b-mutant", mutant); len(bad) == 0 {
			t.Errorf("OUTPUT mutant with query_id: nothing found, the cell cannot fail")
		}
	}
	s1Mutant := strings.Replace(byLabel["S1"], "SELECT runs_before, runs_after,", "SELECT runs_before, runs_after, q_text,", 1)
	s1Mutant = strings.Replace(s1Mutant, "  SELECT countIf(", "  SELECT any(query) AS q_text, countIf(", 1)
	if bad := leaks("S1-mutant", s1Mutant); len(bad) == 0 {
		t.Errorf("OUTPUT S1 mutant with query text: nothing found, the cell cannot fail")
	}
}
