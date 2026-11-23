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

// Throwaway exec proof of a prod read form (READ-14548-PARTS-3): not for merge. pt14548Form is the form file byte for byte.
const (
	pt14548RowsCap  = "> 9000000"
	pt14548BytesCap = "> 4000000000"
)

const pt14548FormSHA256 = "f62687d522d3bb31e9953dc49a913accc08c2182727c147d076be09238122b63"

const pt14548Form = `-- NOT EXECUTED. READ-14548-PARTS-3 (counts only, system tables only). acr-mcp, 2026-11-23. acp FORM read first; then pm runs it once.
-- Supersedes -2 (f297765171b1) and -1 (7556004a0565), which stay unedited. Exec leg: a throwaway CI leg runs each statement (FORM-TEMPLATE-3 6b9e73a3c45a);
--   its run id and job id are in READ-14548-PARTS-3.md only. Until that leg is green and acp reads it, pm does not run this file.
-- Why (CHAOS-14548): projector statement 7cbf3538fee3 = membershipEdgeCountSQL(ingest=true), acr
--   internal/contextfabric/devhealthsource/edge_counts.go:49-56 (same text at de8b1693 and c3e9ae5d) rose from about 114 MB
--   (08:15-08:56Z) to 141-147 MB (09:02-09:23Z, first run after the acr roll at 09:01Z). SQL text, binding (org_id only), settings and order
--   did not change. Its inputs (teams_projects_edges.go at c3e9ae5d): project_membership_transitions (no FINAL), project_membership_presence,
--   work_items FINAL, projects FINAL, repos FINAL.
-- Change from -1: (1) project_membership_presence is a VIEW at CH head 170 (baseline head.json: engine View, reads project_membership_transitions
--   FINAL in 4 places and work_items): it has no parts, so -1 S0 "WANT 5 rows" could never hold. Parts are read for the 4 real tables;
--   S0b reads system.tables to show the view exists. (2) part_log row cap 20 M -> 9 M so that cap + 2P + 1 stays under the 20 M budget.
--   (3) S4a/S4b read system.query_log: the run-by-run discriminator. (4) Output is counts, bytes, ms and part/mark counts only:
--   no query text, no query_id, no hash; S4b gives each run's minute (toStartOfMinute) to place it before or after 09:01Z.
-- Change from -2: (5) S4b has no LIMIT. With ORDER BY + LIMIT, CH 26.7 lazy materialization re-reads the granules of the output rows, so
--   read_rows is more than one scan (-2 exec leg: read_rows 9345 on 8366 log rows; 8399 with lazy materialization off). Without LIMIT the
--   read is one scan. Row-count bound instead: run S4b only when S4a runs_prefix_window <= 1000, else STOP.
-- Read of S4b (the key discriminator): read_rows up about 29 % from the 08:xx runs to the 09:02+ runs = more rows (data growth);
--   read_rows flat but memory_usage up = part count / FINAL fold / server state (S0a, S2, S3 say which); selected_parts up with read_rows flat = more parts.
-- No org scope: system tables hold no org rows, so ORG-1 and the org prefix do not apply. No data table is read.
-- The FINAL double-read rule of FORM-TEMPLATE-3 does not apply: no statement reads a table with FINAL. Each of S1, S2, S4a, S4b reads its
--   one system log table ONCE; the gate reads system.parts twice (rows, bytes). WORK MATH: READ-14548-PARTS-3.md.
-- Named limits: (a) query_log and part_log are per node. If prod CH is a multi-replica service, the projector runs on other replicas are not seen
--   by these statements; S4a runs_prefix_window = 0 while the projector ran then names this. (b) query_log keeps rows only for its TTL;
--   S4a rows_window = 0 = the window is gone or query_log is off. (c) S1/S2 error code 60 UNKNOWN_TABLE = part_log is not enabled: S2 is not run, S3 is the part-state read.
-- Match key for S4: startsWith(query, 'SELECT source, uniqExact(subject_kind') + type = 'QueryFinish' (edge_counts.go:50 is the first token of the
--   text). The query column is read to match, never output. S4a prefix_hashes = how many distinct normalized_query_hash values the match
--   holds (a count; WANT 1 = one statement shape).
-- SELECT only; no SET / SETTINGS.

-- S0a (system.parts only, no gate): one row per table; WANT 4 rows, each active_parts > 0 and rows > 0 (fewer rows = a wrong table name: STOP).
SELECT table, count() AS active_parts, sum(rows) AS rows, sum(bytes_on_disk) AS bytes,
       min(modification_time) AS oldest_part, max(modification_time) AS newest_part
FROM system.parts
WHERE active AND database = currentDatabase()
  AND table IN ('project_membership_transitions', 'projects', 'repos', 'work_items')
GROUP BY table
ORDER BY table;

-- S0b (system.tables, metadata only): WANT 5 rows; project_membership_presence engine = View, the other four ReplacingMergeTree.
SELECT name, engine
FROM system.tables
WHERE database = currentDatabase()
  AND name IN ('project_membership_transitions', 'project_membership_presence', 'projects', 'repos', 'work_items')
ORDER BY name;

-- S1 control: is system.part_log on, and does it cover the window? WANT part_log_rows_day > 0 and part_log_rows_window > 0. Either = 0 means
--   no part events kept for the day or window: then S2 is not run and S3 is the only part-state read. Code 60 = part_log not enabled.
--   Counts only. The gate caps the part_log table (9 M rows, 4 GB).
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = 'system' AND table = 'part_log') > 9000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = 'system' AND table = 'part_log') > 4000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT count() AS part_log_rows_day,
       countIf(event_time >= toDateTime('2026-11-23 08:00:00', 'UTC') AND event_time < toDateTime('2026-11-23 10:30:00', 'UTC')) AS part_log_rows_window
FROM system.part_log
WHERE cost_gate = 0 AND event_date = toDate('2026-11-23');

-- S2: part events per table per 10 min, 08:00-10:30Z. WANT: report. Read: a burst of NewPart (insert) rows or few MergeParts
--   in work_items / projects / repos near 09:00Z = more parts for FINAL to fold; a burst in project_membership_transitions
--   = more rows. Control: total events across the window > 0 when S1 > 0 (0 = wrong database or table names).
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = 'system' AND table = 'part_log') > 9000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = 'system' AND table = 'part_log') > 4000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT table, toStartOfTenMinutes(event_time) AS bucket,
       countIf(event_type = 'NewPart') AS new_parts, sumIf(rows, event_type = 'NewPart') AS new_rows,
       countIf(event_type = 'MergeParts') AS merges, countIf(event_type = 'RemovePart') AS removed_parts,
       countIf(event_type = 'MutatePart') AS mutations
FROM system.part_log
WHERE cost_gate = 0
  AND event_date = toDate('2026-11-23')
  AND event_time >= toDateTime('2026-11-23 08:00:00', 'UTC') AND event_time < toDateTime('2026-11-23 10:30:00', 'UTC')
  AND database = currentDatabase()
  AND table IN ('project_membership_transitions', 'projects', 'repos', 'work_items')
GROUP BY table, bucket
ORDER BY table, bucket;

-- S3 (system.parts only, no gate): active parts by creation bucket (works with part_log off). WANT: report. Parts created 08:00-10:30Z
--   that are still active, per table per 10 min, and their rows. Control: S0a active_parts per table >= the sum here (a larger sum = a wrong filter).
SELECT table, toStartOfTenMinutes(modification_time) AS bucket, count() AS parts_still_active, sum(rows) AS rows
FROM system.parts
WHERE active AND database = currentDatabase()
  AND table IN ('project_membership_transitions', 'projects', 'repos', 'work_items')
  AND modification_time >= toDateTime('2026-11-23 08:00:00', 'UTC') AND modification_time < toDateTime('2026-11-23 10:30:00', 'UTC')
GROUP BY table, bucket
ORDER BY table, bucket;

-- S4a control: does query_log cover the window, and does the match key find the projector? WANT rows_window > 0 (0 = query_log off, wrong day,
--   TTL, or another replica) and runs_prefix_window > 0 (0 = the prefix is wrong or the runs are on another replica: STOP, do not read S4b as "no runs").
--   runs_contains_window counts any QueryFinish in the window that holds both literals and does not read system.query_log (so not these statements); it must be >= runs_prefix_window.
--   The gate caps the query_log table (9 M rows, 4 GB).
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log') > 9000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log') > 4000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT count() AS rows_window,
       countIf(type = 'QueryFinish' AND startsWith(query, 'SELECT source, uniqExact(subject_kind')) AS runs_prefix_window,
       countIf(type = 'QueryFinish'
               AND position(query, 'uniqExact(subject_kind') > 0 AND position(query, 'project_membership_transitions') > 0
               AND position(query, 'system.query_log') = 0) AS runs_contains_window,
       uniqExactIf(normalized_query_hash, type = 'QueryFinish' AND startsWith(query, 'SELECT source, uniqExact(subject_kind')) AS prefix_hashes
FROM system.query_log
WHERE cost_gate = 0
  AND event_date = toDate('2026-11-23')
  AND event_time >= toDateTime('2026-11-23 08:00:00', 'UTC') AND event_time < toDateTime('2026-11-23 10:30:00', 'UTC');

-- S4b: one row per finished run of the statement, 08:00-10:30Z. WANT: report (compare read_rows, selected_parts, memory_usage before and after 09:01Z).
--   Run it only when S4a runs_prefix_window <= 1000 (its row count); no LIMIT (see change 5).
--   No String column: run_minute (DateTime), counts, bytes, ms.
--   Control: row count = S4a runs_prefix_window. selected_parts / selected_marks are NULL when the key is absent from ProfileEvents.
WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log') > 9000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = 'system' AND table = 'query_log') > 4000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT toStartOfMinute(event_time) AS run_minute, memory_usage, read_rows, read_bytes, query_duration_ms,
       if(mapContains(ProfileEvents, 'SelectedParts'), ProfileEvents['SelectedParts'], NULL) AS selected_parts,
       if(mapContains(ProfileEvents, 'SelectedMarks'), ProfileEvents['SelectedMarks'], NULL) AS selected_marks
FROM system.query_log
WHERE cost_gate = 0
  AND event_date = toDate('2026-11-23')
  AND event_time >= toDateTime('2026-11-23 08:00:00', 'UTC') AND event_time < toDateTime('2026-11-23 10:30:00', 'UTC')
  AND type = 'QueryFinish'
  AND startsWith(query, 'SELECT source, uniqExact(subject_kind')
ORDER BY event_time, query_start_time_microseconds;
`

const pt14548Org = "c6a38355-0000-4000-8000-000000000000"

// Synthetic rows. Merges stop so each INSERT stays one part.
// transitions 3 parts (2+3+4 rows) first, a 4th part (5 rows) after run 1; work_items 2 parts (3+2); projects 2 parts (2+2); repos 2 parts (2+1).
const pt14548Fixture = `
SYSTEM STOP MERGES project_membership_transitions;
SYSTEM STOP MERGES work_items;
SYSTEM STOP MERGES projects;
SYSTEM STOP MERGES repos;
INSERT INTO project_membership_transitions (org_id, repo_id, subject_kind, subject_id, provider, from_project_id, to_project_id, occurred_at, last_synced, event_id) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'work_item', 'w1', 'linear', '', 'p1', '2026-11-20 08:00:00.000', '2026-11-20 08:00:00.000', 'e1'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'work_item', 'w2', 'linear', '', 'p1', '2026-11-20 08:00:01.000', '2026-11-20 08:00:01.000', 'e2');
INSERT INTO project_membership_transitions (org_id, repo_id, subject_kind, subject_id, provider, from_project_id, to_project_id, occurred_at, last_synced, event_id) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'work_item', 'w3', 'linear', '', 'p2', '2026-11-20 09:00:00.000', '2026-11-20 09:00:00.000', 'e3'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'work_item', 'w4', 'linear', '', 'p2', '2026-11-20 09:00:01.000', '2026-11-20 09:00:01.000', 'e4'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'work_item', 'w5', 'linear', '', 'p2', '2026-11-20 09:00:02.000', '2026-11-20 09:00:02.000', 'e5');
INSERT INTO project_membership_transitions (org_id, repo_id, subject_kind, subject_id, provider, from_project_id, to_project_id, occurred_at, last_synced, event_id) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 'work_item', 'w6', 'linear', 'p1', 'p2', '2026-11-21 08:00:00.000', '2026-11-21 08:00:00.000', 'e6'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 'work_item', 'w7', 'linear', 'p1', 'p2', '2026-11-21 08:00:01.000', '2026-11-21 08:00:01.000', 'e7'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 'work_item', 'w8', 'linear', 'p1', 'p2', '2026-11-21 08:00:02.000', '2026-11-21 08:00:02.000', 'e8'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 'work_item', 'w9', 'linear', 'p1', 'p2', '2026-11-21 08:00:03.000', '2026-11-21 08:00:03.000', 'e9');
INSERT INTO work_items (repo_id, work_item_id, org_id, last_synced) VALUES
('00000000-0000-4000-8000-000000000001', 'w1', 'c6a38355-0000-4000-8000-000000000000', '2026-11-20 08:00:00.000'),
('00000000-0000-4000-8000-000000000001', 'w2', 'c6a38355-0000-4000-8000-000000000000', '2026-11-20 08:00:00.000'),
('00000000-0000-4000-8000-000000000001', 'w3', 'c6a38355-0000-4000-8000-000000000000', '2026-11-20 08:00:00.000');
INSERT INTO work_items (repo_id, work_item_id, org_id, last_synced) VALUES
('00000000-0000-4000-8000-000000000002', 'w4', 'c6a38355-0000-4000-8000-000000000000', '2026-11-21 08:00:00.000'),
('00000000-0000-4000-8000-000000000002', 'w5', 'c6a38355-0000-4000-8000-000000000000', '2026-11-21 08:00:00.000');
INSERT INTO projects (id, org_id, provider, name, updated_at) VALUES
('p1', 'c6a38355-0000-4000-8000-000000000000', 'linear', 'One', '2026-11-20 08:00:00.000'),
('p2', 'c6a38355-0000-4000-8000-000000000000', 'linear', 'Two', '2026-11-20 08:00:00.000');
INSERT INTO projects (id, org_id, provider, name, updated_at) VALUES
('p3', 'c6a38355-0000-4000-8000-000000000000', 'linear', 'Three', '2026-11-21 08:00:00.000'),
('p4', 'c6a38355-0000-4000-8000-000000000000', 'linear', 'Four', '2026-11-21 08:00:00.000');
INSERT INTO repos (id, repo, created_at, last_synced, org_id) VALUES
('00000000-0000-4000-8000-000000000001', 'org/a', '2026-11-20 08:00:00.000', '2026-11-20 08:00:00.000', 'c6a38355-0000-4000-8000-000000000000'),
('00000000-0000-4000-8000-000000000002', 'org/b', '2026-11-20 08:00:00.000', '2026-11-20 08:00:00.000', 'c6a38355-0000-4000-8000-000000000000');
INSERT INTO repos (id, repo, created_at, last_synced, org_id) VALUES
('00000000-0000-4000-8000-000000000003', 'org/c', '2026-11-21 08:00:00.000', '2026-11-21 08:00:00.000', 'c6a38355-0000-4000-8000-000000000000');
`

const pt14548Fourth = `INSERT INTO project_membership_transitions (org_id, repo_id, subject_kind, subject_id, provider, from_project_id, to_project_id, occurred_at, last_synced, event_id) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'work_item', 'w10', 'linear', '', 'p3', '2026-11-22 08:00:00.000', '2026-11-22 08:00:00.000', 'e10'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'work_item', 'w11', 'linear', '', 'p3', '2026-11-22 08:00:01.000', '2026-11-22 08:00:01.000', 'e11'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'work_item', 'w12', 'linear', '', 'p3', '2026-11-22 08:00:02.000', '2026-11-22 08:00:02.000', 'e12'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'work_item', 'w13', 'linear', '', 'p3', '2026-11-22 08:00:03.000', '2026-11-22 08:00:03.000', 'e13'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'work_item', 'w14', 'linear', '', 'p3', '2026-11-22 08:00:04.000', '2026-11-22 08:00:04.000', 'e14')`

// The same first token as the projector statement (edge_counts.go:50); it reads the whole transitions table.
const pt14548Like = `SELECT source, uniqExact(subject_kind, subject_id) AS edge_count
FROM (SELECT 'transition' AS source, subject_kind, subject_id FROM project_membership_transitions)
GROUP BY source`

type pt14548Statement struct{ label, text string }

func pt14548Split(body string) []pt14548Statement {
	var out []pt14548Statement
	var chunk []string
	names := []string{"S0a", "S0b", "S1", "S2", "S3", "S4a", "S4b"}
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
		out = append(out, pt14548Statement{label: label, text: text})
		chunk = nil
	}
	return out
}

func pt14548Render(v any, dbType string) string {
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

func pt14548Run(ctx context.Context, conn driver.Conn, text, comment string) ([]map[string]string, error) {
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
			row[name] = pt14548Render(cells[i], types[i].DatabaseTypeName())
		}
		got = append(got, row)
	}
	return got, rows.Err()
}

func pt14548Log(ctx context.Context, t *testing.T, conn driver.Conn, comment string) ([]string, uint64) {
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

func pt14548Diff(got, want []map[string]string) string {
	if len(got) != len(want) {
		return fmt.Sprintf("%d rows, want %d: got %v", len(got), len(want), got)
	}
	for i := range want {
		for k, v := range want[i] {
			if got[i][k] != v {
				return fmt.Sprintf("row %d: %s = %s, want %s (got %v)", i, k, got[i][k], v, got[i])
			}
		}
	}
	return ""
}

func pt14548Count(ctx context.Context, t *testing.T, conn driver.Conn, query string, args ...any) uint64 {
	t.Helper()
	var n uint64
	if err := conn.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func TestReadPartsForm14548ExecutesAtTheChainHead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	sum := sha256.Sum256([]byte(pt14548Form))
	if got := hex.EncodeToString(sum[:]); got != pt14548FormSHA256 {
		t.Fatalf("form sha256 = %s, want %s", got, pt14548FormSHA256)
	}
	statements := pt14548Split(pt14548Form)
	byLabel := map[string]string{}
	labels := make([]string, 0, len(statements))
	for _, s := range statements {
		labels = append(labels, s.label)
		byLabel[s.label] = s.text
	}
	if strings.Join(labels, ",") != "S0a,S0b,S1,S2,S3,S4a,S4b" {
		t.Fatalf("form statements = %v", labels)
	}
	code := chmigrate.StripLineComments(pt14548Form)
	if regexp.MustCompile(`(?i)\b(SET|SETTINGS|FORMAT|EXPLAIN|INSERT|FINAL|LIMIT)\b`).MatchString(code) {
		t.Fatalf("form holds a forbidden keyword")
	}
	gateTable := map[string]string{"S1": "part_log", "S2": "part_log", "S4a": "query_log", "S4b": "query_log"}
	for _, s := range statements {
		table, gated := gateTable[s.label]
		want := 0
		if gated {
			want = 1
		}
		if n := strings.Count(s.text, pt14548RowsCap); n != want {
			t.Fatalf("%s: %d rows caps, want %d", s.label, n, want)
		}
		if n := strings.Count(s.text, pt14548BytesCap); n != want {
			t.Fatalf("%s: %d bytes caps, want %d", s.label, n, want)
		}
		if n := strings.Count(s.text, "cost_gate = 0"); n != want {
			t.Fatalf("%s: cost_gate = 0 found %d times, want %d", s.label, n, want)
		}
		if !gated {
			continue
		}
		if n := strings.Count(s.text, "'STOP-COST-GATE'"); n != 1 {
			t.Fatalf("%s: %d STOP-COST-GATE, want 1", s.label, n)
		}
		if n := strings.Count(s.text, "database = 'system' AND table = '"+table+"'"); n != 2 {
			t.Fatalf("%s: gate table found %d times, want 2", s.label, n)
		}
		if n := strings.Count(s.text, "FROM system."+table); n != 1 {
			t.Fatalf("%s: %d reads of system.%s, want 1", s.label, n, table)
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

	for _, statement := range chmigrate.SplitStatements(pt14548Fixture) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("fixture: %v\n%s", err, statement)
		}
	}

	// Three runs of a statement-like query: run 1 on 3 transitions parts, then a 4th part, runs 2 and 3.
	likeComments := []string{
		fmt.Sprintf("pt14548-like-1-%d", time.Now().UnixNano()),
		fmt.Sprintf("pt14548-like-2-%d", time.Now().UnixNano()),
		fmt.Sprintf("pt14548-like-3-%d", time.Now().UnixNano()),
	}
	if _, err := pt14548Run(ctx, conn, pt14548Like, likeComments[0]); err != nil {
		t.Fatalf("like run 1: %v", err)
	}
	if err := conn.Exec(ctx, pt14548Fourth); err != nil {
		t.Fatalf("fourth part: %v", err)
	}
	for _, c := range likeComments[1:] {
		if _, err := pt14548Run(ctx, conn, pt14548Like, c); err != nil {
			t.Fatalf("like run: %v", err)
		}
	}

	type tableState struct{ parts, rows uint64 }
	wantTables := map[string]tableState{
		"project_membership_transitions": {4, 14},
		"projects":                       {2, 4},
		"repos":                          {2, 3},
		"work_items":                     {2, 5},
	}
	for name, w := range wantTables {
		var rows, parts uint64
		if err := conn.QueryRow(ctx, `SELECT sum(rows), count() FROM system.parts
			WHERE active AND database = currentDatabase() AND table = ?`, name).Scan(&rows, &parts); err != nil {
			t.Fatal(err)
		}
		t.Logf("fixture: %s rows=%d active_parts=%d", name, rows, parts)
		if rows != w.rows || parts != w.parts {
			t.Fatalf("fixture %s rows=%d parts=%d, want %d and %d", name, rows, parts, w.rows, w.parts)
		}
	}

	// part_log exists only when the server enables it. Both outcomes are handled.
	partLogOn := pt14548Count(ctx, t, conn, "SELECT count() FROM system.tables WHERE database = 'system' AND name = 'part_log'") == 1
	t.Logf("part_log table present: %v", partLogOn)

	var day, nextDay string
	if err := conn.QueryRow(ctx, "SELECT toString(toDate(now('UTC'))), toString(toDate(now('UTC')) + 1)").Scan(&day, &nextDay); err != nil {
		t.Fatal(err)
	}
	dated := func(text string) string {
		text = strings.ReplaceAll(text, "2026-11-23 08:00:00", day+" 00:00:00")
		text = strings.ReplaceAll(text, "2026-11-23 10:30:00", nextDay+" 00:00:00")
		return strings.ReplaceAll(text, "2026-11-23", day)
	}
	t.Logf("dated copies: window %s 00:00:00 .. %s 00:00:00 UTC (the CI clock, not the prod day)", day, nextDay)

	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	logToday := func() uint64 {
		return pt14548Count(ctx, t, conn, "SELECT count() FROM system.query_log WHERE event_date = toDate(?)", day)
	}
	partsOf := func(table string) uint64 {
		return pt14548Count(ctx, t, conn, "SELECT count() FROM system.parts WHERE database = 'system' AND table = ?", table)
	}
	q0, pq0 := logToday(), partsOf("query_log")
	var pl0, plp0 uint64
	if partLogOn {
		pl0 = pt14548Count(ctx, t, conn, "SELECT count() FROM system.part_log WHERE event_date = toDate(?)", day)
		plp0 = partsOf("part_log")
	}
	t.Logf("before PASS: query_log rows today=%d parts=%d; part_log rows today=%d parts=%d", q0, pq0, pl0, plp0)

	// Expected S4b rows from the log itself, by log_comment.
	type likeRun struct{ mem, readRows, readBytes, dur, parts, marks uint64 }
	var likeWant []likeRun
	for _, c := range likeComments {
		var r likeRun
		if err := conn.QueryRow(ctx, `SELECT memory_usage, read_rows, read_bytes, query_duration_ms,
			ProfileEvents['SelectedParts'], ProfileEvents['SelectedMarks']
			FROM system.query_log WHERE log_comment = ? AND type = 'QueryFinish'`, c).Scan(&r.mem, &r.readRows, &r.readBytes, &r.dur, &r.parts, &r.marks); err != nil {
			t.Fatalf("like run in query_log: %v", err)
		}
		t.Logf("LIKE run %s: %+v", c, r)
		likeWant = append(likeWant, r)
	}
	for i, wr := range []uint64{9, 14, 14} {
		wp := []uint64{3, 4, 4}[i]
		if likeWant[i].readRows != wr || likeWant[i].parts != wp || likeWant[i].marks != wp || likeWant[i].mem == 0 {
			t.Errorf("like run %d: %+v, want read_rows %d, selected_parts %d, selected_marks %d, memory > 0", i+1, likeWant[i], wr, wp, wp)
		}
	}

	str := func(v uint64) string { return fmt.Sprint(v) }
	want := map[string][]map[string]string{
		"S0a": {
			{"table": "project_membership_transitions", "active_parts": "4", "rows": "14"},
			{"table": "projects", "active_parts": "2", "rows": "4"},
			{"table": "repos", "active_parts": "2", "rows": "3"},
			{"table": "work_items", "active_parts": "2", "rows": "5"},
		},
		"S0b": {
			{"name": "project_membership_presence", "engine": "View"},
			{"name": "project_membership_transitions", "engine": "ReplacingMergeTree"},
			{"name": "projects", "engine": "ReplacingMergeTree"},
			{"name": "repos", "engine": "ReplacingMergeTree"},
			{"name": "work_items", "engine": "ReplacingMergeTree"},
		},
		"S4a": {{"runs_prefix_window": "3", "runs_contains_window": "3", "prefix_hashes": "1"}},
	}
	var s4b []map[string]string
	for _, r := range likeWant {
		s4b = append(s4b, map[string]string{
			"memory_usage": str(r.mem), "read_rows": str(r.readRows), "read_bytes": str(r.readBytes),
			"query_duration_ms": str(r.dur), "selected_parts": str(r.parts), "selected_marks": str(r.marks),
		})
	}
	want["S4b"] = s4b

	passComments := map[string]string{}
	passRows := map[string][]map[string]string{}
	for _, s := range statements {
		passComments[s.label] = fmt.Sprintf("pt14548-pass-%s-%d", s.label, time.Now().UnixNano())
		got, err := pt14548Run(ctx, conn, dated(s.text), passComments[s.label])
		if err != nil {
			var ex *clickhouse.Exception
			if (s.label == "S1" || s.label == "S2") && !partLogOn && errors.As(err, &ex) && ex.Code == 60 {
				t.Logf("PASS %s: part_log absent, code 60 UNKNOWN_TABLE as the form says", s.label)
				continue
			}
			t.Errorf("PASS %s: SQL error: %v", s.label, err)
			continue
		}
		passRows[s.label] = got
		t.Logf("PASS %s ROWS %v", s.label, got)
		if w, ok := want[s.label]; ok {
			if d := pt14548Diff(got, w); d != "" {
				t.Errorf("PASS %s: %s", s.label, d)
			}
		}
	}
	for _, r := range passRows["S4b"] {
		if r["run_minute"] == "" || strings.HasPrefix(r["run_minute"], "1970") {
			t.Errorf("S4b run_minute empty: %v", r)
		}
	}
	if a := passRows["S4a"]; len(a) == 1 {
		var rowsWindow uint64
		fmt.Sscan(a[0]["rows_window"], &rowsWindow)
		if rowsWindow < q0 {
			t.Errorf("S4a rows_window %d < query_log rows before the run %d", rowsWindow, q0)
		}
	}

	// S3 control: parts created in the window are a subset of the active parts of S0a.
	activeByTable := map[string]uint64{}
	for _, r := range passRows["S0a"] {
		var n uint64
		fmt.Sscan(r["active_parts"], &n)
		activeByTable[r["table"]] = n
	}
	sumByTable := map[string]uint64{}
	for _, r := range passRows["S3"] {
		var n uint64
		fmt.Sscan(r["parts_still_active"], &n)
		sumByTable[r["table"]] += n
	}
	for table, n := range sumByTable {
		if n > activeByTable[table] {
			t.Errorf("S3 control: %s parts %d > S0a active_parts %d", table, n, activeByTable[table])
		}
	}
	for table, w := range wantTables {
		if sumByTable[table] != w.parts {
			t.Errorf("S3 %s parts_still_active %d, want %d (window is the whole CI day)", table, sumByTable[table], w.parts)
		}
	}

	// S1 and S2 on the CI day when part_log is on: NewPart events equal the fixture parts and rows, no merges.
	if partLogOn {
		if len(passRows["S1"]) != 1 || passRows["S1"][0]["part_log_rows_day"] == "0" || passRows["S1"][0]["part_log_rows_window"] != passRows["S1"][0]["part_log_rows_day"] {
			t.Errorf("S1 with part_log on: %v, want part_log_rows_day > 0 and = part_log_rows_window (window is the whole CI day)", passRows["S1"])
		}
		newParts, newRows := map[string]uint64{}, map[string]uint64{}
		for _, r := range passRows["S2"] {
			var np, nr uint64
			fmt.Sscan(r["new_parts"], &np)
			fmt.Sscan(r["new_rows"], &nr)
			newParts[r["table"]] += np
			newRows[r["table"]] += nr
			if r["merges"] != "0" || r["removed_parts"] != "0" || r["mutations"] != "0" {
				t.Errorf("S2 %s: merges/removed/mutations not 0: %v", r["table"], r)
			}
		}
		for table, w := range wantTables {
			if newParts[table] != w.parts || newRows[table] != w.rows {
				t.Errorf("S2 %s: new_parts %d new_rows %d, want %d and %d", table, newParts[table], newRows[table], w.parts, w.rows)
			}
		}
	}

	// Control mutations: each must change the result so its WANT fails.
	mutateN := func(label, from, to string, n int) []map[string]string {
		mutated := strings.Replace(dated(byLabel[label]), from, to, n)
		if mutated == dated(byLabel[label]) {
			t.Fatalf("MUTATE %s: %q not found", label, from)
		}
		got, err := pt14548Run(ctx, conn, mutated, fmt.Sprintf("pt14548-mutate-%s-%d", label, time.Now().UnixNano()))
		if err != nil {
			t.Fatalf("MUTATE %s %q: %v", label, from, err)
		}
		t.Logf("MUTATE %s %q -> %q ROWS %v", label, from, to, got)
		return got
	}
	mutate := func(label, from, to string) []map[string]string { return mutateN(label, from, to, 1) }
	if got := mutate("S0a", "'repos'", "'reposz'"); len(got) != 3 {
		t.Errorf("MUTATE S0a wrong table: %d rows, want 3 (WANT 4 fails)", len(got))
	}
	if got := mutate("S0b", "'projects'", "'projectz'"); len(got) != 4 {
		t.Errorf("MUTATE S0b wrong table: %d rows, want 4 (WANT 5 fails)", len(got))
	}
	if partLogOn {
		if got := mutate("S1", "event_date = toDate('"+day+"')", "event_date = toDate('2030-01-01')"); len(got) != 1 || got[0]["part_log_rows_day"] != "0" || got[0]["part_log_rows_window"] != "0" {
			t.Errorf("MUTATE S1 day: %v, want part_log_rows_day 0 and part_log_rows_window 0", got)
		}
		if got := mutate("S2", "event_date = toDate('"+day+"')", "event_date = toDate('2030-01-01')"); len(got) != 0 {
			t.Errorf("MUTATE S2 day: %v, want 0 rows", got)
		}
	}
	if got := mutate("S3", "'projects'", "'projectz'"); len(got) == len(passRows["S3"]) && len(got) > 0 && pt14548Diff(got, passRows["S3"]) == "" {
		t.Errorf("MUTATE S3 table list: result equals the clean result")
	}
	if got := mutate("S4a", "event_date = toDate('"+day+"')", "event_date = toDate('2030-01-01')"); len(got) != 1 || got[0]["rows_window"] != "0" || got[0]["runs_prefix_window"] != "0" {
		t.Errorf("MUTATE S4a day: %v, want rows_window 0, runs_prefix_window 0", got)
	}
	if got := mutateN("S4a", "'SELECT source, uniqExact(subject_kind'", "'SELECT zource, uniqExact(subject_kind'", -1); len(got) != 1 || got[0]["runs_prefix_window"] != "0" || got[0]["runs_contains_window"] != "3" || got[0]["prefix_hashes"] != "0" {
		t.Errorf("MUTATE S4a prefix: %v, want runs_prefix_window 0, runs_contains_window 3, prefix_hashes 0", got)
	}
	// QueryStart rows of the runs also match the prefix, so the type mutant uses a type with no matching row.
	if got := mutate("S4a", "type = 'QueryFinish'", "type = 'ExceptionWhileProcessing'"); len(got) != 1 || got[0]["runs_prefix_window"] != "0" {
		t.Errorf("MUTATE S4a type: %v, want runs_prefix_window 0", got)
	}
	if got := mutate("S4b", "AND type = 'QueryFinish'", "AND type = 'QueryStart'"); pt14548Diff(got, want["S4b"]) == "" {
		t.Errorf("MUTATE S4b type: result equals the clean result")
	}
	if got := mutate("S4b", "'SELECT source, uniqExact(subject_kind'", "'SELECT zource, uniqExact(subject_kind'"); len(got) != 0 {
		t.Errorf("MUTATE S4b prefix: %v, want 0 rows", got)
	}

	// PASS-path bounds. The log tables are live, so equality is a bound: rows before <= read_rows <= rows after + 2P + 1.
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	q1, pq1 := logToday(), partsOf("query_log")
	pqMax := pq0
	if pq1 > pqMax {
		pqMax = pq1
	}
	var pl1, plp1 uint64
	if partLogOn {
		pl1 = pt14548Count(ctx, t, conn, "SELECT count() FROM system.part_log WHERE event_date = toDate(?)", day)
		plp1 = partsOf("part_log")
	}
	plpMax := plp0
	if plp1 > plpMax {
		plpMax = plp1
	}
	for _, s := range statements {
		if _, ok := passRows[s.label]; !ok {
			continue
		}
		types, read := pt14548Log(ctx, t, conn, passComments[s.label])
		t.Logf("PASS %s query_log types=%v read_rows=%d", s.label, types, read)
		if len(types) != 1 || types[0] != "QueryFinish" {
			t.Errorf("PASS %s: query_log types %v, want [QueryFinish]", s.label, types)
		}
		switch s.label {
		case "S4a", "S4b":
			lo, hi := q0, q1+2*pqMax+1
			t.Logf("PASS %s bound: %d <= read_rows %d <= %d (query_log rows %d..%d, P %d)", s.label, lo, read, hi, q0, q1, pqMax)
			if read < lo || read > hi {
				t.Errorf("PASS %s: read_rows %d outside [%d, %d]", s.label, read, lo, hi)
			}
		case "S1", "S2":
			lo, hi := pl0, pl1+2*plpMax+1
			t.Logf("PASS %s bound: %d <= read_rows %d <= %d (part_log rows %d..%d, P %d)", s.label, lo, read, hi, pl0, pl1, plpMax)
			if read < lo || read > hi {
				t.Errorf("PASS %s: read_rows %d outside [%d, %d]", s.label, read, lo, hi)
			}
		}
	}

	// Gate fires: each cap literal lowered to 0 in a test copy of the shipped text. WANT code 395 STOP-COST-GATE before any log read.
	type fired struct{ label, cap, comment, table string }
	var fires []fired
	for _, s := range statements {
		table, gated := gateTable[s.label]
		if !gated {
			continue
		}
		if table == "part_log" && !partLogOn {
			t.Logf("GATE %s: not fired, part_log absent (an absent table sums 0, so cap 0 cannot fire)", s.label)
			continue
		}
		for _, literal := range []string{pt14548RowsCap, pt14548BytesCap} {
			lowered := strings.Replace(s.text, literal, "> 0", 1)
			comment := fmt.Sprintf("pt14548-gate-%s-%s-%d", s.label, strings.TrimPrefix(literal, "> "), time.Now().UnixNano())
			_, err := pt14548Run(ctx, conn, lowered, comment)
			var ex *clickhouse.Exception
			if err == nil || !errors.As(err, &ex) || ex.Code != 395 || !strings.Contains(ex.Message, "STOP-COST-GATE") {
				t.Errorf("GATE %s %s lowered to 0: err = %v, want code 395 STOP-COST-GATE", s.label, literal, err)
				continue
			}
			t.Logf("GATE %s %s lowered to 0: fired code=%d", s.label, literal, ex.Code)
			fires = append(fires, fired{s.label, literal, comment, table})
		}
	}
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	pBound := map[string]uint64{"query_log": partsOf("query_log"), "part_log": 0}
	if partLogOn {
		pBound["part_log"] = partsOf("part_log")
	}
	for _, f := range fires {
		types, read := pt14548Log(ctx, t, conn, f.comment)
		p := pBound[f.table]
		if f.table == "query_log" && pqMax > p {
			p = pqMax
		}
		if f.table == "part_log" && plpMax > p {
			p = plpMax
		}
		t.Logf("GATE %s %s query_log types=%v read_rows=%d bound=%d", f.label, f.cap, types, read, 2*p+1)
		if len(types) != 1 || types[0] != "ExceptionBeforeStart" {
			t.Errorf("GATE %s %s: query_log types %v, want [ExceptionBeforeStart]", f.label, f.cap, types)
		}
		if read > 2*p+1 {
			t.Errorf("GATE %s %s: read_rows %d over 2 x P + 1 = %d", f.label, f.cap, read, 2*p+1)
		}
	}
	wantFires := 8
	if !partLogOn {
		wantFires = 4
	}
	if len(fires) != wantFires {
		t.Errorf("gates fired %d times, want %d", len(fires), wantFires)
	}

	// Output cell: S4a and S4b hold no String column (no query text, no query_id). The mutant adds query_id and must be caught.
	stringColumns := func(text string) []string {
		rows, err := conn.Query(ctx, dated(text))
		if err != nil {
			t.Fatalf("output cell: %v", err)
		}
		defer rows.Close()
		var bad []string
		for i, ct := range rows.ColumnTypes() {
			name := rows.Columns()[i]
			if strings.Contains(ct.DatabaseTypeName(), "String") || name == "query" || name == "query_id" || name == "initial_query_id" || name == "formatted_query" {
				bad = append(bad, name+" "+ct.DatabaseTypeName())
			}
		}
		return bad
	}
	for _, label := range []string{"S4a", "S4b"} {
		if bad := stringColumns(byLabel[label]); len(bad) != 0 {
			t.Errorf("OUTPUT %s: text columns %v, want none", label, bad)
		} else {
			t.Logf("OUTPUT %s: no String column", label)
		}
	}
	mutant := strings.Replace(byLabel["S4b"], "SELECT toStartOfMinute(event_time) AS run_minute,", "SELECT toStartOfMinute(event_time) AS run_minute, query_id,", 1)
	if mutant == byLabel["S4b"] {
		t.Fatalf("OUTPUT mutant: select list not found")
	}
	if bad := stringColumns(mutant); len(bad) == 0 {
		t.Errorf("OUTPUT mutant with query_id: no text column found, the cell cannot fail")
	}
}
