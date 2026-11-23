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

// Throwaway exec proof of a prod read form: not for merge. rb14488bForm is the form file byte for byte.
const (
	rb14488bRowsCap  = "> 20000000"
	rb14488bBytesCap = "> 4000000000"
)

const rb14488bFormSHA256 = "e92569d858e6d9640eb84f48eb6fd6eeb852cb4289556ad3b9d26f605a158abb"

const rb14488bForm = `-- NOT EXECUTED. READBACK-14488-2 (ClickHouse, prod, read-only). acr-mcp, 2026-11-23. A CI exec leg first, then acp FORM read; then pm runs it once.
-- Form: lane-scratch/pm-guard/FORM-TEMPLATE-3.md 6b9e73a3c45a (CH shape, one statement per call, self-gating) + PM-RULES MAC §4 (8e04e2613b6f).
-- Not executed: no local CH at the prod schema and no CI exec leg yet. Per the template pm does not run it until an exec proof exists
--   (each gate fired once with cap 0 = code 395 STOP-COST-GATE; a pass-path cell on a fixture; control mutations fail).
-- Why: READBACK-14488-1 (916cca0cb5d1) ran once on prod 2026-11-23 07:15Z = NOT EXERCISED (stmt3 n_null 0: no group with runs and no
--   queue sample was written after 06:24:03Z). This form reads the SOURCE rows to show how often such a group occurs and on which days,
--   so the -1 re-run is not blind. It does not prove the fix; -1 stmt3 does, after the next write of such a day.
-- Rule at ops a7febdf50002 (the workers image of STEP 325):
--   internal/jobs/metrics/daily/testops_native_clickhouse.go:126-130 source = ci_pipeline_runs FINAL, org + repo, started_at in [day, day+1),
--     status != 'deleted' (lowerUTF8(trimBoth(ifNull(status,'')))); :173-207 streams each row into testops.PipelineAccumulator.Add.
--   internal/jobs/metrics/testops/compute.go:596-648 Add: group key = (resolved team_id, raw service_id) per repo-day (:600-604);
--     every counted run adds 1 to pipelines (:610); a queue sample only when retry_count = 0 (:637-641) AND
--     safeDurationSeconds(isQueue) gives one (:465-479): queue_seconds >= 0, else queued_at not NULL and started_at >= queued_at.
--   compute.go:698-702 AvgQueueSeconds stays nil when the group has no sample; testops_risk_native_clickhouse.go:714
--     queueUnknown = PipelinesCount > 0 && AvgQueueSeconds == nil -> queue_wait_hours NULL (:748). That is the case -1 stmt3 waits for.
-- Table at the live CH head (internal/chmigrate/baseline/head.json objects[16], head 170): ci_pipeline_runs(repo_id UUID, run_id String,
--   status Nullable(String), queued_at Nullable(DateTime64(3,'UTC')), started_at DateTime64(3,'UTC'), last_synced DateTime64(3,'UTC'),
--   org_id String, queue_seconds Nullable(Float64), retry_count UInt32, team_id Nullable(String), service_id Nullable(String), ...),
--   ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, run_id), no partition key.
-- Named limit (team key): Go resolves a NULL raw team_id through the repo name (compute.go resolveRepoTeam); SQL cannot. This form groups by
--   the RAW team_id, so stmt2 groups_no_sample is an UPPER bound of the Go groups (a raw-NULL split can count a group Go merges with a
--   sampled one). stmt3 counts repo-days in which NO counted run has a sample: every Go group of such a day has no sample = a LOWER bound.
-- Window: started_at >= 2026-11-09 00:00:00 UTC (14 days before the roll day) and no upper bound (today included).
-- WORK MATH: READBACK-14488-2.md. Each of stmt0..stmt3 reads ci_pipeline_runs once (one FINAL scan in a subquery or the FROM),
--   no CTE reference to data, no join. The gate reads system.parts only.
-- Output: counts and days only. No ids, no names, no payload text.
-- S0: the table exists and has rows. WANT 1 row, rows > 0, active_parts > 0 (0 = wrong name: STOP).
-- stmt0 ORG-1. WANT orgs = 1.
-- stmt1 control that can fail: counted runs in the window and the raw groups with a sample.
--   WANT runs > 0 and groups_with_sample > 0 (0 = a filter or the sample rule is wrong, or no data: STOP).
-- stmt2 report: per day, the raw groups, the raw groups with runs but no queue sample (upper bound), their repos and their runs.
--   WANT: report. The newest day with groups_no_sample > 0 names the day whose next write exercises -1.
-- stmt3 report + cross-check: per day, the repo-days in which no counted run has a sample (lower bound).
--   WANT: for each day, repodays_all_no_sample <= stmt2 repos_no_sample of that day (> = the two statements disagree: STOP).

SELECT 'ci_pipeline_runs' AS t, sum(rows) AS rows, sum(bytes_on_disk) AS bytes, count() AS active_parts
FROM system.parts WHERE active AND database = currentDatabase() AND table = 'ci_pipeline_runs';

WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('ci_pipeline_runs')) > 20000000
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('ci_pipeline_runs')) > 4000000000,
  'STOP-COST-GATE')) AS cost_gate
SELECT uniqExact(org_id) AS orgs
FROM ci_pipeline_runs
WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0;

SELECT sum(n_runs) AS runs, count() AS groups, countIf(n_sample > 0) AS groups_with_sample
FROM (
  WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('ci_pipeline_runs')) > 20000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('ci_pipeline_runs')) > 4000000000,
    'STOP-COST-GATE')) AS cost_gate
  SELECT repo_id, toDate(started_at, 'UTC') AS day, ifNull(team_id, '') AS team, service_id,
         count() AS n_runs,
         countIf(retry_count = 0 AND ((queue_seconds IS NOT NULL AND queue_seconds >= 0)
                                      OR (queued_at IS NOT NULL AND started_at >= queued_at))) AS n_sample
  FROM ci_pipeline_runs FINAL
  WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0
    AND started_at >= toDateTime64('2026-11-09 00:00:00', 3, 'UTC')
    AND lowerUTF8(trimBoth(ifNull(status, ''))) != 'deleted'
  GROUP BY repo_id, day, team, service_id
);

SELECT day, count() AS groups, countIf(n_sample = 0) AS groups_no_sample,
       uniqExactIf(repo_id, n_sample = 0) AS repos_no_sample, sumIf(n_runs, n_sample = 0) AS runs_in_no_sample_groups
FROM (
  WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('ci_pipeline_runs')) > 20000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('ci_pipeline_runs')) > 4000000000,
    'STOP-COST-GATE')) AS cost_gate
  SELECT repo_id, toDate(started_at, 'UTC') AS day, ifNull(team_id, '') AS team, service_id,
         count() AS n_runs,
         countIf(retry_count = 0 AND ((queue_seconds IS NOT NULL AND queue_seconds >= 0)
                                      OR (queued_at IS NOT NULL AND started_at >= queued_at))) AS n_sample
  FROM ci_pipeline_runs FINAL
  WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0
    AND started_at >= toDateTime64('2026-11-09 00:00:00', 3, 'UTC')
    AND lowerUTF8(trimBoth(ifNull(status, ''))) != 'deleted'
  GROUP BY repo_id, day, team, service_id
)
GROUP BY day
ORDER BY day;

SELECT day, countIf(n_sample = 0) AS repodays_all_no_sample, count() AS repodays
FROM (
  WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('ci_pipeline_runs')) > 20000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('ci_pipeline_runs')) > 4000000000,
    'STOP-COST-GATE')) AS cost_gate
  SELECT repo_id, toDate(started_at, 'UTC') AS day,
         countIf(retry_count = 0 AND ((queue_seconds IS NOT NULL AND queue_seconds >= 0)
                                      OR (queued_at IS NOT NULL AND started_at >= queued_at))) AS n_sample
  FROM ci_pipeline_runs FINAL
  WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0
    AND started_at >= toDateTime64('2026-11-09 00:00:00', 3, 'UTC')
    AND lowerUTF8(trimBoth(ifNull(status, ''))) != 'deleted'
  GROUP BY repo_id, day
)
GROUP BY day
ORDER BY day;
`

// Synthetic rows. Org O = prod prefix c6a38355. Window start 2026-11-09 00:00:00 UTC (inclusive).
// Days (started_at UTC):
//
//	11-08 23:59:59 R6 retry run: outside the window.
//	11-09 00:00:00 R11 retry run: boundary, inside.
//	11-20 R1 three retry runs (all-rerun repo-day); R2 retry 0 queue_seconds -1 queued_at NULL (no sample); R3 retry 0 queue 10 (sample);
//	      R7 retry run of org B (other org).
//	11-21 R4 retry 0 queue NULL queued_at 5 s before start (sample by queued_at) + a run with no times (no sample);
//	      R5 two deleted runs (status 'deleted' and ' DELETED ') that look sampled + one retry run.
//	11-22 R8 one run in two versions (v1 sample, v2 FINAL = retry, no sample); R9 two groups (team tA retry; team NULL sample).
const rb14488bFixture = `
SYSTEM STOP MERGES ci_pipeline_runs;
INSERT INTO ci_pipeline_runs (repo_id, run_id, status, queued_at, started_at, last_synced, org_id, queue_seconds, retry_count, team_id, service_id) VALUES
('00000000-0000-4000-8000-000000000006', 'x6', 'success', NULL, '2026-11-08 23:59:59.000', '2026-11-23 06:00:00.000', 'c6a38355-0000-4000-8000-000000000000', 5, 1, NULL, NULL),
('00000000-0000-4000-8000-000000000011', 'h1', 'success', NULL, '2026-11-09 00:00:00.000', '2026-11-23 06:00:00.000', 'c6a38355-0000-4000-8000-000000000000', 5, 1, NULL, NULL),
('00000000-0000-4000-8000-000000000001', 'a1', 'success', NULL, '2026-11-20 08:00:00.000', '2026-11-23 06:00:00.000', 'c6a38355-0000-4000-8000-000000000000', 5, 1, NULL, NULL),
('00000000-0000-4000-8000-000000000001', 'a2', 'failed', NULL, '2026-11-20 09:00:00.000', '2026-11-23 06:00:00.000', 'c6a38355-0000-4000-8000-000000000000', 5, 2, NULL, NULL),
('00000000-0000-4000-8000-000000000001', 'a3', 'success', NULL, '2026-11-20 10:00:00.000', '2026-11-23 06:00:00.000', 'c6a38355-0000-4000-8000-000000000000', 5, 1, NULL, NULL),
('00000000-0000-4000-8000-000000000002', 'b1', 'success', NULL, '2026-11-20 08:00:00.000', '2026-11-23 06:00:00.000', 'c6a38355-0000-4000-8000-000000000000', -1, 0, NULL, NULL),
('00000000-0000-4000-8000-000000000003', 'c1', 'success', NULL, '2026-11-20 08:00:00.000', '2026-11-23 06:00:00.000', 'c6a38355-0000-4000-8000-000000000000', 10, 0, NULL, NULL),
('00000000-0000-4000-8000-000000000007', 'o1', 'success', NULL, '2026-11-20 08:00:00.000', '2026-11-23 06:00:00.000', 'b0000000-0000-4000-8000-000000000000', 5, 1, NULL, NULL),
('00000000-0000-4000-8000-000000000004', 'd1', 'success', '2026-11-21 07:59:55.000', '2026-11-21 08:00:00.000', '2026-11-23 06:00:00.000', 'c6a38355-0000-4000-8000-000000000000', NULL, 0, NULL, NULL),
('00000000-0000-4000-8000-000000000004', 'd2', 'success', NULL, '2026-11-21 09:00:00.000', '2026-11-23 06:00:00.000', 'c6a38355-0000-4000-8000-000000000000', NULL, 0, NULL, NULL),
('00000000-0000-4000-8000-000000000005', 'e1', 'deleted', NULL, '2026-11-21 08:00:00.000', '2026-11-23 06:00:00.000', 'c6a38355-0000-4000-8000-000000000000', 10, 0, NULL, NULL),
('00000000-0000-4000-8000-000000000005', 'e2', ' DELETED ', NULL, '2026-11-21 09:00:00.000', '2026-11-23 06:00:00.000', 'c6a38355-0000-4000-8000-000000000000', 10, 0, NULL, NULL),
('00000000-0000-4000-8000-000000000005', 'e3', 'success', NULL, '2026-11-21 10:00:00.000', '2026-11-23 06:00:00.000', 'c6a38355-0000-4000-8000-000000000000', 5, 1, NULL, NULL),
('00000000-0000-4000-8000-000000000009', 'g1', 'failed', NULL, '2026-11-22 08:00:00.000', '2026-11-23 06:00:00.000', 'c6a38355-0000-4000-8000-000000000000', 5, 1, 'tA', NULL),
('00000000-0000-4000-8000-000000000009', 'g2', 'success', NULL, '2026-11-22 09:00:00.000', '2026-11-23 06:00:00.000', 'c6a38355-0000-4000-8000-000000000000', 5, 0, NULL, NULL);
INSERT INTO ci_pipeline_runs (repo_id, run_id, status, queued_at, started_at, last_synced, org_id, queue_seconds, retry_count, team_id, service_id) VALUES
('00000000-0000-4000-8000-000000000008', 'f1', 'success', NULL, '2026-11-22 08:00:00.000', '2026-11-23 10:00:00.000', 'c6a38355-0000-4000-8000-000000000000', 10, 0, NULL, NULL);
INSERT INTO ci_pipeline_runs (repo_id, run_id, status, queued_at, started_at, last_synced, org_id, queue_seconds, retry_count, team_id, service_id) VALUES
('00000000-0000-4000-8000-000000000008', 'f1', 'success', NULL, '2026-11-22 08:00:00.000', '2026-11-23 11:00:00.000', 'c6a38355-0000-4000-8000-000000000000', 10, 1, NULL, NULL);
INSERT INTO ci_pipeline_runs (repo_id, run_id, status, queued_at, started_at, last_synced, org_id, queue_seconds, retry_count)
SELECT generateUUIDv4(), toString(number), 'success', NULL, toDateTime64('2026-11-01 12:00:00', 3, 'UTC'), toDateTime64('2026-11-23 06:00:00', 3, 'UTC'), 'c6a38355-0000-4000-8000-000000000000', NULL, 1
FROM numbers(2000);
`

type rb14488bStatement struct{ label, text string }

func rb14488bSplit(body string) []rb14488bStatement {
	var out []rb14488bStatement
	var chunk []string
	names := []string{"S0", "stmt0", "stmt1", "stmt2", "stmt3"}
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
		out = append(out, rb14488bStatement{label: label, text: text})
		chunk = nil
	}
	return out
}

func rb14488bRender(v any, dbType string) string {
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

func rb14488bRun(ctx context.Context, conn driver.Conn, text, comment string) ([]map[string]string, error) {
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
			row[name] = rb14488bRender(cells[i], types[i].DatabaseTypeName())
		}
		got = append(got, row)
	}
	return got, rows.Err()
}

func rb14488bLog(ctx context.Context, t *testing.T, conn driver.Conn, comment string) ([]string, uint64) {
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

func rb14488bDiff(got, want []map[string]string) string {
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

func TestReadback144882FormExecutesAtTheChainHead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	sum := sha256.Sum256([]byte(rb14488bForm))
	if got := hex.EncodeToString(sum[:]); got != rb14488bFormSHA256 {
		t.Fatalf("form sha256 = %s, want %s", got, rb14488bFormSHA256)
	}
	statements := rb14488bSplit(rb14488bForm)
	byLabel := map[string]string{}
	labels := make([]string, 0, len(statements))
	for _, s := range statements {
		labels = append(labels, s.label)
		byLabel[s.label] = s.text
	}
	if strings.Join(labels, ",") != "S0,stmt0,stmt1,stmt2,stmt3" {
		t.Fatalf("form statements = %v", labels)
	}
	// stmt0..stmt3 each hold exactly one gate over exactly ci_pipeline_runs; S0 holds none.
	for _, s := range statements {
		want := 1
		if s.label == "S0" {
			want = 0
		}
		if n := strings.Count(s.text, rb14488bRowsCap); n != want {
			t.Fatalf("%s: %d rows caps, want %d", s.label, n, want)
		}
		if n := strings.Count(s.text, rb14488bBytesCap); n != want {
			t.Fatalf("%s: %d bytes caps, want %d", s.label, n, want)
		}
		if s.label == "S0" {
			continue
		}
		if n := strings.Count(s.text, "'STOP-COST-GATE'"); n != 1 {
			t.Fatalf("%s: %d STOP-COST-GATE, want 1", s.label, n)
		}
		if n := strings.Count(s.text, "table IN ('ci_pipeline_runs')"); n != 2 {
			t.Fatalf("%s: gate table list found %d times, want 2", s.label, n)
		}
		if n := strings.Count(s.text, "cost_gate = 0"); n != 1 {
			t.Fatalf("%s: cost_gate = 0 found %d times, want 1", s.label, n)
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

	for _, statement := range chmigrate.SplitStatements(rb14488bFixture) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("fixture: %v\n%s", err, statement)
		}
	}
	var tableRows, parts uint64
	if err := conn.QueryRow(ctx, `SELECT sum(rows), count() FROM system.parts
		WHERE active AND database = currentDatabase() AND table = 'ci_pipeline_runs'`).Scan(&tableRows, &parts); err != nil {
		t.Fatal(err)
	}
	t.Logf("fixture: table_rows=%d active_parts=%d", tableRows, parts)
	if tableRows != 2017 || parts != 4 {
		t.Fatalf("fixture rows=%d parts=%d, want 2017 and 4", tableRows, parts)
	}

	// WANT per statement on the fixture (rows in the order the SQL gives them).
	want := map[string][]map[string]string{
		"S0":    {{"t": "ci_pipeline_runs", "rows": fmt.Sprint(tableRows), "active_parts": fmt.Sprint(parts)}},
		"stmt0": {{"orgs": "1"}},
		"stmt1": {{"runs": "12", "groups": "9", "groups_with_sample": "3"}},
		"stmt2": {
			{"day": "2026-11-09", "groups": "1", "groups_no_sample": "1", "repos_no_sample": "1", "runs_in_no_sample_groups": "1"},
			{"day": "2026-11-20", "groups": "3", "groups_no_sample": "2", "repos_no_sample": "2", "runs_in_no_sample_groups": "4"},
			{"day": "2026-11-21", "groups": "2", "groups_no_sample": "1", "repos_no_sample": "1", "runs_in_no_sample_groups": "1"},
			{"day": "2026-11-22", "groups": "3", "groups_no_sample": "2", "repos_no_sample": "2", "runs_in_no_sample_groups": "2"},
		},
		"stmt3": {
			{"day": "2026-11-09", "repodays_all_no_sample": "1", "repodays": "1"},
			{"day": "2026-11-20", "repodays_all_no_sample": "2", "repodays": "3"},
			{"day": "2026-11-21", "repodays_all_no_sample": "1", "repodays": "2"},
			{"day": "2026-11-22", "repodays_all_no_sample": "1", "repodays": "2"},
		},
	}
	passComments := map[string]string{}
	passRows := map[string][]map[string]string{}
	for _, s := range statements {
		passComments[s.label] = fmt.Sprintf("rb144882-pass-%s-%d", s.label, time.Now().UnixNano())
		got, err := rb14488bRun(ctx, conn, s.text, passComments[s.label])
		if err != nil {
			t.Errorf("PASS %s: SQL error: %v", s.label, err)
			continue
		}
		passRows[s.label] = got
		t.Logf("PASS %s ROWS %v", s.label, got)
		if d := rb14488bDiff(got, want[s.label]); d != "" {
			t.Errorf("PASS %s: %s", s.label, d)
		}
	}

	// stmt3 lower bound <= stmt2 upper bound, day by day (the form's cross-check WANT).
	s2, s3 := passRows["stmt2"], passRows["stmt3"]
	if len(s2) != len(s3) {
		t.Errorf("stmt2 has %d days, stmt3 has %d", len(s2), len(s3))
	} else {
		for i := range s2 {
			var lower, upper int
			fmt.Sscan(s3[i]["repodays_all_no_sample"], &lower)
			fmt.Sscan(s2[i]["repos_no_sample"], &upper)
			if s2[i]["day"] != s3[i]["day"] || lower > upper {
				t.Errorf("cross-check day %s/%s: lower %d > upper %d", s2[i]["day"], s3[i]["day"], lower, upper)
			}
		}
	}

	// Control mutations: each must change the result so its WANT fails.
	mutate := func(label, from, to string) []map[string]string {
		mutated := strings.Replace(byLabel[label], from, to, 1)
		if mutated == byLabel[label] {
			t.Fatalf("MUTATE %s: %q not found", label, from)
		}
		got, err := rb14488bRun(ctx, conn, mutated, fmt.Sprintf("rb144882-mutate-%s-%d", label, time.Now().UnixNano()))
		if err != nil {
			t.Fatalf("MUTATE %s %q: %v", label, from, err)
		}
		t.Logf("MUTATE %s %q -> %q ROWS %v", label, from, to, got)
		return got
	}
	// S0 on a wrong table name: rows 0 and active_parts 0 (WANT rows > 0 fails).
	if got := mutate("S0", "table = 'ci_pipeline_runs'", "table = 'ci_pipeline_runz'"); len(got) != 1 || got[0]["rows"] != "0" || got[0]["active_parts"] != "0" {
		t.Errorf("MUTATE S0 wrong table: %v, want rows 0 and active_parts 0", got)
	}
	// stmt0 on another prefix: orgs 0 (WANT orgs = 1 fails).
	if got := mutate("stmt0", "'c6a38355'", "'zzzzzzzz'"); len(got) != 1 || got[0]["orgs"] != "0" {
		t.Errorf("MUTATE stmt0 prefix: %v, want orgs 0", got)
	}
	// stmt1 with every run a retry: groups_with_sample 0 (WANT > 0 fails).
	if got := mutate("stmt1", "retry_count = 0", "retry_count > 99"); len(got) != 1 || got[0]["groups_with_sample"] != "0" || got[0]["runs"] != "12" {
		t.Errorf("MUTATE stmt1 retry: %v, want groups_with_sample 0, runs 12", got)
	}
	// stmt1 with a window after every row: runs 0 (WANT runs > 0 fails).
	if got := mutate("stmt1", "2026-11-09 00:00:00", "2030-01-01 00:00:00"); len(got) != 1 || got[0]["runs"] != "0" || got[0]["groups_with_sample"] != "0" {
		t.Errorf("MUTATE stmt1 window: %v, want runs 0, groups_with_sample 0", got)
	}
	// stmt2 and stmt3 sensitivity: each filter or key change moves the numbers.
	differs := func(name string, got, base []map[string]string) {
		if rb14488bDiff(got, base) == "" {
			t.Errorf("MUTATE %s: result equals the clean result, the seed does not cover it", name)
		}
	}
	differs("stmt2 no FINAL", mutate("stmt2", "FROM ci_pipeline_runs FINAL", "FROM ci_pipeline_runs"), want["stmt2"])
	differs("stmt3 no FINAL", mutate("stmt3", "FROM ci_pipeline_runs FINAL", "FROM ci_pipeline_runs"), want["stmt3"])
	differs("stmt2 no deleted filter", mutate("stmt2", "!= 'deleted'", "!= 'zzz'"), want["stmt2"])
	differs("stmt3 no deleted filter", mutate("stmt3", "!= 'deleted'", "!= 'zzz'"), want["stmt3"])
	differs("stmt2 no org filter", mutate("stmt2", "startsWith(org_id, 'c6a38355') AND cost_gate = 0", "cost_gate = 0"), want["stmt2"])
	differs("stmt2 wide window", mutate("stmt2", "2026-11-09 00:00:00", "2026-11-08 00:00:00"), want["stmt2"])
	differs("stmt3 wide window", mutate("stmt3", "2026-11-09 00:00:00", "2026-11-08 00:00:00"), want["stmt3"])
	differs("stmt2 retry rule", mutate("stmt2", "retry_count = 0 AND ", ""), want["stmt2"])
	differs("stmt2 queued_at rule", mutate("stmt2", "OR (queued_at IS NOT NULL AND started_at >= queued_at)", ""), want["stmt2"])
	differs("stmt2 key without team", mutate("stmt2", "ifNull(team_id, '') AS team", "'' AS team"), want["stmt2"])

	// PASS-path cells: query_log read_rows = T + 2P + 1 (one table scan + gate system.parts twice + system.one).
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	for _, s := range statements {
		types, read := rb14488bLog(ctx, t, conn, passComments[s.label])
		t.Logf("PASS %s query_log types=%v read_rows=%d (T=%d P=%d)", s.label, types, read, tableRows, parts)
		if len(types) != 1 || types[0] != "QueryFinish" {
			t.Errorf("PASS %s: query_log types %v, want [QueryFinish]", s.label, types)
		}
		if s.label == "S0" {
			continue
		}
		base := tableRows + 2*parts + 1
		if s.label == "stmt0" {
			if read != base {
				t.Errorf("PASS %s: query_log read_rows %d, want T + 2P + 1 = %d", s.label, read, base)
			}
			continue
		}
		if read < base || read > 2*tableRows+2*parts+1 {
			t.Errorf("PASS %s: query_log read_rows %d outside [T + 2P + 1, 2T + 2P + 1] = [%d, %d]", s.label, read, base, 2*tableRows+2*parts+1)
		}
		t.Logf("PASS %s FINAL read_rows - (T + 2P + 1) = %d", s.label, int64(read)-int64(base))
	}

	// Diagnostic: where the FINAL scan reads more than T. Bare scans, no gate.
	partRows, err := conn.Query(ctx, `SELECT name, rows, min_block_number FROM system.parts
		WHERE active AND database = currentDatabase() AND table = 'ci_pipeline_runs' ORDER BY min_block_number`)
	if err != nil {
		t.Fatal(err)
	}
	for partRows.Next() {
		var name string
		var n uint64
		var blk int64
		if err := partRows.Scan(&name, &n, &blk); err != nil {
			t.Fatal(err)
		}
		t.Logf("DIAG part %s rows=%d", name, n)
	}
	partRows.Close()
	diag := map[string]string{
		"nofinal":          "SELECT count() FROM ci_pipeline_runs WHERE startsWith(org_id, 'c6a38355')",
		"final":            "SELECT count() FROM ci_pipeline_runs FINAL WHERE startsWith(org_id, 'c6a38355')",
		"final-noorg":      "SELECT count() FROM ci_pipeline_runs FINAL",
		"final-window":     "SELECT count() FROM ci_pipeline_runs FINAL WHERE startsWith(org_id, 'c6a38355') AND started_at >= toDateTime64('2026-11-09 00:00:00', 3, 'UTC')",
		"final-group":      "SELECT count() FROM (SELECT repo_id, count() AS c FROM ci_pipeline_runs FINAL WHERE startsWith(org_id, 'c6a38355') GROUP BY repo_id)",
		"final-onepart":    "SELECT count() FROM ci_pipeline_runs FINAL WHERE org_id = 'b0000000-0000-4000-8000-000000000000'",
		"final-onepartc6a": "SELECT count() FROM ci_pipeline_runs FINAL WHERE org_id = 'c6a38355-0000-4000-8000-000000000000' AND repo_id = '00000000-0000-4000-8000-000000000008'",
	}
	diagComments := map[string]string{}
	for name, q := range diag {
		diagComments[name] = fmt.Sprintf("rb144882-diag-%s-%d", name, time.Now().UnixNano())
		if _, err := rb14488bRun(ctx, conn, q, diagComments[name]); err != nil {
			t.Errorf("DIAG %s: %v", name, err)
		}
	}
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	for name, c := range diagComments {
		types, read := rb14488bLog(ctx, t, conn, c)
		t.Logf("DIAG %s types=%v read_rows=%d", name, types, read)
	}

	// Gate fires: each cap literal lowered to 0 in a test copy. WANT code 395 STOP-COST-GATE before any table read.
	type fired struct{ label, cap, comment string }
	var fires []fired
	for _, s := range statements {
		if s.label == "S0" {
			continue
		}
		for _, literal := range []string{rb14488bRowsCap, rb14488bBytesCap} {
			lowered := strings.Replace(s.text, literal, "> 0", 1)
			comment := fmt.Sprintf("rb144882-gate-%s-%s-%d", s.label, strings.TrimPrefix(literal, "> "), time.Now().UnixNano())
			_, err := rb14488bRun(ctx, conn, lowered, comment)
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
		types, read := rb14488bLog(ctx, t, conn, f.comment)
		t.Logf("GATE %s %s query_log types=%v read_rows=%d bound=%d", f.label, f.cap, types, read, 2*parts+1)
		if len(types) != 1 || types[0] != "ExceptionBeforeStart" {
			t.Errorf("GATE %s %s: query_log types %v, want [ExceptionBeforeStart]", f.label, f.cap, types)
		}
		if read > 2*parts+1 {
			t.Errorf("GATE %s %s: read_rows %d over 2 x P + 1 = %d", f.label, f.cap, read, 2*parts+1)
		}
	}
	if len(fires) != 8 {
		t.Errorf("gates fired %d times, want 8 (4 statements x 2 caps)", len(fires))
	}
}
