//go:build integration

package chmigrate_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// DO NOT MERGE. This file executes FORM-14190-CH-1.sql on the head schema. The text below is the .sql with its org
// prefix replaced by {{ORG_PREFIX}} and its run prefix by {{RUN_PREFIX}}, nothing else (its sha256 is checked); the cell
// puts a synthetic prefix in at run time and checks that every FROM got it. It checks each WANT in two phases (pending
// old days and no touched key since T0; then drained), an ORG and a RULE mutant on every data statement, the pass-path
// read_rows, and fires each cost gate cap once (pm-guard FORM-TEMPLATE-3).
const (
	formExecCh14190SHA256  = "ee7a38c2e2608ac77f4b61670b91ffb8d23d8bd7f005d2d86eca6b45b54ffb36"
	formExecCh14190OrgSlot = "{{ORG_PREFIX}}"
	formExecCh14190RunSlot = "{{RUN_PREFIX}}"
	formExecCh14190SQL     = `-- FORM-14190-CH-1 (lane-14190c, 2026-11-23, ops pin a7febdf50002). ClickHouse prod, SELECT only, CHAOS-14190.
-- Run FIRST, then FORM-14190-DAILY-3 (PG). The backfill (sync run prefix {{RUN_PREFIX}}, T0 2026-11-23 14:50:56 UTC) records
-- the (day, repository) keys its raw rows touched in daily_metrics_touched_days; a key is pending while its newest
-- 'touched' event is newer than its newest 'dispatched' event, and 'dispatched' is written after the daily run of that
-- day committed in Postgres. WANT: no pending day of the org before 2026-11-09 (the days outside the fan-out's window
-- runs). Per acp READ-FORM-14190-DAILY-2-1 (de27f6d8ff91) P1-1. WANT text and work math: FORM-14190-CH-1.md.
-- Shape: pm-guard/FORM-TEMPLATE-3.md (sha12 6b9e73a3c45a), ClickHouse form. No SET, SETTINGS, EXPLAIN, FORMAT or write.
-- Org prefix {{ORG_PREFIX}} by startsWith on every FROM with org_id. Each data statement starts with WITH (SELECT
-- throwIf(...)) AS cost_gate over daily_metrics_touched_days (system.parts only), and every WHERE of a query level, and
-- the outer WHERE, tests the gate. Rows cap 6000000, bytes cap 4294967296. No FINAL: the reader contract aggregates.
-- Run each statement once, in order. A SQL error (STOP-COST-GATE included) = STOP and report.

-- S0 INFO cost inputs, metadata only (system.one + system.parts). WANT 1 row, table_rows > 0.
SELECT l.t AS table_name, p.table_rows AS table_rows, p.disk_bytes AS disk_bytes, p.active_parts AS active_parts
FROM (SELECT 'daily_metrics_touched_days' AS t FROM system.one) AS l
LEFT JOIN (
    SELECT table, sum(rows) AS table_rows, sum(bytes_on_disk) AS disk_bytes, count() AS active_parts
    FROM system.parts
    WHERE active AND database = currentDatabase() AND table IN ('daily_metrics_touched_days')
    GROUP BY table
) AS p ON p.table = l.t;

-- K0 ORG-1. WANT orgs = 1, org_rows > 0.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('daily_metrics_touched_days')) > 6000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('daily_metrics_touched_days')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT uniqExact(org_id) AS orgs, count() AS org_rows
FROM daily_metrics_touched_days
WHERE startsWith(org_id, '{{ORG_PREFIX}}') AND cost_gate = 0;

-- K1 the org's pending keys, by the reader contract (internal/syncdispatchruntime/touched_days_clickhouse.go:393-398),
-- grouped by (org_id, day, repo_id). WANT pending_days_old = 0 (pending days before 2026-11-09). INFO pending keys,
-- recent pending days (live syncs touch these), the oldest and newest pending day (1970-01-01 = none).
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('daily_metrics_touched_days')) > 6000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('daily_metrics_touched_days')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT uniqExactIf(p.day, p.day < toDate('2026-11-09')) AS pending_days_old,
       countIf(p.day < toDate('2026-11-09')) AS pending_keys_old,
       uniqExactIf(p.day, p.day >= toDate('2026-11-09')) AS pending_days_recent,
       count() AS pending_keys,
       min(p.day) AS oldest_pending_day,
       max(p.day) AS newest_pending_day
FROM (
    SELECT org_id, day, repo_id
    FROM daily_metrics_touched_days
    WHERE startsWith(org_id, '{{ORG_PREFIX}}') AND cost_gate = 0
    GROUP BY org_id, day, repo_id
    HAVING maxIf(at, kind = 'touched') > maxIf(at, kind = 'dispatched')
) AS p
WHERE cost_gate = 0;

-- K2 control: the org's keys with a 'touched' event at or after T0 - 5 min (2026-11-23 14:45:56 UTC). WANT
-- touched_keys > 0 (the backfill touched days at all; live syncs also touch, so it is a floor). INFO days, the first
-- and last day, the first and last event minute (UTC).
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('daily_metrics_touched_days')) > 6000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('daily_metrics_touched_days')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT uniqExact(day, repo_id) AS touched_keys,
       uniqExact(day) AS touched_days,
       min(day) AS first_day,
       max(day) AS last_day,
       formatDateTime(min(at), '%Y-%m-%d %H:%i') AS first_minute,
       formatDateTime(max(at), '%Y-%m-%d %H:%i') AS last_minute
FROM daily_metrics_touched_days
WHERE startsWith(org_id, '{{ORG_PREFIX}}') AND cost_gate = 0
  AND kind = 'touched' AND at >= toDateTime64('2026-11-23 14:45:56', 3, 'UTC');
`
	formExecCh14190OrgPrefix = "f1f1f1f1"
)

type formExecCh14190Result struct {
	columns []string
	rows    [][]string
}

func formExecCh14190SQLLines(text string) string {
	var out strings.Builder
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, "--") {
			out.WriteString(line + "\n")
		}
	}
	return out.String()
}

// formExecCh14190Statements splits the FORM as it is run: a statement ends at a SQL line (not a "--" comment line)
// that ends with ";". Each is named by its first comment line "-- <id> ".
func formExecCh14190Statements(t *testing.T, text string) ([]string, map[string]string) {
	t.Helper()
	statementID := regexp.MustCompile(`^(S0|K[0-2])$`)
	var ids []string
	statements := map[string]string{}
	var current []string
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "--") || !strings.HasSuffix(strings.TrimRight(line, " "), ";") {
			current = append(current, line)
			continue
		}
		current = append(current, strings.TrimSuffix(strings.TrimRight(line, " "), ";"))
		statement := strings.TrimSpace(strings.Join(current, "\n"))
		current = nil
		id := ""
		for _, commentLine := range strings.Split(statement, "\n") {
			if fields := strings.Fields(commentLine); strings.HasPrefix(commentLine, "--") && id == "" && len(fields) > 1 && statementID.MatchString(fields[1]) {
				id = fields[1]
			}
		}
		if id == "" {
			t.Fatalf("a statement has no id line:\n%s", statement)
		}
		if _, seen := statements[id]; seen {
			t.Fatalf("statement %s twice", id)
		}
		ids = append(ids, id)
		statements[id] = statement
	}
	if strings.TrimSpace(formExecCh14190SQLLines(strings.Join(current, "\n"))) != "" {
		t.Fatalf("SQL after the last statement:\n%s", strings.Join(current, "\n"))
	}
	return ids, statements
}

func formExecCh14190Query(ctx context.Context, conn driver.Conn, statement string) (formExecCh14190Result, error) {
	rows, err := conn.Query(ctx, statement)
	if err != nil {
		return formExecCh14190Result{}, err
	}
	defer func() { _ = rows.Close() }()
	result := formExecCh14190Result{columns: rows.Columns()}
	types := rows.ColumnTypes()
	for rows.Next() {
		dest := make([]any, len(types))
		for index, columnType := range types {
			dest[index] = reflect.New(columnType.ScanType()).Interface()
		}
		if err := rows.Scan(dest...); err != nil {
			return formExecCh14190Result{}, fmt.Errorf("scan: %w", err)
		}
		row := make([]string, len(dest))
		for index := range dest {
			value := reflect.ValueOf(dest[index]).Elem()
			for value.Kind() == reflect.Pointer && !value.IsNil() {
				value = value.Elem()
			}
			if value.Kind() == reflect.Pointer {
				row[index] = "NULL"
				continue
			}
			row[index] = fmt.Sprint(value.Interface())
		}
		result.rows = append(result.rows, row)
	}
	return result, rows.Err()
}

func formExecCh14190Cell(t *testing.T, result formExecCh14190Result, label, column string) string {
	t.Helper()
	if len(result.rows) != 1 {
		t.Fatalf("%s: %d row(s), want 1", label, len(result.rows))
	}
	for index, name := range result.columns {
		if name == column {
			return result.rows[0][index]
		}
	}
	t.Fatalf("%s: no column %s in %v", label, column, result.columns)
	return ""
}

// formExecCh14190Want checks the named columns; a value ending in "*" is a prefix.
func formExecCh14190Want(t *testing.T, result formExecCh14190Result, label string, want map[string]string) {
	t.Helper()
	for column, value := range want {
		got := formExecCh14190Cell(t, result, label, column)
		if prefix, isPrefix := strings.CutSuffix(value, "*"); isPrefix {
			if !strings.HasPrefix(got, prefix) {
				t.Errorf("%s %s = %s, want prefix %s", label, column, got, prefix)
			}
			continue
		}
		if got != value {
			t.Errorf("%s %s = %s, want %s", label, column, got, value)
		}
	}
}

// TestFormExecCh14190ReadsThePendingTouchedDays. Org f1f1f1f1-... and org e1e1e1e1-... (must not count). Phase 1 (one
// INSERT, one part): org: 11-02 touched then dispatched (done), 11-03 touched and never dispatched (pending, old),
// 11-12 dispatched then touched again (pending, recent); other org: 11-04 touched (pending, old); no 'touched' event
// since T0 - 5 min. Phase 2 (a second INSERT): org: 11-03 dispatched at 15:10, 11-05 touched 15:00 and dispatched 15:05;
// other org: 11-06 touched 15:00 (pending, old).
func TestFormExecCh14190ReadsThePendingTouchedDays(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	exec := func(statement string) {
		t.Helper()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	sum := sha256.Sum256([]byte(formExecCh14190SQL))
	if got := hex.EncodeToString(sum[:]); got != formExecCh14190SHA256 {
		t.Fatalf("sha256 of the embedded text = %s, want %s (FORM-14190-CH-1.sql with %s and %s)", got, formExecCh14190SHA256,
			formExecCh14190OrgSlot, formExecCh14190RunSlot)
	}
	ids, statements := formExecCh14190Statements(t, formExecCh14190SQL)
	if got := strings.Join(ids, ","); got != "S0,K0,K1,K2" {
		t.Fatalf("statements = %s, want S0,K0,K1,K2", got)
	}
	gates := []struct {
		id              string
		uses            int
		fixtureReadRows uint64
	}{
		// Phase 1 fixture: 6 rows in 1 part. A gate evaluation reads 2 x P + 1, each further cost_gate reference 1 more
		// row, a plain read of a one-part table its rows once.
		{"K0", 1, 6 + 3},
		{"K1", 2, 6 + 3 + 1},
		{"K2", 1, 6 + 3},
	}
	for _, id := range ids {
		sqlLines := formExecCh14190SQLLines(statements[id])
		froms := strings.Count(sqlLines, "FROM daily_metrics_touched_days")
		filters := strings.Count(sqlLines, "startsWith(org_id, '"+formExecCh14190OrgSlot+"')")
		if froms != filters || strings.Count(sqlLines, formExecCh14190OrgSlot) != filters {
			t.Errorf("%s: %d FROM daily_metrics_touched_days, %d org filters, %d org slots", id, froms, filters, strings.Count(sqlLines, formExecCh14190OrgSlot))
		}
		if id != "S0" && filters == 0 {
			t.Errorf("%s: no org filter", id)
		}
		upper := strings.ToUpper(sqlLines)
		for _, banned := range []string{"SET ", "SETTINGS", "EXPLAIN", "FORMAT ", "INSERT", "ALTER", "FINAL"} {
			if strings.Contains(upper, banned) {
				t.Errorf("%s holds %q", id, banned)
			}
		}
		statements[id] = strings.ReplaceAll(strings.ReplaceAll(statements[id], formExecCh14190OrgSlot, formExecCh14190OrgPrefix), formExecCh14190RunSlot, "b2b2b2b2")
	}
	for _, gate := range gates {
		text := statements[gate.id]
		for _, check := range []struct {
			what      string
			got, want int
		}{
			{"WITH (SELECT throwIf(", strings.Count(text, "WITH (SELECT throwIf("), 1},
			{"> 6000000", strings.Count(text, "> 6000000"), 1},
			{"> 4294967296", strings.Count(text, "> 4294967296"), 1},
			{"table IN ('daily_metrics_touched_days')", strings.Count(text, "table IN ('daily_metrics_touched_days')"), 2},
			{"cost_gate = 0", strings.Count(text, "cost_gate = 0"), gate.uses},
		} {
			if check.got != check.want {
				t.Errorf("%s holds %q %d time(s), want %d", gate.id, check.what, check.got, check.want)
			}
		}
	}

	const (
		org, other = formExecCh14190OrgPrefix + "-1419-4000-8000-0000000000b1", "e1e1e1e1-1419-4000-8000-0000000000b2"
		repoA      = "00000000-0000-0000-0000-00000000000a"
		repoB      = "00000000-0000-0000-0000-00000000000b"
		repoC      = "00000000-0000-0000-0000-00000000000c"
	)
	exec("SYSTEM STOP MERGES daily_metrics_touched_days")
	row := func(orgID, day, repo, kind, at string) string {
		return fmt.Sprintf("('%s', '%s', '%s', '%s', '%s')", orgID, day, repo, kind, at)
	}
	insert := "INSERT INTO daily_metrics_touched_days (org_id, day, repo_id, kind, at) VALUES "
	exec(insert + strings.Join([]string{
		row(org, "2026-11-02", repoA, "touched", "2026-11-20 00:00:00.000"),
		row(org, "2026-11-02", repoA, "dispatched", "2026-11-20 01:00:00.000"),
		row(org, "2026-11-03", repoA, "touched", "2026-11-20 02:00:00.000"),
		row(org, "2026-11-12", repoB, "touched", "2026-11-20 02:00:00.000"),
		row(org, "2026-11-12", repoB, "dispatched", "2026-11-20 01:00:00.000"),
		row(other, "2026-11-04", repoC, "touched", "2026-11-20 02:00:00.000"),
	}, ", "))

	var version string
	if err := conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("ClickHouse version %s", version)
	run := func(phase string) (map[string]formExecCh14190Result, map[string]string) {
		t.Helper()
		results, comments := map[string]formExecCh14190Result{}, map[string]string{}
		for _, id := range ids {
			comment := fmt.Sprintf("form14190ch-%s-%s-%d", phase, id, time.Now().UnixNano())
			comments[id] = comment
			qctx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}))
			result, err := formExecCh14190Query(qctx, conn, statements[id])
			if err != nil {
				t.Fatalf("FORM-14190-CH-1 %s %s: %v", phase, id, err)
			}
			for index, values := range result.rows {
				t.Logf("FORM-14190-CH-1 %s %s row %d %v %v", phase, id, index, result.columns, values)
			}
			results[id] = result
		}
		return results, comments
	}

	// Phase 1: an old pending day and no touched key since T0 - 5 min: the K1 WANT and the K2 control fail.
	results, passComments := run("phase1")
	formExecCh14190Want(t, results["S0"], "phase1 S0", map[string]string{"table_name": "daily_metrics_touched_days", "table_rows": "6", "active_parts": "1"})
	formExecCh14190Want(t, results["K0"], "phase1 K0", map[string]string{"orgs": "1", "org_rows": "5"})
	formExecCh14190Want(t, results["K1"], "phase1 K1", map[string]string{"pending_days_old": "1", "pending_keys_old": "1",
		"pending_days_recent": "1", "pending_keys": "2", "oldest_pending_day": "2026-11-03*", "newest_pending_day": "2026-11-12*"})
	formExecCh14190Want(t, results["K2"], "phase1 K2", map[string]string{"touched_keys": "0", "touched_days": "0"})

	// PASS-path cells: each data statement's query_log read_rows = the work math on this fixture.
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	queryLog := func(label, comment string) ([]string, uint64) {
		t.Helper()
		rows, err := conn.Query(ctx, `SELECT toString(type), read_rows FROM system.query_log
			WHERE log_comment = ? AND type != 'QueryStart' ORDER BY event_time_microseconds`, comment)
		if err != nil {
			t.Fatalf("%s: query log: %v", label, err)
		}
		defer func() { _ = rows.Close() }()
		var types []string
		var read uint64
		for rows.Next() {
			var typ string
			var readRows uint64
			if err := rows.Scan(&typ, &readRows); err != nil {
				t.Fatalf("%s: query log scan: %v", label, err)
			}
			types = append(types, typ)
			read += readRows
		}
		t.Logf("%s query_log types=%v read_rows=%d", label, types, read)
		return types, read
	}
	for _, gate := range gates {
		types, read := queryLog("PASS "+gate.id, passComments[gate.id])
		if len(types) != 1 || types[0] != "QueryFinish" {
			t.Errorf("PASS %s: query_log types %v, want [QueryFinish]", gate.id, types)
		}
		if read != gate.fixtureReadRows {
			t.Errorf("PASS %s: query_log read_rows %d, want %d (work math on this fixture)", gate.id, read, gate.fixtureReadRows)
		}
	}

	// The gates fire: each cap literal lowered to 0 in a test copy. WANT code 395 STOP-COST-GATE, ExceptionBeforeStart,
	// read_rows <= 2 x P + 1 (P = 1 part).
	for _, gate := range gates {
		for _, literal := range []string{"> 6000000", "> 4294967296"} {
			comment := fmt.Sprintf("form14190ch-gate-%s-%s-%d", gate.id, strings.TrimPrefix(literal, "> "), time.Now().UnixNano())
			qctx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}))
			_, err := formExecCh14190Query(qctx, conn, strings.Replace(statements[gate.id], literal, "> 0", 1))
			if err == nil || !strings.Contains(err.Error(), "STOP-COST-GATE") || !strings.Contains(err.Error(), "395") {
				t.Errorf("GATE %s %s lowered to 0: err = %v, want code 395 STOP-COST-GATE", gate.id, literal, err)
				continue
			}
			if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
				t.Fatal(err)
			}
			types, read := queryLog("GATE "+gate.id+" "+literal, comment)
			if len(types) != 1 || types[0] != "ExceptionBeforeStart" {
				t.Errorf("GATE %s %s: query_log types %v, want [ExceptionBeforeStart]", gate.id, literal, types)
			}
			if read > 2*1+1 {
				t.Errorf("GATE %s %s: read_rows %d over 2 x P + 1 = 3", gate.id, literal, read)
			}
		}
	}

	// Phase 2: the old day drained, a touched key since T0 - 5 min: every WANT holds.
	exec(insert + strings.Join([]string{
		row(org, "2026-11-03", repoA, "dispatched", "2026-11-23 15:10:00.000"),
		row(org, "2026-11-05", repoA, "touched", "2026-11-23 15:00:00.000"),
		row(org, "2026-11-05", repoA, "dispatched", "2026-11-23 15:05:00.000"),
		row(other, "2026-11-06", repoC, "touched", "2026-11-23 15:00:00.000"),
	}, ", "))
	results, _ = run("phase2")
	formExecCh14190Want(t, results["K0"], "phase2 K0", map[string]string{"orgs": "1", "org_rows": "8"})
	formExecCh14190Want(t, results["K1"], "phase2 K1", map[string]string{"pending_days_old": "0", "pending_keys_old": "0",
		"pending_days_recent": "1", "pending_keys": "1", "oldest_pending_day": "2026-11-12*", "newest_pending_day": "2026-11-12*"})
	formExecCh14190Want(t, results["K2"], "phase2 K2", map[string]string{"touched_keys": "1", "touched_days": "1",
		"first_day": "2026-11-05*", "last_day": "2026-11-05*", "first_minute": "2026-11-23 15:00", "last_minute": "2026-11-23 15:00"})

	// ORG and RULE mutants on every data statement (phase 2 rows; a test copy; the embedded text stays as checked).
	orgFilter := "startsWith(org_id, '" + formExecCh14190OrgPrefix + "') AND "
	for _, mutant := range []struct {
		name, id, from, to, column, unmutated string
	}{
		{"K0-org", "K0", orgFilter, "", "orgs", "1"},
		{"K0-rule-count-rows", "K0", "uniqExact(org_id) AS orgs", "count() AS orgs", "orgs", "1"},
		{"K1-org", "K1", orgFilter, "", "pending_days_old", "0"},
		{"K1-rule-pending-inverted", "K1", "HAVING maxIf(at, kind = 'touched') > maxIf(at, kind = 'dispatched')",
			"HAVING maxIf(at, kind = 'touched') < maxIf(at, kind = 'dispatched')", "pending_days_old", "0"},
		{"K2-org", "K2", orgFilter, "", "touched_keys", "1"},
		{"K2-rule-kind", "K2", "kind = 'touched' AND at >=", "kind = 'dispatched' AND at >=", "touched_keys", "1"},
	} {
		text := statements[mutant.id]
		if got := strings.Count(text, mutant.from); got != 1 {
			t.Fatalf("MUTANT %s: %q %d time(s) in %s, want 1", mutant.name, mutant.from, got, mutant.id)
		}
		result, err := formExecCh14190Query(ctx, conn, strings.Replace(text, mutant.from, mutant.to, 1))
		if err != nil {
			t.Fatalf("MUTANT %s: %v", mutant.name, err)
		}
		got := formExecCh14190Cell(t, result, "MUTANT "+mutant.name, mutant.column)
		t.Logf("MUTANT %s: %s = %s (unmutated %s)", mutant.name, mutant.column, got, mutant.unmutated)
		if got == mutant.unmutated {
			t.Errorf("MUTANT %s SURVIVED: %s = %s", mutant.name, mutant.column, got)
		}
	}
}
