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

// DO NOT MERGE. This file executes a prod read FORM (the reads around one backfill re-walk and one recompute) byte for byte
// with every prod value and every comment text swapped to a synthetic one (the sha256 below is the sha256 of the synthetic
// copy), on the head schema with a synthetic fixture: the replica gate, the narrowed slot sizing of two repos (cut A and cut B),
// cut A's retraction rows, the per-key write set of both metrics tables, and cut B's full repo id read, before the re-walk,
// after it and after the recompute. Every cost gate cap is fired once.
const formExecRewalkCHSHA256 = "0f663a9dfd6997ee30db86eceb924af8b208905dd597e9e731660f7eaa466915"

const formExecRewalkCHSQL = ``

type formExecRewalkCHResult struct {
	columns []string
	rows    [][]string
}

func formExecRewalkCHStatements(t *testing.T, text string) ([]string, map[string]string) {
	t.Helper()
	statementID := regexp.MustCompile(`^(stmtR|Q1|K2A|K3A|K1|K1b|RID)$`)
	var ids []string
	statements := map[string]string{}
	for _, piece := range strings.Split(text, ";\n") {
		statement := strings.TrimSpace(piece)
		id, hasSQL := "", false
		for _, line := range strings.Split(statement, "\n") {
			if strings.HasPrefix(line, "--") {
				if fields := strings.Fields(line); id == "" && len(fields) > 1 && statementID.MatchString(fields[1]) {
					id = fields[1]
				}
				continue
			}
			if strings.TrimSpace(line) != "" {
				hasSQL = true
			}
		}
		if !hasSQL {
			continue
		}
		if id == "" {
			t.Fatalf("a statement has no marker line:\n%s", statement)
		}
		if _, seen := statements[id]; seen {
			t.Fatalf("statement %s twice", id)
		}
		ids = append(ids, id)
		statements[id] = statement
	}
	return ids, statements
}

func formExecRewalkCHQuery(ctx context.Context, conn driver.Conn, statement string) (formExecRewalkCHResult, error) {
	rows, err := conn.Query(ctx, statement)
	if err != nil {
		return formExecRewalkCHResult{}, err
	}
	defer func() { _ = rows.Close() }()
	result := formExecRewalkCHResult{columns: rows.Columns()}
	types := rows.ColumnTypes()
	for rows.Next() {
		dest := make([]any, len(types))
		for index, columnType := range types {
			dest[index] = reflect.New(columnType.ScanType()).Interface()
		}
		if err := rows.Scan(dest...); err != nil {
			return formExecRewalkCHResult{}, fmt.Errorf("scan: %w", err)
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

func formExecRewalkCHRow(t *testing.T, result formExecRewalkCHResult, label string, row int, want map[string]string) {
	t.Helper()
	if row >= len(result.rows) {
		t.Errorf("%s: %d row(s), want a row %d", label, len(result.rows), row)
		return
	}
	for column, value := range want {
		found := false
		for index, name := range result.columns {
			if name != column {
				continue
			}
			found = true
			got := result.rows[row][index]
			if prefix, isPrefix := strings.CutSuffix(value, "*"); isPrefix {
				if !strings.HasPrefix(got, prefix) {
					t.Errorf("%s row %d %s = %s, want prefix %s", label, row, column, got, prefix)
				}
			} else if got != value {
				t.Errorf("%s row %d %s = %s, want %s", label, row, column, got, value)
			}
		}
		if !found {
			t.Errorf("%s: no column %s in %v", label, column, result.columns)
		}
	}
}

func formExecRewalkCHQueryLog(ctx context.Context, t *testing.T, conn driver.Conn, label, comment string) ([]string, uint64, []string) {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT toString(type), read_rows, arrayStringConcat(tables, ',')
		FROM system.query_log WHERE log_comment = ? AND type != 'QueryStart' ORDER BY event_time_microseconds`, comment)
	if err != nil {
		t.Fatalf("%s: query log: %v", label, err)
	}
	defer func() { _ = rows.Close() }()
	var types, tableLists []string
	var read uint64
	for rows.Next() {
		var typ, tables string
		var readRows uint64
		if err := rows.Scan(&typ, &readRows, &tables); err != nil {
			t.Fatalf("%s: query log scan: %v", label, err)
		}
		types = append(types, typ)
		tableLists = append(tableLists, tables)
		read += readRows
		t.Logf("%s type=%s read_rows=%d tables=%s", label, typ, readRows, tables)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: query log: %v", label, err)
	}
	return types, read, tableLists
}

// formExecRewalkCHFill is the card's step M3: the placeholder literal of the src_cut line replaced by the cut, once.

// The fixture (org f0f0f0f0-...01; another org d0d0d0d0-...02 holds a repo with cut A's prefix that must not count).
//
//	pre:      cut A (b2b2b2b2) run aS on 2026-03-09: slots p1 {s1}, p2 {s2}, agg {s1, s2} with equal heads = 3 candidate slots.
//	          A third github repo (c3c3c3c3) holds the same shape: not in either narrowed read.
//	          cut B (a1a1a1a1) run bS on 2026-03-09: one slot p1 {s1} = no candidate. cut B run bR: suite k1 on 2026-03-09
//	          retracted (stamp 10:00), k2 on 2026-03-10 live; metrics rows on both days, no record = 2 MISSING-MARK keys (the
//	          run's retracted suite day 2026-03-09 is the run day the rule prints). cut B run bO: k1 on 2026-01-20 retracted, a metrics row = 1 MISSING-MARK
//	          key outside the window (run_outside_window 1).
//	post A1:  aS's agg slot retracted (stamp 11:00): cut A has no candidate slot on 2026-03-09 and 2 retraction keys that day.
//	post W:   bR and bO get their own record (touched + window events at 10:30) and recomputes at 10:40: no W key is left.
//	Q mutant: cut B run bQ on 2026-03-09 with the aS shape: Q1 shows the day row (the card STOPs).
//	RID:      cut B's full repos.id: 1 row; a repo of another org with cut B's prefix is not counted; a second repo of the org
//	          with that prefix = 2 rows (the card STOPs). The other org also holds cut A's prefix with candidate slots: not counted.
func TestFormExecRewalkCH(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
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
	const (
		org    = "f0f0f0f0-0000-4000-8000-000000000001"
		other  = "d0d0d0d0-0000-4000-8000-000000000002"
		cutA   = "b2b2b2b2-0000-4000-8000-0000000000a1"
		cutB   = "a1a1a1a1-0000-4000-8000-0000000000b1"
		third  = "c3c3c3c3-0000-4000-8000-0000000000c1"
		otherA = "b2b2b2b2-0000-4000-8000-0000000000d1"
		otherB = "a1a1a1a1-0000-4000-8000-0000000000d2"
		cutB2  = "a1a1a1a1-0000-4000-8000-0000000000b2"
		d25    = "2026-03-16 "
	)
	for _, table := range []string{"test_suite_results", "daily_metrics_touched_days", "testops_test_metrics_daily", "testops_release_confidence", "repos"} {
		exec("SYSTEM STOP MERGES " + table)
	}
	suite := func(orgID, repo, run, suiteID, slot, name string, total, passed int, day, stamp string, retracted int) string {
		return fmt.Sprintf("('%s', '%s', '%s', '%s', '%s', '%s', %d, %d, 0, '%s 10:00:00.000', '%s 10:05:00.000', '%s', 1, %d)",
			orgID, repo, run, suiteID, slot, name, total, passed, day, day, stamp, retracted)
	}
	suites := `INSERT INTO test_suite_results (org_id, repo_id, run_id, suite_id, report_slot, suite_name, total_count, passed_count, failed_count, started_at, finished_at, last_synced, run_attempt, retracted) VALUES `
	aggShapeOf := func(orgID, repo, run string) []string {
		return []string{
			suite(orgID, repo, run, run+"-p1", "p1", "s1", 3, 3, "2026-03-09", "2026-03-10 00:00:00.000", 0),
			suite(orgID, repo, run, run+"-p2", "p2", "s2", 2, 2, "2026-03-09", "2026-03-10 00:00:00.000", 0),
			suite(orgID, repo, run, run+"-agg1", "agg", "s1", 3, 3, "2026-03-09", "2026-03-10 00:00:00.000", 0),
			suite(orgID, repo, run, run+"-agg2", "agg", "s2", 2, 2, "2026-03-09", "2026-03-10 00:00:00.000", 0),
		}
	}
	aggShape := func(repo, run string) []string { return aggShapeOf(org, repo, run) }
	mark := func(repo, day, kind, at string) string {
		return fmt.Sprintf("('%s', '%s', '%s', '%s', '%s')", org, day, repo, kind, at)
	}
	marks := `INSERT INTO daily_metrics_touched_days (org_id, day, repo_id, kind, at) VALUES `
	metric := func(repo, day, computed string) string {
		return fmt.Sprintf("('%s', '%s', '%s', '%s')", org, repo, day, computed)
	}
	tmd := `INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, computed_at) VALUES `
	trc := `INSERT INTO testops_release_confidence (org_id, repo_id, day, computed_at) VALUES `
	repo := func(orgID, id string) string {
		return fmt.Sprintf("('%s', 'fx/%s', '2025-04-22 00:00:00.000', '2026-02-20 00:00:00.000', '%s', 'github')", id, id[:8], orgID)
	}
	exec(`INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider) VALUES ` + strings.Join([]string{
		repo(org, cutA), repo(org, cutB), repo(org, third), repo(other, otherA), repo(other, otherB)}, ", "))

	sum := sha256.Sum256([]byte(formExecRewalkCHSQL))
	if got := hex.EncodeToString(sum[:]); got != formExecRewalkCHSHA256 {
		t.Fatalf("sha256 of the embedded text = %s, want %s (the synthetic copy)", got, formExecRewalkCHSHA256)
	}
	ids, statements := formExecRewalkCHStatements(t, formExecRewalkCHSQL)
	if got := strings.Join(ids, ","); got != "stmtR,Q1,K2A,K3A,K1,K1b,RID" {
		t.Fatalf("statements = %s, want stmtR,Q1,K2A,K3A,K1,K1b,RID", got)
	}
	for index, line := range strings.Split(formExecRewalkCHSQL, "\n") {
		if strings.HasPrefix(line, "--") && strings.Contains(line, ";") {
			t.Errorf("comment line %d holds a ';': %s", index+1, line)
		}
	}
	gates := []struct{ id, rowsCap, bytesCap string }{
		{"Q1", "> 1600000", "> 4294967296"},
		{"K2A", "> 1600000", "> 4294967296"},
		{"K3A", "> 6000000", "> 4294967296"},
		{"K1", "> 6000000", "> 4294967296"},
		{"K1b", "> 6000000", "> 4294967296"},
		{"RID", "> 1000000", "> 4294967296"},
	}
	gateTables := map[string][]string{}
	tableList := regexp.MustCompile(`table IN \(([^)]*)\)`)
	for _, gate := range gates {
		text := statements[gate.id]
		for _, literal := range []string{gate.rowsCap, gate.bytesCap, "WITH (SELECT throwIf(", "'STOP-COST-GATE'"} {
			if got := strings.Count(text, literal); got != 1 {
				t.Errorf("%s holds %q %d time(s), want 1", gate.id, literal, got)
			}
		}
		match := tableList.FindStringSubmatch(text)
		if match == nil {
			t.Fatalf("%s: no gate table list", gate.id)
		}
		for _, item := range strings.Split(match[1], ",") {
			gateTables[gate.id] = append(gateTables[gate.id], strings.Trim(strings.TrimSpace(item), "'"))
		}
	}
	if strings.Contains(statements["stmtR"], "throwIf") || !strings.Contains(statements["stmtR"], "clusterAllReplicas('default', system.one)") {
		t.Errorf("stmtR must be the replica count only")
	}
	var version string
	if err := conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("ClickHouse version %s", version)

	query := func(label, statement string) (formExecRewalkCHResult, string, error) {
		comment := fmt.Sprintf("formrewalkch-%s-%d", label, time.Now().UnixNano())
		qctx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}))
		result, err := formExecRewalkCHQuery(qctx, conn, statement)
		if err == nil {
			t.Logf("REWALK-CH %s columns %v", label, result.columns)
			for index, row := range result.rows {
				t.Logf("REWALK-CH %s row %d %v", label, index, row)
			}
		}
		return result, comment, err
	}
	run := func(phase string) (map[string]formExecRewalkCHResult, map[string]string) {
		results, comments := map[string]formExecRewalkCHResult{}, map[string]string{}
		for _, id := range ids {
			result, comment, err := query(phase+"-"+id, statements[id])
			comments[id] = comment
			if err != nil {
				t.Errorf("%s %s: %v", phase, id, err)
				continue
			}
			results[id] = result
		}
		return results, comments
	}
	rowCount := func(results map[string]formExecRewalkCHResult, label, id string, want int) {
		t.Helper()
		if got := len(results[id].rows); got != want {
			t.Errorf("%s %s: %d row(s), want %d", label, id, got, want)
		}
	}
	sizing := func(results map[string]formExecRewalkCHResult, label, id string, wantDayRow bool, wantSlots string) {
		t.Helper()
		formExecRewalkCHRow(t, results[id], label+" "+id+" control", 0, map[string]string{"kind": "control", "runs": "1"})
		if wantDayRow {
			rowCount(results, label, id, 2)
			formExecRewalkCHRow(t, results[id], label+" "+id+" day", 1, map[string]string{"kind": "day", "day": "2026-03-09", "runs": "1", "slots": wantSlots})
		} else {
			rowCount(results, label, id, 1)
		}
	}
	keyRows := func(results map[string]formExecRewalkCHResult, label string, want [][]string) {
		t.Helper()
		for _, id := range []string{"K1", "K1b"} {
			rowCount(results, label, id, len(want))
			for index, row := range want {
				formExecRewalkCHRow(t, results[id], label+" "+id, index, map[string]string{
					"repo_cut": row[0], "day": row[1], "cls": row[2], "run_day_min": row[3], "run_day_max": row[4], "run_outside_window": row[5]})
			}
		}
	}

	// Pre: before the re-walk.
	exec(suites + strings.Join(append(append(aggShape(cutA, "aS"), aggShape(third, "xS")...),
		suite(org, cutB, "bS", "bS-p1", "p1", "s1", 4, 4, "2026-03-09", "2026-03-10 00:00:00.000", 0),
		suite(org, cutB, "bR", "bR-k1", "agg", "k1", 5, 5, "2026-03-09", d25+"09:00:00.000", 0),
		suite(org, cutB, "bR", "bR-k2", "p1", "k2", 5, 5, "2026-03-10", d25+"09:00:00.000", 0),
		suite(org, cutB, "bO", "bO-k1", "agg", "k1", 2, 2, "2026-01-20", d25+"09:00:00.000", 0),
	), ", "))
	exec(suites + strings.Join(aggShapeOf(other, otherA, "oS"), ", "))
	exec(suites + strings.Join([]string{
		suite(org, cutB, "bR", "bR-k1", "agg", "k1", 0, 0, "2026-03-09", d25+"10:00:00.000", 1),
		suite(org, cutB, "bO", "bO-k1", "agg", "k1", 0, 0, "2026-01-20", d25+"10:00:00.000", 1),
	}, ", "))
	exec(tmd + strings.Join([]string{metric(cutB, "2026-03-09", "2026-03-15 00:00:00"), metric(cutB, "2026-03-10", "2026-03-15 00:00:00"),
		metric(cutB, "2026-01-20", "2026-03-15 00:00:00")}, ", "))
	exec(trc + strings.Join([]string{metric(cutB, "2026-03-09", "2026-03-15 00:00:00"), metric(cutB, "2026-03-10", "2026-03-15 00:00:00"),
		metric(cutB, "2026-01-20", "2026-03-15 00:00:00")}, ", "))
	results, passComments := run("pre")
	formExecRewalkCHRow(t, results["stmtR"], "pre stmtR", 0, map[string]string{"replicas": "1"})
	sizing(results, "pre", "Q1", false, "")
	sizing(results, "pre", "K2A", true, "3")
	formExecRewalkCHRow(t, results["K3A"], "pre K3A", 0, map[string]string{"newest_keys": "4", "retracted_keys": "0", "retracted_keys_day": "0", "retracted_runs_day": "0"})
	window := [][]string{
		{"a1a1a1a1", "2026-01-20", "missing_mark", "2026-01-20", "2026-01-20", "1"},
		{"a1a1a1a1", "2026-03-09", "missing_mark", "2026-03-09", "2026-03-09", "0"},
		{"a1a1a1a1", "2026-03-10", "missing_mark", "2026-03-09", "2026-03-09", "0"},
	}
	keyRows(results, "pre", window)
	rowCount(results, "pre", "RID", 1)
	formExecRewalkCHRow(t, results["RID"], "pre RID", 0, map[string]string{"repo_id": cutB})

	// The live parts of this database: the work math of the pass path and P of each gate.
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	type partSize struct{ parts, rows uint64 }
	sizes := map[string]partSize{}
	partRows, err := conn.Query(ctx, `SELECT table, count(), sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
		AND table IN ('test_suite_results', 'daily_metrics_touched_days', 'testops_test_metrics_daily', 'testops_release_confidence', 'repos') GROUP BY table ORDER BY table`)
	if err != nil {
		t.Fatal(err)
	}
	for partRows.Next() {
		var table string
		var count, rows uint64
		if err := partRows.Scan(&table, &count, &rows); err != nil {
			t.Fatal(err)
		}
		sizes[table] = partSize{count, rows}
		t.Logf("fixture pre %s parts=%d rows=%d", table, count, rows)
	}
	_ = partRows.Close()
	gateParts := func(id string) uint64 {
		var p uint64
		for _, table := range gateTables[id] {
			p += sizes[table].parts
		}
		return p
	}

	// The gates fire: each cap literal lowered to 0 in a test copy only. WANT code 395 STOP-COST-GATE, query_log
	// ExceptionBeforeStart, system.parts in the tables list (the control of the check), no data table, read_rows <= 2P + 1.
	type fired struct{ id, cap, comment string }
	var fires []fired
	for _, gate := range gates {
		for _, literal := range []string{gate.rowsCap, gate.bytesCap} {
			_, comment, err := query("gate-"+gate.id+"-"+strings.TrimPrefix(literal, "> "), strings.Replace(statements[gate.id], literal, "> 0", 1))
			if err == nil || !strings.Contains(err.Error(), "STOP-COST-GATE") || !strings.Contains(err.Error(), "395") {
				t.Errorf("GATE %s %s lowered to 0: err = %v, want code 395 STOP-COST-GATE", gate.id, literal, err)
				continue
			}
			fires = append(fires, fired{gate.id, literal, comment})
		}
	}
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatal(err)
	}
	for _, f := range fires {
		label := "GATE " + f.id + " " + f.cap
		types, read, tableLists := formExecRewalkCHQueryLog(ctx, t, conn, label, f.comment)
		if len(types) != 1 || types[0] != "ExceptionBeforeStart" {
			t.Errorf("%s: query_log types %v, want [ExceptionBeforeStart]", label, types)
		}
		hasParts := false
		for _, list := range tableLists {
			for _, table := range strings.Split(list, ",") {
				switch {
				case table == "system.parts":
					hasParts = true
				case table != "" && !strings.HasPrefix(table, "system."):
					t.Errorf("%s: read data table %s before the stop", label, table)
				}
			}
		}
		if !hasParts {
			t.Errorf("%s: system.parts is not in the tables list %v", label, tableLists)
		}
		if p := gateParts(f.id); read > 2*p+1 {
			t.Errorf("%s: read_rows %d > 2P+1 = %d", label, read, 2*p+1)
		}
		t.Logf("%s read_rows=%d 2P+1=%d", label, read, 2*gateParts(f.id)+1)
	}
	if len(fires) != 2*len(gates) {
		t.Errorf("gates fired %d times, want %d", len(fires), 2*len(gates))
	}
	finalRead := func(table string) uint64 { return 2*sizes[table].rows + 2*sizes[table].parts + 1 }
	plain := func(table string) uint64 { return sizes[table].rows }
	dataBounds := map[string]uint64{
		"Q1":  5*finalRead("test_suite_results") + 6*finalRead("repos"),
		"K2A": 5*finalRead("test_suite_results") + 6*finalRead("repos"),
		"K3A": finalRead("test_suite_results"),
		"K1":  plain("test_suite_results") + finalRead("test_suite_results") + 2*plain("daily_metrics_touched_days") + plain("testops_test_metrics_daily"),
		"K1b": plain("test_suite_results") + finalRead("test_suite_results") + 2*plain("daily_metrics_touched_days") + plain("testops_release_confidence"),
		"RID": finalRead("repos"),
	}
	for _, gate := range gates {
		types, read, _ := formExecRewalkCHQueryLog(ctx, t, conn, "PASS "+gate.id, passComments[gate.id])
		bound := dataBounds[gate.id] + 2*gateParts(gate.id) + 1
		if len(types) != 1 || types[0] != "QueryFinish" || read == 0 || read > bound {
			t.Errorf("PASS %s: query_log types %v read_rows %d, want [QueryFinish] and 1 .. %d", gate.id, types, read, bound)
		}
		t.Logf("PASS %s read_rows=%d bound=%d", gate.id, read, bound)
	}

	// Post A1: the re-walk retracted cut A's aggregate slot on 2026-03-09.
	exec(suites + strings.Join([]string{
		suite(org, cutA, "aS", "aS-agg1", "agg", "s1", 0, 0, "2026-03-09", d25+"11:00:00.000", 1),
		suite(org, cutA, "aS", "aS-agg2", "agg", "s2", 0, 0, "2026-03-09", d25+"11:00:00.000", 1),
	}, ", "))
	results, _ = run("postA1")
	formExecRewalkCHRow(t, results["stmtR"], "postA1 stmtR", 0, map[string]string{"replicas": "1"})
	sizing(results, "postA1", "K2A", false, "")
	formExecRewalkCHRow(t, results["K3A"], "postA1 K3A", 0, map[string]string{"newest_keys": "4", "retracted_keys": "2", "retracted_keys_day": "2", "retracted_runs_day": "1"})
	keyRows(results, "postA1", window)

	// Post W: own records, window events and recomputes 600 s after the marks.
	var markRows []string
	for _, day := range []string{"2026-03-09", "2026-03-10"} {
		markRows = append(markRows, mark(cutB, day, "touched", d25+"10:30:00.000"))
	}
	for day := 10; day <= 16; day++ {
		markRows = append(markRows, mark(cutB, fmt.Sprintf("2026-03-%02d", day), "touched_testops_window", d25+"10:30:00.000"))
	}
	markRows = append(markRows, mark(cutB, "2026-01-20", "touched", d25+"10:30:00.000"))
	for i := 1; i <= 29; i++ {
		markRows = append(markRows, mark(cutB, time.Date(2026, 1, 20, 0, 0, 0, 0, time.UTC).AddDate(0, 0, i).Format(time.DateOnly), "touched_testops_window", d25+"10:30:00.000"))
	}
	exec(marks + strings.Join(markRows, ", "))
	for _, insert := range []string{tmd, trc} {
		exec(insert + strings.Join([]string{metric(cutB, "2026-03-09", d25+"10:40:00"), metric(cutB, "2026-03-10", d25+"10:40:00"),
			metric(cutB, "2026-01-20", d25+"10:40:00")}, ", "))
	}
	results, _ = run("postW")
	keyRows(results, "postW", nil)
	sizing(results, "postW", "Q1", false, "")

	// Q mutant: cut B also holds an unretracted aggregate slot on 2026-03-09. Q1 shows the day row (the card STOPs).
	exec(suites + strings.Join(aggShape(cutB, "bQ"), ", "))
	results, _ = run("qmutant")
	sizing(results, "qmutant", "Q1", true, "3")

	// RID mutant: a second github repo of the org with cut B's prefix. RID reads 2 rows (the card STOPs before W).
	exec(`INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider) VALUES ` + repo(org, cutB2))
	results, _ = run("ridmutant")
	rowCount(results, "ridmutant", "RID", 2)
	formExecRewalkCHRow(t, results["RID"], "ridmutant RID", 0, map[string]string{"repo_id": cutB})
	formExecRewalkCHRow(t, results["RID"], "ridmutant RID", 1, map[string]string{"repo_id": cutB2})
}
