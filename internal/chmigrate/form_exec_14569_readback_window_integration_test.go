//go:build integration

package chmigrate_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// DO NOT MERGE. Executes every statement of three prod read FORMs (read-back, gap, residual) on the head schema with
// one synthetic fixture. Each testdata file is the prod FORM text with the org prefix c6a38355 swapped for f0f0f0f0
// (the repo is public); the test pins the sha256 of the swapped file and of the text swapped back.
const (
	form14569Window = "+ toIntervalMillisecond(1000)"
	form14569Org    = "f0f0f0f0-1569-4000-8000-000000000001"
	form14569Other  = "d6a38355-1569-4000-8000-000000000002"
	form14569Zero   = "00000000-0000-0000-0000-000000000000"
	form14569Synced = "2026-11-20 03:00:00.000"
)

type form14569Result struct {
	columns []string
	rows    [][]string
}

func form14569Query(ctx context.Context, conn driver.Conn, statement string) (form14569Result, error) {
	rows, err := conn.Query(ctx, statement)
	if err != nil {
		return form14569Result{}, err
	}
	defer func() { _ = rows.Close() }()
	result := form14569Result{columns: rows.Columns()}
	types := rows.ColumnTypes()
	for rows.Next() {
		dest := make([]any, len(types))
		for index, columnType := range types {
			dest[index] = reflect.New(columnType.ScanType()).Interface()
		}
		if err := rows.Scan(dest...); err != nil {
			return form14569Result{}, fmt.Errorf("scan: %w", err)
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

func form14569Statements(t *testing.T, text string) ([]string, map[string]string) {
	t.Helper()
	statementID := regexp.MustCompile(`^[SFRC][0-9]+$`)
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
			t.Fatalf("a statement has no id line:\n%s", statement)
		}
		if _, seen := statements[id]; seen {
			t.Fatalf("statement %s twice", id)
		}
		ids = append(ids, id)
		statements[id] = statement
	}
	return ids, statements
}

func form14569Cell(t *testing.T, result form14569Result, label string, row int, column string) string {
	t.Helper()
	if row >= len(result.rows) {
		t.Fatalf("%s: %d row(s), want a row %d", label, len(result.rows), row)
	}
	for index, name := range result.columns {
		if name == column {
			return result.rows[row][index]
		}
	}
	t.Fatalf("%s: no column %s in %v", label, column, result.columns)
	return ""
}

// form14569Row checks the named columns of one row; a value ending in "*" is a prefix.
func form14569Row(t *testing.T, result form14569Result, label string, row int, want map[string]string) {
	t.Helper()
	for column, value := range want {
		got := form14569Cell(t, result, label, row, column)
		if prefix, isPrefix := strings.CutSuffix(value, "*"); isPrefix {
			if !strings.HasPrefix(got, prefix) {
				t.Errorf("%s row %d %s = %s, want prefix %s", label, row, column, got, prefix)
			}
			continue
		}
		if got != value {
			t.Errorf("%s row %d %s = %s, want %s", label, row, column, got, value)
		}
	}
}

func form14569Sum(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

// The fixture (org f0f0f0f0-..., a second org d6a38355-... must not count). Every item is Linear unless named:
//
//	FX-A: reopen 10-15 10:00:00.552, its close stored 7 ms later at .559.
//	FX-B: reopen 10-20 12:00:00.000, its close exactly 1 s later (the window end).
//	FX-C: reopen 11-01 12:00:00.000, its close 1 s + 1 ms later (outside the window): the only missing close.
//	FX-D: close 09-10, reopen 09-12 (the ordinary case). FX-E: close and reopen at 10-06 10:00, the reopen's own
//	    transition lost in the tie (the lost reopen). gh:fx/repo#1 and jira:FX-9: closed then reopened (controls).
//	The other org holds a FX-C look-alike with no close.
func TestFormExecChaos14569ReadBackWindow(t *testing.T) {
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
	for _, table := range []string{"work_item_reopen_events", "work_item_transitions", "work_items"} {
		exec("SYSTEM STOP MERGES " + table)
	}
	reopen := func(orgID, id, from, to, fromRaw, toRaw, occurred string) string {
		return fmt.Sprintf("('%s', '%s', '%s', '%s', '%s', '%s', '%s', NULL, '%s')", orgID, id, occurred, from, to, fromRaw, toRaw, form14569Synced)
	}
	transition := func(orgID, id, provider, from, to, toRaw, occurred string) string {
		return fmt.Sprintf("('%s', '%s', '%s', '%s', '%s', '%s', '', '%s', '', '%s', '%s', 0)",
			orgID, form14569Zero, id, provider, from, to, toRaw, occurred, form14569Synced)
	}
	item := func(orgID, id, updated, lastSynced string) string {
		return fmt.Sprintf("('%s', '%s', '%s', 'linear', 'todo', '%s', '%s', '%s')", orgID, form14569Zero, id, updated, updated, lastSynced)
	}
	org, other := form14569Org, form14569Other
	exec(`INSERT INTO work_item_reopen_events (org_id, work_item_id, occurred_at, from_status, to_status, from_status_raw, to_status_raw, actor, last_synced) VALUES ` +
		strings.Join([]string{
			reopen(org, "linear:FX-A", "done", "in_progress", "Done", "In Progress", "2026-10-15 10:00:00.552"),
			reopen(org, "linear:FX-B", "done", "in_progress", "Done", "In Progress", "2026-10-20 12:00:00.000"),
			reopen(org, "linear:FX-C", "done", "todo", "Done", "Todo", "2026-11-01 12:00:00.000"),
			reopen(org, "linear:FX-D", "done", "todo", "Done", "Todo", "2026-09-12 10:00:00.000"),
			reopen(org, "linear:FX-E", "done", "todo", "Done", "Todo", "2026-10-06 10:00:00.000"),
			reopen(org, "gh:fx/repo#1", "done", "todo", "closed", "reopened", "2026-10-02 10:00:00.000"),
			reopen(org, "jira:FX-9", "done", "todo", "Done", "Open", "2026-10-08 10:00:00.000"),
			reopen(other, "linear:FX-C", "done", "todo", "Done", "Todo", "2026-11-01 12:00:00.000"),
		}, ", "))
	exec(`INSERT INTO work_item_transitions (org_id, repo_id, work_item_id, provider, from_status, to_status, from_status_raw, to_status_raw, actor, occurred_at, last_synced, tie_ordinal) VALUES ` +
		strings.Join([]string{
			transition(org, "linear:FX-A", "linear", "todo", "in_progress", "In Progress", "2026-10-01 10:00:00.000"),
			transition(org, "linear:FX-A", "linear", "done", "in_progress", "In Progress", "2026-10-15 10:00:00.552"),
			transition(org, "linear:FX-A", "linear", "in_progress", "done", "Done", "2026-10-15 10:00:00.559"),
			transition(org, "linear:FX-B", "linear", "todo", "in_progress", "In Progress", "2026-10-10 10:00:00.000"),
			transition(org, "linear:FX-B", "linear", "done", "in_progress", "In Progress", "2026-10-20 12:00:00.000"),
			transition(org, "linear:FX-B", "linear", "in_progress", "done", "Done", "2026-10-20 12:00:01.000"),
			transition(org, "linear:FX-C", "linear", "todo", "in_progress", "In Progress", "2026-10-25 10:00:00.000"),
			transition(org, "linear:FX-C", "linear", "done", "todo", "Todo", "2026-11-01 12:00:00.000"),
			transition(org, "linear:FX-C", "linear", "in_progress", "done", "Done", "2026-11-01 12:00:01.001"),
			transition(org, "linear:FX-D", "linear", "todo", "in_progress", "In Progress", "2026-09-01 10:00:00.000"),
			transition(org, "linear:FX-D", "linear", "in_progress", "done", "Done", "2026-09-10 10:00:00.000"),
			transition(org, "linear:FX-D", "linear", "done", "todo", "Todo", "2026-09-12 10:00:00.000"),
			transition(org, "linear:FX-E", "linear", "todo", "in_progress", "In Progress", "2026-10-01 10:00:00.000"),
			transition(org, "linear:FX-E", "linear", "in_progress", "done", "Done", "2026-10-06 10:00:00.000"),
			transition(org, "gh:fx/repo#1", "github", "todo", "done", "closed", "2026-10-01 10:00:00.000"),
			transition(org, "gh:fx/repo#1", "github", "done", "todo", "reopened", "2026-10-02 10:00:00.000"),
			transition(org, "jira:FX-9", "jira", "in_progress", "done", "Done", "2026-10-07 10:00:00.000"),
			transition(org, "jira:FX-9", "jira", "done", "todo", "Open", "2026-10-08 10:00:00.000"),
			transition(other, "linear:FX-C", "linear", "todo", "in_progress", "In Progress", "2026-10-25 10:00:00.000"),
			transition(other, "linear:FX-C", "linear", "done", "todo", "Todo", "2026-11-01 12:00:00.000"),
		}, ", "))
	exec(`INSERT INTO work_items (org_id, repo_id, work_item_id, provider, status, created_at, updated_at, last_synced) VALUES ` +
		strings.Join([]string{
			item(org, "linear:FX-C", "2026-11-01 12:00:00.000", "2026-11-02 00:00:00.000"),
			item(org, "linear:FX-E", "2026-05-20 10:00:00.000", "2026-11-01 00:00:00.000"),
			item(other, "linear:FX-C", "2026-11-01 12:00:00.000", "2026-11-02 00:00:00.000"),
		}, ", "))
	exec(`INSERT INTO work_items (org_id, repo_id, work_item_id, provider, status, created_at, updated_at, last_synced) VALUES ` +
		item(org, "linear:FX-E", "2026-05-20 10:00:00.000", "2026-10-01 00:00:00.000"))

	forms := []struct {
		name, swappedSHA, swappedBackSHA, wantIDs string
		read                                      func() ([]byte, error)
		want                                      func(t *testing.T, r map[string]form14569Result)
	}{
		{"FORM-14569-READBACK-1", "44dd711e73703896db177cfca7537f120f3048aed658552f2d09c8755c15acea",
			"548989e5d7de9ec41e2cb301e668b9c0a1139131bf66c9d7ae0cd47534d2ae00", "S0,S1,F0,F1,F2,F3,F4",
			func() ([]byte, error) { return os.ReadFile("testdata/form14569/readback.sql") }, wantForm14569Readback},
		{"FORM-14569-GAP-1", "c58dc0bfd6d3c88dd0153e43e0b78048784d0d1df26d799eb945c78c6efbe225",
			"cb4e0a2c37a057f144e05133bf98a38e74b71edf4be3fa438f3f506c8e29d209", "S0,F0,F1,F2,F3",
			func() ([]byte, error) { return os.ReadFile("testdata/form14569/gap.sql") }, wantForm14569Gap},
		{"FORM-14569-RESIDUAL-1", "145317d99ec154b8b3f2c8582160ca480696800dae2545250feaa4af930b554f",
			"dc0856e9132085e2fe3fa1eee2d4eb40406057c69baf1f6cbfc572daa888eca8", "S0,S1,S2,R1,R2,R3",
			func() ([]byte, error) { return os.ReadFile("testdata/form14569/residual.sql") }, wantForm14569Residual},
	}
	capLiteral := regexp.MustCompile(`> (5000000|6000000|4294967296)`)
	for _, form := range forms {
		t.Run(form.name, func(t *testing.T) {
			raw, err := form.read()
			if err != nil {
				t.Fatal(err)
			}
			text := string(raw)
			if got := form14569Sum(text); got != form.swappedSHA {
				t.Fatalf("sha256 of the swapped file = %s, want %s", got, form.swappedSHA)
			}
			if got := form14569Sum(strings.ReplaceAll(text, "f0f0f0f0", "c6a38355")); got != form.swappedBackSHA {
				t.Fatalf("sha256 of the file swapped back = %s, want %s (the lane FORM file)", got, form.swappedBackSHA)
			}
			if strings.Contains(text, "c6a38355") {
				t.Fatal("the prod org prefix is in a public test file")
			}
			ids, statements := form14569Statements(t, text)
			if got := strings.Join(ids, ","); got != form.wantIDs {
				t.Fatalf("statements = %s, want %s", got, form.wantIDs)
			}
			results := map[string]form14569Result{}
			for _, id := range ids {
				result, err := form14569Query(ctx, conn, statements[id])
				if err != nil {
					t.Fatalf("%s %s: %v", form.name, id, err)
				}
				for index, row := range result.rows {
					t.Logf("%s %s row %d %v", form.name, id, index, row)
				}
				results[id] = result
			}
			form.want(t, results)

			// The window literal is read: with the window end at the reopen (0 ms) or 1 ms short (999 ms) every
			// statement that holds it changes its result.
			mutated := 0
			for _, id := range ids {
				count := strings.Count(statements[id], form14569Window)
				if count == 0 {
					continue
				}
				mutated++
				baseline := fmt.Sprint(results[id].rows)
				for _, ms := range []string{"0", "999"} {
					mutant := strings.ReplaceAll(statements[id], form14569Window, "+ toIntervalMillisecond("+ms+")")
					result, err := form14569Query(ctx, conn, mutant)
					if err != nil {
						t.Errorf("MUTANT %s window %s ms: %v", id, ms, err)
						continue
					}
					if fmt.Sprint(result.rows) == baseline {
						t.Errorf("MUTANT %s window %s ms SURVIVED: same rows %v", id, ms, result.rows)
					} else {
						t.Logf("MUTANT %s window %s ms killed: %v", id, ms, result.rows)
					}
				}
			}
			if mutated == 0 {
				t.Error("no statement holds the 1 s window")
			}

			// Each cost gate cap lowered to 0 in a test copy fires code 395 STOP-COST-GATE.
			fired := 0
			for _, id := range ids {
				if !strings.Contains(statements[id], "throwIf(") {
					continue
				}
				for _, literal := range capLiteral.FindAllString(statements[id], -1) {
					lowered := strings.Replace(statements[id], literal, "> 0", 1)
					_, err := form14569Query(ctx, conn, lowered)
					if err == nil || !strings.Contains(err.Error(), "STOP-COST-GATE") || !strings.Contains(err.Error(), "395") {
						t.Errorf("GATE %s %s lowered to 0: err = %v, want code 395 STOP-COST-GATE", id, literal, err)
						continue
					}
					fired++
				}
			}
			if fired == 0 {
				t.Error("no cost gate fired")
			}
			t.Logf("%s: %d gate cap(s) fired", form.name, fired)
		})
	}
}

func wantForm14569Control(t *testing.T, result form14569Result, id string) {
	t.Helper()
	if len(result.rows) != 3 {
		t.Fatalf("%s: %d row(s), want 3", id, len(result.rows))
	}
	form14569Row(t, result, id, 0, map[string]string{"close_offset_ms": "0", "closes_found": "1"})
	form14569Row(t, result, id, 1, map[string]string{"close_offset_ms": "1000", "closes_found": "1"})
	form14569Row(t, result, id, 2, map[string]string{"close_offset_ms": "1001", "closes_found": "0"})
}

func wantForm14569Counts(t *testing.T, r map[string]form14569Result, f1, f2 string) {
	t.Helper()
	form14569Row(t, r["F0"], "F0", 0, map[string]string{"orgs": "1", "org_rows": "7"})
	if len(r[f1].rows) != 2 {
		t.Fatalf("F1: %d row(s), want 2", len(r[f1].rows))
	}
	form14569Row(t, r[f1], "F1", 0, map[string]string{
		"provider": "github", "reopens_all": "1", "items_kept": "1", "reopens_kept": "1", "items_lost": "0", "reopens_lost": "0"})
	form14569Row(t, r[f1], "F1", 1, map[string]string{
		"provider": "linear", "reopens_all": "5", "items_kept": "4", "reopens_kept": "4", "items_lost": "1", "reopens_lost": "1",
		"reopens_lost_tie": "1", "reopens_lost_bare": "0", "lost_first_day": "2026-10-06*", "lost_last_day": "2026-10-06*"})
	if len(r[f2].rows) != 3 {
		t.Fatalf("F2: %d row(s), want 3", len(r[f2].rows))
	}
	for row, provider := range []string{"github", "jira"} {
		form14569Row(t, r[f2], "F2", row, map[string]string{
			"provider": provider, "reopens_all": "1", "closes_found": "1", "closes_missing": "0", "items_close_missing": "0",
			"closes_missing_reopen_kept": "0", "reopens_no_history": "0", "reopens_kept": "1", "reopens_pair_mismatch": "0",
			"reopens_kept_provider_empty": "0", "reopens_kept_synced_after_1119": "1"})
	}
	form14569Row(t, r[f2], "F2", 2, map[string]string{
		"provider": "linear", "reopens_all": "5", "closes_found": "4", "closes_missing": "1", "items_close_missing": "1",
		"closes_missing_reopen_kept": "1", "reopens_no_history": "0", "reopens_kept": "4", "reopens_pair_mismatch": "0",
		"reopens_kept_provider_empty": "0", "reopens_kept_synced_after_1119": "4"})
}

func wantForm14569Readback(t *testing.T, r map[string]form14569Result) {
	t.Helper()
	if len(r["S0"].rows) != 3 {
		t.Fatalf("S0: %d row(s), want 3", len(r["S0"].rows))
	}
	form14569Row(t, r["S0"], "S0", 0, map[string]string{"table_name": "work_item_reopen_events", "table_rows": "8", "active_parts": "1"})
	form14569Row(t, r["S0"], "S0", 1, map[string]string{"table_name": "work_item_transitions", "table_rows": "20", "active_parts": "1"})
	form14569Row(t, r["S0"], "S0", 2, map[string]string{"table_name": "work_items", "table_rows": "4", "active_parts": "2"})
	form14569Row(t, r["S1"], "S1", 0, map[string]string{
		"table_name": "work_item_transitions", "sorting_key": "org_id, repo_id, work_item_id, occurred_at, tie_ordinal"})
	wantForm14569Counts(t, r, "F1", "F2")
	form14569Row(t, r["F3"], "F3", 0, map[string]string{
		"items_affected": "2", "items_reopen_lost": "1", "items_close_missing": "1", "reopens_lost": "1", "closes_missing": "1",
		"items_with_row": "2", "items_updated_before_since": "1", "updated_first_day": "2026-05-20*", "items_synced_last_day": "0",
		"event_first_day": "2026-10-06*", "event_last_day": "2026-11-01*"})
	wantForm14569Control(t, r["F4"], "F4")
}

func wantForm14569Gap(t *testing.T, r map[string]form14569Result) {
	t.Helper()
	if len(r["S0"].rows) != 2 {
		t.Fatalf("S0: %d row(s), want 2", len(r["S0"].rows))
	}
	form14569Row(t, r["S0"], "S0", 0, map[string]string{"table_name": "work_item_reopen_events", "table_rows": "8", "active_parts": "1"})
	form14569Row(t, r["S0"], "S0", 1, map[string]string{"table_name": "work_item_transitions", "table_rows": "20", "active_parts": "1"})
	wantForm14569Counts(t, r, "F1", "F2")
	wantForm14569Control(t, r["F3"], "F3")
}

func wantForm14569Residual(t *testing.T, r map[string]form14569Result) {
	t.Helper()
	form14569Row(t, r["S0"], "S0", 0, map[string]string{"orgs": "1", "org_rows": "7"})
	if len(r["S1"].rows) != 3 {
		t.Fatalf("S1: %d row(s), want 3", len(r["S1"].rows))
	}
	form14569Row(t, r["S1"], "S1", 0, map[string]string{"table_name": "work_item_reopen_events", "table_rows": "8", "active_parts": "1"})
	form14569Row(t, r["S1"], "S1", 1, map[string]string{
		"table_name": "work_item_transitions", "table_rows": "20", "active_parts": "1",
		"sorting_key": "org_id, repo_id, work_item_id, occurred_at, tie_ordinal"})
	form14569Row(t, r["S1"], "S1", 2, map[string]string{"table_name": "work_items", "table_rows": "4", "active_parts": "2"})
	if len(r["S2"].rows) != 1 {
		t.Fatalf("S2: %d row(s), want 1", len(r["S2"].rows))
	}
	form14569Row(t, r["S2"], "S2", 0, map[string]string{
		"provider": "linear", "item_keys": "2", "items": "2", "version_rows": "3", "versions_over_keys": "1",
		"keys_with_versions": "1", "keys_written_last_day": "0", "keys_repo_not_nil": "0"})
	if len(r["R1"].rows) != 2 {
		t.Fatalf("R1: %d row(s), want 2 (FX-C, FX-E)", len(r["R1"].rows))
	}
	form14569Row(t, r["R1"], "R1", 0, map[string]string{
		"linear_key": "FX-C", "id_cut8": "FX-C", "reopens_all": "1", "transitions_all": "3", "reopens_lost": "0", "closes_missing": "1",
		"missing_close_days": "2026-11-01", "has_row": "yes", "synced_last_day": "no", "updated_minute": "2026-11-01 12:00",
		"event_first_day": "2026-11-01*", "event_last_day": "2026-11-01*"})
	form14569Row(t, r["R1"], "R1", 1, map[string]string{
		"linear_key": "FX-E", "reopens_all": "1", "transitions_all": "2", "reopens_lost": "1", "closes_missing": "0",
		"lost_reopen_days": "2026-10-06", "has_row": "yes", "updated_minute": "2026-05-20 10:00", "not_reread_in_window": "no"})
	if len(r["R2"].rows) != 1 {
		t.Fatalf("R2: %d row(s), want 1 (FX-C, one transition at its reopen instant)", len(r["R2"].rows))
	}
	form14569Row(t, r["R2"], "R2", 0, map[string]string{
		"linear_key": "FX-C", "reopen_at": "2026-11-01 12:00:00 +0000 UTC*", "reopen_kept": "1", "provider_close_any": "yes",
		"stored_close_any": "yes", "window_transitions": "2", "last_before_to": "in_progress", "rows_at_instant": "1",
		"tie_ordinal": "0", "from_status": "done", "to_status": "todo", "to_status_raw": "Todo"})
	wantForm14569Control(t, r["R3"], "R3")
}
