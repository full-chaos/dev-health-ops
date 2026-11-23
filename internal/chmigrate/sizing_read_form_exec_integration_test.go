//go:build integration

package chmigrate_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// Throwaway exec proof of a prod read form: not for merge.
const sizingFormSHA256 = "f38fec483020d4c1a1edc0ef9f233a4ff85299b3266e0f2646c098a6d7f3fd01"

type formStatement struct {
	label string
	text  string
}

// splitForm cuts the form at lines whose code part ends with ";", keeping each statement's comment lines.
func splitForm(body string) []formStatement {
	var out []formStatement
	var chunk []string
	for _, line := range strings.Split(body, "\n") {
		chunk = append(chunk, line)
		code := strings.TrimSpace(chmigrate.StripLineComments(line))
		if !strings.HasSuffix(code, ";") {
			continue
		}
		text := strings.TrimSuffix(strings.TrimRight(strings.Join(chunk, "\n"), " \t\n"), ";")
		label := ""
		for _, l := range chunk {
			if t := strings.TrimSpace(l); strings.HasPrefix(t, "-- S0") || strings.HasPrefix(t, "-- stmt") {
				label = strings.Fields(strings.TrimPrefix(t, "-- "))[0]
				break
			}
		}
		out = append(out, formStatement{label: label, text: text})
		chunk = nil
	}
	return out
}

func renderCell(v any) string {
	value := reflect.ValueOf(v)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return "NULL"
		}
		value = value.Elem()
	}
	if ts, ok := value.Interface().(time.Time); ok {
		return ts.UTC().Format("2006-01-02")
	}
	return fmt.Sprint(value.Interface())
}

func TestSizingReadFormExecutesAtTheChainHead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	body, err := os.ReadFile("testdata/sizingform/form.sql")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	if got := hex.EncodeToString(sum[:]); got != sizingFormSHA256 {
		t.Fatalf("form.sql sha256 = %s, want %s", got, sizingFormSHA256)
	}
	statements := splitForm(string(body))
	labels := make([]string, 0, len(statements))
	for _, s := range statements {
		labels = append(labels, s.label)
	}
	if strings.Join(labels, ",") != "S0,stmt0,stmt1,stmt2,stmt3" {
		t.Fatalf("form statements = %v", labels)
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
	t.Logf("schema head %s", result.Head)

	fixture, err := os.ReadFile("testdata/sizingform/fixture.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range chmigrate.SplitStatements(string(fixture)) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("fixture: %v\n%s", err, statement)
		}
	}

	want := map[string][]map[string]string{
		"stmt0": {{"raw_orgs": "1", "cicd_orgs": "1", "testops_orgs": "1"}},
		"stmt1": {
			{"t": "cicd", "live_repo_days": "5", "first_day": "2026-11-10", "last_day": "2026-11-22", "days": "5"},
			{"t": "testops", "live_repo_days": "5", "first_day": "2026-11-10", "last_day": "2026-11-22", "days": "5"},
		},
	}
	moveWant := map[string]string{
		"live_repo_days": "5", "with_raw": "4", "no_raw": "1", "no_raw_first_day": "2026-11-12", "no_raw_last_day": "2026-11-12",
		"move_repo_days": "3", "move_days": "3", "move_first_day": "2026-11-10", "move_last_day": "2026-11-22", "to_null": "1",
		"move_after_roll": "1", "move_raw_changed": "1", "old_checked": "1", "old_match": "1", "value_move": "1",
		"ctrl_checked": "1", "ctrl_match": "1",
	}
	want["stmt2"] = []map[string]string{moveWant}
	want["stmt3"] = []map[string]string{moveWant}
	reads := map[string][]string{
		"stmt0": {"ci_pipeline_runs", "cicd_metrics_daily", "testops_pipeline_metrics_daily"},
		"stmt1": {"cicd_metrics_daily", "testops_pipeline_metrics_daily"},
		"stmt2": {"cicd_metrics_daily", "ci_pipeline_runs"},
		"stmt3": {"testops_pipeline_metrics_daily", "ci_pipeline_runs"},
	}

	tableRows := map[string]uint64{}
	for _, statement := range statements {
		var readRows uint64
		qctx := clickhouse.Context(ctx, clickhouse.WithProgress(func(p *clickhouse.Progress) { readRows += p.Rows }))
		rows, err := conn.Query(qctx, statement.text)
		if err != nil {
			t.Fatalf("%s: SQL error: %v", statement.label, err)
		}
		columns := rows.Columns()
		types := rows.ColumnTypes()
		var got []map[string]string
		for rows.Next() {
			cells := make([]any, len(types))
			for i, ct := range types {
				cells[i] = reflect.New(ct.ScanType()).Interface()
			}
			if err := rows.Scan(cells...); err != nil {
				t.Fatalf("%s: scan: %v", statement.label, err)
			}
			row := map[string]string{}
			parts := make([]string, 0, len(columns))
			for i, name := range columns {
				row[name] = renderCell(cells[i])
				parts = append(parts, name+"="+row[name])
			}
			t.Logf("%s ROW %s", statement.label, strings.Join(parts, " "))
			got = append(got, row)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("%s: SQL error: %v", statement.label, err)
		}
		_ = rows.Close()

		if statement.label == "S0" {
			if len(got) != 3 {
				t.Fatalf("S0: %d rows, want 3", len(got))
			}
			for _, row := range got {
				var n uint64
				if _, err := fmt.Sscan(row["table_rows"], &n); err != nil || n == 0 {
					t.Fatalf("S0: table %s table_rows %q, want > 0", row["table"], row["table_rows"])
				}
				tableRows[row["table"]] = n
			}
			t.Logf("S0 read_rows=%d", readRows)
			continue
		}

		var bound uint64
		for _, table := range reads[statement.label] {
			bound += tableRows[table]
		}
		t.Logf("%s read_rows=%d gate_rows=%d ratio=%.2f", statement.label, readRows, bound, float64(readRows)/float64(bound))
		if readRows > 2*bound {
			t.Errorf("%s: read_rows %d over 2x the S0 rows %d of its tables", statement.label, readRows, bound)
		}

		expected := want[statement.label]
		if len(got) != len(expected) {
			t.Errorf("%s: %d rows, want %d", statement.label, len(got), len(expected))
			continue
		}
		for i, row := range expected {
			actual := got[i]
			if key, ok := row["t"]; ok {
				for _, candidate := range got {
					if candidate["t"] == key {
						actual = candidate
					}
				}
			}
			for name, value := range row {
				if actual[name] != value {
					t.Errorf("%s row %d: %s = %s, want %s", statement.label, i, name, actual[name], value)
				}
			}
		}
	}
}
