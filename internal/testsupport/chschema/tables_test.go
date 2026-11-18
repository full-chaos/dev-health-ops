package chschema

import (
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

func TestTableStatementsReplaysOnlyTheShapeOfTheTable(t *testing.T) {
	baseline := chmigrate.Baseline{Objects: []chmigrate.Object{
		{Name: "runs", Engine: "MergeTree", Create: "CREATE TABLE runs (a UInt8) ENGINE = MergeTree ORDER BY a"},
		{Name: "runs_view", Engine: "View", Create: "CREATE VIEW runs_view AS SELECT a FROM runs"},
	}}
	chain := []chmigrate.ChainFile{
		{Version: "200_a.sql", SQL: "-- runs gets b\nALTER TABLE runs ADD COLUMN IF NOT EXISTS b UInt8;\nALTER TABLE runs_v2 ADD COLUMN c UInt8;"},
		{Version: "201_b.sql", SQL: "INSERT INTO runs (a) SELECT 1;\nOPTIMIZE TABLE runs FINAL;\nCREATE MATERIALIZED VIEW IF NOT EXISTS runs_mv TO other AS SELECT a FROM runs;\nALTER TABLE other ATTACH PARTITION tuple() FROM runs"},
		{Version: "202_c.sql", SQL: "CREATE TABLE IF NOT EXISTS log\n(\n    a UInt8\n)\nENGINE = MergeTree ORDER BY a;\nALTER TABLE `runs` MODIFY COLUMN b UInt16"},
	}
	got, err := TableStatements(baseline, chain, "runs")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"CREATE TABLE runs (a UInt8) ENGINE = MergeTree ORDER BY a",
		"ALTER TABLE runs ADD COLUMN IF NOT EXISTS b UInt8",
		"ALTER TABLE `runs` MODIFY COLUMN b UInt16",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("runs: got %q, want %q", got, want)
	}
	got, err = TableStatements(baseline, chain, "log")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !strings.HasPrefix(got[0], "CREATE TABLE IF NOT EXISTS log") {
		t.Fatalf("log (created by the chain): got %q", got)
	}
}

func TestTableStatementsRefusesAFormItDoesNotReplay(t *testing.T) {
	baseline := chmigrate.Baseline{Objects: []chmigrate.Object{
		{Name: "runs", Engine: "MergeTree", Create: "CREATE TABLE runs (a UInt8) ENGINE = MergeTree ORDER BY a"},
		{Name: "runs_view", Engine: "View", Create: "CREATE VIEW runs_view AS SELECT a FROM runs"},
	}}
	for name, tc := range map[string]struct {
		table, sql, want string
	}{
		"exchange":           {"runs", "CREATE TABLE runs_new AS runs;\nEXCHANGE TABLES runs AND runs_new", "does not replay"},
		"rename":             {"runs", "RENAME TABLE runs TO runs_old, runs_new TO runs", "does not replay"},
		"drop":               {"runs", "DROP TABLE IF EXISTS runs", "does not replay"},
		"detach":             {"runs", "DETACH TABLE runs", "does not replay"},
		"becomes a view":     {"runs", "CREATE OR REPLACE VIEW runs AS SELECT 1", "a view"},
		"alter before":       {"later", "ALTER TABLE later ADD COLUMN b UInt8", "before any statement creates it"},
		"unknown":            {"missing", "ALTER TABLE runs ADD COLUMN b UInt8", "not in the baseline"},
		"a view":             {"runs_view", "", "a view in the baseline"},
		"word boundary only": {"runs", "DROP TABLE runs_v2", ""},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := TableStatements(baseline, []chmigrate.ChainFile{{Version: "300_x.sql", SQL: tc.sql}}, tc.table)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("got %v, want no error", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error containing %q", err, tc.want)
			}
		})
	}
}

func TestChainTablesNamesEveryTableAStatementChanges(t *testing.T) {
	chain := []chmigrate.ChainFile{{Version: "400_x.sql", SQL: strings.Join([]string{
		"ALTER TABLE a ADD COLUMN x UInt8",
		"CREATE TABLE IF NOT EXISTS b (x UInt8) ENGINE = MergeTree ORDER BY x",
		"DROP TABLE IF EXISTS c",
		"RENAME TABLE d TO d_old, e TO d",
		"EXCHANGE TABLES f AND f_new",
		"INSERT INTO g SELECT 1",
		"OPTIMIZE TABLE h FINAL",
		"CREATE MATERIALIZED VIEW IF NOT EXISTS i TO j AS SELECT x FROM a",
		"DROP VIEW IF EXISTS k",
	}, ";\n")}}
	want := []string{"a", "b", "c", "d", "d_old", "e", "f", "f_new"}
	if got := ChainTables(chain); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}
