//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// refsSeedRows are rows stored before 149: work unit -> structural_evidence_json.
var refsSeedRows = map[string]string{
	"both":         `{"issues":["i-2","i-1"],"prs":["p-1"],"commits":["c-1"],"edges":["e-1"]}`,
	"issues-only":  `{"issues":["i-3"]}`,
	"prs-only":     `{"issues":[],"prs":["p-2","p-3"],"commits":[],"edges":[]}`,
	"empty-object": `{}`,
	"empty-text":   ``,
	"not-json":     `not json`,
}

// refsWant is what each seeded row reads in issue_refs and pr_refs after 149.
var refsWant = map[string][2][]string{
	"both":         {{"i-2", "i-1"}, {"p-1"}},
	"issues-only":  {{"i-3"}, {}},
	"prs-only":     {{}, {"p-2", "p-3"}},
	"empty-object": {{}, {}},
	"empty-text":   {{}, {}},
	"not-json":     {{}, {}},
}

func refsChainBefore(t *testing.T) (chmigrate.Baseline, []chmigrate.ChainFile, []chmigrate.ChainFile, chmigrate.ChainFile) {
	t.Helper()
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var before []chmigrate.ChainFile
	var migration chmigrate.ChainFile
	for _, file := range chain {
		if file.Version == workUnitInvestmentRefsVersion {
			migration = file
			continue
		}
		before = append(before, file)
	}
	if migration.Version == "" {
		t.Fatalf("the live chain does not hold %s", workUnitInvestmentRefsVersion)
	}
	return baseline, chain, before, migration
}

func refsExec(ctx context.Context, t *testing.T, conn driver.Conn, query string) {
	t.Helper()
	if err := conn.Exec(ctx, query); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

func refsSeed(ctx context.Context, t *testing.T, conn driver.Conn, units ...string) {
	t.Helper()
	for _, unit := range units {
		if err := conn.Exec(ctx, `INSERT INTO work_unit_investments
			(work_unit_id, from_ts, to_ts, effort_metric, effort_value, structural_evidence_json, evidence_quality,
			 evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version,
			 categorization_input_hash, categorization_run_id, computed_at, org_id)
			VALUES (?, now(), now(), 'm', 1, ?, 1, 'high', 'ok', '[]', 'v', 'h', 'r', now(), 'org-149')`, unit, refsSeedRows[unit]); err != nil {
			t.Fatal(err)
		}
	}
}

// refColumns gives name -> "type default_kind default_expression" of the two columns that exist.
func refColumns(ctx context.Context, t *testing.T, conn driver.Conn) map[string]string {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT name, type, default_kind, default_expression FROM system.columns
WHERE database = currentDatabase() AND table = 'work_unit_investments' AND name IN ('issue_refs', 'pr_refs')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var name, columnType, kind, expression string
		if err := rows.Scan(&name, &columnType, &kind, &expression); err != nil {
			t.Fatal(err)
		}
		out[name] = columnType + " " + kind + " " + expression
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// refsStorage is the table's active parts, its mutations and the part columns
// that hold the reference columns, read at one moment.
type refsStorage struct {
	Parts        []string
	Mutations    uint64
	StoredColumn uint64
}

func readRefsStorage(ctx context.Context, t *testing.T, conn driver.Conn) refsStorage {
	t.Helper()
	var storage refsStorage
	if err := conn.QueryRow(ctx, `SELECT arraySort(groupArray(name)) FROM system.parts
WHERE database = currentDatabase() AND table = 'work_unit_investments' AND active`).Scan(&storage.Parts); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 'work_unit_investments'`).Scan(&storage.Mutations); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'work_unit_investments' AND active AND column IN ('issue_refs', 'pr_refs')`).Scan(&storage.StoredColumn); err != nil {
		t.Fatal(err)
	}
	return storage
}

func checkRefsRead(ctx context.Context, t *testing.T, conn driver.Conn, when string) {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT work_unit_id, issue_refs, pr_refs FROM work_unit_investments WHERE org_id = 'org-149'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var unit string
		var issues, prs []string
		if err := rows.Scan(&unit, &issues, &prs); err != nil {
			t.Fatal(err)
		}
		want, ok := refsWant[unit]
		if !ok {
			t.Fatalf("%s: a row %s that was not seeded", when, unit)
		}
		if !slices.Equal(issues, want[0]) || !slices.Equal(prs, want[1]) {
			t.Errorf("%s: %s reads issue_refs %q pr_refs %q, want %q %q", when, unit, issues, prs, want[0], want[1])
		}
		seen++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if seen != len(refsSeedRows) {
		t.Fatalf("%s: read %d rows, want %d", when, seen, len(refsSeedRows))
	}
	var differ uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM work_unit_investments
WHERE issue_refs != JSONExtract(structural_evidence_json, 'issues', 'Array(String)')
   OR pr_refs != JSONExtract(structural_evidence_json, 'prs', 'Array(String)')`).Scan(&differ); err != nil {
		t.Fatal(err)
	}
	if differ != 0 {
		t.Errorf("%s: %d rows read references that differ from the readers' JSONExtract of their evidence", when, differ)
	}
}

// INVARIANT (CHAOS-10726): 149 adds issue_refs and pr_refs as metadata only. The
// rows stored before it keep their parts (the same active part names, no
// mutation, no part holds the new columns) and read the keys the readers parse
// from their evidence today, malformed or empty evidence as []. The migration
// runs again with no change.
func TestMigration149AddsTheRefColumnsWithoutRewritingAPart(t *testing.T) {
	ctx := context.Background()
	scratch := scratchDatabase(t)
	conn := openDatabase(t, scratch.URI, "")
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	baseline, chain, before, migration := refsChainBefore(t)
	if _, err := chmigrate.Upgrade(ctx, db, baseline, before); err != nil {
		t.Fatalf("the chain before 149: %v", err)
	}
	if columns := refColumns(ctx, t, conn); len(columns) != 0 {
		t.Fatalf("before 149 work_unit_investments already holds %v", columns)
	}
	// A merge would rewrite parts and fill the columns: none runs while the parts are compared.
	refsExec(ctx, t, conn, "SYSTEM STOP MERGES work_unit_investments")
	refsSeed(ctx, t, conn, "both", "issues-only", "prs-only")
	refsSeed(ctx, t, conn, "empty-object", "empty-text", "not-json")
	stored := readRefsStorage(ctx, t, conn)
	if len(stored.Parts) < 2 || stored.Mutations != 0 || stored.StoredColumn != 0 {
		t.Fatalf("before 149: %+v, want at least 2 parts, no mutation, no reference column", stored)
	}

	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil || len(result.Applied) != 1 || result.Applied[0] != workUnitInvestmentRefsVersion {
		t.Fatalf("the upgrade = %+v, %v; want %s applied", result, err, workUnitInvestmentRefsVersion)
	}
	for name, key := range map[string]string{"issue_refs": "'issues'", "pr_refs": "'prs'"} {
		column := refColumns(ctx, t, conn)[name]
		if !strings.HasPrefix(column, "Array(String) DEFAULT ") || !strings.Contains(column, "JSONExtract(structural_evidence_json, "+key) {
			t.Errorf("%s is %q, want Array(String) DEFAULT JSONExtract(structural_evidence_json, %s, ...)", name, column, key)
		}
	}
	if after := readRefsStorage(ctx, t, conn); !reflect.DeepEqual(after, stored) {
		t.Fatalf("149 changed the stored data: before %+v, after %+v; want the same parts, no mutation, no part holding a reference column", stored, after)
	}
	checkRefsRead(ctx, t, conn, "after 149")

	for _, statement := range chmigrate.SplitStatements(migration.SQL) {
		refsExec(ctx, t, conn, statement)
	}
	if again := readRefsStorage(ctx, t, conn); !reflect.DeepEqual(again, stored) {
		t.Fatalf("a second run of 149 changed the stored data: %+v, want %+v", again, stored)
	}
	checkRefsRead(ctx, t, conn, "after a second run")
}

// The hand-only rollback, rollback/149_work_unit_investment_refs.down.sql,
// rehearsed up -> down -> up on this build: down drops both columns and the
// version row, and the rows still read their evidence; the next upgrade applies
// 149 again and the rows read their keys again.
func TestRollback149UpDownUp(t *testing.T) {
	ctx := context.Background()
	scratch := scratchDatabase(t)
	conn := openDatabase(t, scratch.URI, "")
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	baseline, chain, _, _ := refsChainBefore(t)
	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatalf("real chain: %v", err)
	}
	units := make([]string, 0, len(refsSeedRows))
	for unit := range refsSeedRows {
		units = append(units, unit)
	}
	refsSeed(ctx, t, conn, units...)
	checkRefsRead(ctx, t, conn, "at the head")
	versionRows := func() uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM schema_migrations WHERE version = ?", workUnitInvestmentRefsVersion).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if versionRows() == 0 {
		t.Fatalf("at the head schema_migrations holds no %s", workUnitInvestmentRefsVersion)
	}

	body, err := os.ReadFile("rollback/149_work_unit_investment_refs.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range chmigrate.SplitStatements(string(body)) {
		refsExec(ctx, t, conn, statement)
	}
	if columns := refColumns(ctx, t, conn); len(columns) != 0 {
		t.Fatalf("after the rollback work_unit_investments still holds %v", columns)
	}
	if versionRows() != 0 {
		t.Fatalf("after the rollback schema_migrations still holds %s", workUnitInvestmentRefsVersion)
	}
	var rows uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM work_unit_investments WHERE org_id = 'org-149'").Scan(&rows); err != nil || rows != uint64(len(refsSeedRows)) {
		t.Fatalf("after the rollback %d rows remain (%v), want %d", rows, err, len(refsSeedRows))
	}

	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil || len(result.Applied) != 1 || result.Applied[0] != workUnitInvestmentRefsVersion {
		t.Fatalf("the upgrade after the rollback = %+v, %v; want %s applied", result, err, workUnitInvestmentRefsVersion)
	}
	checkRefsRead(ctx, t, conn, "after the upgrade again")
}
