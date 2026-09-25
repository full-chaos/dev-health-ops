//go:build integration

package pgmigrate_test

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// ledgerEnv, set to 1, rewrites testdata/hand_ddl_ledger.tsv from the tree
// instead of comparing against it. CI never sets it.
const ledgerEnv = "DHO_HAND_DDL_LEDGER_UPDATE"

const ledgerPath = "testdata/hand_ddl_ledger.tsv"

// probeMaxColumns bounds a PROBE shell: `id` plus a probe column or two.
const probeMaxColumns = 3

// probeVenues are the privilege venues whose hand tables are deliberate shells:
// each test builds a scratch database with hand-made roles and creates a table
// under a production name only so the posture manifest has something to GRANT
// on (the manifest is table-level; no query reads a column). A table here that
// carries a column production lacks is recorded as PROBE, not INVENTED. A file
// is added only with the reason for it.
var probeVenues = map[string]string{
	"internal/storage/postgres/runtime_authorization_integration_test.go":            "runtime posture: GRANT targets for the domain/queue/elevated roles",
	"internal/storage/postgres/domain_authorization_integration_test.go":             "domain posture: GRANT targets, DDL-forbidden probes",
	"internal/storage/postgres/domain_grant_reconciliation_integration_test.go":      "grant reconciliation over shell tables",
	"internal/storage/postgres/coordinator_statement_privileges_integration_test.go": "coordinator statement privileges over shell tables",
	"internal/storage/river/migrate_integration_test.go":                             "river migration role grants over shell tables",
	"internal/syncreconciler/kernel_integration_test.go":                             "reconciler roles over shell tables",
	"internal/syncdispatchruntime/publisher_integration_test.go":                     "publisher roles over shell outbox tables",
	"internal/joboperator/postgres_integration_test.go":                              "operator role posture over shell tables",
	"internal/api/policy/store_integration_test.go":                                  "API posture: readiness requires every declared table",
}

// TestHandWrittenTestDDLMatchesTheMigratedSchema is the guard for CHAOS-6769
// (Trap #412: integration tests never hand-write DDL for production tables).
//
// #3134 made the ownership matcher read integrations.credential_id; the
// external-ingest tests built their own `integrations` table without it, and
// main's storage-integration shard failed with a generic 500 for hours. The
// class: a hand-written CREATE TABLE in a test drifts from the schema the
// migrations produce, and the test either fails far from the cause or, worse,
// passes against a table production does not have.
//
// It builds the REAL schema (the pgmigrate baseline and chain), then compares
// every non-temporary CREATE TABLE found in a *_test.go file whose table
// exists in the real public schema:
//
//	INVENTED  the hand table declares a column the real table does not have.
//	          The test exercises a schema production never had.
//	PROBE     INVENTED, in a privilege venue (probeVenues), and a shell of at
//	          most probeMaxColumns columns: a GRANT target for the posture
//	          manifest with a probe column (`state`), not a model of the table.
//	SUBSET    the hand table lacks columns the real table has: a query that
//	          reads one fails with the 500 above.
//	EXACT     same column set (the shared-loader case; needs no ledger row).
//
// Existing drift is recorded in testdata/hand_ddl_ledger.tsv (a ratchet, not
// an approval): the test fails on drift that is not in the ledger, and on a
// ledger row that no longer drifts (delete it: the ledger only shrinks). New
// tests load the schema (see accept_batch_integration_test.go in
// internal/api/externalingest) instead of adding a row.
//
// Not seen: DDL assembled at run time, tables in schemas other than public,
// and ClickHouse tables (files that never mention Postgres are skipped).
func TestHandWrittenTestDDLMatchesTheMigratedSchema(t *testing.T) {
	ctx := context.Background()
	real := loadRealColumns(ctx, t)
	found := scanHandTables(t)
	if len(found) < 50 {
		t.Fatalf("scanned only %d hand-written tables; the scan is broken, not the tree", len(found))
	}

	current := map[string]string{} // key -> kind + invented
	for _, h := range found {
		cols, ok := real[h.Table]
		if !ok || h.Schema != "public" {
			continue
		}
		if len(h.Unreadable) > 0 {
			t.Errorf("%s: table %s: cannot read column definitions %q; write them as plain identifiers or extend the parser", h.File, h.Table, h.Unreadable)
			continue
		}
		var invented, missing []string
		have := map[string]bool{}
		for _, c := range h.Columns {
			have[c] = true
			if !cols[c] {
				invented = append(invented, c)
			}
		}
		for c := range cols {
			if !have[c] {
				missing = append(missing, c)
			}
		}
		if len(invented) == 0 && len(missing) == 0 {
			continue
		}
		sort.Strings(invented)
		kind := "SUBSET"
		if len(invented) > 0 {
			kind = "INVENTED"
			if _, ok := probeVenues[h.File]; ok && len(h.Columns) <= probeMaxColumns {
				kind = "PROBE"
			}
		}
		key := h.File + "\t" + h.Table
		// two CREATEs of one table in one file: the worse kind, the union.
		if prev, seen := current[key]; seen && (strings.HasPrefix(prev, "INVENTED") || strings.HasPrefix(prev, "PROBE")) && kind == "SUBSET" {
			continue
		}
		current[key] = kind + "\t" + strings.Join(invented, ",")
	}

	if os.Getenv(ledgerEnv) == "1" {
		writeLedger(t, current)
		t.Skipf("%s=1: ledger rewritten with %d rows", ledgerEnv, len(current))
	}
	ledger := readLedger(t)

	var problems []string
	for key, val := range current {
		if want, ok := ledger[key]; !ok {
			problems = append(problems, fmt.Sprintf("NEW DRIFT  %s  %s: hand-written DDL differs from the migrated schema; load the schema instead (see the test's doc comment)", strings.ReplaceAll(key, "\t", "  table "), strings.ReplaceAll(val, "\t", " invented=")))
		} else if want != val {
			problems = append(problems, fmt.Sprintf("CHANGED    %s: ledger has %q, tree has %q", strings.ReplaceAll(key, "\t", "  table "), want, val))
		}
	}
	for key := range ledger {
		if _, ok := current[key]; !ok {
			problems = append(problems, fmt.Sprintf("STALE ROW  %s no longer drifts (or is gone): delete its ledger row", strings.ReplaceAll(key, "\t", "  table ")))
		}
	}
	sort.Strings(problems)
	if len(problems) > 0 {
		t.Errorf("%d hand-DDL problem(s) against the migrated schema (%s):\n%s", len(problems), ledgerPath, strings.Join(problems, "\n"))
	}
}

func loadRealColumns(ctx context.Context, t *testing.T) map[string]map[string]bool {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	conn, err := pgx.Connect(ctx, instance.URI)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatalf("load baseline: %v", err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatalf("load chain: %v", err)
	}
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain); err != nil {
		t.Fatalf("apply the schema: %v", err)
	}
	rows, err := conn.Query(ctx, `SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'public'`)
	if err != nil {
		t.Fatalf("read the schema: %v", err)
	}
	defer rows.Close()
	real := map[string]map[string]bool{}
	for rows.Next() {
		var table, column string
		if err := rows.Scan(&table, &column); err != nil {
			t.Fatal(err)
		}
		if real[table] == nil {
			real[table] = map[string]bool{}
		}
		real[table][column] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(real) < 50 {
		t.Fatalf("the migrated schema has only %d public tables; the measurement did not happen", len(real))
	}
	return real
}

func repoRootDir(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the test's directory")
		}
		dir = parent
	}
}

func scanHandTables(t *testing.T) []handTable {
	t.Helper()
	root := repoRootDir(t)
	var found []handTable
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "vendor", "testdata":
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") || strings.HasPrefix(d.Name(), "handddl_") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		src := string(raw)
		if !strings.Contains(strings.ToLower(src), "postgres") && !strings.Contains(src, "pgx") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		tables, err := parseHandTables(filepath.ToSlash(rel), src)
		if err != nil {
			return err
		}
		found = append(found, tables...)
		return nil
	})
	if err != nil {
		t.Fatalf("scan test files: %v", err)
	}
	return found
}

func readLedger(t *testing.T) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(ledgerPath)
	if err != nil {
		t.Fatalf("read the ledger: %v (regenerate with %s=1)", err, ledgerEnv)
	}
	ledger := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 4 {
			t.Fatalf("ledger row %q: want 4 tab-separated fields (file, table, kind, invented)", line)
		}
		ledger[f[0]+"\t"+f[1]] = f[2] + "\t" + f[3]
	}
	return ledger
}

func writeLedger(t *testing.T, current map[string]string) {
	t.Helper()
	keys := make([]string, 0, len(current))
	for k := range current {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString("# Hand-written test DDL that differs from the migrated Postgres schema (CHAOS-6769).\n")
	b.WriteString("# file<TAB>table<TAB>kind<TAB>invented columns. A ratchet: it only shrinks. See the test's doc comment.\n")
	for _, k := range keys {
		b.WriteString(k + "\t" + current[k] + "\n")
	}
	if err := os.WriteFile(ledgerPath, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}
