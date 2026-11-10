package clickhouse

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

func textOf(value string) *string { return &value }

// prodWorkerGrantRows are prod dho_app's system.grants rows as pm read them at
// 2026-11-10 ~06:40Z (ch-grants-prod-20261110T0640Z.tsv, sha256
// fbe9c2e10be5a540984624d1677b0e63d1eb02b5535b7e4076a888e3371dfd49), expanded
// from pm's aggregated lines back to one row per privilege and table.
func prodWorkerGrantRows() []SystemGrantRow {
	row := func(database string, table *string, accessType string) SystemGrantRow {
		return SystemGrantRow{UserName: textOf(WorkerUser), AccessType: accessType, Database: textOf(database), Table: table}
	}
	return []SystemGrantRow{
		row("default", nil, "SELECT"),
		row("default", nil, "INSERT"),
		row("default", nil, "ALTER DELETE"),
		row("default", nil, "OPTIMIZE"),
		row("system", textOf("columns"), "SELECT"),
		row("system", textOf("parts"), "SELECT"),
		row("system", textOf("tables"), "SELECT"),
		row("system", textOf("view_refreshes"), "SELECT"),
	}
}

func TestWorkerGrantStatements(t *testing.T) {
	want := []string{
		"GRANT SELECT, INSERT, ALTER DELETE, OPTIMIZE ON default.* TO dho_app",
		"GRANT SELECT ON system.columns TO dho_app",
		"GRANT SELECT ON system.parts TO dho_app",
		"GRANT SELECT ON system.tables TO dho_app",
		"GRANT SELECT ON system.view_refreshes TO dho_app",
	}
	got := WorkerGrantStatements(WorkerUser)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("WorkerGrantStatements:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestWorkerPostureCitesAReaderPerPrivilege(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	for _, grant := range WorkerPosture() {
		path, _, found := strings.Cut(grant.Reader, ":")
		if !found || path == "" {
			t.Errorf("%s ON %s: reader %q is not file:line", grant.AccessType, grant.scope(), grant.Reader)
			continue
		}
		if _, err := os.Stat(filepath.Join(root, path)); err != nil {
			t.Errorf("%s ON %s: reader %q: %v", grant.AccessType, grant.scope(), grant.Reader, err)
		}
	}
}

func TestWorkerGrantsSelectReadsTheCompareColumns(t *testing.T) {
	want := "SELECT " + strings.Join(WorkerGrantsColumns, ", ") + "\nFROM system.grants\n"
	if !strings.HasPrefix(WorkerGrantsSelect, want) {
		t.Fatalf("WorkerGrantsSelect does not start with %q:\n%s", want, WorkerGrantsSelect)
	}
}

func TestCompareWorkerGrants(t *testing.T) {
	with := func(edit func([]SystemGrantRow) []SystemGrantRow) []SystemGrantRow {
		return edit(prodWorkerGrantRows())
	}
	add := func(extra SystemGrantRow) []SystemGrantRow {
		return with(func(rows []SystemGrantRow) []SystemGrantRow { return append(rows, extra) })
	}
	user := textOf(WorkerUser)
	cases := []struct {
		name    string
		rows    []SystemGrantRow
		problem string
	}{
		{"prod 06:40Z rows", prodWorkerGrantRows(), ""},
		{"prod rows in another order", with(func(rows []SystemGrantRow) []SystemGrantRow {
			for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
				rows[i], rows[j] = rows[j], rows[i]
			}
			return rows
		}), ""},
		{"missing privilege", with(func(rows []SystemGrantRow) []SystemGrantRow {
			return append(rows[:3], rows[4:]...)
		}), "missing privilege: OPTIMIZE ON default.*"},
		{"no rows", nil, "missing privilege: SELECT ON default.*"},
		{"extra privilege", add(SystemGrantRow{UserName: user, AccessType: "ALTER UPDATE", Database: textOf("default")}),
			"extra privilege: ALTER UPDATE ON default.*"},
		{"alias name the server never writes", add(SystemGrantRow{UserName: user, AccessType: "DELETE", Database: textOf("default")}),
			"extra privilege: DELETE ON default.*"},
		{"extra database", add(SystemGrantRow{UserName: user, AccessType: "SELECT", Database: textOf("analytics")}),
			"extra database/table: SELECT ON analytics.*"},
		{"extra table", add(SystemGrantRow{UserName: user, AccessType: "SELECT", Database: textOf("system"), Table: textOf("query_log")}),
			"extra database/table: SELECT ON system.query_log"},
		{"global grant", add(SystemGrantRow{UserName: user, AccessType: "SELECT"}),
			"extra database/table: SELECT ON *.*"},
		{"wildcard table", add(SystemGrantRow{UserName: user, AccessType: "SELECT", Database: textOf("default"), Table: textOf("*")}),
			"wildcard scope: SELECT ON default.*"},
		{"grant option", with(func(rows []SystemGrantRow) []SystemGrantRow {
			rows[5].GrantOption = 1
			return rows
		}), "grant option: SELECT ON system.parts"},
		{"partial revoke", add(SystemGrantRow{UserName: user, AccessType: "SELECT", Database: textOf("default"), Table: textOf("secrets"), IsPartialRevoke: 1}),
			"partial revoke: SELECT ON default.secrets"},
		{"column-level grant", add(SystemGrantRow{UserName: user, AccessType: "SELECT", Database: textOf("default"), Table: textOf("teams"), Column: textOf("name")}),
			"column-level grant: SELECT ON default.teams(name)"},
		{"role grant", add(SystemGrantRow{RoleName: textOf("worker_role"), AccessType: "SELECT", Database: textOf("default"), Table: textOf("teams")}),
			"role grant: role worker_role holds SELECT ON default.teams"},
		{"another user", add(SystemGrantRow{UserName: textOf("dho_api_ch"), AccessType: "SELECT", Database: textOf("default"), Table: textOf("teams")}),
			"row for another user dho_api_ch: SELECT ON default.teams"},
		{"no user and no role", add(SystemGrantRow{AccessType: "SELECT", Database: textOf("default"), Table: textOf("teams")}),
			"row for another user NULL: SELECT ON default.teams"},
		{"duplicate row", add(prodWorkerGrantRows()[0]), "duplicate row: SELECT ON default.*"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := CompareWorkerGrants(tc.rows)
			if tc.problem == "" {
				if err != nil {
					t.Fatalf("want a match, got: %v", err)
				}
				return
			}
			if !errors.Is(err, ErrWorkerPostureMismatch) || !strings.Contains(err.Error(), tc.problem) {
				t.Fatalf("want ErrWorkerPostureMismatch naming %q, got: %v", tc.problem, err)
			}
		})
	}
}
