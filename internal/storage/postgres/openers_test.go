package postgres

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// Every place the repository opens a PostgreSQL connection or pool with pgx
// directly is found in the code (not from a list somebody keeps) and must be
// classified: a driver failure text carries the effective login (and, from a
// server that echoes, the password) whatever configuration source supplied them, so
// each opener either builds its redaction boundary from the resolved
// configuration, returns a fixed generic message, is the Open helper that redacts,
// discards the error, is a server that logs, or is test-only. A new opener fails
// this test until it is classified (CHAOS-6665).

type openerKind string

const (
	// kindBoundary: the file builds its boundary with Boundary (this package).
	kindBoundary openerKind = "boundary"
	// kindGeneric: a connect failure becomes a fixed message (RedactedConnectError).
	kindGeneric openerKind = "generic"
	// kindHelper: the Open helper itself, which redacts with WithRedactedCauseAlso.
	kindHelper openerKind = "helper"
	// kindDiscarded: the error is dropped, never printed.
	kindDiscarded openerKind = "discarded"
	// kindServer: a long-running server, its errors go to its own logs.
	kindServer openerKind = "server"
	// kindTest: test support and compatibility probes, not a shipped verb.
	kindTest openerKind = "test"
)

// otherBoundaries is how many DSN-only boundaries (secrets.NewBoundary) a
// `boundary` file may still build, for a database that is not PostgreSQL (a Valkey
// URI): a file that builds one for PostgreSQL too would print the resolved
// credentials, so any other count fails.
var otherBoundaries = map[string]int{
	"internal/fixturescli/fixturescli.go": 1, // the VALKEY_URI boundary
}

var openers = map[string]struct {
	kind openerKind
	why  string
}{
	"internal/admincli/admincli.go":                        {kindBoundary, "admin features seed"},
	"internal/adminops/users.go":                           {kindBoundary, "admin users and orgs: the pool and the error redactor"},
	"internal/backfillrun/command.go":                      {kindBoundary, "backfill run"},
	"internal/fixturescli/fixturescli.go":                  {kindBoundary, "fixtures finalize-synthetic-sync"},
	"internal/goapicli/prove/main.go":                      {kindBoundary, "go-api-prove: the boundary run() composes"},
	"internal/goapicli/routing/main.go":                    {kindBoundary, "goapi routing: connectPostgres, the helper of status, enable, disable, repoint and carry"},
	"internal/maintenancecli/maintenance.go":               {kindBoundary, "maintenance"},
	"internal/pgmigrate/command.go":                        {kindBoundary, "migrate postgres upgrade, status, current"},
	"internal/goapicli/restprove/main.go":                  {kindGeneric, "a failed connect is the fixed message"},
	"internal/migrationmatrix/live.go":                     {kindGeneric, "a failed connect is the fixed message"},
	"internal/migrationmatrix/restproven.go":               {kindGeneric, "a failed connect is the fixed message"},
	"internal/storage/postgres/factory.go":                 {kindHelper, "Open and New: WithRedactedCauseAlso with the resolved credentials"},
	"internal/synccli/dblookup.go":                         {kindDiscarded, "a connect or query error is swallowed into 'not found'"},
	"internal/queryapi/server/home_route.go":               {kindServer, "lazy pool of a running server; errors reach its logs"},
	"internal/queryapi/server/investment_explain_route.go": {kindServer, "lazy pool of a running server; errors reach its logs"},
	"internal/queryapi/server/query_route.go":              {kindServer, "lazy pool of a running server; errors reach its logs"},
	"internal/queryapi/server/workunit_explain_route.go":   {kindServer, "lazy pool of a running server; errors reach its logs"},
	"internal/testsupport/containers/remote.go":            {kindTest, "test containers"},
	"internal/testsupport/pgschema/pgschema.go":            {kindTest, "test schema helper"},
	"internal/testsupport/venueoracle/venueoracle.go":      {kindTest, "venue oracle harness"},
	"tests/compatibility/river/go/probe.go":                {kindTest, "compatibility probe"},
	"tests/compatibility/river/nminus1/probe.go":           {kindTest, "compatibility probe"},
}

// repoRoot is the module root, found from this package's directory.
func repoRoot(t *testing.T) string {
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
			t.Fatal("no go.mod above the test directory")
		}
		dir = parent
	}
}

// fileCalls counts the selector calls (pkg.Func) a file makes.
func fileCalls(t *testing.T, path string) map[string]int {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	found := map[string]int{}
	ast.Inspect(parsed, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
			if pkg, ok := selector.X.(*ast.Ident); ok {
				found[pkg.Name+"."+selector.Sel.Name]++
			} else {
				found["."+selector.Sel.Name]++
			}
		}
		return true
	})
	return found
}

func TestEveryPostgresOpenerIsClassified(t *testing.T) {
	root := repoRoot(t)
	opened := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := entry.Name()
		if entry.IsDir() {
			switch name {
			case ".git", ".venv", "node_modules", "vendor", ".remember", "worktrees":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		calls := fileCalls(t, path)
		for _, opener := range []string{"pgx.Connect", "pgx.ConnectConfig", "pgxpool.New", "pgxpool.NewWithConfig"} {
			if calls[opener] > 0 {
				relative, _ := filepath.Rel(root, path)
				opened[filepath.ToSlash(relative)] = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	for path := range opened {
		found = append(found, path)
	}
	sort.Strings(found)
	if len(found) < 15 {
		t.Fatalf("only %d openers found: the scan measures nothing: %v", len(found), found)
	}
	for _, path := range found {
		class, known := openers[path]
		if !known {
			t.Errorf("%s opens PostgreSQL with pgx and is not classified: build its boundary with Boundary(), or classify it here with the reason it needs none", path)
			continue
		}
		calls := fileCalls(t, filepath.Join(root, path))
		switch class.kind {
		case kindBoundary:
			if calls["pgstorage.Boundary"]+calls["postgres.Boundary"] == 0 {
				t.Errorf("%s is classified %q (%s) but does not call the resolved-credential Boundary", path, class.kind, class.why)
			}
			if got := calls["secrets.NewBoundary"]; got != otherBoundaries[path] {
				t.Errorf("%s builds %d DSN-only secrets.NewBoundary boundaries, the table allows %d (only for a database that is not PostgreSQL): a DSN-only boundary leaves the credentials pgx resolved from the environment", path, got, otherBoundaries[path])
			}
		case kindGeneric:
			if calls["secrets.RedactedConnectError"] == 0 {
				t.Errorf("%s is classified %q (%s) but does not return secrets.RedactedConnectError", path, class.kind, class.why)
			}
		case kindHelper:
			if calls["secrets.WithRedactedCauseAlso"] == 0 {
				t.Errorf("%s is classified %q (%s) but does not call secrets.WithRedactedCauseAlso", path, class.kind, class.why)
			}
		}
	}
	for path := range openers {
		if !opened[path] {
			t.Errorf("%s is classified but no longer opens PostgreSQL with pgx: remove it from the table", path)
		}
	}
}
