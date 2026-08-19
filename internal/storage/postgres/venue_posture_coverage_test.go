package postgres

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// This guard exists because of a defect that reached CI three times over: an
// integration venue that builds its schema BY HAND, a posture manifest that
// grows a table, and nothing connecting the two until a container run fails
// with CheckDomainAuthorization's deliberately opaque "PostgreSQL readiness
// check failed".
//
// The concrete instance: #1529 added sync_run_unit_effect_snapshots to
// domainPosture (migration 0088) and a matching domain GRANT in
// riverstore.ApplyPinnedMigrations. That GRANT is guarded by
// `IF to_regclass(...) IS NOT NULL`, so in a venue that never CREATEs the
// table the grant is silently skipped, the readiness check fails closed, and
// the error names neither the table nor the venue. Three tests in two packages
// went red for one missing CREATE.
//
// The fix for that instance was two CREATE TABLE lines. This test is the fix
// for the CLASS: it fails at the next posture addition, in the fast unit lane
// with the table and package named, instead of at the next container run with
// an opaque readiness error. It needs no Docker on purpose -- a guard that
// only runs where the original failure ran would be no earlier than the
// failure it replaces.
//
// It is deliberately a SOURCE analysis rather than a runtime one. A runtime
// version could only re-discover what CheckDomainAuthorization already
// discovers, at the same moment, in the same job.

// venuePackagesWithoutSchema is the ONLY place a package that calls
// CheckDomainAuthorization may be exempt from the coverage requirement below,
// and every entry needs a real reason. The default for such a package is "it
// builds a venue and that venue must be complete"; discovery finding no
// CREATE TABLE in it is treated as a possible relocation of the DDL (which
// would silently un-cover the venue), not as self-evidently fine.
//
// A stale entry -- one naming a package that discovery no longer classifies
// this way -- FAILS, for the same reason INTEGRATION_DENYLIST entries do in
// ci/check_go.sh: an exemption list that drifts unnoticed is how the next
// gap gets pre-approved.
// It is empty today, and that is a real result rather than a placeholder:
// every package discovery enrols does build a venue. The one historical
// candidate for an entry was internal/domaingrants, whose grant_surface_test.go
// named CheckDomainAuthorization inside a message string and started no
// database -- discovery matches parsed CALL sites, so it was never enrolled and
// needed no exemption. That package was removed under CHAOS-3875.
var venuePackagesWithoutSchema = map[string]string{}

var createTablePattern = regexp.MustCompile(
	`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?public\.([a-z0-9_]+)`,
)

// TestHandBuiltVenuesCreateEveryDomainPostureTable requires every package that
// gates a test on CheckDomainAuthorization to create every table
// domainPosture() declares. Discovery is repo-wide and inclusive: a venue
// added tomorrow is covered the day it is written, without anyone remembering
// to add it here.
func TestHandBuiltVenuesCreateEveryDomainPostureTable(t *testing.T) {
	root := repositoryRoot(t)
	required := requiredPostureTables()
	if len(required) == 0 {
		t.Fatal("domainPosture() declared no tables -- the extraction below is " +
			"reading the wrong thing, not a genuinely empty manifest")
	}

	packages := discoverDomainAuthorizationPackages(t, root)
	if len(packages) == 0 {
		t.Fatal("no package calls CheckDomainAuthorization from a test -- discovery " +
			"is broken (this repository has several), and a guard that silently " +
			"checks nothing reads exactly like a guard that passes")
	}

	exempt := map[string]bool{}
	for _, pkg := range packages {
		created := createdTables(t, filepath.Join(root, pkg))
		if len(created) == 0 {
			reason, allowed := venuePackagesWithoutSchema[pkg]
			if !allowed {
				t.Errorf("%s gates a test on CheckDomainAuthorization but creates no "+
					"tables of its own. Either its venue DDL moved (which un-covers it "+
					"from this guard -- point the guard at wherever it went) or it "+
					"genuinely has no venue, in which case add it to "+
					"venuePackagesWithoutSchema with the reason.", pkg)
				continue
			}
			exempt[pkg] = true
			t.Logf("%s: no venue schema, exempt (%s)", pkg, reason)
			continue
		}
		var missing []string
		for _, table := range required {
			if !created[table] {
				missing = append(missing, table)
			}
		}
		if len(missing) > 0 {
			t.Errorf("%s builds a venue by hand but never creates %v, which "+
				"domainPosture() requires. CheckDomainAuthorization fails closed on a "+
				"missing table, and ApplyPinnedMigrations' domain GRANT for it is "+
				"guarded by `IF to_regclass(...) IS NOT NULL`, so the venue reports the "+
				"opaque \"PostgreSQL readiness check failed\" instead. Add a CREATE "+
				"TABLE for each to this package's fixture.", pkg, missing)
			continue
		}
		t.Logf("%s: covers all %d required tables", pkg, len(required))
	}

	for pkg := range venuePackagesWithoutSchema {
		if !exempt[pkg] {
			t.Errorf("venuePackagesWithoutSchema names %q, which discovery did not "+
				"classify as a CheckDomainAuthorization package without schema -- a "+
				"stale exemption suppressing nothing, or a misspelled path", pkg)
		}
	}
}

// requiredPostureTables is every table the domain role's readiness check
// touches: the RequiredTables manifest plus the tables behind its
// column-scoped privileges, which must equally exist for
// has_table_privilege() to answer at all. It reads domainPosture() directly
// rather than restating it -- a second hand-maintained list here would
// reintroduce the drift this guard exists to catch, one level up.
func requiredPostureTables() []string {
	posture := domainPosture()
	unique := map[string]bool{}
	for _, table := range posture.RequiredTables {
		unique[table.TableName] = true
	}
	for _, column := range posture.ColumnScoped {
		unique[column.TableName] = true
	}
	tables := make([]string, 0, len(unique))
	for table := range unique {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return tables
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	// internal/storage/postgres -> repository root.
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("resolved repository root %s has no go.mod: %v", root, err)
	}
	return root
}

// discoverDomainAuthorizationPackages returns every repository-relative
// directory holding a _test.go file that CALLS CheckDomainAuthorization. The
// match is on the parsed identifier, not on the file text, so naming the
// function in a comment or a doc string does not enrol a package that never
// runs the check.
func discoverDomainAuthorizationPackages(t *testing.T, root string) []string {
	t.Helper()
	found := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			// Every dot-directory in this repo is tooling output, not source:
			// .git and .venv were already enumerated here, and .gitlab-ci-local
			// (which caches a FULL copy of the tree under builds/.docker/) made
			// it three. A stale copy still parses, still calls
			// CheckDomainAuthorization, and is still missing whatever tables
			// domainPosture() has gained since it was cached -- so this test
			// false-reds on any machine that has run gitlab-ci-local, naming
			// packages that do not exist. Skipping the class beats extending
			// the list once per artifact directory anyone adds.
			if path != root && strings.HasPrefix(entry.Name(), ".") {
				return filepath.SkipDir
			}
			switch entry.Name() {
			case "vendor", "node_modules", "site", "docs":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		if !callsDomainAuthorization(t, path) {
			return nil
		}
		relative, relErr := filepath.Rel(root, filepath.Dir(path))
		if relErr != nil {
			return relErr
		}
		found[filepath.ToSlash(relative)] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	packages := make([]string, 0, len(found))
	for pkg := range found {
		packages = append(packages, pkg)
	}
	sort.Strings(packages)
	return packages
}

func callsDomainAuthorization(t *testing.T, path string) bool {
	t.Helper()
	// Parsing WITHOUT comments is what makes this a call-site match: a file
	// that only discusses the check in prose never reaches ast.Ident.
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	called := false
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch function := call.Fun.(type) {
		case *ast.SelectorExpr:
			if function.Sel.Name == "CheckDomainAuthorization" {
				called = true
			}
		case *ast.Ident:
			if function.Name == "CheckDomainAuthorization" {
				called = true
			}
		}
		return !called
	})
	return called
}

// createdTables collects the tables a package's tests create, reading only
// STRING LITERALS. A regex over raw file text would count a table named in a
// comment as created -- which is precisely the direction that must never be
// wrong here, since a false "created" reading silently exempts a real gap.
func createdTables(t *testing.T, dir string) map[string]bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	created := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		file, parseErr := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if parseErr != nil {
			t.Fatalf("parse %s: %v", path, parseErr)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			text, unquoteErr := strconv.Unquote(literal.Value)
			if unquoteErr != nil {
				// A raw string with an embedded backtick cannot occur in Go, so
				// this only fires on a literal this walk should not see at all.
				return true
			}
			for _, match := range createTablePattern.FindAllStringSubmatch(text, -1) {
				created[strings.ToLower(match[1])] = true
			}
			return true
		})
	}
	return created
}
