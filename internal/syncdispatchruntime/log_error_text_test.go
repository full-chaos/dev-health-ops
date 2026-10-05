package syncdispatchruntime

import (
	"bytes"
	"context"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/tools/go/packages"
)

// No log line of this package can carry the text of an error (CHAOS-7933, D4270). The package logs through the synclog package,
// whose logger holds the *slog.Logger in an unexported field with no accessor, whose messages, keys and labels are opaque values
// declared only there, whose identifiers and tokens pass a fail-closed parser, and whose only error input is Failure(err) (class,
// Go type, SQLSTATE). Every form that would put error text in a log line therefore does not compile. The remaining guards are
// the two ways to bypass it without a compile error: importing the log packages, and obtaining a log-package value from another
// package without importing it (logging.NewJSON(...).Warn(...)).
func TestNoFileOutsideSynclogImportsTheLogPackages(t *testing.T) {
	// the package AND its neighbour syncbudget (the budget loader logs through synclog too): no exemption
	fset := token.NewFileSet()
	files := 0
	var violations []string
	var names []string
	for _, dir := range []string{"."} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			names = append(names, dir+"/"+entry.Name())
		}
	}
	for _, name := range names {
		file, err := parser.ParseFile(fset, name, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		files++
		for _, spec := range file.Imports {
			path, _ := strconv.Unquote(spec.Path.Value)
			if path == "log/slog" || path == "log" {
				violations = append(violations, name+" imports "+path)
			}
		}
	}
	t.Logf("%d non-test files read; none imports log/slog or log", files)
	if files < 30 {
		t.Fatalf("read %d files, want at least 30: the walk reads nothing", files)
	}
	if len(violations) != 0 {
		t.Fatalf("files of the package import the log packages: %v", violations)
	}
	synclogFiles, slogImporters, plainLogImporters := 0, 0, 0
	inner, err := os.ReadDir("synclog")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range inner {
		if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, "synclog/"+entry.Name(), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		synclogFiles++
		for _, spec := range file.Imports {
			switch path, _ := strconv.Unquote(spec.Path.Value); path {
			case "log/slog":
				slogImporters++
			case "log":
				plainLogImporters++
			}
		}
	}
	if plainLogImporters != 0 {
		t.Fatalf("synclog: %d files import the standard log package; only log/slog is allowed, in one file", plainLogImporters)
	}
	if synclogFiles == 0 || slogImporters != 1 {
		t.Fatalf("synclog: %d files, %d import log/slog; want exactly one importer", synclogFiles, slogImporters)
	}
}

const modulePrefix = "github.com/full-chaos/dev-health-ops/internal/"

// legacyLogUsers are the first-party dependencies of this package that import log/slog or log TODAY, for their own purposes
// (CHAOS-7940 sweeps them). The list is CLOSED and only shrinks: a dependency that imports a log package and is not listed
// fails, a listed package that no longer imports one fails (delete its row). Every other first-party dependency is covered by
// the import check and the value check below.
var legacyLogUsers = map[string]bool{
	"jobruntime": true, "platform/health": true, "joboutbox": true, "platform/secrets": true, "platform/logging": true,
	"providerfoundation": true, "storedversion": true, "providersync": true, "platform/tracing": true, "scheduler/sync": true,
	"synccoverage": true,
}

// derivedDependencies loads the package with its dependencies and returns every first-party package it depends on, transitively,
// except synclog itself (no hand list).
func derivedDependencies(t *testing.T) (root *packages.Package, deps []*packages.Package) {
	t.Helper()
	config := &packages.Config{Dir: ".", Mode: packages.NeedName | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedDeps}
	loaded, err := packages.Load(config, ".")
	if err != nil || len(loaded) != 1 {
		t.Fatalf("load: %v %v", err, loaded)
	}
	root = loaded[0]
	seen := map[string]bool{}
	var walk func(*packages.Package)
	walk = func(pkg *packages.Package) {
		for _, imported := range pkg.Imports {
			if seen[imported.PkgPath] || !strings.HasPrefix(imported.PkgPath, modulePrefix) {
				continue
			}
			seen[imported.PkgPath] = true
			if !strings.HasSuffix(imported.PkgPath, "/synclog") {
				deps = append(deps, imported)
			}
			walk(imported)
		}
	}
	walk(root)
	return root, deps
}

func TestTheDerivedDependencySetIsCoveredAndTheLegacyListOnlyShrinks(t *testing.T) {
	_, deps := derivedDependencies(t)
	names := map[string]bool{}
	users := map[string]bool{}
	for _, dep := range deps {
		name := strings.TrimPrefix(dep.PkgPath, modulePrefix)
		names[name] = true
		if _, slog := dep.Imports["log/slog"]; slog {
			users[name] = true
		} else if _, std := dep.Imports["log"]; std {
			users[name] = true
		}
	}
	if len(deps) < 20 || !names["syncbudget"] {
		t.Fatalf("the derived set has %d packages (syncbudget in it: %v): the derivation shrank", len(deps), names["syncbudget"])
	}
	for name := range users {
		if !legacyLogUsers[name] {
			t.Errorf("%s imports a log package and is not on the closed legacy list: route it through synclog or list it", name)
		}
	}
	for name := range legacyLogUsers {
		if !names[name] {
			t.Errorf("%s is on the legacy list but is no longer a dependency: delete its row", name)
		} else if !users[name] {
			t.Errorf("%s is on the legacy list but imports no log package any more: delete its row", name)
		}
	}
	t.Logf("%d first-party dependencies derived; %d legacy log users listed; the rest (%d) are checked", len(deps), len(users), len(deps)-len(users))
}

// legacyErrorLogCalls is the number of log calls in the legacy-listed dependencies that carry an error-typed operand TODAY (a
// NAMED LIMIT, reported by the test with file:line; the sweep is its own work item). It only shrinks: a new such call fails.
// The 47th is internal/platform/secrets/hidden.go (Hidden.LogValue): the walker counts any String() call in a log call's
// arguments, and h.String() there returns only the redaction marker, never error text (CHAOS-8716).
const legacyErrorLogCalls = 47

// logCallsWithErrors returns the log calls of one package (a callee in log/slog or log, a method of their types) that take an
// error-typed operand, or an Error()/String() call, anywhere in their arguments.
func logCallsWithErrors(pkg *packages.Package) []string {
	errorType := types.Universe.Lookup("error").Type().Underlying().(*types.Interface)
	inLogPackage := func(object types.Object) bool {
		return object != nil && object.Pkg() != nil && (object.Pkg().Path() == "log/slog" || object.Pkg().Path() == "log")
	}
	isErrorish := func(expr ast.Expr) bool {
		kind := pkg.TypesInfo.TypeOf(expr)
		if kind == nil {
			return false
		}
		if pointer, ok := kind.(*types.Pointer); ok {
			kind = pointer.Elem()
		}
		if _, basic := kind.Underlying().(*types.Basic); basic {
			return false
		}
		return types.Implements(kind, errorType) || types.Implements(types.NewPointer(kind), errorType)
	}
	var found []string
	for _, file := range pkg.Syntax {
		if strings.HasSuffix(pkg.Fset.Position(file.Pos()).Filename, "_test.go") {
			continue
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			var callee types.Object
			switch fun := call.Fun.(type) {
			case *ast.SelectorExpr:
				callee = pkg.TypesInfo.Uses[fun.Sel]
			case *ast.Ident:
				callee = pkg.TypesInfo.Uses[fun]
			}
			if !inLogPackage(callee) {
				return true
			}
			for _, argument := range call.Args {
				carries := false
				ast.Inspect(argument, func(inner ast.Node) bool {
					switch typed := inner.(type) {
					case *ast.CallExpr:
						if selector, ok := typed.Fun.(*ast.SelectorExpr); ok && (selector.Sel.Name == "Error" || selector.Sel.Name == "String") && len(typed.Args) == 0 {
							carries = true
						}
					case ast.Expr:
						if isErrorish(typed) {
							carries = true
						}
					}
					return !carries
				})
				if carries {
					found = append(found, pkg.Fset.Position(call.Pos()).String())
					break
				}
			}
			return true
		})
	}
	return found
}

// The VALUE check covers ALL derived first-party dependencies, with no exemption: in a non-legacy package no log-package value may
// exist at all; in a legacy package (which logs for its own purposes) a log call that takes an error is a named limit, counted and
// listed, and may not grow.
func TestNoLogPackageValueExistsInThePackageOrItsDependencies(t *testing.T) {
	root, deps := derivedDependencies(t)
	all := append([]*packages.Package{root}, deps...)
	isLog := func(kind types.Type) bool {
		kind = types.Unalias(kind)
		if pointer, ok := kind.(*types.Pointer); ok {
			kind = types.Unalias(pointer.Elem())
		}
		named, ok := kind.(*types.Named)
		return ok && named.Obj().Pkg() != nil && (named.Obj().Pkg().Path() == "log/slog" || named.Obj().Pkg().Path() == "log")
	}
	totalFiles, totalExpressions, legacyPackages := 0, 0, 0
	var limit []string
	for _, pkg := range all {
		if len(pkg.Errors) > 0 {
			t.Fatalf("%s: %v", pkg.PkgPath, pkg.Errors[0])
		}
		legacy := legacyLogUsers[strings.TrimPrefix(pkg.PkgPath, modulePrefix)]
		if legacy {
			legacyPackages++
			limit = append(limit, logCallsWithErrors(pkg)...)
		}
		for _, file := range pkg.Syntax {
			name := pkg.Fset.Position(file.Pos()).Filename
			if strings.HasSuffix(name, "_test.go") {
				continue
			}
			totalFiles++
			ast.Inspect(file, func(node ast.Node) bool {
				expr, ok := node.(ast.Expr)
				if !ok {
					return true
				}
				if recorded, ok := pkg.TypesInfo.Types[expr]; ok && recorded.IsType() {
					return true
				}
				if ident, ok := expr.(*ast.Ident); ok && pkg.TypesInfo.Defs[ident] != nil {
					return true
				}
				totalExpressions++
				if !legacy && pkg != root {
					if kind := pkg.TypesInfo.TypeOf(expr); kind != nil && isLog(kind) {
						t.Errorf("%s: a %s value in %s", pkg.Fset.Position(expr.Pos()), kind.String(), pkg.PkgPath)
					}
				}
				if pkg == root {
					if kind := pkg.TypesInfo.TypeOf(expr); kind != nil && isLog(kind) {
						t.Errorf("%s: a %s value in %s", pkg.Fset.Position(expr.Pos()), kind.String(), pkg.PkgPath)
					}
				}
				return true
			})
		}
	}
	t.Logf("%d packages checked (%d legacy): %d files, %d expressions type-checked", len(all), legacyPackages, totalFiles, totalExpressions)
	t.Logf("NAMED LIMIT: %d log call(s) with an error operand in legacy dependencies: %v", len(limit), limit)
	if legacyPackages != len(legacyLogUsers) || len(all) < 25 || totalFiles < 200 || totalExpressions < 40000 {
		t.Fatalf("%d packages (%d legacy), %d files, %d expressions: the walk reads too little", len(all), legacyPackages, totalFiles, totalExpressions)
	}
	if legacyErrorLogCalls >= 0 && len(limit) > legacyErrorLogCalls {
		t.Errorf("legacy dependencies hold %d log calls with an error operand, the limit is %d: %v", len(limit), legacyErrorLogCalls, limit)
	}
}

func TestPostSyncFailureLogsCarryNoErrorText(t *testing.T) {
	const secret = "the planted detail of ticket 7933"
	planted := fmt.Errorf("GET https://svc:%s@internal-host.example.test/orgs/42?access_token=%s: %w", secret, secret,
		&pgconn.PgError{Code: "23505", Message: "duplicate", Detail: "Key (token)=(" + secret + ")", ConstraintName: "teams_pkey"})
	var out bytes.Buffer
	service := &NativePostSyncService{logger: synclog.New(slog.New(slog.NewJSONHandler(&out, nil)))}
	plan := PostSyncPlan{OrganizationID: "20000000-0000-4000-8000-000000000002", SyncRunID: "30000000-0000-4000-8000-000000000003"}
	service.observeTeamAutoimportFailure(context.Background(), plan, planted)
	service.observeTeamRepoOwnershipDerivationFailure(context.Background(), plan, planted)
	text := out.String()
	for _, leak := range []string{"planted", "internal-host", "access_token", "orgs/42", "Key (", "teams_pkey", "constraint"} {
		if strings.Contains(text, leak) {
			t.Fatalf("the log carries %q:\n%s", leak, text)
		}
	}
	for _, want := range []string{`"class":"postgres"`, `"type":"*pgconn.PgError"`, `"code":"23505"`, PostSyncTeamAutoimportFailedMessage, PostSyncTeamRepoOwnershipDerivationFailedMessage, "30000000-0000-4000-8000-000000000003"} {
		if !strings.Contains(text, want) {
			t.Fatalf("the log lacks %q:\n%s", want, text)
		}
	}
}
