package main

import (
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

const modulePath = "github.com/full-chaos/dev-health-ops"

// migrationSurfaces are what a long-running process must never reach: the
// River migration command, the river library's own migrator, and the
// pinned-bundle apply. internal/storage/river defines ApplyPinnedMigrations
// and reads schema versions through the library migrator (CheckSchema, which
// the services call), so it may import the migrator; inside it, only
// ApplyPinnedMigrations' own body may call itself or a schema-changing
// migrator method.
const (
	migrationCommandPackage = modulePath + "/internal/rivermigrate"
	riverLibraryMigrator    = "github.com/riverqueue/river/rivermigrate"
	riverStorePackage       = modulePath + "/internal/storage/river"
	applyCall               = "ApplyPinnedMigrations"
)

// schemaChangingMigratorCalls are the river library migrator's methods that
// change the schema.
var schemaChangingMigratorCalls = []string{"Migrate", "MigrateTx"}

type serviceVerb struct {
	path    string
	pkgPath string
}

// serviceVerbs walks the command tree and returns every cli.Service with
// the Go package its Run function is defined in.
func serviceVerbs(t *testing.T, tree []cli.Command, parent string) []serviceVerb {
	t.Helper()
	var found []serviceVerb
	for _, command := range tree {
		path := strings.TrimSpace(parent + " " + command.Name)
		if command.Kind == cli.Service {
			function := runtime.FuncForPC(reflect.ValueOf(command.Run).Pointer())
			if function == nil {
				t.Fatalf("service %q: cannot resolve its Run function", path)
			}
			found = append(found, serviceVerb{path: path, pkgPath: packageOfFunction(function.Name())})
		}
		found = append(found, serviceVerbs(t, command.Children, path)...)
	}
	return found
}

// packageOfFunction turns "a/b/pkg.Command.func1" into "a/b/pkg".
func packageOfFunction(name string) string {
	slash := strings.LastIndex(name, "/")
	dot := strings.Index(name[slash+1:], ".")
	if dot < 0 {
		return name
	}
	return name[:slash+1+dot]
}

type listedPackage struct {
	ImportPath string
	Dir        string
	GoFiles    []string
	Imports    []string
	Module     *struct{ Path string }
}

// TestServiceVerbsCannotMigrate is the dho form of the rule that a
// long-running process can never apply a schema migration: River migrations
// run only as the one-shot `dho migrate river` verb (the chart hook, Compose
// go-river-migrate), never as a side effect of a service starting. For
// every Service in dho's command tree, the package that defines its Run and
// that package's whole dependency closure must not contain the migration
// command, import the river library's migrator, or call the pinned-bundle
// apply. The rule is checked on the command tree itself, so a service folded
// into dho later is covered with no list to update.
func TestServiceVerbsCannotMigrate(t *testing.T) {
	services := serviceVerbs(t, commands(), "")
	names := make([]string, 0, len(services))
	for _, service := range services {
		names = append(names, service.path)
	}
	for _, want := range []string{"api", "reconciler", "scheduler", "stream-runner"} {
		if !containsString(names, want) {
			t.Fatalf("service verbs = %v; %q is missing, so the walk no longer sees the tree", names, want)
		}
	}
	for _, service := range services {
		if !strings.HasPrefix(service.pkgPath, modulePath+"/") {
			t.Fatalf("service %q resolved to package %q, outside this module", service.path, service.pkgPath)
		}
		for _, pkg := range goListDeps(t, service.pkgPath) {
			if pkg.ImportPath == migrationCommandPackage {
				t.Errorf("service %q links the migration command %s", service.path, migrationCommandPackage)
			}
			if pkg.Module == nil || pkg.Module.Path != modulePath {
				continue
			}
			forbidden := []string{applyCall}
			if pkg.ImportPath == riverStorePackage {
				forbidden = append(forbidden, schemaChangingMigratorCalls...)
			} else if containsString(pkg.Imports, riverLibraryMigrator) {
				t.Errorf("service %q: %s imports the river library migrator %s", service.path, pkg.ImportPath, riverLibraryMigrator)
			}
			for _, file := range pkg.GoFiles {
				if name := callsOutside(t, filepath.Join(pkg.Dir, file), forbidden, applyCall); name != "" {
					t.Errorf("service %q: %s/%s calls %s", service.path, pkg.ImportPath, file, name)
				}
			}
		}
	}
}

func goListDeps(t *testing.T, pkg string) []listedPackage {
	t.Helper()
	command := exec.Command("go", "list", "-deps", "-json=ImportPath,Dir,GoFiles,Imports,Module", pkg)
	output, err := command.Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			t.Fatalf("go list -deps %s: %v\n%s", pkg, err, exit.Stderr)
		}
		t.Fatalf("go list -deps %s: %v", pkg, err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(output)))
	var packages []listedPackage
	for {
		var listed listedPackage
		if err := decoder.Decode(&listed); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		packages = append(packages, listed)
	}
	if len(packages) == 0 {
		t.Fatalf("go list -deps %s listed nothing", pkg)
	}
	return packages
}

// callsOutside returns the first of names that the Go file calls as a
// function or method (a call expression, not a comment or a string that
// mentions it), skipping the body of the top-level function named exempt. It
// returns "" when there is none.
func callsOutside(t *testing.T, path string, names []string, exempt string) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := ""
	ast.Inspect(file, func(node ast.Node) bool {
		if found != "" {
			return false
		}
		if decl, ok := node.(*ast.FuncDecl); ok && decl.Recv == nil && decl.Name.Name == exempt {
			return false
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		var called string
		switch function := call.Fun.(type) {
		case *ast.Ident:
			called = function.Name
		case *ast.SelectorExpr:
			called = function.Sel.Name
		}
		if containsString(names, called) {
			found = called
		}
		return found == ""
	})
	return found
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
