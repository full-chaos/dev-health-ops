package queryapiservice

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// Every setting the query routes read is declared in the service's option registry.
// The routes read settings through the declared-settings reader (Config.Setting),
// which answers only for a declared name: a route that reads an undeclared one would
// see it as unset and stay unmounted, silently. So the names the server package reads
// (getenv("X"), or a constant naming X) are extracted from its source, and each must
// be answered by Config.Setting for this service.
func TestEverySettingTheQueryRoutesReadIsDeclared(t *testing.T) {
	dir := filepath.Join("..", "queryapi", "server")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, entry.Name()), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
	}
	constants := map[string]string{}
	for _, file := range files {
		for _, declaration := range file.Decls {
			generic, ok := declaration.(*ast.GenDecl)
			if !ok || generic.Tok != token.CONST {
				continue
			}
			for _, spec := range generic.Specs {
				value := spec.(*ast.ValueSpec)
				for i, name := range value.Names {
					if i < len(value.Values) {
						if literal, ok := value.Values[i].(*ast.BasicLit); ok && literal.Kind == token.STRING {
							text, _ := strconv.Unquote(literal.Value)
							constants[name.Name] = text
						}
					}
				}
			}
		}
	}
	names := map[string]bool{}
	for _, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || len(call.Args) != 1 {
				return true
			}
			callee, ok := call.Fun.(*ast.Ident)
			if !ok || callee.Name != "getenv" {
				return true
			}
			switch argument := call.Args[0].(type) {
			case *ast.BasicLit:
				if text, err := strconv.Unquote(argument.Value); err == nil {
					names[text] = true
				}
			case *ast.Ident:
				if text, known := constants[argument.Name]; known {
					names[text] = true
				} else {
					t.Errorf("getenv(%s): a name the test cannot resolve to a string constant; declare it in the registry and read it through a constant", argument.Name)
				}
			default:
				t.Errorf("getenv is called with an expression the test cannot read (%T): read settings by literal or constant name", argument)
			}
			return true
		})
	}
	if len(names) < 30 {
		t.Fatalf("found only %d settings read through getenv: the extraction no longer covers the routes", len(names))
	}
	// Only the names the routes read are present (validated ones would otherwise trip
	// on a placeholder value); the two listener addresses hold valid ones.
	lookup := func(name string) (string, bool) {
		switch name {
		case "QUERY_API_ADDR":
			return "127.0.0.1:1", true
		case "QUERY_API_INTERNAL_ADDR":
			return "127.0.0.1:2", true
		case "CLICKHOUSE_URI":
			return "clickhouse://ch:ch@localhost:9000/default", true
		case "RIVER_DATABASE_SCHEMA":
			return "river", true
		case "GO_API_REGISTRY_POSTGRES_URI", "POSTGRES_URI", "DATABASE_URI":
			return "postgresql://u:p@localhost:5432/d", true
		}
		if names[name] {
			return "1", true
		}
		return "", false
	}
	cfg, err := config.Load(config.Spec{Service: config.QueryAPIServiceName, LookupEnv: lookup})
	if err != nil {
		t.Fatalf("config.Load with every setting present: %v", err)
	}
	var undeclared []string
	for name := range names {
		if _, answered := cfg.Setting(name); !answered {
			undeclared = append(undeclared, name)
		}
	}
	sort.Strings(undeclared)
	if len(undeclared) > 0 {
		t.Fatalf("the query routes read settings the registry does not declare for %s (they would read as unset): %v", config.QueryAPIServiceName, undeclared)
	}
}
