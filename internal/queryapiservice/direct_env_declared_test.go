package queryapiservice

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// The routes under internal/queryapi read a few settings straight from the process
// environment (os.Getenv), not through the declared-settings reader the server package
// uses. TestEverySettingTheQueryRoutesReadIsDeclared scans the server package only, and
// queryapi/server's TestDirectEnvironmentReads pins the LIST of those reads but not that
// the registry still declares them. This test closes that: every name a non-test file under
// internal/queryapi reads with os.Getenv/os.LookupEnv, resolved to a string (a literal, a
// package constant, or the elements of the package variable a `range` loop iterates), must
// be a setting the registry declares for dho query-api, so --help, the declared-only
// environment contract and the registry stay the one description of what the service reads.
func TestEveryDirectEnvironmentReadUnderQueryAPIIsDeclared(t *testing.T) {
	root := filepath.Join("..", "queryapi")
	byDir := map[string][]*ast.File{}
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		dir := filepath.Dir(path)
		byDir[dir] = append(byDir[dir], file)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	names := map[string]string{} // name -> first site
	unresolved := []string{}
	for dir, files := range byDir {
		constants := map[string]string{}
		variables := map[string][]string{} // package variable -> every string literal in its initializer
		for _, file := range files {
			for _, declaration := range file.Decls {
				generic, ok := declaration.(*ast.GenDecl)
				if !ok || (generic.Tok != token.CONST && generic.Tok != token.VAR) {
					continue
				}
				for _, spec := range generic.Specs {
					value := spec.(*ast.ValueSpec)
					for i, name := range value.Names {
						if i >= len(value.Values) {
							continue
						}
						if literal, ok := value.Values[i].(*ast.BasicLit); ok && literal.Kind == token.STRING && generic.Tok == token.CONST {
							constants[name.Name], _ = strconv.Unquote(literal.Value)
						}
						ast.Inspect(value.Values[i], func(node ast.Node) bool {
							if literal, ok := node.(*ast.BasicLit); ok && literal.Kind == token.STRING {
								text, _ := strconv.Unquote(literal.Value)
								variables[name.Name] = append(variables[name.Name], text)
							}
							return true
						})
					}
				}
			}
		}
		for _, file := range files {
			for _, declaration := range file.Decls {
				function, ok := declaration.(*ast.FuncDecl)
				if !ok || function.Body == nil {
					continue
				}
				// range variable -> the package variable it iterates
				ranged := map[string]string{}
				ast.Inspect(function.Body, func(node ast.Node) bool {
					loop, ok := node.(*ast.RangeStmt)
					if !ok {
						return true
					}
					var source string
					switch expression := loop.X.(type) {
					case *ast.Ident:
						source = expression.Name
					case *ast.IndexExpr:
						if base, ok := expression.X.(*ast.Ident); ok {
							source = base.Name
						}
					}
					if value, ok := loop.Value.(*ast.Ident); ok && source != "" {
						ranged[value.Name] = source
					}
					return true
				})
				ast.Inspect(function.Body, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok || len(call.Args) != 1 {
						return true
					}
					selector, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					pkg, ok := selector.X.(*ast.Ident)
					if !ok || pkg.Name != "os" || (selector.Sel.Name != "Getenv" && selector.Sel.Name != "LookupEnv") {
						return true
					}
					site := filepath.ToSlash(filepath.Join(filepath.Base(dir), filepath.Base(fset.Position(call.Pos()).Filename)))
					switch argument := call.Args[0].(type) {
					case *ast.BasicLit:
						text, _ := strconv.Unquote(argument.Value)
						names[text] = site
					case *ast.Ident:
						if text, known := constants[argument.Name]; known {
							names[text] = site
						} else if source, isRange := ranged[argument.Name]; isRange && len(variables[source]) > 0 {
							for _, text := range variables[source] {
								names[text] = site
							}
						} else {
							unresolved = append(unresolved, site+": "+argument.Name)
						}
					default:
						unresolved = append(unresolved, site+": expression")
					}
					return true
				})
			}
		}
	}
	sort.Strings(unresolved)
	if len(unresolved) > 0 {
		t.Fatalf("direct environment reads whose name the test cannot resolve to a string: %v -- read it through a constant, or extend the test", unresolved)
	}
	if len(names) < 7 {
		t.Fatalf("found only %d direct environment reads under internal/queryapi (%v): the extraction no longer sees them", len(names), names)
	}

	// Every name is a valid one for the placeholder lookup: only these names are present.
	lookup := func(name string) (string, bool) {
		switch name {
		case "QUERY_API_ADDR":
			return "127.0.0.1:1", true
		case "QUERY_API_INTERNAL_ADDR":
			return "127.0.0.1:2", true
		case "CLICKHOUSE_URI":
			return "clickhouse://ch:ch@localhost:9000/default", true
		case "GO_API_REGISTRY_POSTGRES_URI", "POSTGRES_URI", "DATABASE_URI":
			return "postgresql://u:p@localhost:5432/d", true
		}
		if _, read := names[name]; read {
			return "1", true
		}
		return "", false
	}
	cfg, err := config.Load(config.Spec{Service: config.QueryAPIServiceName, LookupEnv: lookup})
	if err != nil {
		t.Fatalf("config.Load with every directly-read setting present: %v", err)
	}
	var undeclared []string
	for name, site := range names {
		if _, answered := cfg.Setting(name); !answered {
			undeclared = append(undeclared, name+" ("+site+")")
		}
	}
	sort.Strings(undeclared)
	if len(undeclared) > 0 {
		t.Fatalf("code under internal/queryapi reads settings from the environment that the registry does not declare for %s: %v", config.QueryAPIServiceName, undeclared)
	}
}
