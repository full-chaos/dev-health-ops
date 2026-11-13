package home

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
)

// Every caller in the module of a home or opportunities BuildResponse* (CHAOS-6119) reaches ResolveScope: it calls home.ResolveScope
// earlier in the same function, or the callee resolves inside. The callees that resolve inside are named here; any other
// BuildResponse* name, today's or a new one, must have the call before it.
var resolvesInside = map[string]bool{
	"home.BuildResponseWithFilterEmptyReason": true,
	"opportunities.BuildResponse":             true,
}

// The callers known today: a census that matches fewer has measured nothing.
var knownCallers = map[string]int{
	"home.BuildResponse":                      2,
	"home.BuildResponseWithFilterEmptyReason": 2,
	"opportunities.BuildResponse":             3,
}

func TestEveryCallerOfAHomeOrOpportunitiesBuildResolvesTheScope(t *testing.T) {
	const module = "github.com/full-chaos/dev-health-ops/"
	pkgs := map[string]string{module + "internal/queryapi/home": "home", module + "internal/queryapi/opportunities": "opportunities"}
	root := filepath.Join("..", "..", "..")
	found := map[string]int{}
	fset := token.NewFileSet()
	for _, top := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == "testdata" || d.Name() == "node_modules" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			aliases := map[string]string{}
			for _, spec := range file.Imports {
				importPath, _ := strconv.Unquote(spec.Path.Value)
				name, ok := pkgs[importPath]
				if !ok {
					continue
				}
				if spec.Name != nil {
					aliases[spec.Name.Name] = name
				} else {
					aliases[name] = name
				}
			}
			if len(aliases) == 0 {
				return nil
			}
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				var resolveAt token.Pos
				type call struct {
					name string
					pos  token.Pos
				}
				var builds []call
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					c, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := c.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					x, ok := sel.X.(*ast.Ident)
					if !ok || aliases[x.Name] == "" {
						return true
					}
					switch name := aliases[x.Name] + "." + sel.Sel.Name; {
					case name == "home.ResolveScope":
						if resolveAt == 0 || c.Pos() < resolveAt {
							resolveAt = c.Pos()
						}
					case strings.HasPrefix(sel.Sel.Name, "BuildResponse"):
						builds = append(builds, call{name, c.Pos()})
					}
					return true
				})
				for _, b := range builds {
					found[b.name]++
					if resolvesInside[b.name] {
						continue
					}
					if resolveAt == 0 || resolveAt > b.pos {
						t.Errorf("%s: %s calls %s without home.ResolveScope before it", path, fn.Name.Name, b.name)
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	total := 0
	var names []string
	for name, n := range found {
		total += n
		names = append(names, name)
	}
	sort.Strings(names)
	if total == 0 {
		t.Fatal("the census matched no caller: it measured nothing")
	}
	for name, floor := range knownCallers {
		if found[name] < floor {
			t.Errorf("%s: %d callers found, want at least %d known today (found: %v)", name, found[name], floor, names)
		}
	}
}
