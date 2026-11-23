package latestrow_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"

	golatestrow "github.com/full-chaos/dev-health-go/latestrow"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/latestrow"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// The ops census of the latest-row key checks three things: the ops key and
// renders are the dev-health-go ones; no string literal names an insert-order
// virtual column (_part, _part_offset, ...) as a whole identifier (a spelling
// scan of the non-test files of the module tree, nested modules, vendor,
// testdata and dot directories excluded); and the calls to latestrow funcs and
// the uses of its OrderKey const, bound by go/types (latestrowCalls), match
// the named list below.

var latestrowImports = map[string]bool{
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/latestrow": true,
	"github.com/full-chaos/dev-health-go/latestrow":                              true,
}

// latestrowSites are the calls to latestrow in non-test files, as
// "file:declaration:name" -> count: 30 calls in 10 declarations, plus the
// 3 forwards and the OrderKey const in latestrow.go itself.
var latestrowSites = map[string]int{
	"internal/jobs/investment/chquery/entities.go:Reader.FetchExistingInvestmentKeys:ArgMax":          5,
	"internal/jobs/investment/chquery/entities.go:Reader.FetchExistingInvestmentKeys:ArgMaxKeepNull":  1,
	"internal/jobs/metrics/remaining/membership_native_clickhouse.go:latestDistributionsQuery:ArgMax": 3,
	"internal/queryapi/aianalytics/loaders_b.go:queryRepoNames:ArgMaxKeepNullBy":                      1,
	"internal/queryapi/aianalytics/names.go:loadPRTitles:ArgMaxKeepNullBy":                            1,
	"internal/queryapi/aianalytics/scope.go:loadRepoNames:ArgMaxKeepNullBy":                           1,
	"internal/queryapi/aianalytics/scope.go:loadTeams:ArgMaxKeepNullBy":                               1,
	"internal/queryapi/analytics/investment.go:LatestWorkUnitInvestmentsSource:ArgMax":                12,
	"internal/queryapi/analytics/investment.go:LatestWorkUnitInvestmentsSource:ArgMaxKeepNull":        4,
	"internal/queryapi/testopsrisk/testopsrisk.go:quadrantStatement:ArgMaxKeepNullBy":                 1,
	"internal/storage/clickhouse/latestrow/latestrow.go:ArgMax:ArgMax":                                1,
	"internal/storage/clickhouse/latestrow/latestrow.go:ArgMaxKeepNull:ArgMaxKeepNull":                1,
	"internal/storage/clickhouse/latestrow/latestrow.go:ArgMaxKeepNullBy:ArgMaxKeepNullBy":            1,
	"internal/storage/clickhouse/latestrow/latestrow.go:OrderKey:OrderKey":                            1,
}

func TestOrderKeyIsTheDevHealthGoKey(t *testing.T) {
	if latestrow.OrderKey != golatestrow.OrderKey {
		t.Fatalf("ops OrderKey = %q, want the dev-health-go key %q", latestrow.OrderKey, golatestrow.OrderKey)
	}
	cases := []struct{ name, got, want string }{
		{"ArgMax", latestrow.ArgMax("from_ts"), golatestrow.ArgMax("from_ts")},
		{"ArgMaxKeepNull", latestrow.ArgMaxKeepNull("repo_id"), golatestrow.ArgMaxKeepNull("repo_id")},
		{"ArgMaxKeepNullBy", latestrow.ArgMaxKeepNullBy("team_id", "computed_at"), golatestrow.ArgMaxKeepNullBy("team_id", "computed_at")},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %q, want the dev-health-go render %q", c.name, c.got, c.want)
		}
	}
}

type moduleFile struct {
	rel  string
	file *ast.File
}

func moduleFiles(t *testing.T) []moduleFile {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	out, err := collectModuleFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func collectModuleFiles(root string) ([]moduleFile, error) {
	var out []moduleFile
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if name := entry.Name(); path != root && (strings.HasPrefix(name, ".") || name == "vendor" || name == "node_modules" || name == "testdata") {
				return filepath.SkipDir
			}
			if path != root {
				if _, statErr := os.Stat(filepath.Join(path, "go.mod")); statErr == nil {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		out = append(out, moduleFile{rel: filepath.ToSlash(rel), file: file})
		return nil
	})
	return out, err
}

func TestModuleFilesStopAtNestedModules(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		"go.mod":            "module example.com/root\n",
		"own/own.go":        "package own\n",
		"nested/go.mod":     "module example.com/nested\n",
		"nested/nested.go":  "package nested\n",
		"nested/sub/sub.go": "package sub\n",
	}
	for name, body := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	got, err := collectModuleFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	var rels []string
	for _, f := range got {
		rels = append(rels, f.rel)
	}
	if strings.Join(rels, " ") != "own/own.go" {
		t.Fatalf("module files = %v, want only own/own.go (a nested go.mod is another module)", rels)
	}
}

func TestDeclNameUnwrapsParenReceivers(t *testing.T) {
	src := "package p\ntype Reader struct{}\nfunc (r *Reader) A() {}\nfunc (r *(Reader)) B() {}\nfunc (r (*Reader)) C() {}\nfunc (r *((Reader))) D() {}\n"
	file, err := parser.ParseFile(token.NewFileSet(), "p.go", src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok {
			got = append(got, declName(fn, fn))
		}
	}
	if want := "Reader.A Reader.B Reader.C Reader.D"; strings.Join(got, " ") != want {
		t.Fatalf("declName = %v, want %s", got, want)
	}
}

func declName(decl ast.Decl, node ast.Node) string {
	switch d := decl.(type) {
	case *ast.FuncDecl:
		if d.Recv != nil && len(d.Recv.List) == 1 {
			return exprName(d.Recv.List[0].Type) + "." + d.Name.Name
		}
		return d.Name.Name
	case *ast.GenDecl:
		for _, spec := range d.Specs {
			if vs, ok := spec.(*ast.ValueSpec); ok && vs.Pos() <= node.Pos() && node.End() <= vs.End() {
				names := make([]string, len(vs.Names))
				for i, name := range vs.Names {
					names[i] = name.Name
				}
				return strings.Join(names, ",")
			}
		}
	}
	return "?"
}

func exprName(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.ParenExpr:
		return exprName(x.X)
	case *ast.StarExpr:
		return exprName(x.X)
	case *ast.Ident:
		return x.Name
	case *ast.IndexExpr:
		return exprName(x.X)
	case *ast.IndexListExpr:
		return exprName(x.X)
	}
	return "?"
}

func TestNoSiteBuildsItsOwnKey(t *testing.T) {
	insertOrderColumn := regexp.MustCompile(`(?i)(?:^|[^a-z0-9_])(_part|_part_offset|_part_index|_part_starting_offset|_part_uuid|_block_number|_block_offset)(?:$|[^a-z0-9_])`)
	for _, f := range moduleFiles(t) {
		for _, decl := range f.file.Decls {
			ast.Inspect(decl, func(n ast.Node) bool {
				lit, ok := n.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				value, err := strconv.Unquote(lit.Value)
				if err != nil {
					return true
				}
				if m := insertOrderColumn.FindStringSubmatch(value); m != nil {
					t.Errorf("%s:%s reads the insert-order column %s: build the latest-row order through latestrow, not a key of its own", f.rel, declName(decl, lit), m[1])
				}
				return true
			})
		}
	}
}

// latestrowCalls binds the census to go/types. It type-checks the packages of
// the module at dir that import a latestrow package (and the latestrow package
// itself), in the default build and in the integration build, and counts the
// ast.CallExpr whose callee resolves (types.Info.Uses) to a func of a package
// in paths, per "file:declaration:function". Any other use of such a func (a
// store into a variable, an argument, a method value) is returned as
// uncounted: a call through a stored func value is not resolved, so the store
// itself must fail. A const of such a package (OrderKey) is counted as a
// reference, not a call, under the same key form. Not checked: _test.go
// files, build tags other than the default and integration, nested modules
// (Go's ./... excludes them, as it does testdata and dot directories),
// reflection.
func latestrowCalls(dir string, paths map[string]bool) (found map[string]int, uncounted []string, err error) {
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		return nil, nil, err
	}
	found = map[string]int{}
	seen := map[string]bool{}
	for _, flags := range [][]string{nil, {"-tags=integration"}} {
		env := append(os.Environ(), "GOWORK=off")
		listed, err := packages.Load(&packages.Config{
			Mode: packages.NeedName | packages.NeedImports, Dir: dir, Env: env, BuildFlags: flags,
		}, "./...")
		if err != nil {
			return nil, nil, err
		}
		var patterns []string
		for _, pkg := range listed {
			use := paths[pkg.PkgPath]
			for imp := range pkg.Imports {
				use = use || paths[imp]
			}
			if use {
				patterns = append(patterns, pkg.PkgPath)
			}
		}
		if len(patterns) == 0 {
			continue
		}
		typed, err := packages.Load(&packages.Config{
			Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
			Dir:  dir, Env: env, BuildFlags: flags,
		}, patterns...)
		if err != nil {
			return nil, nil, err
		}
		for _, pkg := range typed {
			if len(pkg.Errors) > 0 {
				return nil, nil, fmt.Errorf("%s: %v", pkg.PkgPath, pkg.Errors[0])
			}
			for _, file := range pkg.Syntax {
				pos := pkg.Fset.Position(file.Pos())
				rel, err := filepath.Rel(dir, pos.Filename)
				if err != nil {
					return nil, nil, err
				}
				rel = filepath.ToSlash(rel)
				resolve := func(id *ast.Ident) types.Object {
					obj := pkg.TypesInfo.Uses[id]
					switch obj.(type) {
					case *types.Func, *types.Const:
						if obj.Pkg() != nil && paths[obj.Pkg().Path()] {
							return obj
						}
					}
					return nil
				}
				counted := map[*ast.Ident]bool{}
				for _, decl := range file.Decls {
					ast.Inspect(decl, func(n ast.Node) bool {
						call, ok := n.(*ast.CallExpr)
						if !ok {
							return true
						}
						var id *ast.Ident
						switch f := ast.Unparen(call.Fun).(type) {
						case *ast.Ident:
							id = f
						case *ast.SelectorExpr:
							id = f.Sel
						}
						if id == nil {
							return true
						}
						if fn, ok := resolve(id).(*types.Func); ok {
							counted[id] = true
							at := fmt.Sprintf("%s:%d", rel, pkg.Fset.Position(id.Pos()).Offset)
							if !seen[at] {
								seen[at] = true
								found[rel+":"+declName(decl, call)+":"+fn.Name()]++
							}
						}
						return true
					})
					ast.Inspect(decl, func(n ast.Node) bool {
						id, ok := n.(*ast.Ident)
						if !ok || counted[id] {
							return true
						}
						if fn := resolve(id); fn != nil {
							at := fmt.Sprintf("%s:%d", rel, pkg.Fset.Position(id.Pos()).Offset)
							if _, isConst := fn.(*types.Const); isConst {
								if !seen[at] {
									seen[at] = true
									found[rel+":"+declName(decl, id)+":"+fn.Name()]++
								}
								return true
							}
							if !seen[at] {
								seen[at] = true
								uncounted = append(uncounted, fmt.Sprintf("%s:%d:%s", rel, pkg.Fset.Position(id.Pos()).Line, fn.Name()))
							}
						}
						return true
					})
				}
			}
		}
	}
	sort.Strings(uncounted)
	return found, uncounted, nil
}

func TestLatestrowCallSitesAreNamed(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	found, uncounted, err := latestrowCalls(root, latestrowImports)
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range uncounted {
		t.Errorf("latestrow func used but not called at %s: call it where it is named, do not store or pass it", u)
	}
	keys := map[string]bool{}
	for k := range found {
		keys[k] = true
	}
	for k := range latestrowSites {
		keys[k] = true
	}
	var sorted []string
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	for _, k := range sorted {
		if found[k] != latestrowSites[k] {
			t.Errorf("latestrow site %s: %d calls, census names %d", k, found[k], latestrowSites[k])
		}
	}
}
