package latestrow_test

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

	golatestrow "github.com/full-chaos/dev-health-go/latestrow"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/latestrow"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// The ops census of the latest-row key is a static guard over the non-test Go
// files of the ops module tree (nested modules, vendor, testdata and dot
// directories excluded). It checks three things: the ops key and renders are
// the dev-health-go ones; no string literal names an insert-order virtual
// column (_part, _part_offset, ...) as a whole identifier; and the selectors
// spelled <latestrow import name>.<func>, per selector and per enclosing
// declaration, match the named list below. It reads spelling, not Go
// bindings: a shadowed name, a dot import, an unqualified call inside this
// package or a function value called more than once is not counted.

var latestrowImports = map[string]bool{
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/latestrow": true,
	"github.com/full-chaos/dev-health-go/latestrow":                              true,
}

// latestrowSites are the calls to latestrow in non-test files, as
// "file:declaration:function" -> count: 30 calls in 10 declarations, plus the
// forwards in latestrow.go itself.
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
	"internal/queryapi/testopsrisk/testopsrisk.go:quadrantQuery:ArgMaxKeepNullBy":                     1,
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

func TestLatestrowCallSitesAreNamed(t *testing.T) {
	found := map[string]int{}
	for _, f := range moduleFiles(t) {
		local := map[string]bool{}
		for _, imp := range f.file.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil || !latestrowImports[path] {
				continue
			}
			name := filepath.Base(path)
			if imp.Name != nil {
				name = imp.Name.Name
			}
			local[name] = true
		}
		if len(local) == 0 {
			continue
		}
		for _, decl := range f.file.Decls {
			ast.Inspect(decl, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if id, ok := sel.X.(*ast.Ident); ok && local[id.Name] {
					found[f.rel+":"+declName(decl, sel)+":"+sel.Sel.Name]++
				}
				return true
			})
		}
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
