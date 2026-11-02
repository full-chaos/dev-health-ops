package queuecreds_test

import (
	"crypto/sha256"
	"encoding/hex"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var censusDirs = []string{"internal/joboutbox", "internal/syncreconciler", "internal/joboperator", "internal/jobruntime", "internal/workersctl"}

type srcFile struct {
	path string
	file *ast.File
	src  []byte
}

// formatCensus parses the Go files of the census directories under root and
// returns one finding per queue-row value formatted into an error or log
// output without a guard, and the classified outputs it met.
func formatCensus(t *testing.T, root string, dirs []string) ([]string, map[string]bool) {
	t.Helper()
	fset := token.NewFileSet()
	var findings []string
	seen := map[string]bool{}
	for _, dir := range dirs {
		packages := map[string][]srcFile{}
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() && entry.Name() == "testdata" {
				return filepath.SkipDir
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			source, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			file, err := parser.ParseFile(fset, path, source, 0)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			pkgDir := filepath.ToSlash(filepath.Dir(rel))
			packages[pkgDir] = append(packages[pkgDir], srcFile{rel, file, source})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, pkg := range packages {
			findings = append(findings, unguardedOutputs(fset, pkg, seen)...)
		}
	}
	sort.Strings(findings)
	return findings, seen
}

// sinkCall names the calls that put a value into an error, a log line, a
// printed line, a span attribute or a metric label.
var sinkCall = regexp.MustCompile(`^(fmt\.(Errorf|Sprintf|Sprint|Sprintln|Fprintf|Fprint|Fprintln|Printf|Print|Println)|log\.(Print|Printf|Println|Fatal|Fatalf|Fatalln|Panic|Panicf|Panicln)|errors\.New|slog\.[A-Z][A-Za-z]*|slog\.Default\(\)\.[A-Za-z]+|.*(?:ogger|slog)(?:\(\))?\.(Debug|Info|Warn|Error|With|Log)[A-Za-z]*|.*\.SetAttributes|attribute\.[A-Za-z]+|.*\.WithLabelValues|.*\.AddEvent|metric\.With[A-Za-z]+)$`)

var sinkLiteral = regexp.MustCompile(`^(prometheus\.Labels|attribute\.Set)$`)

// flow is the data-flow state of one package: the struct types that hold a
// queue-row field and the helpers that return one.
type flow struct {
	fset    *token.FileSet
	structs map[string]bool
	helpers map[string]bool
}

func newFlow(fset *token.FileSet, pkg []srcFile) *flow {
	f := &flow{fset: fset, structs: map[string]bool{}, helpers: map[string]bool{}}
	for _, sf := range pkg {
		for _, decl := range sf.file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range gen.Specs {
				ts, ok := spec.(*ast.TypeSpec)
				if !ok {
					continue
				}
				st, ok := ts.Type.(*ast.StructType)
				if !ok {
					continue
				}
				for _, field := range st.Fields.List {
					for _, name := range field.Names {
						if rowKeyName.MatchString(name.Name) {
							f.structs[ts.Name.Name] = true
						}
					}
				}
			}
		}
	}
	for changed := true; changed; {
		changed = false
		for _, sf := range pkg {
			for _, decl := range sf.file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil || f.helpers[fn.Name.Name] {
					continue
				}
				scope := f.scope(fn)
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					if _, lit := node.(*ast.FuncLit); lit {
						return false
					}
					if ret, ok := node.(*ast.ReturnStmt); ok {
						for _, result := range ret.Results {
							if scope.valueTainted(result) && !f.helpers[fn.Name.Name] {
								f.helpers[fn.Name.Name] = true
								changed = true
							}
						}
					}
					return true
				})
			}
		}
	}
	return f
}

// scope is the taint state of one function: the locals that hold a queue-row
// value, the struct variables that hold one as a field, and the names that
// were assigned from one another.
type scope struct {
	flow    *flow
	locals  map[string]bool
	structs map[string]bool
	link    map[string]string
}

func (s *scope) find(name string) string {
	for {
		parent, ok := s.link[name]
		if !ok || parent == name {
			return name
		}
		name = parent
	}
}

func (s *scope) union(a, b string) {
	ra, rb := s.find(a), s.find(b)
	if ra != rb {
		s.link[ra] = rb
	}
}

func typeName(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.StarExpr:
		return typeName(typed.X)
	case *ast.UnaryExpr:
		return typeName(typed.X)
	case *ast.CompositeLit:
		return typeName(typed.Type)
	}
	return ""
}

func (f *flow) scope(fn *ast.FuncDecl) *scope {
	s := &scope{flow: f, locals: map[string]bool{}, structs: map[string]bool{}, link: map[string]string{}}
	fields := []*ast.FieldList{fn.Type.Params, fn.Recv}
	for _, list := range fields {
		if list == nil {
			continue
		}
		for _, field := range list.List {
			if f.structs[typeName(field.Type)] {
				for _, name := range field.Names {
					s.structs[name.Name] = true
				}
			}
		}
	}
	for changed := true; changed; {
		changed = false
		mark := func(name string, set map[string]bool) {
			if name != "_" && name != "err" && name != "ok" && !set[name] {
				set[name] = true
				changed = true
			}
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.AssignStmt:
				for i, lhs := range typed.Lhs {
					id, ok := lhs.(*ast.Ident)
					if !ok {
						continue
					}
					rhs := typed.Rhs[0]
					if len(typed.Rhs) == len(typed.Lhs) {
						rhs = typed.Rhs[i]
					}
					for _, name := range keys(rhs) {
						s.union(id.Name, name)
					}
					if f.structs[typeName(rhs)] {
						mark(id.Name, s.structs)
					}
					if s.tainted(rhs) {
						mark(id.Name, s.locals)
					}
				}
			case *ast.ValueSpec:
				for i, id := range typed.Names {
					if f.structs[typeName(typed.Type)] {
						mark(id.Name, s.structs)
					}
					if i < len(typed.Values) {
						for _, name := range keys(typed.Values[i]) {
							s.union(id.Name, name)
						}
						if f.structs[typeName(typed.Values[i])] {
							mark(id.Name, s.structs)
						}
						if s.tainted(typed.Values[i]) {
							mark(id.Name, s.locals)
						}
					}
				}
			case *ast.RangeStmt:
				if s.tainted(typed.X) {
					for _, side := range []ast.Expr{typed.Key, typed.Value} {
						if id, ok := side.(*ast.Ident); ok {
							mark(id.Name, s.locals)
						}
					}
				}
			}
			return true
		})
	}
	return s
}

// keys names the identifiers and selector paths an expression reads.
func keys(expr ast.Expr) []string {
	var out []string
	ast.Inspect(expr, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.SelectorExpr:
			out = append(out, selectorPath(typed))
			return false
		case *ast.Ident:
			out = append(out, typed.Name)
		}
		return true
	})
	return out
}

func selectorPath(sel *ast.SelectorExpr) string {
	switch base := sel.X.(type) {
	case *ast.Ident:
		return base.Name + "." + sel.Sel.Name
	case *ast.SelectorExpr:
		return selectorPath(base) + "." + sel.Sel.Name
	}
	return sel.Sel.Name
}

// tainted reports whether an expression carries a queue-row value: a field
// or variable with a row-key name, a local assigned from one, a struct that
// holds one, or the result of a helper that returns one.
func (s *scope) tainted(expr ast.Expr) bool {
	switch typed := expr.(type) {
	case *ast.Ident:
		return rowKeyName.MatchString(typed.Name) || s.locals[typed.Name] || s.structs[typed.Name]
	case *ast.SelectorExpr:
		if rowKeyName.MatchString(typed.Sel.Name) {
			return true
		}
		return false
	case *ast.ParenExpr:
		return s.tainted(typed.X)
	case *ast.StarExpr:
		return s.tainted(typed.X)
	case *ast.UnaryExpr:
		return s.tainted(typed.X)
	case *ast.IndexExpr:
		return s.tainted(typed.X)
	case *ast.BinaryExpr:
		return s.tainted(typed.X) || s.tainted(typed.Y)
	case *ast.CallExpr:
		switch fun := typed.Fun.(type) {
		case *ast.Ident:
			if s.flow.helpers[fun.Name] {
				return true
			}
		case *ast.SelectorExpr:
			if s.flow.helpers[fun.Sel.Name] {
				return true
			}
			if s.tainted(fun.X) && !s.structsOnly(fun.X) {
				return true
			}
		}
		if id, ok := typed.Fun.(*ast.Ident); ok && (id.Name == "string" || id.Name == "append") {
			for _, arg := range typed.Args {
				if s.tainted(arg) {
					return true
				}
			}
		}
		if sel, ok := typed.Fun.(*ast.SelectorExpr); ok {
			if base, ok := sel.X.(*ast.Ident); ok && (base.Name == "strings" || (base.Name == "fmt" && strings.HasPrefix(sel.Sel.Name, "Sprint"))) {
				for _, arg := range typed.Args {
					if s.tainted(arg) {
						return true
					}
				}
			}
		}
		return false
	}
	return false
}

// valueTainted is tainted without the whole-struct rule: a helper that hands
// back a struct is not a helper that hands back a queue-row value.
func (s *scope) valueTainted(expr ast.Expr) bool {
	if id, ok := expr.(*ast.Ident); ok && s.structs[id.Name] && !s.locals[id.Name] && !rowKeyName.MatchString(id.Name) {
		return false
	}
	return s.tainted(expr)
}

func (s *scope) structsOnly(expr ast.Expr) bool {
	id, ok := expr.(*ast.Ident)
	return ok && s.structs[id.Name] && !s.locals[id.Name] && !rowKeyName.MatchString(id.Name)
}

func isGuardCall(fset *token.FileSet, src []byte, call *ast.CallExpr) bool {
	return exprText(fset, src, call.Fun) == strings.TrimSuffix(guardCall, "(")
}

// leaves lists the tainted sub-expressions of one sink argument.
func (s *scope) leaves(fset *token.FileSet, src []byte, expr ast.Expr) []ast.Expr {
	if expr == nil {
		return nil
	}
	if call, ok := expr.(*ast.CallExpr); ok && isGuardCall(fset, src, call) {
		return nil
	}
	if s.tainted(expr) {
		return []ast.Expr{expr}
	}
	var out []ast.Expr
	switch typed := expr.(type) {
	case *ast.CallExpr:
		if sel, ok := typed.Fun.(*ast.SelectorExpr); ok {
			if _, plain := sel.X.(*ast.Ident); !plain {
				out = append(out, s.leaves(fset, src, sel.X)...)
			}
		}
		for _, arg := range typed.Args {
			out = append(out, s.leaves(fset, src, arg)...)
		}
	case *ast.SelectorExpr:
		if _, plain := typed.X.(*ast.Ident); !plain {
			out = append(out, s.leaves(fset, src, typed.X)...)
		}
	case *ast.BinaryExpr:
		out = append(out, s.leaves(fset, src, typed.X)...)
		out = append(out, s.leaves(fset, src, typed.Y)...)
	case *ast.ParenExpr:
		out = append(out, s.leaves(fset, src, typed.X)...)
	case *ast.StarExpr:
		out = append(out, s.leaves(fset, src, typed.X)...)
	case *ast.UnaryExpr:
		out = append(out, s.leaves(fset, src, typed.X)...)
	case *ast.IndexExpr:
		out = append(out, s.leaves(fset, src, typed.X)...)
	case *ast.KeyValueExpr:
		out = append(out, s.leaves(fset, src, typed.Value)...)
	case *ast.CompositeLit:
		for _, elt := range typed.Elts {
			out = append(out, s.leaves(fset, src, elt)...)
		}
	}
	return out
}

func parentMap(root ast.Node) map[ast.Node]ast.Node {
	parents := map[ast.Node]ast.Node{}
	var stack []ast.Node
	ast.Inspect(root, func(node ast.Node) bool {
		if node == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		if len(stack) > 0 {
			parents[node] = stack[len(stack)-1]
		}
		stack = append(stack, node)
		return true
	})
	return parents
}

func stmtList(node ast.Node) ([]ast.Stmt, bool) {
	switch typed := node.(type) {
	case *ast.BlockStmt:
		return typed.List, true
	case *ast.CaseClause:
		return typed.Body, true
	case *ast.CommClause:
		return typed.Body, true
	}
	return nil, false
}

func within(outer, inner ast.Node) bool {
	return outer != nil && inner != nil && inner.Pos() >= outer.Pos() && inner.End() <= outer.End()
}

// evaluatedFirst reports whether the guard sits where the statement always
// evaluates it before the statement's own body: the init, condition or tag of
// a compound statement, or anywhere in a simple statement.
func evaluatedFirst(stmt ast.Stmt, guard ast.Node) bool {
	switch typed := stmt.(type) {
	case *ast.IfStmt:
		return within(typed.Init, guard) || within(typed.Cond, guard)
	case *ast.ForStmt:
		return within(typed.Init, guard) || within(typed.Cond, guard)
	case *ast.SwitchStmt:
		return within(typed.Init, guard) || within(typed.Tag, guard)
	case *ast.RangeStmt:
		return within(typed.X, guard)
	case *ast.BlockStmt, *ast.TypeSwitchStmt, *ast.SelectStmt:
		return false
	}
	return true
}

// dominates reports whether the guard call runs on every path that reaches
// the sink: the guard's statement is in the same or an enclosing statement
// list and ahead of the sink, or the guard is evaluated first by the
// statement that holds the sink. A guard in a sibling branch, or after the
// sink, does not dominate it.
func dominates(parents map[ast.Node]ast.Node, guard, sink ast.Node) bool {
	stmt := guard
	var container ast.Node
	for {
		parent := parents[stmt]
		if parent == nil {
			return false
		}
		if _, ok := stmtList(parent); ok {
			container = parent
			break
		}
		stmt = parent
	}
	list, _ := stmtList(container)
	for i, candidate := range list {
		if ast.Node(candidate) != stmt {
			continue
		}
		if within(candidate, sink) {
			return sink.Pos() >= guard.End() && evaluatedFirst(candidate, guard)
		}
		for _, later := range list[i+1:] {
			if within(later, sink) {
				return true
			}
		}
	}
	return false
}

func unguardedOutputs(fset *token.FileSet, pkg []srcFile, seen map[string]bool) []string {
	fl := newFlow(fset, pkg)
	found := map[string]bool{}
	for _, sf := range pkg {
		for _, decl := range sf.file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			where := sf.path + "#" + fn.Name.Name
			sc := fl.scope(fn)
			parents := parentMap(fn.Body)
			var guards []*ast.CallExpr
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if call, ok := node.(*ast.CallExpr); ok && isGuardCall(fset, sf.src, call) {
					guards = append(guards, call)
				}
				return true
			})
			check := func(sink ast.Node, args []ast.Expr) {
				for _, arg := range args {
					for _, leaf := range sc.leaves(fset, sf.src, arg) {
						if sc.guarded(parents, guards, leaf) {
							continue
						}
						if _, ok := decodedRowFieldOutputs[where]; ok {
							seen[where] = true
							continue
						}
						found[where+" formats "+exprText(fset, sf.src, leaf)+" into an error or log line without a dominating "+guardCall+")"] = true
					}
				}
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch typed := node.(type) {
				case *ast.CallExpr:
					if sinkCall.MatchString(exprText(fset, sf.src, typed.Fun)) {
						check(typed, typed.Args)
					}
				case *ast.CompositeLit:
					if typed.Type != nil && sinkLiteral.MatchString(exprText(fset, sf.src, typed.Type)) {
						check(typed, typed.Elts)
					}
				}
				return true
			})
		}
	}
	out := make([]string, 0, len(found))
	for finding := range found {
		out = append(out, finding)
	}
	return out
}

// guarded reports whether a guard call dominates the leaf and its arguments
// reach the same value: the same name or selector path, a name linked to it by
// an assignment, or a path that contains it.
func (s *scope) guarded(parents map[ast.Node]ast.Node, guards []*ast.CallExpr, leaf ast.Expr) bool {
	leafKeys := keys(leaf)
	for _, guard := range guards {
		if !dominates(parents, guard, leaf) {
			continue
		}
		for _, arg := range guard.Args {
			for _, guardKey := range keys(arg) {
				for _, leafKey := range leafKeys {
					if s.find(guardKey) == s.find(leafKey) ||
						strings.HasPrefix(guardKey, leafKey+".") || strings.HasPrefix(leafKey, guardKey+".") {
						return true
					}
				}
			}
		}
	}
	return false
}

func treeSHA(t *testing.T, root string, dirs []string) string {
	t.Helper()
	sum := sha256.New()
	for _, dir := range dirs {
		_ = filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			data, rerr := os.ReadFile(path)
			if rerr != nil {
				t.Fatal(rerr)
			}
			sum.Write([]byte(path))
			sum.Write(data)
			return nil
		})
	}
	return hex.EncodeToString(sum.Sum(nil))
}

const fixtureHead = `package fx

import (
	"fmt"
	"log"
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/platform/credshape"
)

type Row struct {
	DedupeKey     string
	CorrelationID string
	N             int
}

`

// plantCensus writes the files under a temporary tree and runs the census on it.
func plantCensus(t *testing.T, files map[string]string) []string {
	t.Helper()
	root := t.TempDir()
	for rel, body := range files {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(fixtureHead+body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	findings, _ := formatCensus(t, root, []string{"internal/joboutbox"})
	return findings
}

var formatPlants = []struct {
	name  string
	body  string
	clean string
}{
	{"F01-renamed-local",
		"func f(row Row) { k := row.DedupeKey; slog.Info(\"x\", \"k\", k) }\n",
		"func f(row Row) { k := row.DedupeKey; if credshape.AnyCredentialShaped(k) { return }; slog.Info(\"x\", \"k\", k) }\n"},
	{"F02-slog-default",
		"func f(row Row) { slog.Default().Warn(\"x\", \"k\", row.DedupeKey) }\n",
		"func f(row Row) { slog.Default().Warn(\"x\", \"k\", row.N) }\n"},
	{"F03-helper-return",
		"func keyOf(r Row) string { return r.DedupeKey }\nfunc f(r Row) { slog.Info(\"x\", \"k\", keyOf(r)) }\n",
		"func keyOf(r Row) string { return r.DedupeKey }\nfunc f(r Row) { if credshape.AnyCredentialShaped(keyOf(r)) { return }; slog.Info(\"x\", \"k\", keyOf(r)) }\n"},
	{"F04-whole-struct",
		"func f(r Row) error { return fmt.Errorf(\"bad %+v\", r) }\n",
		"func f(r Row) error { return fmt.Errorf(\"bad %d\", r.N) }\n"},
	{"F05-format-before-guard",
		"func f(row Row) { slog.Info(\"x\", \"k\", row.DedupeKey); if credshape.AnyCredentialShaped(row.DedupeKey) { return } }\n",
		"func f(row Row) { if credshape.AnyCredentialShaped(row.DedupeKey) { return }; slog.Info(\"x\", \"k\", row.DedupeKey) }\n"},
	{"F06-guard-in-other-branch",
		"func f(row Row, c bool) { if c { if credshape.AnyCredentialShaped(row.DedupeKey) { return } } else { slog.Info(\"x\", \"k\", row.DedupeKey) } }\n",
		"func f(row Row, c bool) { if credshape.AnyCredentialShaped(row.DedupeKey) { return }; if c { slog.Info(\"x\", \"k\", row.DedupeKey) } }\n"},
	{"F07-guard-of-other-value",
		"func f(row Row) { if credshape.AnyCredentialShaped(row.CorrelationID) { return }; slog.Info(\"x\", \"k\", row.DedupeKey) }\n",
		"func f(row Row) { if credshape.AnyCredentialShaped(row.DedupeKey) { return }; slog.Info(\"x\", \"k\", row.DedupeKey) }\n"},
	{"F08-logger-with",
		"func f(logger *slog.Logger, row Row) { l := logger.With(\"k\", row.DedupeKey); _ = l }\n",
		"func f(logger *slog.Logger, row Row) { l := logger.With(\"k\", row.N); _ = l }\n"},
	{"F09-log-printf",
		"func f(row Row) { log.Printf(\"x %s\", row.DedupeKey) }\n",
		"func f(row Row) { log.Printf(\"x %d\", row.N) }\n"},
	{"F10-fmt-print",
		"func f(row Row) { fmt.Print(row.DedupeKey) }\n",
		"func f(row Row) { fmt.Print(row.N) }\n"},
	{"F11-span-attribute",
		"func f(span Span, row Row) { span.SetAttributes(attribute.String(\"k\", row.DedupeKey)) }\n",
		"func f(span Span, row Row) { span.SetAttributes(attribute.Int(\"k\", row.N)) }\n"},
	{"F12-metric-label",
		"func f(counter Vec, row Row) { counter.WithLabelValues(row.DedupeKey).Inc() }\n",
		"func f(counter Vec, row Row) { counter.WithLabelValues(\"fixed\").Inc() }\n"},
}

func TestQueueRowFormatCensusFindsEachPlantedShape(t *testing.T) {
	root := repoRoot(t)
	before := treeSHA(t, root, censusDirs)
	for _, plant := range formatPlants {
		t.Run(plant.name, func(t *testing.T) {
			if got := plantCensus(t, map[string]string{"internal/joboutbox/x.go": plant.body}); len(got) == 0 {
				t.Errorf("%s: the census did not turn red", plant.name)
			}
		})
		t.Run(plant.name+"-clean", func(t *testing.T) {
			if got := plantCensus(t, map[string]string{"internal/joboutbox/x.go": plant.clean}); len(got) != 0 {
				t.Errorf("%s: the clean twin is reported: %v", plant.name, got)
			}
		})
	}
	if after := treeSHA(t, root, censusDirs); after != before {
		t.Errorf("the plants changed the real tree: sha256 %s != %s", before, after)
	}
}

func TestQueueRowFormatCensusRecursesIntoSubpackages(t *testing.T) {
	got := plantCensus(t, map[string]string{"internal/joboutbox/sub/deep/x.go": "func f(row Row) { slog.Info(\"x\", \"k\", row.DedupeKey) }\n"})
	if len(got) == 0 {
		t.Error("R01-subpackage: a plant in a subpackage was not found")
	}
}

func TestQueueRowSQLCensusFindsArgsAtAnyPosition(t *testing.T) {
	cases := []struct {
		name, sql string
		want      bool
	}{
		{"S01-last-in-projection", "SELECT id, args FROM jobs", true},
		{"S02-returning", "UPDATE jobs SET state = 'x' RETURNING id, args", true},
		{"S03-first-in-projection", "SELECT args, id FROM jobs", true},
		{"S04-before-newline-from", "SELECT id,\n  args\nFROM jobs", true},
		{"S05-in-call", "SELECT coalesce(args) FROM jobs", true},
		{"S06-unrelated-column", "SELECT id, state FROM jobs", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := false
			for _, key := range sqlKeys(c.sql) {
				got = got || key == "args"
			}
			if got != c.want {
				t.Errorf("sqlKeys(%q) names args = %v, want %v", c.sql, got, c.want)
			}
		})
	}
}
