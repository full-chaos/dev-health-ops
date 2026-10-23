package otlpmetrics

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The census reads every non-test Go source under internal/ and cmd/ and works out, per instrument, the label keys its
// metric.WithAttributes sites carry today. labels.tsv must equal it: a new label key on an instrument, a new instrument, or a
// removed one fails here until the table says so. Binding is by identifier within a package (the variable or field that holds
// the instrument), so two instruments held by one identifier of a package share their keys: that makes the table looser, never
// tighter than the code.

var instrumentKinds = map[string]bool{
	"Int64Counter": true, "Float64Counter": true, "Int64Histogram": true, "Float64Histogram": true,
	"Int64UpDownCounter": true, "Float64UpDownCounter": true, "Int64Gauge": true, "Float64Gauge": true,
	"Int64ObservableCounter": true, "Float64ObservableCounter": true, "Int64ObservableGauge": true, "Float64ObservableGauge": true,
	"Int64ObservableUpDownCounter": true, "Float64ObservableUpDownCounter": true,
}

// manualInstruments are the instruments whose name or labels the census cannot read from the call site: the gauges are named by
// the field of a table row and created in a loop. Each entry is checked against the source (the file must hold the constant).
var manualInstruments = map[string]struct {
	file, constant string
	keys           []string
}{
	"workgraph_issue_pr_fallback_beside_native_rows":              {"internal/jobs/workgraph/issuepredges/invariant.go", "FallbackBesideNativeGauge", []string{"organization_id"}},
	"workgraph_issue_pr_fallback_beside_native_pull_request_rows": {"internal/jobs/workgraph/issuepredges/invariant.go", "FallbackBesideNativePullRequestGauge", []string{"organization_id"}},
	"workgraph_issue_pr_fallback_beside_native_issue_rows":        {"internal/jobs/workgraph/issuepredges/invariant.go", "FallbackBesideNativeIssueGauge", []string{"organization_id"}},
	"workgraph_issue_pr_fallback_pull_request_issue_side_rows":    {"internal/jobs/workgraph/issuepredges/invariant.go", "FallbackPullRequestIssueSideGauge", []string{"organization_id"}},
}

type sourceFile struct {
	rel, pkg string
	ast      *ast.File
	src      []byte
}

type census struct {
	keys     map[string]map[string]bool // instrument -> label keys
	files    map[string]string          // instrument -> creating file
	unsolved []string                   // creation sites whose name is neither a literal, a constant nor a wrapper parameter
	loose    []string                   // identifiers that hold more than one instrument in a package
}

func loadSources(t *testing.T, root string) []sourceFile {
	t.Helper()
	var files []sourceFile
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if rel != "." && rel != "internal" && rel != "cmd" && !strings.HasPrefix(rel, "internal/") && !strings.HasPrefix(rel, "cmd/") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, perr := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
		if perr != nil {
			return perr
		}
		files = append(files, sourceFile{rel: rel, pkg: filepath.ToSlash(filepath.Dir(rel)), ast: parsed})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func stringLit(e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	v, err := strconv.Unquote(lit.Value)
	return v, err == nil
}

func buildCensus(t *testing.T, files []sourceFile) census {
	t.Helper()
	consts := map[string]string{}
	for pass := 0; pass < 3; pass++ { // a constant may name another constant
		for _, f := range files {
			for _, decl := range f.ast.Decls {
				gen, ok := decl.(*ast.GenDecl)
				if !ok || gen.Tok != token.CONST {
					continue
				}
				for _, spec := range gen.Specs {
					vs := spec.(*ast.ValueSpec)
					for i, name := range vs.Names {
						if i >= len(vs.Values) {
							continue
						}
						if v, ok := stringLit(vs.Values[i]); ok {
							consts[f.pkg+"/"+name.Name] = v
						} else if id, ok := vs.Values[i].(*ast.Ident); ok {
							if v, ok := consts[f.pkg+"/"+id.Name]; ok {
								consts[f.pkg+"/"+name.Name] = v
							}
						}
					}
				}
			}
		}
	}
	var locals map[string]string
	resolve := func(f sourceFile, e ast.Expr) (string, bool) {
		if v, ok := stringLit(e); ok {
			return v, true
		}
		if id, ok := e.(*ast.Ident); ok {
			if v, ok := locals[id.Name]; ok {
				return v, true
			}
			v, ok := consts[f.pkg+"/"+id.Name]
			return v, ok
		}
		return "", false
	}
	localStrings := func(f sourceFile, body ast.Node) map[string]string {
		m := map[string]string{}
		ast.Inspect(body, func(n ast.Node) bool {
			var names []ast.Expr
			var values []ast.Expr
			switch x := n.(type) {
			case *ast.AssignStmt:
				names, values = x.Lhs, x.Rhs
			case *ast.ValueSpec:
				for _, id := range x.Names {
					names = append(names, id)
				}
				values = x.Values
			}
			for i, name := range names {
				id, ok := name.(*ast.Ident)
				if !ok || i >= len(values) {
					continue
				}
				if v, ok := stringLit(values[i]); ok {
					m[id.Name] = v
				} else if vid, ok := values[i].(*ast.Ident); ok {
					if v, ok := consts[f.pkg+"/"+vid.Name]; ok {
						m[id.Name] = v
					}
				}
			}
			return true
		})
		return m
	}
	kindCall := func(n ast.Node) (*ast.CallExpr, bool) {
		c, ok := n.(*ast.CallExpr)
		if !ok || len(c.Args) == 0 {
			return nil, false
		}
		s, ok := c.Fun.(*ast.SelectorExpr)
		return c, ok && instrumentKinds[s.Sel.Name]
	}
	// wrappers take the name as a parameter; creators build one fixed name.
	type wrapper struct{ index int }
	wrappers := map[string]wrapper{}
	creators := map[string]string{}
	var unsolved []string
	for _, f := range files {
		for _, decl := range f.ast.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			params := map[string]int{}
			index := 0
			for _, field := range fn.Type.Params.List {
				for _, name := range field.Names {
					params[name.Name] = index
					index++
				}
			}
			locals = localStrings(f, fn.Body)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := kindCall(n)
				if !ok {
					return true
				}
				if id, isIdent := call.Args[0].(*ast.Ident); isIdent {
					if i, isParam := params[id.Name]; isParam {
						wrappers[f.pkg+"/"+fn.Name.Name] = wrapper{i}
						return true
					}
				}
				if name, ok := resolve(f, call.Args[0]); ok {
					if _, dup := creators[f.pkg+"/"+fn.Name.Name]; !dup {
						creators[f.pkg+"/"+fn.Name.Name] = name
					}
				}
				return true
			})
			locals = nil
		}
	}
	bound := map[string][]string{} // pkg/identifier -> instruments
	created := map[string]string{}
	lastIdent := func(e ast.Expr) string {
		switch x := e.(type) {
		case *ast.Ident:
			return x.Name
		case *ast.SelectorExpr:
			return x.Sel.Name
		}
		return ""
	}
	for _, f := range files {
		for _, decl := range f.ast.Decls {
			scope := scopeOf(f, decl)
			walkScoped(decl, scope, func(n ast.Node, scope string) bool {
				var lhs, rhs []ast.Expr
				switch x := n.(type) {
				case *ast.AssignStmt:
					lhs, rhs = x.Lhs, x.Rhs
				case *ast.ValueSpec:
					for _, id := range x.Names {
						lhs = append(lhs, id)
					}
					rhs = x.Values
				case *ast.KeyValueExpr:
					lhs, rhs = []ast.Expr{fieldOf(x.Key)}, []ast.Expr{x.Value}
				}
				for ri, r := range rhs {
					call, ok := r.(*ast.CallExpr)
					if !ok {
						continue
					}
					name, found := "", false
					switch fn := call.Fun.(type) {
					case *ast.SelectorExpr:
						if instrumentKinds[fn.Sel.Name] && len(call.Args) > 0 {
							name, found = resolve(f, call.Args[0])
						}
					case *ast.Ident:
						if w, ok := wrappers[f.pkg+"/"+fn.Name]; ok && w.index < len(call.Args) {
							name, found = resolve(f, call.Args[w.index])
						} else if nm, ok := creators[f.pkg+"/"+fn.Name]; ok {
							name, found = nm, true
						}
					case *ast.FuncLit:
						locals = localStrings(f, fn.Body)
						ast.Inspect(fn.Body, func(m ast.Node) bool {
							if c, ok := kindCall(m); ok && !found {
								name, found = resolve(f, c.Args[0])
							}
							return !found
						})
						locals = nil
					}
					if !found {
						continue
					}
					li := ri
					if ri >= len(lhs) || len(lhs) < len(rhs) {
						li = 0
					}
					if li >= len(lhs) {
						continue
					}
					if id := lastIdent(lhs[li]); id != "" && id != "_" {
						key := bindKey(f, scope, lhs[li], id)
						bound[key] = appendUnique(bound[key], name)
						created[name] = f.rel
					}
				}
				return true
			})
		}
	}
	// every creation call must have a resolved name, a wrapper parameter, or a manual entry
	manualFiles := map[string]bool{}
	for _, m := range manualInstruments {
		manualFiles[m.file] = true
	}
	for _, f := range files {
		for _, decl := range f.ast.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			params := map[string]bool{}
			for _, field := range fn.Type.Params.List {
				for _, name := range field.Names {
					params[name.Name] = true
				}
			}
			locals = localStrings(f, fn.Body)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := kindCall(n)
				if !ok {
					return true
				}
				if id, isIdent := call.Args[0].(*ast.Ident); isIdent && params[id.Name] {
					return true
				}
				if _, ok := resolve(f, call.Args[0]); !ok && !manualFiles[f.rel] {
					unsolved = append(unsolved, f.rel)
				}
				return true
			})
			locals = nil
		}
		// package-level var initialisers (func literals) are scanned above only inside FuncDecls; scan them here
		for _, decl := range f.ast.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.VAR {
				continue
			}
			ast.Inspect(gen, func(n ast.Node) bool {
				lit, ok := n.(*ast.FuncLit)
				if !ok {
					return true
				}
				locals = localStrings(f, lit.Body)
				ast.Inspect(lit.Body, func(m ast.Node) bool {
					if call, ok := kindCall(m); ok {
						if _, ok := resolve(f, call.Args[0]); !ok && !manualFiles[f.rel] {
							unsolved = append(unsolved, f.rel)
						}
					}
					return true
				})
				locals = nil
				return false
			})
		}
	}
	// aliases: x = bound, or {field: bound}
	for pass := 0; pass < 2; pass++ {
		for _, f := range files {
			for _, decl := range f.ast.Decls {
				scope := scopeOf(f, decl)
				walkScoped(decl, scope, func(n ast.Node, scope string) bool {
					var lhs, rhs []ast.Expr
					switch x := n.(type) {
					case *ast.AssignStmt:
						lhs, rhs = x.Lhs, x.Rhs
					case *ast.KeyValueExpr:
						lhs, rhs = []ast.Expr{fieldOf(x.Key)}, []ast.Expr{x.Value}
					}
					for i, r := range rhs {
						id, ok := r.(*ast.Ident)
						if !ok || i >= len(lhs) {
							continue
						}
						dst := lastIdent(lhs[i])
						if names := lookupBound(bound, f, scope, id.Name); dst != "" && dst != "_" {
							dstKey := bindKey(f, scope, lhs[i], dst)
							if dstKey == bindKey(f, scope, id, id.Name) {
								continue
							}
							for _, name := range names {
								bound[dstKey] = appendUnique(bound[dstKey], name)
							}
						}
					}
					return true
				})
			}
		}
	}
	// every creation counts, bound to a name or not
	for _, f := range files {
		for _, decl := range f.ast.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil {
				locals = localStrings(f, fn.Body)
			}
			ast.Inspect(decl, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				name, found := "", false
				switch fn := call.Fun.(type) {
				case *ast.SelectorExpr:
					if instrumentKinds[fn.Sel.Name] && len(call.Args) > 0 {
						name, found = resolve(f, call.Args[0])
					}
				case *ast.Ident:
					if w, ok := wrappers[f.pkg+"/"+fn.Name]; ok && w.index < len(call.Args) {
						name, found = resolve(f, call.Args[w.index])
					} else if nm, ok := creators[f.pkg+"/"+fn.Name]; ok {
						name, found = nm, true
					}
				}
				if found {
					if _, seen := created[name]; !seen {
						created[name] = f.rel
					}
				}
				return true
			})
			locals = nil
		}
	}
	keys := map[string]map[string]bool{}
	for name := range created {
		keys[name] = map[string]bool{}
	}
	for _, f := range files {
		for _, decl := range f.ast.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			locals = localStrings(f, fn.Body)
			// attrs := metric.WithAttributes(...) held in a variable, used by the Add/Record below
			held := map[string][]string{}
			attrKeys := func(call *ast.CallExpr) ([]string, bool) {
				var out []string
				for _, a := range call.Args {
					inner, ok := a.(*ast.CallExpr)
					if !ok {
						return nil, false
					}
					sel, ok := inner.Fun.(*ast.SelectorExpr)
					if !ok || len(inner.Args) == 0 {
						return nil, false
					}
					if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "attribute" {
						return nil, false
					}
					key, ok := resolve(f, inner.Args[0])
					if !ok {
						return nil, false
					}
					out = append(out, key)
				}
				return out, call.Ellipsis == token.NoPos
			}
			isMetricWith := func(call *ast.CallExpr) bool {
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return false
				}
				pkg, ok := sel.X.(*ast.Ident)
				return ok && pkg.Name == "metric" && (sel.Sel.Name == "WithAttributes" || sel.Sel.Name == "WithAttributeSet")
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if as, ok := n.(*ast.AssignStmt); ok && len(as.Lhs) == 1 && len(as.Rhs) == 1 {
					if id, ok := as.Lhs[0].(*ast.Ident); ok {
						if call, ok := as.Rhs[0].(*ast.CallExpr); ok && isMetricWith(call) {
							if ks, ok := attrKeys(call); ok {
								held[id.Name] = ks
							}
						}
					}
				}
				return true
			})
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || (sel.Sel.Name != "Add" && sel.Sel.Name != "Record") {
					return true
				}
				var ks []string
				seen := false
				for _, a := range call.Args {
					switch arg := a.(type) {
					case *ast.CallExpr:
						if isMetricWith(arg) {
							got, ok := attrKeys(arg)
							if !ok {
								t.Errorf("%s: a metric.WithAttributes site with a key the census cannot read", f.rel)
							}
							ks = append(ks, got...)
							seen = true
						}
					case *ast.Ident:
						if got, ok := held[arg.Name]; ok {
							ks = append(ks, got...)
							seen = true
						}
					}
				}
				if !seen {
					return true
				}
				names := lookupBound(bound, f, scopeOf(f, decl), lastIdent(sel.X))
				if len(names) == 0 {
					if manualFiles[f.rel] {
						return true
					}
					t.Errorf("%s: an Add/Record with labels on %q, whose instrument the census cannot find", f.rel, lastIdent(sel.X))
					return true
				}
				for _, name := range names {
					for _, k := range ks {
						keys[name][k] = true
					}
				}
				return true
			})
			locals = nil
		}
	}
	var loose []string
	for id, names := range bound {
		if len(names) > 1 {
			loose = append(loose, id+" -> "+strings.Join(names, ","))
		}
	}
	sort.Strings(loose)
	return census{keys: keys, files: created, unsolved: unsolved, loose: loose}
}

func appendUnique(list []string, v string) []string {
	for _, e := range list {
		if e == v {
			return list
		}
	}
	return append(list, v)
}

func (c census) rows() map[string][]string {
	out := map[string][]string{}
	for name, set := range c.keys {
		var ks []string
		for k := range set {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		out[name] = ks
	}
	return out
}

// TestLabelsTableEqualsTheCensus: the table is exactly what the code emits today, per instrument, plus the pinned manual and
// third-party rows. Both directions: a key or instrument in the code but not the table, and a row the code no longer backs.
func TestLabelsTableEqualsTheCensus(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	files := loadSources(t, root)
	c := buildCensus(t, files)
	if len(c.unsolved) > 0 {
		t.Fatalf("instrument creations with a name the census cannot read (a literal, a constant or a wrapper parameter), outside the manual list: %v", c.unsolved)
	}
	table, err := ParseLabelRows(embeddedLabels)
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]LabelRow{}
	for _, row := range table {
		have[row.Instrument] = row
	}
	want := c.rows()
	for name, m := range manualInstruments {
		sort.Strings(m.keys)
		want[name] = m.keys
		src, err := readSource(files, m.file)
		if err != nil || !strings.Contains(string(src), m.constant) {
			t.Errorf("manual instrument %s: %s no longer holds %s", name, m.file, m.constant)
		}
	}
	// the producer of the manual instruments: the label keys of its metric.WithAttributes sites are the union of their rows
	manualKeys := map[string]map[string]bool{}
	for _, m := range manualInstruments {
		if manualKeys[m.file] == nil {
			manualKeys[m.file] = map[string]bool{}
		}
		for _, k := range m.keys {
			manualKeys[m.file][k] = true
		}
	}
	for file, want := range manualKeys {
		got := metricLabelKeys(files, file)
		if strings.Join(got, ",") != strings.Join(sortedKeys(want), ",") {
			t.Errorf("%s: its metric.WithAttributes sites carry %v, the manual instruments pin %v", file, got, sortedKeys(want))
		}
	}
	for name, keys := range want {
		row, ok := have[name]
		if !ok {
			t.Errorf("instrument %s (%s) has no row in labels.tsv; its keys today: %v", name, c.files[name], keys)
			continue
		}
		if name == DroppedMetricName {
			continue // observed, not added: its two keys are checked by the drop-counter tests
		}
		if strings.Join(row.Keys, ",") != strings.Join(keys, ",") {
			t.Errorf("instrument %s: labels.tsv has %v, the code emits %v", name, row.Keys, keys)
		}
	}
	for name, row := range have {
		if _, ok := want[name]; ok || strings.HasPrefix(row.Reason, "third-party:") || name == DroppedMetricName {
			continue
		}
		t.Errorf("labels.tsv row %s names no instrument in the source", name)
	}
	for _, row := range table {
		if len(row.Keys) > MaxLabelKeys && !strings.HasPrefix(row.Reason, "third-party:") {
			t.Errorf("%s: %d keys over the budget", row.Instrument, len(row.Keys))
		}
	}
	t.Logf("%d instruments in the source, %d rows, identifiers holding several instruments: %v", len(want), len(table), c.loose)
}

func readSource(files []sourceFile, rel string) ([]byte, error) {
	for _, f := range files {
		if f.rel == rel {
			return readFile(filepath.Join("..", "..", "..", filepath.FromSlash(rel)))
		}
	}
	return nil, fs.ErrNotExist
}

func readFile(path string) ([]byte, error) { return os.ReadFile(path) }

// scopeOf names the function a declaration is, or "" for a package-level declaration: a bare local identifier binds inside its
// function only, a field or package-level variable binds for the package.
func scopeOf(f sourceFile, decl ast.Decl) string {
	if fn, ok := decl.(*ast.FuncDecl); ok {
		return f.rel + "#" + strconv.Itoa(int(fn.Pos()))
	}
	return ""
}

func bindKey(f sourceFile, scope string, lhs ast.Expr, id string) string {
	if _, bare := lhs.(*ast.Ident); bare && scope != "" {
		return scope + "/" + id
	}
	return f.pkg + "/" + id
}

func lookupBound(bound map[string][]string, f sourceFile, scope, id string) []string {
	for s := scope; s != ""; {
		if names := bound[s+"/"+id]; len(names) > 0 {
			return names
		}
		cut := strings.LastIndex(s, "/lit")
		if cut < 0 {
			break
		}
		s = s[:cut]
	}
	return bound[f.pkg+"/"+id]
}

// walkScoped visits a declaration; a function literal is its own scope nested in the enclosing one.
func walkScoped(n ast.Node, scope string, visit func(ast.Node, string) bool) {
	ast.Inspect(n, func(c ast.Node) bool {
		if lit, ok := c.(*ast.FuncLit); ok && c != n {
			walkScoped(lit.Body, scope+"/lit"+strconv.Itoa(int(lit.Pos())), visit)
			return false
		}
		return visit(c, scope)
	})
}

// fieldOf marks a struct-literal key as a field: it binds for the package, not for the function.
func fieldOf(key ast.Expr) ast.Expr {
	if id, ok := key.(*ast.Ident); ok {
		return &ast.SelectorExpr{X: ast.NewIdent("_"), Sel: id}
	}
	return key
}

// metricLabelKeys is the sorted set of literal label keys at the metric.WithAttributes sites of one file.
func metricLabelKeys(files []sourceFile, rel string) []string {
	set := map[string]bool{}
	for _, f := range files {
		if f.rel != rel {
			continue
		}
		ast.Inspect(f.ast, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "WithAttributes" || !isPackageIdent(sel.X, "metric") {
				return true
			}
			for _, arg := range call.Args {
				inner, ok := arg.(*ast.CallExpr)
				if !ok || len(inner.Args) == 0 {
					continue
				}
				if key, ok := stringLit(inner.Args[0]); ok {
					set[key] = true
				}
			}
			return true
		})
	}
	return sortedKeys(set)
}

func isPackageIdent(e ast.Expr, name string) bool {
	id, ok := e.(*ast.Ident)
	return ok && id.Name == name
}

func sortedKeys(set map[string]bool) []string {
	var out []string
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
