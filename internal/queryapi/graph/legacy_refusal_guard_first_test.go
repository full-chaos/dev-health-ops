package graph

import (
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
)

// guardFirstSpec is the guard a legacy-refusal root's resolver must call first, and the closed list of calls allowed
// before it. A call is named "<import path suffix>.Name" for a package function, "Name" for a function of this package.
type guardFirstSpec struct {
	guard   string
	allowed []string
}

// guardFirst is the expected guard of every root in the legacy-refusal registry (pdp.LegacyRefusalRoots).
var guardFirst = map[string]guardFirstSpec{
	"Query.dataHealth": {guard: "internal/queryapi/datahealth.RequireOperator",
		allowed: []string{"startDataHealthSpan", "internal/queryapi/authctx.FromContext"}},
	"Mutation.cloneSavedReport":  {guard: "requireMutationOrg"},
	"Mutation.createSavedReport": {guard: "requireMutationOrg"},
	"Mutation.deleteSavedReport": {guard: "requireMutationOrg"},
	"Mutation.triggerReport":     {guard: "requireMutationOrg"},
	"Mutation.updateSavedReport": {guard: "requireMutationOrg"},
}

// sourceFunc is a function of this package's source with the imports of its file.
type sourceFunc struct {
	decl     *ast.FuncDecl
	importOf map[string]string
}

// packageFuncs parses this package's non-generated sources: "Name" for a function, "recv.Name" for a method.
func packageFuncs(t *testing.T) (map[string]sourceFunc, map[string]*ast.File) {
	t.Helper()
	funcs, files := map[string]sourceFunc{}, map[string]*ast.File{}
	fset := token.NewFileSet()
	for name, src := range graphSources(t) {
		file, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		files[name] = file
		importOf := fileImports(file)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			key := fn.Name.Name
			if fn.Recv != nil && len(fn.Recv.List) == 1 {
				recv := fn.Recv.List[0].Type
				if star, ok := recv.(*ast.StarExpr); ok {
					recv = star.X
				}
				if id, ok := recv.(*ast.Ident); ok {
					key = id.Name + "." + key
				}
			}
			funcs[key] = sourceFunc{decl: fn, importOf: importOf}
		}
	}
	return funcs, files
}

// fileImports maps the local name of each import of file to its path.
func fileImports(file *ast.File) map[string]string {
	importOf := map[string]string{}
	for _, spec := range file.Imports {
		path, _ := strconv.Unquote(spec.Path.Value)
		local := path[strings.LastIndex(path, "/")+1:]
		if spec.Name != nil {
			local = spec.Name.Name
		}
		importOf[local] = path
	}
	return importOf
}

// callName names a call: "<import path>.Name" for a package function, "Name" for a local one, "x.Name" for a method.
func callName(call *ast.CallExpr, importOf map[string]string) string {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return fun.Name
	case *ast.SelectorExpr:
		if x, ok := fun.X.(*ast.Ident); ok {
			if path, imported := importOf[x.Name]; imported {
				return path + "." + fun.Sel.Name
			}
			return x.Name + "." + fun.Sel.Name
		}
		return "?." + fun.Sel.Name
	}
	return "?"
}

func callIs(name, want string) bool { return name == want || strings.HasSuffix(name, "/"+want) }

func calls(node ast.Node) []*ast.CallExpr {
	var out []*ast.CallExpr
	ast.Inspect(node, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			out = append(out, call)
		}
		return true
	})
	return out
}

// resolverBody is the body that runs a root's resolver: the generated method's, or, when that is one
// "return r.helper(...)" delegation to a method of the same resolver, the helper's.
func resolverBody(t *testing.T, funcs map[string]sourceFunc, key string) sourceFunc {
	t.Helper()
	kind, name, _ := strings.Cut(key, ".")
	recv := strings.ToLower(kind) + "Resolver"
	fn, ok := funcs[recv+"."+strings.ToUpper(name[:1])+name[1:]]
	if !ok {
		t.Fatalf("%s: no resolver method %s", key, recv)
	}
	if len(fn.decl.Body.List) == 1 {
		if ret, ok := fn.decl.Body.List[0].(*ast.ReturnStmt); ok && len(ret.Results) == 1 {
			if call, ok := ret.Results[0].(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					if x, ok := sel.X.(*ast.Ident); ok && len(fn.decl.Recv.List[0].Names) == 1 && x.Name == fn.decl.Recv.List[0].Names[0].Name {
						if helper, ok := funcs[recv+"."+sel.Sel.Name]; ok {
							return helper
						}
					}
				}
			}
		}
	}
	return fn
}

// acp condition (i), CHAOS-13334: for every root of the legacy-refusal registry, the first call its resolver makes that
// is not on the closed allowed list IS its guard, and its fieldPrechecks entry calls that same function. A read or a
// write before the guard, or a table entry that copies the guard's logic, fails here: the legacy-refusal path runs a
// denied root's resolver only because that guard refuses before any I/O.
func TestLegacyRefusalRootsCallTheirGuardFirst(t *testing.T) {
	funcs, files := packageFuncs(t)
	var registered []string
	for _, keys := range pdp.LegacyRefusalRoots() {
		registered = append(registered, keys...)
	}
	for _, key := range registered {
		if _, ok := guardFirst[key]; !ok {
			t.Errorf("%s is in the legacy-refusal registry but has no guard-first row", key)
		}
	}
	for key := range guardFirst {
		if !slices.Contains(registered, key) {
			t.Errorf("guard-first row %s is not in the legacy-refusal registry", key)
		}
	}
	entries := precheckEntries(t, funcs, files)
	for key, spec := range guardFirst {
		t.Run(key, func(t *testing.T) {
			body := resolverBody(t, funcs, key)
			all := calls(body.decl.Body)
			var guard *ast.CallExpr
			for _, call := range all {
				if callIs(callName(call, body.importOf), spec.guard) {
					guard = call
					break
				}
			}
			if guard == nil {
				t.Fatalf("%s: the resolver (%s) never calls its guard %s", key, body.decl.Name.Name, spec.guard)
			}
			for _, call := range all {
				if call == guard {
					continue
				}
				before := call.End() <= guard.Pos() || (call.Pos() > guard.Pos() && call.End() <= guard.End())
				name := callName(call, body.importOf)
				if before && !slices.ContainsFunc(spec.allowed, func(want string) bool { return callIs(name, want) }) {
					t.Errorf("%s: the resolver (%s) calls %s before its guard %s; only %v may run first", key, body.decl.Name.Name, name, spec.guard, spec.allowed)
				}
			}
			entry, ok := entries[key]
			if !ok {
				t.Fatalf("%s: no fieldPrechecks entry", key)
			}
			var named []string
			for _, call := range calls(entry.node) {
				named = append(named, callName(call, entry.importOf))
			}
			if !slices.ContainsFunc(named, func(name string) bool { return callIs(name, spec.guard) }) {
				t.Errorf("%s: the fieldPrechecks entry calls %v, not the resolver's guard %s", key, named, spec.guard)
			}
		})
	}
}

// precheckEntry is the code of one fieldPrechecks entry: its function literal, or the body of the function it names.
type precheckEntry struct {
	node     ast.Node
	importOf map[string]string
}

func precheckEntries(t *testing.T, funcs map[string]sourceFunc, files map[string]*ast.File) map[string]precheckEntry {
	t.Helper()
	file, ok := files["field_precheck.go"]
	if !ok {
		t.Fatal("field_precheck.go not found")
	}
	importOf := fileImports(file)
	out := map[string]precheckEntry{}
	ast.Inspect(file, func(n ast.Node) bool {
		spec, ok := n.(*ast.ValueSpec)
		if !ok || len(spec.Names) != 1 || spec.Names[0].Name != "fieldPrechecks" || len(spec.Values) != 1 {
			return true
		}
		table, ok := spec.Values[0].(*ast.CompositeLit)
		if !ok {
			t.Fatal("fieldPrechecks is not a composite literal")
		}
		for _, elt := range table.Elts {
			kv := elt.(*ast.KeyValueExpr)
			lit, ok := kv.Key.(*ast.BasicLit)
			if !ok {
				continue
			}
			key, _ := strconv.Unquote(lit.Value)
			switch value := kv.Value.(type) {
			case *ast.FuncLit:
				out[key] = precheckEntry{node: value.Body, importOf: importOf}
			case *ast.Ident:
				if fn, ok := funcs[value.Name]; ok {
					out[key] = precheckEntry{node: fn.decl.Body, importOf: fn.importOf}
				}
			}
		}
		return false
	})
	return out
}
