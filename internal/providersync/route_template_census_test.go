package providersync

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/constant"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"golang.org/x/tools/go/packages"
)

// routeCensusPackages is the load pattern of the route census: every package
// that sends a provider request, except providerfoundation itself (the
// library; its walkers send the Route of an option literal that the census
// checks at the caller). The load is the shared census load
// (census_load_test.go). A caller outside these packages is not seen, so a
// route parameter is proved only by the call sites in these packages.
var routeCensusPackages = []string{
	"./internal/providersync",
	"./internal/scheduler/sync",
	"./internal/api/teamsidentity",
}

// routeCensusFloor is the least number of DoRoute / DoUnauthenticatedRoute
// call sites the census must see in each package (counted when CHAOS-10268
// made the census type-aware). A load that silently misses files fails here.
// Lower a floor only when the calls are removed.
var routeCensusFloor = map[string]int{
	"./internal/providersync":      35,
	"./internal/scheduler/sync":    1,
	"./internal/api/teamsidentity": 12,
}

// CHAOS-9776 / CHAOS-9801 / CHAOS-10268 census: every provider page-walk
// options literal, every DoRoute / DoUnauthenticatedRoute call and every
// value that a pass-through helper sends there names a static route template,
// so a status or transport failure says which route failed without printing
// a path that holds an owner, repository, project, member or issue name.
//
// Static is proved from types, not from names (9801 ruling, chris 10-15:
// a string literal, a constant, or a concatenation of those with static
// suffix variables). The census follows a value to its sources: a constant;
// a local where every assignment is static; a parameter whose argument is
// static at every call site in the loaded packages; an unexported struct
// field that every composite literal sets to a static value; an index into a
// local slice of static literals. A source it cannot prove (a call result, a
// map read, a channel receive, a func value, an interface method, a field set
// outside a composite literal) fails with its file:line.
func TestEveryPageWalkAndObjectFetchNamesARouteTemplate(t *testing.T) {
	start := time.Now()
	pkgs, err := censusLoad(routeCensusPackages...)
	if err != nil {
		t.Fatal(err)
	}
	loaded := time.Since(start)
	requireRouteCensusLoad(t, pkgs)
	census := runRouteCensus(pkgs)
	for i, pattern := range routeCensusPackages {
		got := census.requestSites[pkgs[i].PkgPath]
		if got == 0 || got < routeCensusFloor[pattern] {
			t.Errorf("%s: the census saw %d DoRoute / DoUnauthenticatedRoute call sites, want at least %d", pattern, got, routeCensusFloor[pattern])
		}
	}
	if len(census.optionTypes) < 4 {
		t.Errorf("the census saw %d providerfoundation option types with a Route field, want at least 4: %v", len(census.optionTypes), census.optionTypes)
	}
	for _, finding := range census.findings {
		t.Errorf("%s", finding)
	}
	if len(census.findings) > 0 {
		t.Errorf("%d provider requests without a provably static route template: use Route / fetchObjectRoute / DoRoute with a constant from route_templates.go (or a literal)", len(census.findings))
	}
	t.Logf("route census: load %s, census %s, request sites %v, option sites %d", loaded.Round(time.Millisecond), (time.Since(start) - loaded).Round(time.Millisecond), census.requestSites, census.optionSites)
}

// The census proves only what it loads: every package of the module that
// names DoRoute or DoUnauthenticatedRoute (a call or a method value) must be
// in routeCensusPackages (providerfoundation is the library). A new caller
// package outside the list fails here, not silently. Every non-test Go file
// of the module is read.
func TestRouteCensusLoadListHoldsEveryDoRouteCaller(t *testing.T) {
	callers, scanned := routeCallerDirs(t, "../..")
	if scanned < 1000 {
		t.Fatalf("read %d non-test Go files of the module; the scan must see the module", scanned)
	}
	for _, pattern := range routeCensusPackages {
		if !callers[strings.TrimPrefix(pattern, "./")] {
			t.Errorf("%s is in the census list but the scan found no DoRoute caller in it: the scan or the list is wrong", pattern)
		}
	}
	allowed := map[string]bool{"internal/providerfoundation": true}
	for _, pattern := range routeCensusPackages {
		allowed[strings.TrimPrefix(pattern, "./")] = true
	}
	if outside := routeCallersOutside(callers, allowed); len(outside) > 0 {
		t.Errorf("packages name DoRoute / DoUnauthenticatedRoute outside the census list %v: add them to routeCensusPackages and routeCensusFloor: %v", routeCensusPackages, outside)
	}
}

// The scan finds a package that only keeps DoRoute or DoUnauthenticatedRoute
// as a method value, not only one that calls it.
func TestRouteCallerScanFindsAMethodValueInAnUnlistedPackage(t *testing.T) {
	callers, scanned := routeCallerDirs(t, "testdata/routecensus/callers")
	if scanned != 3 {
		t.Fatalf("read %d files in testdata/routecensus/callers, want 3", scanned)
	}
	got := routeCallersOutside(callers, map[string]bool{"listed": true})
	if want := []string{"unlisted", "unlistedredirect"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("packages outside the list: %v, want %v", got, want)
	}
}

// routeCallerDirs is every directory under root (relative, slash form) with a
// non-test Go file that names DoRoute or DoUnauthenticatedRoute as a selector.
func routeCallerDirs(t *testing.T, root string) (map[string]bool, int) {
	t.Helper()
	callers := map[string]bool{}
	scanned := 0
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "vendor", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		scanned++
		if !bytes.Contains(source, []byte("DoRoute")) && !bytes.Contains(source, []byte("DoUnauthenticatedRoute")) {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, source, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			sel, ok := node.(*ast.SelectorExpr)
			if ok && (sel.Sel.Name == "DoRoute" || sel.Sel.Name == "DoUnauthenticatedRoute") {
				if dir, err := filepath.Rel(root, filepath.Dir(path)); err == nil {
					callers[filepath.ToSlash(dir)] = true
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return callers, scanned
}

func routeCallersOutside(callers, allowed map[string]bool) []string {
	var outside []string
	for dir := range callers {
		if !allowed[dir] {
			outside = append(outside, dir)
		}
	}
	sort.Strings(outside)
	return outside
}

// requireRouteCensusLoad fails on a load that the census cannot read: a
// package with an error or without syntax is never read as 0 findings.
func requireRouteCensusLoad(t *testing.T, pkgs []*packages.Package) {
	t.Helper()
	if len(pkgs) == 0 {
		t.Fatal("the route census loaded no package")
	}
	for _, pkg := range pkgs {
		for _, loadErr := range pkg.Errors {
			t.Errorf("%s: %v", pkg.PkgPath, loadErr)
		}
		if len(pkg.Syntax) == 0 || pkg.TypesInfo == nil || pkg.Types == nil {
			t.Errorf("%s: loaded without typed syntax", pkg.PkgPath)
		}
	}
	if t.Failed() {
		t.FailNow()
	}
}

type routeCensusResult struct {
	findings     []string
	requestSites map[string]int
	optionSites  int
	optionTypes  []string
}

type routeSource struct {
	pkg  *packages.Package
	expr ast.Expr // nil: the zero value ""
	kind string   // "": expr; "range": a range value over expr; or a kind that is never static
}

type routeParam struct {
	fn       string // types.Func.FullName of the origin; "" for a func literal
	index    int
	method   bool
	variadic bool
}

type routeCall struct {
	pkg  *packages.Package
	call *ast.CallExpr
}

type routeIndex struct {
	locals     map[*types.Var][]routeSource
	escaped    map[any]string
	params     map[*types.Var]routeParam
	pkgVarInit map[any]routeSource
	fieldVals  map[*types.Var][]routeSource
	fieldSet   map[*types.Var]string
	calls      map[string][]routeCall
	funcValue  map[string]string
}

func runRouteCensus(pkgs []*packages.Package) routeCensusResult {
	ix := &routeIndex{
		locals: map[*types.Var][]routeSource{}, escaped: map[any]string{}, params: map[*types.Var]routeParam{},
		pkgVarInit: map[any]routeSource{}, fieldVals: map[*types.Var][]routeSource{}, fieldSet: map[*types.Var]string{},
		calls: map[string][]routeCall{}, funcValue: map[string]string{},
	}
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			ix.indexFile(pkg, file)
		}
	}
	result := routeCensusResult{requestSites: map[string]int{}}
	optionTypes := map[string]bool{}
	for _, pkg := range pkgs {
		result.requestSites[pkg.PkgPath] += 0
		for _, file := range pkg.Syntax {
			ix.censusFile(pkg, file, &result, optionTypes)
		}
	}
	for name := range optionTypes {
		result.optionTypes = append(result.optionTypes, name)
	}
	sort.Strings(result.optionTypes)
	sort.Strings(result.findings)
	return result
}

func routePos(pkg *packages.Package, node ast.Node) string {
	position := pkg.Fset.Position(node.Pos())
	return fmt.Sprintf("%s:%d", shortRoutePath(position.Filename), position.Line)
}

func shortRoutePath(name string) string {
	if i := strings.LastIndex(name, "/internal/"); i >= 0 {
		return name[i+1:]
	}
	return name
}

func routeCallee(info *types.Info, fun ast.Expr) (*types.Func, *ast.Ident) {
	fun = ast.Unparen(fun)
	switch e := fun.(type) {
	case *ast.IndexExpr:
		fun = e.X
	case *ast.IndexListExpr:
		fun = e.X
	}
	var ident *ast.Ident
	switch e := ast.Unparen(fun).(type) {
	case *ast.Ident:
		ident = e
	case *ast.SelectorExpr:
		ident = e.Sel
	default:
		return nil, nil
	}
	fn, _ := info.Uses[ident].(*types.Func)
	return fn, ident
}

func routeVarOf(info *types.Info, ident *ast.Ident) *types.Var {
	if obj, ok := info.Defs[ident].(*types.Var); ok && obj != nil {
		return obj
	}
	if obj, ok := info.Uses[ident].(*types.Var); ok {
		return obj
	}
	return nil
}

func routePackageLevel(v *types.Var) bool {
	return v.Pkg() != nil && v.Parent() == v.Pkg().Scope()
}

// routeVarKey keys a package var by its path and name, so the same var seen
// from two loaded packages is one key; any other var by its object.
func routeVarKey(v *types.Var) any {
	if routePackageLevel(v) {
		return v.Pkg().Path() + "." + v.Name()
	}
	return v
}

func routeStructOf(info *types.Info, expr ast.Expr) *types.Struct {
	t := info.TypeOf(expr)
	if t == nil {
		return nil
	}
	if p, ok := t.Underlying().(*types.Pointer); ok {
		t = p.Elem()
	}
	st, _ := t.Underlying().(*types.Struct)
	return st
}

func (ix *routeIndex) indexParams(info *types.Info, fields *ast.FieldList, param routeParam) {
	if fields == nil {
		return
	}
	index := 0
	for i, field := range fields.List {
		_, variadic := field.Type.(*ast.Ellipsis)
		names := field.Names
		if len(names) == 0 {
			index++
			continue
		}
		for _, name := range names {
			if v, ok := info.Defs[name].(*types.Var); ok && v != nil {
				p := param
				p.index = index
				p.variadic = variadic && i == len(fields.List)-1
				ix.params[v] = p
			}
			index++
		}
	}
}

func (ix *routeIndex) addLHS(pkg *packages.Package, lhs ast.Expr, source routeSource) {
	info := pkg.TypesInfo
	switch e := ast.Unparen(lhs).(type) {
	case *ast.Ident:
		if e.Name == "_" {
			return
		}
		v := routeVarOf(info, e)
		if v == nil {
			return
		}
		if routePackageLevel(v) {
			ix.escaped[routeVarKey(v)] = "package var assigned at " + routePos(pkg, e)
			return
		}
		ix.locals[v] = append(ix.locals[v], source)
	case *ast.SelectorExpr:
		if sel := info.Selections[e]; sel != nil && sel.Kind() == types.FieldVal {
			if field, ok := sel.Obj().(*types.Var); ok {
				ix.fieldSet[field.Origin()] = "field set outside a composite literal at " + routePos(pkg, e)
			}
			return
		}
		if v, ok := info.Uses[e.Sel].(*types.Var); ok {
			ix.escaped[routeVarKey(v)] = "package var assigned at " + routePos(pkg, e)
		}
	case *ast.IndexExpr:
		if base, ok := ast.Unparen(e.X).(*ast.Ident); ok {
			if v := routeVarOf(info, base); v != nil {
				ix.escaped[routeVarKey(v)] = "element assigned at " + routePos(pkg, e)
			}
		}
	}
}

// indexFile records every source of every local, parameter, package var and
// struct field, every static call and every func used as a value.
func (ix *routeIndex) indexFile(pkg *packages.Package, file *ast.File) {
	info := pkg.TypesInfo
	called := map[*ast.Ident]bool{}
	var stack []ast.Node
	ast.Inspect(file, func(node ast.Node) bool {
		if node == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		var parent ast.Node
		if len(stack) > 0 {
			parent = stack[len(stack)-1]
		}
		stack = append(stack, node)
		switch n := node.(type) {
		case *ast.FuncDecl:
			if fn, ok := info.Defs[n.Name].(*types.Func); ok && fn != nil {
				ix.indexParams(info, n.Type.Params, routeParam{fn: fn.Origin().FullName(), method: n.Recv != nil})
			}
		case *ast.FuncLit:
			ix.indexParams(info, n.Type.Params, routeParam{})
		case *ast.AssignStmt:
			if len(n.Lhs) == len(n.Rhs) {
				for i := range n.Lhs {
					ix.addLHS(pkg, n.Lhs[i], routeSource{pkg: pkg, expr: n.Rhs[i]})
				}
			} else {
				for _, lhs := range n.Lhs {
					ix.addLHS(pkg, lhs, routeSource{pkg: pkg, kind: "multi-value assignment at " + routePos(pkg, n)})
				}
			}
		case *ast.ValueSpec:
			for i, name := range n.Names {
				v, _ := info.Defs[name].(*types.Var)
				if v == nil {
					continue
				}
				source := routeSource{pkg: pkg}
				switch {
				case len(n.Values) == len(n.Names):
					source.expr = n.Values[i]
				case len(n.Values) > 0:
					source.kind = "multi-value declaration at " + routePos(pkg, n)
				}
				if routePackageLevel(v) {
					ix.pkgVarInit[routeVarKey(v)] = source
				} else {
					ix.locals[v] = append(ix.locals[v], source)
				}
			}
		case *ast.RangeStmt:
			if n.Key != nil {
				ix.addLHS(pkg, n.Key, routeSource{pkg: pkg, kind: "range key at " + routePos(pkg, n)})
			}
			if n.Value != nil {
				ix.addLHS(pkg, n.Value, routeSource{pkg: pkg, expr: n.X, kind: "range"})
			}
		case *ast.UnaryExpr:
			if n.Op != token.AND {
				break
			}
			switch x := ast.Unparen(n.X).(type) {
			case *ast.Ident:
				if v := routeVarOf(info, x); v != nil {
					ix.escaped[routeVarKey(v)] = "address taken at " + routePos(pkg, n)
				}
			case *ast.SelectorExpr:
				if sel := info.Selections[x]; sel != nil && sel.Kind() == types.FieldVal {
					if field, ok := sel.Obj().(*types.Var); ok {
						ix.fieldSet[field.Origin()] = "field address taken at " + routePos(pkg, n)
					}
				}
			}
		case *ast.CompositeLit:
			st := routeStructOf(info, n)
			if st == nil {
				break
			}
			for i, element := range n.Elts {
				if pair, ok := element.(*ast.KeyValueExpr); ok {
					key, _ := pair.Key.(*ast.Ident)
					for f := 0; key != nil && f < st.NumFields(); f++ {
						if st.Field(f).Name() == key.Name {
							ix.fieldVals[st.Field(f).Origin()] = append(ix.fieldVals[st.Field(f).Origin()], routeSource{pkg: pkg, expr: pair.Value})
						}
					}
				} else if i < st.NumFields() {
					ix.fieldVals[st.Field(i).Origin()] = append(ix.fieldVals[st.Field(i).Origin()], routeSource{pkg: pkg, expr: element})
				}
			}
		case *ast.CallExpr:
			if fn, ident := routeCallee(info, n.Fun); fn != nil {
				called[ident] = true
				ix.calls[fn.Origin().FullName()] = append(ix.calls[fn.Origin().FullName()], routeCall{pkg: pkg, call: n})
			}
		case *ast.Ident:
			if fn, ok := info.Uses[n].(*types.Func); ok && !called[n] {
				if _, isDecl := parent.(*ast.FuncDecl); !isDecl {
					ix.funcValue[fn.Origin().FullName()] = "used as a value at " + routePos(pkg, n)
				}
			}
			if v, ok := info.Uses[n].(*types.Var); ok && !v.IsField() {
				if _, isSlice := v.Type().Underlying().(*types.Slice); isSlice && !routeSliceUseOK(info, n, stack) {
					ix.escaped[routeVarKey(v)] = "slice used outside an index, a range or len at " + routePos(pkg, n)
				}
			}
		}
		return true
	})
}

// routeSliceUseOK: a local slice of routes stays static only while it is read
// by index, ranged over, measured, or assigned as a whole.
func routeSliceUseOK(info *types.Info, ident *ast.Ident, stack []ast.Node) bool {
	if len(stack) < 2 {
		return false
	}
	switch p := stack[len(stack)-2].(type) {
	case *ast.IndexExpr:
		return ast.Unparen(p.X) == ident
	case *ast.RangeStmt:
		return p.X == ident
	case *ast.AssignStmt:
		for _, lhs := range p.Lhs {
			if lhs == ident {
				return true
			}
		}
	case *ast.CallExpr:
		if b, ok := info.Uses[routeFunIdent(p.Fun)].(*types.Builtin); ok {
			return b.Name() == "len" || b.Name() == "cap"
		}
	case *ast.ParenExpr:
		return false
	}
	return false
}

func routeFunIdent(fun ast.Expr) *ast.Ident {
	ident, _ := ast.Unparen(fun).(*ast.Ident)
	if ident == nil {
		return &ast.Ident{}
	}
	return ident
}

// providerfoundationRouteType names a providerfoundation struct with a Route
// field: a page-walk option (it must name a Route) or a ProviderError (a Route
// it names must be static).
func providerfoundationRouteType(info *types.Info, expr ast.Expr) (string, bool) {
	t := info.TypeOf(expr)
	if t == nil {
		return "", false
	}
	if p, ok := t.Underlying().(*types.Pointer); ok {
		t = p.Elem()
	}
	named, ok := types.Unalias(t).(*types.Named)
	if !ok || named.Obj().Pkg() == nil || !strings.HasSuffix(named.Obj().Pkg().Path(), "/internal/providerfoundation") {
		return "", false
	}
	st, ok := named.Underlying().(*types.Struct)
	if !ok {
		return "", false
	}
	for i := 0; i < st.NumFields(); i++ {
		if st.Field(i).Name() == "Route" {
			return named.Obj().Name(), true
		}
	}
	return "", false
}

// censusFile checks every census site of one file.
func (ix *routeIndex) censusFile(pkg *packages.Package, file *ast.File, result *routeCensusResult, optionTypes map[string]bool) {
	info := pkg.TypesInfo
	bad := func(node ast.Node, format string, args ...any) {
		result.findings = append(result.findings, routePos(pkg, node)+": "+fmt.Sprintf(format, args...))
	}
	check := func(site ast.Node, what string, expr ast.Expr) {
		if why := ix.static(pkg, expr, map[any]bool{}); why != "" {
			bad(site, "%s: cannot prove static: %s", what, why)
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.CompositeLit:
			name, ok := providerfoundationRouteType(info, n)
			if !ok {
				return true
			}
			option := strings.HasSuffix(name, "Options")
			if option {
				optionTypes[name] = true
				result.optionSites++
			}
			found := false
			for _, element := range n.Elts {
				pair, ok := element.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				if key, ok := pair.Key.(*ast.Ident); ok && key.Name == "Route" {
					found = true
					check(n, "providerfoundation."+name+" Route", pair.Value)
				}
			}
			if !found && option {
				bad(n, "providerfoundation.%s literal without a Route", name)
			}
		case *ast.AssignStmt:
			for i, lhs := range n.Lhs {
				sel, ok := ast.Unparen(lhs).(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Route" {
					continue
				}
				name, ok := providerfoundationRouteType(info, sel.X)
				if !ok {
					continue
				}
				if len(n.Lhs) != len(n.Rhs) || n.Tok != token.ASSIGN {
					bad(n, "providerfoundation.%s Route set by %s", name, n.Tok)
					continue
				}
				check(n, "providerfoundation."+name+" Route set", n.Rhs[i])
			}
		case *ast.CallExpr:
			switch fun := ast.Unparen(n.Fun).(type) {
			case *ast.Ident:
				if fun.Name == "fetchObject" {
					bad(n, "un-templated object fetch: use fetchObjectRoute")
				}
			case *ast.SelectorExpr:
				switch {
				case fun.Sel.Name == "Do" && len(n.Args) == 4:
					bad(n, "un-templated client request: use DoRoute")
				case fun.Sel.Name == "DoUnauthenticated" && len(n.Args) == 3:
					bad(n, "un-templated redirect request: use DoUnauthenticatedRoute")
				case fun.Sel.Name == "DoRoute" || fun.Sel.Name == "DoUnauthenticatedRoute":
					result.requestSites[pkg.PkgPath]++
					if len(n.Args) < 3 {
						bad(n, "%s with %d arguments", fun.Sel.Name, len(n.Args))
						return true
					}
					check(n, fun.Sel.Name+" route", n.Args[2])
				}
			}
		}
		return true
	})
}

// static returns "" when expr is provably a static route value, else why not
// (with the file:line of the value it cannot prove). seen holds the objects
// on the current proof path: a cycle adds no new value, so it proves nothing
// false.
func (ix *routeIndex) static(pkg *packages.Package, expr ast.Expr, seen map[any]bool) string {
	expr = ast.Unparen(expr)
	info := pkg.TypesInfo
	at := routePos(pkg, expr)
	if tv, ok := info.Types[expr]; ok && tv.Value != nil {
		if tv.Value.Kind() == constant.String {
			return ""
		}
		return at + ": a constant that is not a string"
	}
	switch e := expr.(type) {
	case *ast.BinaryExpr:
		if e.Op != token.ADD {
			return at + ": operator " + e.Op.String()
		}
		if why := ix.static(pkg, e.X, seen); why != "" {
			return why
		}
		return ix.static(pkg, e.Y, seen)
	case *ast.Ident:
		return ix.object(pkg, e, info.Uses[e], seen)
	case *ast.SelectorExpr:
		if sel := info.Selections[e]; sel != nil {
			if field, ok := sel.Obj().(*types.Var); ok && sel.Kind() == types.FieldVal {
				return ix.field(pkg, e, field.Origin(), seen)
			}
			return at + ": a method value"
		}
		return ix.object(pkg, e.Sel, info.Uses[e.Sel], seen)
	case *ast.IndexExpr:
		xt := info.TypeOf(e.X)
		if xt == nil {
			return at + ": an index read of an untyped value"
		}
		switch xt.Underlying().(type) {
		case *types.Map:
			return at + ": a map read"
		case *types.Slice, *types.Array:
			return ix.sliceStatic(pkg, e.X, seen)
		}
		return at + ": an index read"
	case *ast.CallExpr:
		if tv, ok := info.Types[e.Fun]; ok && tv.IsType() && len(e.Args) == 1 {
			return ix.static(pkg, e.Args[0], seen)
		}
		return at + ": a call result"
	case *ast.UnaryExpr:
		if e.Op == token.ARROW {
			return at + ": a channel receive"
		}
	case *ast.StarExpr:
		return at + ": a pointer read"
	case *ast.TypeAssertExpr:
		return at + ": a type assertion"
	}
	return fmt.Sprintf("%s: an unknown expression kind %T", at, expr)
}

func (ix *routeIndex) object(pkg *packages.Package, ident *ast.Ident, obj types.Object, seen map[any]bool) string {
	at := routePos(pkg, ident)
	switch o := obj.(type) {
	case *types.Const:
		if o.Val().Kind() == constant.String {
			return ""
		}
		return at + ": a constant that is not a string"
	case *types.Var:
		if o.IsField() {
			return ix.field(pkg, ident, o.Origin(), seen)
		}
		key := routeVarKey(o)
		if seen[key] {
			return ""
		}
		seen[key] = true
		param, isParam := ix.params[o]
		if isParam {
			if why := routeParamKind(at, o.Name(), param); why != "" {
				return why
			}
		}
		if why := ix.escaped[key]; why != "" {
			return at + ": " + o.Name() + ": " + why
		}
		if routePackageLevel(o) {
			source, ok := ix.pkgVarInit[key]
			if !ok {
				return at + ": package var " + o.Name() + " without a declaration in the loaded packages"
			}
			return ix.sourceStatic(source, seen)
		}
		sources := ix.locals[o]
		if isParam {
			for _, source := range sources {
				if why := ix.sourceStatic(source, seen); why != "" {
					return why + " (parameter " + o.Name() + " assigned in its function)"
				}
			}
			return ix.param(at, o.Name(), param, seen)
		}
		if len(sources) == 0 {
			return at + ": " + o.Name() + " has no assignment the census can read"
		}
		for _, source := range sources {
			if why := ix.sourceStatic(source, seen); why != "" {
				return why
			}
		}
		return ""
	case nil:
		return at + ": an unresolved name"
	}
	return fmt.Sprintf("%s: %s is a %T, not a value", at, ident.Name, obj)
}

// routeParamKind rejects a parameter whose callers the census cannot list.
func routeParamKind(at, name string, param routeParam) string {
	switch {
	case param.fn == "":
		return at + ": parameter " + name + " of a func literal (a call through a func value)"
	case param.method:
		return at + ": parameter " + name + " of a method of " + param.fn + " (a call through an interface cannot be ruled out)"
	case param.variadic:
		return at + ": variadic parameter " + name
	}
	return ""
}

// param proves a parameter by its argument at every call site.
func (ix *routeIndex) param(at, name string, param routeParam, seen map[any]bool) string {
	if why := ix.funcValue[param.fn]; why != "" {
		return at + ": parameter " + name + " of " + param.fn + ", " + why + " (a call through a func value)"
	}
	calls := ix.calls[param.fn]
	if len(calls) == 0 {
		return at + ": parameter " + name + " of " + param.fn + " has no call site in the loaded packages " + strings.Join(routeCensusPackages, " ")
	}
	for _, call := range calls {
		if call.call.Ellipsis.IsValid() {
			return routePos(call.pkg, call.call) + ": a call with a spread argument"
		}
		if param.index >= len(call.call.Args) {
			return routePos(call.pkg, call.call) + ": a call without argument " + name
		}
		if why := ix.static(call.pkg, call.call.Args[param.index], seen); why != "" {
			return why + " (argument " + name + " of " + param.fn + ")"
		}
	}
	return ""
}

func (ix *routeIndex) field(pkg *packages.Package, at ast.Node, field *types.Var, seen map[any]bool) string {
	where := routePos(pkg, at)
	switch {
	case field.Pkg() == nil || field.Pkg().Path() != pkg.PkgPath:
		return where + ": field " + field.Name() + " of a type from another package"
	case field.Exported():
		return where + ": exported field " + field.Name() + " (a decoder or another package can set it)"
	}
	if why := ix.fieldSet[field]; why != "" {
		return where + ": field " + field.Name() + ": " + why
	}
	if seen[field] {
		return ""
	}
	seen[field] = true
	for _, source := range ix.fieldVals[field] {
		if why := ix.sourceStatic(source, seen); why != "" {
			return why
		}
	}
	return ""
}

func (ix *routeIndex) sourceStatic(source routeSource, seen map[any]bool) string {
	switch {
	case source.kind == "range":
		if xt := source.pkg.TypesInfo.TypeOf(source.expr); xt != nil {
			switch xt.Underlying().(type) {
			case *types.Slice, *types.Array:
				return ix.sliceStatic(source.pkg, source.expr, seen)
			}
		}
		return routePos(source.pkg, source.expr) + ": a range over a value that is not a slice of routes"
	case source.kind != "":
		return source.kind
	case source.expr == nil:
		return ""
	}
	return ix.static(source.pkg, source.expr, seen)
}

// sliceStatic proves every element of a slice: a composite literal of static
// values, or a local slice whose every source is one.
func (ix *routeIndex) sliceStatic(pkg *packages.Package, expr ast.Expr, seen map[any]bool) string {
	expr = ast.Unparen(expr)
	at := routePos(pkg, expr)
	switch e := expr.(type) {
	case *ast.CompositeLit:
		for _, element := range e.Elts {
			if pair, ok := element.(*ast.KeyValueExpr); ok {
				element = pair.Value
			}
			if why := ix.static(pkg, element, seen); why != "" {
				return why
			}
		}
		return ""
	case *ast.Ident:
		v, ok := pkg.TypesInfo.Uses[e].(*types.Var)
		if !ok || v.IsField() || routePackageLevel(v) {
			return at + ": a read of a package slice, not a local slice of routes"
		}
		if _, isParam := ix.params[v]; isParam {
			return at + ": a read of slice parameter " + v.Name()
		}
		if why := ix.escaped[routeVarKey(v)]; why != "" {
			return at + ": " + v.Name() + ": " + why
		}
		if seen[v] {
			return ""
		}
		seen[v] = true
		sources := ix.locals[v]
		if len(sources) == 0 {
			return at + ": slice " + v.Name() + " has no assignment the census can read"
		}
		for _, source := range sources {
			switch {
			case source.kind != "":
				return at + ": slice " + v.Name() + ": " + source.kind
			case source.expr == nil:
				continue
			}
			literal, ok := ast.Unparen(source.expr).(*ast.CompositeLit)
			if !ok {
				return routePos(source.pkg, source.expr) + ": slice " + v.Name() + " is set from a value that is not a literal"
			}
			if why := ix.sliceStatic(source.pkg, literal, seen); why != "" {
				return why
			}
		}
		return ""
	}
	return at + ": a slice read of an expression, not a local slice of routes"
}
