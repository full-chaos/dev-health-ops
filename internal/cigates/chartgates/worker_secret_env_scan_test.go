package chartgates_test

import (
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/tools/go/packages"

	"github.com/full-chaos/dev-health-ops/internal/platform/dsnclass"
)

// A name is read by a Go source when a non-test file holds it as a string constant or literal in code (a comment is not
// code) and the same file calls an environment lookup. A key read only by a test is not a product read. The scan
// type-checks the packages (go/types, with their test files and the CI build tags) and decides by the callee object:
//
//   - a function in envLookupFuncs, matched by package path: os.Getenv, os.LookupEnv, syscall.Getenv and the env readers
//     of internal/platform/secrets, under any import alias, a dot import, or unqualified inside the secrets package;
//
//   - a variable, parameter or struct field (embedded too) whose type is, or is a named type or alias of,
//     func(string) string or func(string) (string, bool), unless it is a non-env holder (nonEnvLookupHolders by package
//     and identifier, or a redactor word of nonEnvLookupWords). Copies count by their own type, however they are made.
//
//   - a function or method that passes one of its string parameters straight to such a lookup (envWrappers), also
//     when called through an interface method (or a constraint's) that such a concrete method implements.
//
// Any other method never counts, whatever its name or signature.
// A name that appears only in a comment, or in a file that never looks anything up, is not read.
const (
	modulePath        = "github.com/full-chaos/dev-health-ops"
	secretsImportPath = modulePath + "/internal/platform/secrets"
	secretsDir        = "internal/platform/secrets"
)

// nonEnvLookupWords are the name words of string-lookup-shaped holders that redact a value and read no environment.
var nonEnvLookupWords = map[string]string{
	"scrub":    "redacts a value",
	"redact":   "redacts a value",
	"mask":     "masks a value",
	"sanitize": "cleans a value",
}

type lookupHolder struct{ dir, name string }

// nonEnvLookupHolders are the string-lookup-shaped holders and types of the tree that read no environment, each by its
// package directory and identifier, with the reason. A holder of the same name in another package still counts. Every
// other func(string) string or func(string) (string, bool) holder counts as an environment lookup.
var nonEnvLookupHolders = map[lookupHolder]string{
	{"internal/auth/identity/oidc", "Cookie"}:                  "reads a request cookie",
	{"internal/auth/identity/saml", "cookie"}:                  "reads a request cookie",
	{"internal/websmoke", "Documents"}:                         "reads a registered GraphQL document",
	{"internal/cigates/docsbuild", "Revision"}:                 "reads a git revision",
	{"internal/testsupport/authrows", "Rewrite"}:               "rewrites a recorded row value",
	{"internal/queryapi/analytics", "rewrite"}:                 "rewrites an SQL expression",
	{"internal/jobs/metrics/aiworkflow", "find"}:               "matches a body",
	{"internal/syncdispatchruntime", "build"}:                  "builds an SQL predicate",
	{"internal/platform/logging", "cut"}:                       "cuts a value",
	{"internal/goapiproof", "digestFn"}:                        "digests a document",
	{"internal/cigates/affected", "importPath"}:                "maps an import path",
	{"internal/queryapi/investmentexplain", "label"}:           "labels a key",
	{"internal/streamrunner", "read"}:                          "reads a stored row field",
	{"internal/llmorgsettings", "value"}:                       "reads a stored setting value",
	{"internal/jobs/metrics/daily/icfinalize", "TeamNames"}:    "lists team names",
	{"internal/jobs/metrics/daily/icfinalize", "TeamResolver"}: "resolves a team name",
	{"internal/api/syncadmin", "component"}:                    "reads a PagerDuty config object key",
	{"internal/apiservice/admin", "text"}:                      "reads a response detail key",
	{"internal/auth/contracts/tokencorpus", "q"}:               "quotes a string",
	{"internal/cigates/affected", "table"}:                     "maps a file to its table path",
	{"internal/cigates/docstools", "cell"}:                     "reads a table row cell",
	{"internal/cigates/docstools", "get"}:                      "reads a CSV row field",
	{"internal/cigates/docstools", "invTxThemeOf"}:             "maps a subcategory to its theme",
	{"internal/fixturescli", "flag"}:                           "formats a target flag",
	{"internal/gqlgenguard", "rel"}:                            "maps a path to the module root",
	{"internal/platform/credshape", "segment"}:                 "encodes a probe segment",
	{"internal/providersync", "canonical"}:                     "canonicalises a work item id",
	{"internal/providersync", "label"}:                         "labels a value",
	{"internal/providersync", "writerOf"}:                      "names a relation writer",
	{"internal/queryapi/digest", "Document"}:                   "digests a document",
	{"internal/queryapi/hotspots", "repoLabel"}:                "labels a repository id",
	{"internal/scheduler/sync", "derive"}:                      "derives a materialization id",
	{"internal/scheduler/sync", "field"}:                       "reads a project JSON field",
	{"internal/storage/postgres/authschema", "qualified"}:      "qualifies a table name",
	{"internal/storage/roleacl", "on"}:                         "renders an SQL class condition",
	{"internal/storage/roleacl", "own"}:                        "renders an SQL ownership case",
	{"internal/syncdispatchruntime", "get"}:                    "reads a budget bucket key",
	{"internal/syncdispatchruntime", "text"}:                   "reads an observation key",
	{"internal/testsupport/chlogin", "with"}:                   "builds a DSN with a password",
	{"internal/testsupport/idpstub", "esc"}:                    "escapes a string",
	{"internal/testsupport/sessionscenario", "aliceAccess"}:    "reads a scenario token",
	{"internal/testsupport/sessionscenario", "aliceRefresh"}:   "reads a scenario token",
	{"internal/testsupport/sessionscenario", "latest"}:         "reads a scenario token",
	{"internal/testsupport/sessionscenario", "successor"}:      "reads a scenario token",
}

var envLookupFuncs = map[string]map[string]bool{
	"os":      {"Getenv": true, "LookupEnv": true},
	"syscall": {"Getenv": true},
	secretsImportPath: {
		"GetenvSecret": true, "GetenvNamed": true, "GetenvLogin": true, "GetenvDSN": true, "LookupSecretEnv": true,
		"ProcessLookup": true, "ProcessGetenv": true, "Resolve": true, "ResolveSecret": true, "ResolveDSNSetting": true,
		"ResolveFlag": true,
	},
}

// envUnit is one type-checked package: its files by path relative to the module root, test files included.
type envUnit struct {
	files map[string]*ast.File
	info  *types.Info
	pkg   *types.Package
	fset  *token.FileSet
}

func scanEnvReaders(sources map[string]string) map[string][]string {
	return scanEnvUnits(checkSources(sources))
}

// scanEnvUnits scans each build variant (the units of one load, which share a FileSet) with the wrappers of that variant
// only, and joins the readers: a package path can declare a wrapper under one build tag set and a same-named
// non-wrapper under the other.
func scanEnvUnits(units []envUnit) map[string][]string {
	var order []*token.FileSet
	variants := map[*token.FileSet][]envUnit{}
	for _, unit := range units {
		if _, seen := variants[unit.fset]; !seen {
			order = append(order, unit.fset)
		}
		variants[unit.fset] = append(variants[unit.fset], unit)
	}
	found := map[string]map[string]bool{}
	for _, fset := range order {
		scanEnvVariant(variants[fset], found)
	}
	readers := map[string][]string{}
	for name, paths := range found {
		for path := range paths {
			readers[name] = append(readers[name], path)
		}
		sort.Strings(readers[name])
	}
	return readers
}

func scanEnvVariant(units []envUnit, found map[string]map[string]bool) {
	wrappers := envWrappers(units)
	for _, unit := range units {
		for path, file := range unit.files {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			lookups := false
			names := map[string]bool{}
			ast.Inspect(file, func(node ast.Node) bool {
				switch typed := node.(type) {
				case *ast.CallExpr:
					lookups = lookups || isEnvLookupCall(unit, typed, wrappers)
				case *ast.BasicLit:
					if typed.Kind == token.STRING {
						if value, err := strconv.Unquote(typed.Value); err == nil {
							names[value] = true
						}
					}
				}
				return true
			})
			if !lookups {
				continue
			}
			for name := range names {
				if found[name] == nil {
					found[name] = map[string]bool{}
				}
				found[name][path] = true
			}
		}
	}
}

// isEnvLookupCall reports whether a call's callee is an environment lookup by its object. wrappers holds the functions
// and methods that pass one of their string parameters straight to a lookup. An explicit instance of a generic function
// (Read[int], pkg.Read[K, V]) is its generic declaration.
func isEnvLookupCall(unit envUnit, call *ast.CallExpr, wrappers *envWrapperSet) bool {
	info := unit.info
	var obj types.Object
	callee := ast.Unparen(call.Fun)
	switch index := callee.(type) {
	case *ast.IndexExpr:
		callee = ast.Unparen(index.X)
	case *ast.IndexListExpr:
		callee = ast.Unparen(index.X)
	}
	switch fun := callee.(type) {
	case *ast.Ident:
		obj = info.Uses[fun]
	case *ast.SelectorExpr:
		if selection, ok := info.Selections[fun]; ok {
			if selection.Kind() != types.FieldVal {
				method, isFunc := selection.Obj().(*types.Func)
				return isFunc && wrappers.calls(unit.pkg, method)
			}
			obj = selection.Obj()
		} else {
			obj = info.Uses[fun.Sel]
		}
	}
	switch typed := obj.(type) {
	case *types.Func:
		if typed.Pkg() == nil {
			return false
		}
		if wrappers.calls(unit.pkg, typed) {
			return true
		}
		if typed.Signature().Recv() != nil {
			return false
		}
		return envLookupFuncs[typed.Pkg().Path()][typed.Name()]
	case *types.Var:
		return wrappers.holds(typed) || isStringLookup(typed.Type()) && !wrappers.nonEnvLookupHolder(unit, typed)
	}
	return false
}

// envWrappers finds the functions and methods whose body passes one of their string parameters, as the argument itself,
// to an environment lookup (or to another wrapper): loader.getenv(name) { return loader.Getenv(name) }. A call of a
// wrapper is a lookup wherever it is, so a file that reads its names through a wrapper method or a helper of another
// package is a reader. Keys are types.Func.FullName of the generic declaration (Origin), the same for a package loaded
// from source and from export data and for every instance of a generic receiver. A function literal held by a
// variable (var read = func(name, fallback string) string {...}, or assigned to it) is a wrapper the same way, and a
// call through that variable is a lookup.
// Only non-test files define wrappers: a test fake that implements a product interface must not make every call
// through that interface a read.
func envWrappers(units []envUnit) *envWrapperSet {
	set := &envWrapperSet{keys: map[string]bool{}, vars: map[string]bool{}, reach: map[*types.Package]map[string]*types.Package{}, dirs: map[*token.FileSet]map[string]string{}}
	for _, unit := range units {
		if unit.fset == nil {
			continue
		}
		if set.dirs[unit.fset] == nil {
			set.dirs[unit.fset] = map[string]string{}
		}
		for path, file := range unit.files {
			set.dirs[unit.fset][unit.fset.PositionFor(file.Pos(), false).Filename] = filepath.ToSlash(filepath.Dir(path))
		}
	}
	wraps := func(unit envUnit, sig *ast.FuncType, body *ast.BlockStmt) bool {
		params := map[types.Object]bool{}
		if sig.Params != nil {
			for _, field := range sig.Params.List {
				for _, name := range field.Names {
					if param, ok := unit.info.Defs[name].(*types.Var); ok && isStringName(param.Type()) {
						params[param] = true
					}
				}
			}
		}
		found := false
		if len(params) == 0 || body == nil {
			return false
		}
		ast.Inspect(body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if found || !ok || !isEnvLookupCall(unit, call, set) {
				return !found
			}
			for _, arg := range call.Args {
				if id, ok := unconvert(unit.info, arg).(*ast.Ident); ok && params[unit.info.Uses[id]] {
					found = true
				}
			}
			return !found
		})
		return found
	}
	for changed := true; changed; {
		changed = false
		for _, unit := range units {
			for path, file := range unit.files {
				if strings.HasSuffix(path, "_test.go") {
					continue
				}
				for _, decl := range file.Decls {
					fn, ok := decl.(*ast.FuncDecl)
					if !ok {
						continue
					}
					obj, ok := unit.info.Defs[fn.Name].(*types.Func)
					if !ok || set.keys[obj.FullName()] || !wraps(unit, fn.Type, fn.Body) {
						continue
					}
					set.keys[obj.FullName()], changed = true, true
					if obj.Signature().Recv() != nil {
						set.methods = append(set.methods, obj)
					}
				}
				ast.Inspect(file, func(node ast.Node) bool {
					var names, values []ast.Expr
					switch typed := node.(type) {
					case *ast.ValueSpec:
						for _, name := range typed.Names {
							names = append(names, name)
						}
						values = typed.Values
					case *ast.AssignStmt:
						names, values = typed.Lhs, typed.Rhs
					default:
						return true
					}
					if len(names) != len(values) {
						return true
					}
					for i, value := range values {
						lit, ok := ast.Unparen(value).(*ast.FuncLit)
						id, named := ast.Unparen(names[i]).(*ast.Ident)
						if !ok || !named {
							continue
						}
						holder, ok := unit.info.Defs[id].(*types.Var)
						if !ok {
							holder, ok = unit.info.Uses[id].(*types.Var)
						}
						if !ok || set.holds(holder) || !wraps(unit, lit.Type, lit.Body) {
							continue
						}
						set.vars[varKey(holder)], changed = true, true
					}
					return true
				})
			}
		}
	}
	return set
}

// isStringName reports whether a parameter of type t can carry a key name: its underlying type is string (string or a
// named string type), or it is a type parameter whose every type term is such a type (T ~string, T key | ~string).
func isStringName(t types.Type) bool {
	if param, ok := types.Unalias(t).(*types.TypeParam); ok {
		iface, ok := param.Constraint().Underlying().(*types.Interface)
		return ok && stringTerms(iface)
	}
	return types.Identical(t.Underlying(), types.Typ[types.String])
}

// stringTerms reports whether a constraint has at least one type term and every term is a string type.
func stringTerms(iface *types.Interface) bool {
	terms := false
	for i := 0; i < iface.NumEmbeddeds(); i++ {
		switch embedded := types.Unalias(iface.EmbeddedType(i)).(type) {
		case *types.Union:
			for j := 0; j < embedded.Len(); j++ {
				if !types.Identical(embedded.Term(j).Type().Underlying(), types.Typ[types.String]) {
					return false
				}
			}
			terms = true
		default:
			if inner, ok := embedded.Underlying().(*types.Interface); ok {
				if inner.IsMethodSet() {
					continue
				}
				if !stringTerms(inner) {
					return false
				}
				terms = true
				continue
			}
			if !isStringName(embedded) {
				return false
			}
			terms = true
		}
	}
	return terms
}

// unconvert strips parentheses and type conversions from an expression: string(name), key(name), pkg.Key((name)).
func unconvert(info *types.Info, expr ast.Expr) ast.Expr {
	for {
		expr = ast.Unparen(expr)
		call, ok := expr.(*ast.CallExpr)
		if !ok || len(call.Args) != 1 || call.Ellipsis.IsValid() {
			return expr
		}
		var target types.Object
		switch fun := ast.Unparen(call.Fun).(type) {
		case *ast.Ident:
			target = info.Uses[fun]
		case *ast.SelectorExpr:
			target = info.Uses[fun.Sel]
		}
		if _, isType := target.(*types.TypeName); !isType {
			return expr
		}
		expr = call.Args[0]
	}
}

// varKey names a variable by its package path and name when it is package-level (the same from source and from export
// data), else also by its position.
func varKey(v *types.Var) string {
	if v.Pkg() == nil {
		return ""
	}
	if v.Parent() == v.Pkg().Scope() {
		return v.Pkg().Path() + "." + v.Name()
	}
	return v.Pkg().Path() + "." + v.Name() + "@" + strconv.Itoa(int(v.Pos()))
}

// envWrapperSet holds the wrappers by types.Func.FullName and the concrete wrapper methods, so a call through an
// interface method (a named or literal interface, an embedded one, a type parameter's constraint) that a concrete
// wrapper method implements counts too.
type envWrapperSet struct {
	keys    map[string]bool
	vars    map[string]bool
	methods []*types.Func
	reach   map[*types.Package]map[string]*types.Package
	dirs    map[*token.FileSet]map[string]string
}

// holds reports whether a variable holds a function literal wrapper.
func (w *envWrapperSet) holds(v *types.Var) bool {
	key := varKey(v)
	return key != "" && w.vars[key]
}

func (w *envWrapperSet) calls(caller *types.Package, fn *types.Func) bool {
	if w.keys[fn.Origin().FullName()] {
		return true
	}
	recv := fn.Signature().Recv()
	if recv == nil {
		return false
	}
	iface, ok := recv.Type().Underlying().(*types.Interface)
	if !ok {
		return false
	}
	for _, method := range w.methods {
		if method.Name() != fn.Name() {
			continue
		}
		for _, concrete := range w.concreteTypes(caller, method, iface) {
			if types.Implements(concrete, iface) || types.Satisfies(concrete, iface) {
				return true
			}
		}
	}
	return false
}

// concreteTypes is the receiver type of a wrapper method and its pointer, from the caller's own type universe when the
// caller's import graph holds the method's package (the same objects the interface was checked against), and from the
// method's own universe. A generic receiver type is instantiated with its own type parameters (a method whose signature
// does not use them matches an interface as every instance does), and with the type arguments that unify its methods
// with the interface's methods (Lookup(string) T against Lookup(string) string gives T = string), checked against the
// type's constraints.
func (w *envWrapperSet) concreteTypes(caller *types.Package, method *types.Func, iface *types.Interface) []types.Type {
	recv := method.Signature().Recv().Type()
	if ptr, ok := recv.(*types.Pointer); ok {
		recv = ptr.Elem()
	}
	named, ok := types.Unalias(recv).(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return nil
	}
	var out []types.Type
	add := func(t types.Type) {
		generic, ok := t.(*types.Named)
		if !ok || generic.TypeParams().Len() == 0 {
			out = append(out, t, types.NewPointer(t))
			return
		}
		origin := generic.Origin()
		own := make([]types.Type, origin.TypeParams().Len())
		for i := range own {
			own[i] = origin.TypeParams().At(i)
		}
		instance, err := types.Instantiate(nil, origin, own, false)
		if err != nil {
			return
		}
		out = append(out, instance, types.NewPointer(instance))
		if args := unifiedTypeArgs(instance, origin.TypeParams(), iface); args != nil {
			if bound, err := types.Instantiate(nil, origin, args, true); err == nil {
				out = append(out, bound, types.NewPointer(bound))
			}
		}
	}
	add(named)
	if pkg := w.reachable(caller)[named.Obj().Pkg().Path()]; pkg != nil && pkg != named.Obj().Pkg() {
		if obj, ok := pkg.Scope().Lookup(named.Obj().Name()).(*types.TypeName); ok {
			add(obj.Type())
		}
	}
	return out
}

// unifiedTypeArgs binds the type parameters of a generic type, instantiated with its own parameters, by unifying each
// interface method with the type's method of the same name. It is nil when no parameter is bound or a method does not
// unify; an unbound parameter stays itself.
func unifiedTypeArgs(instance types.Type, tparams *types.TypeParamList, iface *types.Interface) []types.Type {
	own := map[*types.TypeParam]bool{}
	for i := 0; i < tparams.Len(); i++ {
		own[tparams.At(i)] = true
	}
	bound := map[*types.TypeParam]types.Type{}
	for i := 0; i < iface.NumMethods(); i++ {
		want := iface.Method(i)
		obj, _, _ := types.LookupFieldOrMethod(instance, true, want.Pkg(), want.Name())
		have, ok := obj.(*types.Func)
		if !ok {
			return nil
		}
		if !unify(have.Signature(), want.Signature(), own, bound) {
			return nil
		}
	}
	if len(bound) == 0 {
		return nil
	}
	args := make([]types.Type, tparams.Len())
	for i := range args {
		args[i] = tparams.At(i)
		if arg, ok := bound[tparams.At(i)]; ok {
			args[i] = arg
		}
	}
	return args
}

// unify matches x, which may use the type parameters own, against y, binding each parameter at most one type. It looks
// through aliases (a generic alias instance is its right-hand side) and into every composite type form that can hold a
// parameter: pointer, slice, array, map, channel, signature, struct, method-set interface and generic named instance.
func unify(x, y types.Type, own map[*types.TypeParam]bool, bound map[*types.TypeParam]types.Type) bool {
	x, y = types.Unalias(x), types.Unalias(y)
	if param, ok := x.(*types.TypeParam); ok && own[param] {
		if arg, ok := bound[param]; ok {
			return types.Identical(arg, y)
		}
		bound[param] = y
		return true
	}
	switch x := x.(type) {
	case *types.Pointer:
		y, ok := y.(*types.Pointer)
		return ok && unify(x.Elem(), y.Elem(), own, bound)
	case *types.Slice:
		y, ok := y.(*types.Slice)
		return ok && unify(x.Elem(), y.Elem(), own, bound)
	case *types.Array:
		y, ok := y.(*types.Array)
		return ok && x.Len() == y.Len() && unify(x.Elem(), y.Elem(), own, bound)
	case *types.Map:
		y, ok := y.(*types.Map)
		return ok && unify(x.Key(), y.Key(), own, bound) && unify(x.Elem(), y.Elem(), own, bound)
	case *types.Chan:
		y, ok := y.(*types.Chan)
		return ok && x.Dir() == y.Dir() && unify(x.Elem(), y.Elem(), own, bound)
	case *types.Signature:
		y, ok := y.(*types.Signature)
		return ok && x.Variadic() == y.Variadic() && unifyTuple(x.Params(), y.Params(), own, bound) && unifyTuple(x.Results(), y.Results(), own, bound)
	case *types.Struct:
		y, ok := y.(*types.Struct)
		if !ok || x.NumFields() != y.NumFields() {
			return false
		}
		for i := 0; i < x.NumFields(); i++ {
			fx, fy := x.Field(i), y.Field(i)
			if fx.Id() != fy.Id() || fx.Embedded() != fy.Embedded() || x.Tag(i) != y.Tag(i) || !unify(fx.Type(), fy.Type(), own, bound) {
				return false
			}
		}
		return true
	case *types.Interface:
		y, ok := y.(*types.Interface)
		if !ok || !x.IsMethodSet() || !y.IsMethodSet() || x.NumMethods() != y.NumMethods() {
			return ok && types.Identical(x, y)
		}
		for i := 0; i < x.NumMethods(); i++ {
			mx, my := x.Method(i), y.Method(i)
			if mx.Id() != my.Id() || !unify(mx.Signature(), my.Signature(), own, bound) {
				return false
			}
		}
		return true
	case *types.Named:
		y, ok := y.(*types.Named)
		if !ok || x.TypeArgs().Len() == 0 || x.TypeArgs().Len() != y.TypeArgs().Len() || x.Origin().Obj() != y.Origin().Obj() {
			return types.Identical(x, y)
		}
		for i := 0; i < x.TypeArgs().Len(); i++ {
			if !unify(x.TypeArgs().At(i), y.TypeArgs().At(i), own, bound) {
				return false
			}
		}
		return true
	}
	return types.Identical(x, y)
}

func unifyTuple(x, y *types.Tuple, own map[*types.TypeParam]bool, bound map[*types.TypeParam]types.Type) bool {
	if x.Len() != y.Len() {
		return false
	}
	for i := 0; i < x.Len(); i++ {
		if !unify(x.At(i).Type(), y.At(i).Type(), own, bound) {
			return false
		}
	}
	return true
}

func (w *envWrapperSet) reachable(caller *types.Package) map[string]*types.Package {
	if caller == nil {
		return nil
	}
	if seen, ok := w.reach[caller]; ok {
		return seen
	}
	seen := map[string]*types.Package{}
	queue := []*types.Package{caller}
	for len(queue) > 0 {
		pkg := queue[0]
		queue = queue[1:]
		if _, done := seen[pkg.Path()]; done {
			continue
		}
		seen[pkg.Path()] = pkg
		queue = append(queue, pkg.Imports()...)
	}
	w.reach[caller] = seen
	return seen
}

// isStringLookup reports whether a type is, or is a named type or alias of, func(string) string or
// func(string) (string, bool).
func isStringLookup(t types.Type) bool {
	sig, ok := t.Underlying().(*types.Signature)
	if !ok || sig.Variadic() || sig.Params().Len() != 1 || !types.Identical(sig.Params().At(0).Type(), types.Typ[types.String]) {
		return false
	}
	results := sig.Results()
	switch results.Len() {
	case 1:
		return types.Identical(results.At(0).Type(), types.Typ[types.String])
	case 2:
		return types.Identical(results.At(0).Type(), types.Typ[types.String]) && types.Identical(results.At(1).Type(), types.Typ[types.Bool])
	}
	return false
}

// nonEnvLookupHolder reports whether a lookup-shaped holder reads no environment: the holder, or the named type it has,
// is in nonEnvLookupHolders by source directory and identifier, or its name has a word of nonEnvLookupWords and no
// word that names the environment (env, getenv, environment, lookup): redactEnv still counts.
func (w *envWrapperSet) nonEnvLookupHolder(unit envUnit, holder *types.Var) bool {
	if _, ok := nonEnvLookupHolders[lookupHolder{w.sourceDir(unit, holder), holder.Name()}]; ok {
		return true
	}
	if named, ok := types.Unalias(holder.Type()).(*types.Named); ok {
		if _, ok := nonEnvLookupHolders[lookupHolder{w.sourceDir(unit, named.Obj()), named.Obj().Name()}]; ok {
			return true
		}
	}
	exempt := false
	for _, word := range nameWords.FindAllString(holder.Name(), -1) {
		word = strings.ToLower(word)
		switch word {
		case "env", "getenv", "environment", "lookup":
			return false
		}
		if _, ok := nonEnvLookupWords[word]; ok {
			exempt = true
		}
	}
	return exempt
}

// nameWords splits a Go name into its words: KeyScrub -> Key Scrub, lookupEnv -> lookup Env, team_names -> team names.
var nameWords = regexp.MustCompile(`[A-Z]+[a-z0-9]*|[a-z0-9]+`)

// sourceDir is the directory, relative to the module root, of the scanned file that declares obj (the position is in
// the FileSet of the caller's unit, the same type universe). An object of a package the scan did not load from source
// has its package path relative to the module root. An external test package (dir_test) and a package in the
// directory dir_test are told apart by the file, never by the path.
func (w *envWrapperSet) sourceDir(unit envUnit, obj types.Object) string {
	if unit.fset != nil {
		if dir, ok := w.dirs[unit.fset][unit.fset.PositionFor(obj.Pos(), false).Filename]; ok {
			return dir
		}
	}
	if obj.Pkg() == nil {
		return ""
	}
	return strings.TrimPrefix(obj.Pkg().Path(), modulePath+"/")
}

// checkSources type-checks in-memory sources by package directory and package clause. An import of a module package the
// sources hold is checked from them; any other import comes from the installed export data, and one that resolves
// nowhere stays unresolved (its uses are no lookup).
func checkSources(sources map[string]string) []envUnit {
	fset := token.NewFileSet()
	type pkgKey struct{ dir, name string }
	byPkg := map[pkgKey]map[string]*ast.File{}
	for path, src := range sources {
		file, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			continue
		}
		key := pkgKey{filepath.ToSlash(filepath.Dir(path)), file.Name.Name}
		if byPkg[key] == nil {
			byPkg[key] = map[string]*ast.File{}
		}
		byPkg[key][path] = file
	}
	im := &sourceImporter{fset: fset, std: importer.ForCompiler(fset, "gc", goListExport), done: map[string]*types.Package{}, pkgs: map[string]map[string]*ast.File{}}
	for key, files := range byPkg {
		if !strings.HasSuffix(key.name, "_test") {
			im.pkgs[key.dir] = files
		}
	}
	var units []envUnit
	for key, files := range byPkg {
		path := modulePath + "/" + key.dir
		if strings.HasSuffix(key.name, "_test") {
			path += "_test"
		}
		info := im.check(path, files)
		units = append(units, envUnit{files: files, info: info, pkg: im.done[path], fset: fset})
	}
	return units
}

type sourceImporter struct {
	fset *token.FileSet
	std  types.Importer
	done map[string]*types.Package
	pkgs map[string]map[string]*ast.File
}

func (im *sourceImporter) Import(path string) (*types.Package, error) {
	if pkg, ok := im.done[path]; ok {
		return pkg, nil
	}
	if dir, ok := strings.CutPrefix(path, modulePath+"/"); ok {
		if files, held := im.pkgs[dir]; held {
			im.check(path, files)
			return im.done[path], nil
		}
	}
	pkg, err := im.std.Import(path)
	if err != nil && (!strings.Contains(strings.SplitN(path, "/", 2)[0], ".") || strings.HasPrefix(path, modulePath+"/")) {
		panic(fmt.Sprintf("reader scan fixture: import %q failed, so its lookups would resolve to nothing: %v", path, err))
	}
	return pkg, err
}

// goListExport finds a package's export data with the go command on PATH. importer.Default asks go/build's GOROOT,
// which is empty in a -trimpath test binary when GOROOT is not in the environment (the CI legs run go test -trimpath
// with GOTOOLCHAIN=local), so every standard import failed there and resolved to nothing.
func goListExport(path string) (io.ReadCloser, error) {
	file, ok := goListExports.Load(path)
	if !ok {
		out, err := exec.Command("go", "list", "-export", "-f", "{{.Export}}", path).Output()
		if err != nil {
			return nil, fmt.Errorf("go list -export %s: %w", path, err)
		}
		file, _ = goListExports.LoadOrStore(path, strings.TrimSpace(string(out)))
	}
	if file.(string) == "" {
		return nil, fmt.Errorf("go list -export %s: no export data", path)
	}
	return os.Open(file.(string))
}

var goListExports sync.Map

func (im *sourceImporter) check(path string, files map[string]*ast.File) *types.Info {
	info := &types.Info{Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	list := make([]*ast.File, 0, len(files))
	for _, file := range files {
		list = append(list, file)
	}
	config := types.Config{Importer: im, Error: func(error) {}}
	pkg, _ := config.Check(path, im.fset, list, info)
	im.done[path] = pkg
	return info
}

// realEnvTree is the type-checked tree, loaded once per test binary: the packages under internal and cmd, with their
// test files, once without build tags and once with -tags=integration (the two CI legs).
var realEnvTree struct {
	once    sync.Once
	units   []envUnit
	errs    []string
	elapsed time.Duration
	readers envReaderCache
}

// loadEnvUnits keeps each packages.Load's units and errors private until it completes.
// An optional environment lets tiny fixture modules disable proxies and inherited GOFLAGS.
func loadEnvUnits(root string, flags []string, env ...[]string) (units []envUnit, errs []string) {
	config := &packages.Config{
		Mode:       packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:        root,
		Tests:      true,
		BuildFlags: flags,
	}
	if len(env) > 0 {
		config.Env = env[0]
	}
	pkgs, err := packages.Load(config, "./internal/...", "./cmd/...")
	if err != nil {
		return nil, []string{err.Error()}
	}
	for _, pkg := range pkgs {
		for _, perr := range pkg.Errors {
			errs = append(errs, pkg.ID+": "+perr.Error())
		}
		files := map[string]*ast.File{}
		for _, file := range pkg.Syntax {
			rel, rerr := filepath.Rel(root, pkg.Fset.Position(file.Pos()).Filename)
			if rerr != nil || strings.HasPrefix(rel, "..") {
				continue
			}
			files[filepath.ToSlash(rel)] = file
		}
		units = append(units, envUnit{files: files, info: pkg.TypesInfo, pkg: pkg.Types, fset: pkg.Fset})
	}
	return units, errs
}

func envTreeSerial(env []string) bool {
	for i := len(env) - 1; i >= 0; i-- {
		if value, ok := strings.CutPrefix(env[i], "CHARTGATES_ENVTREE_SERIAL="); ok {
			return value == "1"
		}
	}
	return false
}

func loadEnvTree(root string, env []string) (units []envUnit, errs []string) {
	flags := [][]string{nil, {"-tags=integration"}}
	type result struct {
		units []envUnit
		errs  []string
	}
	results := make([]result, len(flags))
	load := func(i int) { results[i].units, results[i].errs = loadEnvUnits(root, flags[i], env) }
	if envTreeSerial(env) {
		for i := range flags {
			load(i)
		}
	} else {
		var done sync.WaitGroup
		for i := range flags {
			done.Add(1)
			go func() {
				defer done.Done()
				load(i)
			}()
		}
		done.Wait()
	}
	// Preserve the serial order, including errors: default tags, then integration.
	for _, result := range results {
		units = append(units, result.units...)
		errs = append(errs, result.errs...)
	}
	return units, errs
}

func loadRealEnvTree(root string) {
	start := time.Now()
	env := os.Environ()
	realEnvTree.units, realEnvTree.errs = loadEnvTree(root, env)
	realEnvTree.elapsed = time.Since(start)
	mode := "concurrently"
	if envTreeSerial(env) {
		mode = "serially"
	}
	fmt.Fprintf(os.Stderr, "chartgates: env tree loaded %s in %s\n", mode, realEnvTree.elapsed)
}

// envReaderCache publishes one scan and returns independent maps and nested slices.
type envReaderCache struct {
	once    sync.Once
	readers map[string][]string
}

func (cache *envReaderCache) scan(units []envUnit) map[string][]string {
	cache.once.Do(func() { cache.readers = scanEnvUnits(units) })
	out := make(map[string][]string, len(cache.readers))
	for name, files := range cache.readers {
		out[name] = append([]string(nil), files...)
	}
	return out
}

// realEnvReaders scans the non-test Go sources of the tree under g.root.
func realEnvReaders(g *guard) map[string][]string {
	realEnvTree.once.Do(func() { loadRealEnvTree(g.root) })
	g.expect(len(realEnvTree.errs) == 0, "the reader scan could not type-check the tree: %s", strings.Join(firstN(realEnvTree.errs, 5), "; "))
	g.expect(len(realEnvTree.units) > 0, "no Go package was found under %s, so the reader scan measured nothing", g.root)
	return realEnvTree.readers.scan(realEnvTree.units)
}

func firstN(in []string, n int) []string {
	if len(in) > n {
		return in[:n]
	}
	return in
}

// secretsStub stands in for the secrets package in the single-file cells, so they type-check without the real tree.
const secretsStub = "package secrets\nimport \"os\"\ntype LookupEnv func(string) (string, bool)\ntype Value struct{}\n" +
	"func GetenvSecret(name string) string { return os.Getenv(name) }\n" +
	"func ResolveSecret(key string, lookup LookupEnv) (Value, bool, error) {\n_, ok := lookup(key)\nreturn Value{}, ok, nil\n}\n"

func TestWorkerSecretKeyReaderScan(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want bool
	}{
		{"E01-name-only-in-a-comment", "package x\nimport \"os\"\n// reads \"SOME_KEY\"\nfunc f() string { return os.Getenv(\"OTHER\") }\n", false},
		{"E02-name-in-a-file-with-no-lookup", "package x\nvar table = map[string]string{\"SOME_KEY\": \"v\"}\n", false},
		{"E03-os-lookupenv-literal", "package x\nimport \"os\"\nfunc f() { _, _ = os.LookupEnv(\"SOME_KEY\") }\n", true},
		{"E04-constant-name-and-getenv", "package x\nimport \"os\"\nconst envKey = \"SOME_KEY\"\nfunc f() string { return os.Getenv(envKey) }\n", true},
		{"E05-secrets-getenv-family", "package x\nimport envsecrets \"" + secretsImportPath + "\"\nfunc f() string { return envsecrets.GetenvSecret(\"SOME_KEY\") }\n", true},
		{"E06-lookup-function-passed-in", "package x\nfunc f(lookup func(string) (string, bool)) { lookup(\"SOME_KEY\") }\n", true},
		// CHAOS-12688: a call is a lookup by its identity, not by its name.
		{"E07-same-named-method-two-args", "package x\ntype S struct{}\nfunc (S) lookup(ctx, key string) string { return key }\nfunc f(s S) string { return s.lookup(\"ctx\", \"SOME_KEY\") }\n", false},
		{"E08-same-named-method-one-arg", "package x\ntype S struct{}\nfunc (S) lookup(key string) string { return key }\nfunc f(s S) string { return s.lookup(\"SOME_KEY\") }\n", false},
		{"E09-getenv-of-another-package", "package x\nimport \"example.com/fake\"\nfunc f() string { return fake.Getenv(\"SOME_KEY\") }\n", false},
		{"E10-aliased-secrets-import", "package x\nimport (\n\"os\"\nplatformsecrets \"" + secretsImportPath + "\"\n)\nfunc f() { _, _, _ = platformsecrets.ResolveSecret(\"SOME_KEY\", os.LookupEnv) }\n", true},
		{"E11-injected-struct-field", "package x\ntype H struct{ Getenv func(string) string }\nfunc (h H) f() string { return h.Getenv(\"SOME_KEY\") }\n", true},
		{"E12-syscall-getenv", "package x\nimport \"syscall\"\nfunc f() { _, _ = syscall.Getenv(\"SOME_KEY\") }\n", true},
		{"E13-named-func-type-param", "package x\ntype lookupFn func(string) (string, bool)\nfunc f(get lookupFn) { get(\"SOME_KEY\") }\n", true},
		{"E14-func-type-alias-param", "package x\ntype getenvFn = func(string) string\nfunc f(get getenvFn) string { return get(\"SOME_KEY\") }\n", true},
		{"E15-name-default-helper-reads-os", "package x\nimport \"os\"\nfunc get(name, def string) string {\nif v, ok := os.LookupEnv(name); ok {\nreturn v\n}\nreturn def\n}\nfunc f() string { return get(\"SOME_KEY\", \"x\") }\n", true},
		{"E16-local-variable-of-os-lookupenv", "package x\nimport \"os\"\nfunc f() { lookup := os.LookupEnv; _, _ = lookup(\"SOME_KEY\") }\n", true},
		{"E17-local-copy-of-a-lookup-field", "package x\ntype Env struct{ Lookup func(string) (string, bool) }\nfunc f(env Env) { lookup := env.Lookup; _, _ = lookup(\"SOME_KEY\") }\n", true},
		{"E18-redactor-of-the-lookup-shape", "package x\ntype R struct{ Scrub func(string) string }\nfunc f(r R) string { return r.Scrub(\"SOME_KEY\") }\n", false},
		{"E19-two-arg-method-named-like-a-lookup-field", "package x\ntype H struct{ Getenv func(string) string }\ntype M struct{}\nfunc (M) Getenv(ctx, key string) string { return key }\nfunc f(m M) string { return m.Getenv(\"ctx\", \"SOME_KEY\") }\n", false},
		{"E20-parameter-shadows-an-import", "package x\nimport \"context\"\nvar _ context.Context\ntype H struct{ Getenv func(string) string }\nfunc g(context H) string { return context.Getenv(\"SOME_KEY\") }\n", true},
		{"E21-neutral-names-hold-a-lookup", "package x\nfunc f(fn func(string) string, get func(string) (string, bool), g func(string) string) {\n_ = fn(\"SOME_KEY\")\n}\n", true},
		{"E22-env-word-beats-an-exempt-word", "package x\ntype R struct{ redactEnv func(string) string }\nfunc f(r R) string { return r.redactEnv(\"SOME_KEY\") }\n", true},
		{"E23-exempt-name-in-another-package", "package x\nfunc f(read func(string) string, value func(string) (string, bool)) string {\n_, _ = value(\"OTHER\")\nreturn read(\"SOME_KEY\")\n}\n", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := len(scanEnvReaders(map[string]string{"internal/x/x.go": c.src, secretsDir + "/stub.go": secretsStub})["SOME_KEY"]) > 0
			if got != c.want {
				t.Errorf("%s: SOME_KEY read = %v, want %v", c.name, got, c.want)
			}
		})
	}
}

// The pushcli form: a helper takes cli.Env and calls its Lookup field, whose type secrets.LookupEnv (a named string
// lookup) is declared in two other packages. The helper's callers in the same file read their names.
func TestWorkerSecretKeyReaderScanInjectedFieldOfAnotherPackage(t *testing.T) {
	sources := map[string]string{
		secretsDir + "/source.go": "package secrets\ntype LookupEnv func(string) (string, bool)\n",
		"internal/cli/cli.go":     "package cli\nimport \"" + secretsImportPath + "\"\ntype Env struct{ Lookup secrets.LookupEnv }\n",
		"internal/pushcli/batch.go": "package pushcli\nimport \"github.com/full-chaos/dev-health-ops/internal/cli\"\n" +
			"func lookup(env cli.Env, key string) string {\nif env.Lookup == nil {\nreturn \"\"\n}\nvalue, _ := env.Lookup(key)\nreturn value\n}\n" +
			"func f(env cli.Env) string { return lookup(env, \"SOME_KEY\") }\n",
	}
	if got := scanEnvReaders(sources)["SOME_KEY"]; len(got) != 1 || got[0] != "internal/pushcli/batch.go" {
		t.Errorf("SOME_KEY readers = %v, want [internal/pushcli/batch.go]", got)
	}
}

// Multi-file cells, each a small module tree: the forms the r1 review of CHAOS-12688 executed against the name-keyed
// scan, and the ones the typed scan adds. want is whether wantFile is a reader of SOME_KEY.
// A fixture whose standard import cannot be loaded must fail loudly, never scan as "no reader".
func TestWorkerSecretKeyReaderScanImportFailureIsLoud(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered == nil || !strings.Contains(fmt.Sprint(recovered), "import \"nosuchstd/pkg\" failed") {
			t.Errorf("a failed standard import gave %v, want a panic naming the import", recovered)
		}
	}()
	scanEnvReaders(map[string]string{"internal/x/x.go": "package x\nimport \"nosuchstd/pkg\"\nfunc f() string { return pkg.Getenv(\"SOME_KEY\") }\n"})
}

func TestWorkerSecretKeyReaderScanPackages(t *testing.T) {
	mod := strings.TrimSuffix(secretsImportPath, "/"+secretsDir)
	envStruct := "type Env struct{ Lookup func(string) (string, bool) }\n"
	cases := []struct {
		name     string
		files    map[string]string
		wantFile string
		want     bool
	}{
		{"V01-var-copy-of-a-lookup-field", map[string]string{
			"internal/x/x.go": "package x\n" + envStruct + "func f(env Env) {\nvar lookup = env.Lookup\n_, _ = lookup(\"SOME_KEY\")\n}\n",
		}, "internal/x/x.go", true},
		{"V02-package-var-chain-from-os-lookupenv", map[string]string{
			"internal/x/x.go": "package x\nimport \"os\"\nvar lookupEnv = os.LookupEnv\nvar second = lookupEnv\nfunc f() { _, _ = second(\"SOME_KEY\") }\n",
		}, "internal/x/x.go", true},
		{"V03-embedded-named-lookup-field", map[string]string{
			"internal/x/x.go": "package x\ntype Lookup func(string) (string, bool)\ntype Env struct{ Lookup }\nfunc f(e Env) { _, _ = e.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/x.go", true},
		{"V04-dot-imported-os-getenv", map[string]string{
			"internal/x/x.go": "package x\nimport . \"os\"\nfunc f() string { return Getenv(\"SOME_KEY\") }\n",
		}, "internal/x/x.go", true},
		{"V05-chained-copy-of-a-lookup-field", map[string]string{
			"internal/x/x.go": "package x\n" + envStruct + "func f(env Env) {\na := env.Lookup\nb := a\n_, _ = b(\"SOME_KEY\")\n}\n",
		}, "internal/x/x.go", true},
		{"V06-unqualified-secrets-reader", map[string]string{
			secretsDir + "/use.go": "package secrets\nimport \"os\"\nvar _ = os.Args\nfunc GetenvSecret(name string) string { return getenv(name) }\n" +
				"func getenv(name string) string { return name }\nfunc f() string { return GetenvSecret(\"SOME_KEY\") }\n",
		}, secretsDir + "/use.go", true},
		{"V07-same-named-imported-types-with-different-signatures", map[string]string{
			"internal/a/a.go": "package a\ntype Getter func(string) string\n",
			"internal/b/b.go": "package b\ntype Getter func(string) int\n",
			"internal/x/x.go": "package x\nimport (\n\"" + mod + "/internal/a\"\n\"" + mod + "/internal/b\"\n)\nvar _ a.Getter\nfunc f(g b.Getter) int { return g(\"SOME_KEY\") }\n",
		}, "internal/x/x.go", false},
		{"V08-method-named-like-a-lookup-field-of-another-package", map[string]string{
			"internal/localgit/sync.go":               "package localgit\ntype Options struct{ Lookup func(string) (string, bool) }\n",
			"internal/syncdispatchcontract/routes.go": "package syncdispatchcontract\ntype Registry struct{}\nfunc (Registry) Lookup(kind string) (string, bool) { return kind, true }\n",
			"internal/syncreconciler/observer.go": "package syncreconciler\nimport \"" + mod + "/internal/syncdispatchcontract\"\n" +
				"func f(r syncdispatchcontract.Registry) {\n_, _ = r.Lookup(\"SOME_KEY\")\n_, _ = syncdispatchcontract.Registry.Lookup(r, \"SOME_KEY\")\n}\n",
		}, "internal/syncreconciler/observer.go", false},
		{"V09-a-key-read-only-in-a-test-file", map[string]string{
			"internal/x/x.go":      "package x\nconst key = \"SOME_KEY\"\n",
			"internal/x/x_test.go": "package x\nimport \"os\"\nfunc use() string { return os.Getenv(key) + os.Getenv(\"SOME_KEY\") }\n",
		}, "", false},
		{"V10-holder-of-an-exempt-named-type-in-another-package", map[string]string{
			"internal/jobs/metrics/daily/icfinalize/executor.go": "package icfinalize\ntype TeamResolver func(string) string\n",
			"internal/y/y.go": "package y\nimport \"" + mod + "/internal/jobs/metrics/daily/icfinalize\"\nfunc f(r icfinalize.TeamResolver) string { return r(\"SOME_KEY\") }\n",
		}, "internal/y/y.go", false},
		{"V12-env-wrapper-method-in-another-file", map[string]string{
			"internal/syncbudget/loader.go":    "package syncbudget\ntype Loader struct{ Getenv func(string) string }\nfunc (l Loader) getenv(name string) string { return l.Getenv(name) }\n",
			"internal/syncbudget/hydration.go": "package syncbudget\nfunc f(l Loader) string { return l.getenv(\"SOME_KEY\") }\n",
		}, "internal/syncbudget/hydration.go", true},
		{"V13-env-wrapper-function-of-another-package", map[string]string{
			"internal/envutil/envutil.go": "package envutil\nimport \"os\"\nfunc Read(name string) string { return os.Getenv(name) }\nfunc Hash(name string) string { return name }\n",
			"internal/x/x.go":             "package x\nimport \"" + mod + "/internal/envutil\"\nfunc f() string { return envutil.Read(\"SOME_KEY\") }\n",
			"internal/y/y.go":             "package y\nimport \"" + mod + "/internal/envutil\"\nfunc g() string { return envutil.Hash(\"OTHER_KEY\") }\n",
		}, "internal/x/x.go", true},
		// The r2 review of CHAOS-12688: a wrapper method called through an interface it implements.
		{"V14-wrapper-through-an-interface-selector", map[string]string{
			"internal/x/env.go": "package x\nimport \"os\"\ntype Env struct{}\nfunc (Env) Lookup(name string) string { return os.Getenv(name) }\n",
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) string }\nfunc f(g Getter) string { return g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V15-wrapper-through-an-interface-method-expression", map[string]string{
			"internal/x/env.go": "package x\nimport \"os\"\ntype Env struct{}\nfunc (Env) Lookup(name string) string { return os.Getenv(name) }\n",
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) string }\nfunc f(g Getter) string { return Getter.Lookup(g, \"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V16-wrapper-through-an-embedded-interface", map[string]string{
			"internal/x/env.go": "package x\nimport \"os\"\ntype Env struct{}\nfunc (Env) Lookup(name string) string { return os.Getenv(name) }\n",
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) string }\ntype Holder struct{ Getter }\nfunc f(h Holder) string { return h.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V17-wrapper-through-a-generic-constraint", map[string]string{
			"internal/x/env.go": "package x\nimport \"os\"\ntype Env struct{}\nfunc (Env) Lookup(name string) string { return os.Getenv(name) }\n",
			"internal/x/use.go": "package x\nfunc f[T interface{ Lookup(string) string }](t T) string { return t.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V18-wrapper-through-an-interface-of-another-package", map[string]string{
			"internal/envimpl/env.go": "package envimpl\nimport \"os\"\ntype Env struct{}\nfunc (Env) Lookup(name string) string { return os.Getenv(name) }\n",
			"internal/x/use.go":       "package x\ntype Getter interface{ Lookup(string) string }\nfunc f(g Getter) string { return g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V19-interface-implemented-only-by-a-non-wrapper", map[string]string{
			"internal/x/reg.go": "package x\ntype Registry struct{}\nfunc (Registry) Lookup(kind string) string { return kind }\n",
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) string }\nfunc f(g Getter) string { return g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", false},
		{"V20-test-fake-of-an-interface-is-no-wrapper", map[string]string{
			"internal/x/x_test.go": "package x\ntype fakeConn struct{ lookupAnswer func(string) string }\nfunc (f fakeConn) Query(q string) string { return f.lookupAnswer(q) }\n",
			"internal/x/use.go":    "package x\ntype Conn interface{ Query(string) string }\nfunc f(c Conn) string { return c.Query(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", false},
		{"V11-getenv-of-a-resolved-non-env-package", map[string]string{
			"internal/fakeenv/fakeenv.go": "package fakeenv\nfunc Getenv(name string) string { return name }\n",
			"internal/x/x.go":             "package x\nimport \"" + mod + "/internal/fakeenv\"\nfunc f() string { return fakeenv.Getenv(\"SOME_KEY\") }\n",
		}, "internal/x/x.go", false},
		// A wrapper method of a generic type, called through an interface or on an instance.
		{"V21-generic-wrapper-through-interface-selector", map[string]string{
			"internal/x/env.go": genericEnv("x", "Env[T]"),
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) string }\nfunc f(g Getter) string { return g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V22-generic-wrapper-through-method-expression", map[string]string{
			"internal/x/env.go": genericEnv("x", "Env[T]"),
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) string }\nfunc f(g Getter) string { return Getter.Lookup(g, \"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V23-generic-wrapper-through-embedded-interface", map[string]string{
			"internal/x/env.go": genericEnv("x", "Env[T]"),
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) string }\ntype Holder struct{ Getter }\nfunc f(h Holder) string { return h.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V24-generic-wrapper-through-generic-constraint", map[string]string{
			"internal/x/env.go": genericEnv("x", "Env[T]"),
			"internal/x/use.go": "package x\nfunc f[G interface{ Lookup(string) string }](g G) string { return g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V25-generic-wrapper-through-cross-package-interface", map[string]string{
			"internal/envimpl/env.go": genericEnv("envimpl", "Env[T]"),
			"internal/x/use.go":       "package x\ntype Getter interface{ Lookup(string) string }\nfunc f(g Getter) string { return g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V26-generic-pointer-wrapper-through-interface-selector", map[string]string{
			"internal/x/env.go": genericEnv("x", "*Env[T]"),
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) string }\nfunc f(g Getter) string { return g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V27-generic-pointer-wrapper-through-method-expression", map[string]string{
			"internal/x/env.go": genericEnv("x", "*Env[T]"),
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) string }\nfunc f(g Getter) string { return Getter.Lookup(g, \"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V28-generic-pointer-wrapper-through-embedded-interface", map[string]string{
			"internal/x/env.go": genericEnv("x", "*Env[T]"),
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) string }\ntype Holder struct{ Getter }\nfunc f(h Holder) string { return h.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V29-generic-pointer-wrapper-through-generic-constraint", map[string]string{
			"internal/x/env.go": genericEnv("x", "*Env[T]"),
			"internal/x/use.go": "package x\nfunc f[G interface{ Lookup(string) string }](g G) string { return g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V30-generic-wrapper-called-on-an-instance", map[string]string{
			"internal/x/env.go": genericEnv("x", "Env[T]"),
			"internal/x/use.go": "package x\nfunc f() string { return Env[int]{}.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V31-generic-pointer-wrapper-called-on-an-instance", map[string]string{
			"internal/x/env.go": genericEnv("x", "*Env[T]"),
			"internal/x/use.go": "package x\nfunc f(e *Env[string]) string { return e.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V32-generic-wrapper-method-expression-of-an-instance", map[string]string{
			"internal/x/env.go": genericEnv("x", "Env[T]"),
			"internal/x/use.go": "package x\nfunc f() string { return Env[int].Lookup(Env[int]{}, \"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V33-generic-wrapper-instance-of-another-package", map[string]string{
			"internal/envimpl/env.go": genericEnv("envimpl", "Env[T]"),
			"internal/x/use.go":       "package x\nimport \"" + mod + "/internal/envimpl\"\nfunc f() string { return envimpl.Env[string]{}.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V34-generic-wrapper-with-two-type-parameters", map[string]string{
			"internal/x/env.go": "package x\nimport \"os\"\ntype Env[K comparable, V any] struct{}\nfunc (Env[K, V]) Lookup(name string) string { return os.Getenv(name) }\n",
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) string }\nfunc f(g Getter) string { return g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V35-generic-non-wrapper-through-interface", map[string]string{
			"internal/x/reg.go": "package x\ntype Registry[T any] struct{}\nfunc (Registry[T]) Lookup(kind string) string { return kind }\n",
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) string }\nfunc f(g Getter) string { return g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", false},
		{"V36-generic-wrapper-result-of-its-type-parameter-through-interface", map[string]string{
			"internal/x/env.go": "package x\nimport \"os\"\ntype Env[T ~string] struct{}\nfunc (Env[T]) Lookup(name string) T { return T(os.Getenv(name)) }\n",
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) string }\nfunc f(g Getter) string { return g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V37-generic-wrapper-result-of-its-type-parameter-through-constraint", map[string]string{
			"internal/x/env.go": "package x\nimport \"os\"\ntype Env[T ~string] struct{}\nfunc (Env[T]) Lookup(name string) T { return T(os.Getenv(name)) }\n",
			"internal/x/use.go": "package x\nfunc f[G interface{ Lookup(string) string }](g G) string { return g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V38-generic-wrapper-whose-constraint-excludes-the-interface", map[string]string{
			"internal/x/env.go": "package x\nimport \"os\"\ntype Env[T ~string] struct{}\nfunc (Env[T]) Lookup(name string) T { return T(os.Getenv(name)) }\n",
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) int }\nfunc f(g Getter) int { return g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", false},
		{"V39-explicit-instance-of-a-generic-wrapper-function", map[string]string{
			"internal/x/env.go": "package x\nimport \"os\"\nfunc Read[T any](name string) string { return os.Getenv(name) }\n",
			"internal/x/use.go": "package x\nfunc f() string { return Read[int](\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V50-explicit-two-argument-instance-of-another-package", map[string]string{
			"internal/envutil/envutil.go": "package envutil\nimport \"os\"\nfunc Read[K comparable, V any](name string) string { return os.Getenv(name) }\n",
			"internal/x/x.go":             "package x\nimport \"" + mod + "/internal/envutil\"\nfunc f() string { return (envutil.Read[int, bool])(\"SOME_KEY\") }\n",
		}, "internal/x/x.go", true},
		// A function literal held by a variable that passes a string parameter to a lookup is a wrapper too.
		{"V40-package-var-func-literal-wrapper", map[string]string{
			"internal/x/env.go": "package x\nimport \"os\"\n" + literalEnv("var read = "),
			"internal/x/use.go": "package x\nfunc f() string { return read(\"SOME_KEY\", \"d\") }\n",
		}, "internal/x/use.go", true},
		{"V41-exported-var-func-literal-of-another-package", map[string]string{
			"internal/envutil/envutil.go": "package envutil\nimport \"os\"\n" + literalEnv("var Read = "),
			"internal/x/x.go":             "package x\nimport \"" + mod + "/internal/envutil\"\nfunc f() string { return envutil.Read(\"SOME_KEY\", \"d\") }\n",
		}, "internal/x/x.go", true},
		{"V42-var-block-with-two-names", map[string]string{
			"internal/x/env.go": "package x\nimport \"os\"\n" + literalEnv("var n, read = 1, "),
			"internal/x/use.go": "package x\nfunc f() string { _ = n\nreturn read(\"SOME_KEY\", \"d\") }\n",
		}, "internal/x/use.go", true},
		{"V44-func-literal-assigned-in-init", map[string]string{
			"internal/x/env.go": "package x\nimport \"os\"\nvar read func(name, fallback string) string\nfunc init() {\n" + literalEnv("read = ") + "}\n",
			"internal/x/use.go": "package x\nfunc f() string { return read(\"SOME_KEY\", \"d\") }\n",
		}, "internal/x/use.go", true},
		{"V45-func-literal-through-a-wrapper-function", map[string]string{
			"internal/x/env.go": "package x\nimport \"os\"\nfunc get(name string) string { return os.Getenv(name) }\n" +
				"var read = func(name, fallback string) string {\nif v := get(name); v != \"\" {\nreturn v\n}\nreturn fallback\n}\n",
			"internal/x/use.go": "package x\nfunc f() string { return read(\"SOME_KEY\", \"d\") }\n",
		}, "internal/x/use.go", true},
		{"V46-func-literal-that-reads-no-environment", map[string]string{
			"internal/x/x.go": "package x\nvar join = func(a, b string) string { return a + b }\nfunc f() string { return join(\"SOME_KEY\", \"d\") }\n",
		}, "internal/x/x.go", false},
		{"V47-func-literal-that-looks-up-a-fixed-name", map[string]string{
			"internal/x/env.go": "package x\nimport \"os\"\nvar read = func(name, fallback string) string { return os.Getenv(\"FIXED\") + fallback }\n",
			"internal/x/use.go": "package x\nfunc f() string { return read(\"SOME_KEY\", \"d\") }\n",
		}, "internal/x/use.go", false},
		{"V48-func-literal-in-a-test-file-is-no-wrapper", map[string]string{
			"internal/x/x_test.go": "package x\nimport \"os\"\n" + literalEnv("var read = "),
			"internal/x/use.go":    "package x\nfunc f() string { return read(\"SOME_KEY\", \"d\") }\n",
		}, "internal/x/use.go", false},
		// A generic wrapper whose result holds its type parameter in any type form matches an interface by unification.
		{"V51-generic-wrapper-result-anonymous-struct", map[string]string{
			"internal/x/env.go": genericResultEnv("", "struct{ Value T }", "struct{ Value T }{}"),
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) struct{ Value string } }\nfunc f(g Getter) { _ = g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V52-generic-wrapper-result-anonymous-interface", map[string]string{
			"internal/x/env.go": genericResultEnv("", "interface{ Value() T }", "nil"),
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) interface{ Value() string } }\nfunc f(g Getter) { _ = g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V53-generic-wrapper-result-generic-alias", map[string]string{
			"internal/x/env.go": genericResultEnv("type Result[T any] = []T\n", "Result[T]", "nil"),
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) Result[string] }\nfunc f(g Getter) { _ = g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V54-generic-wrapper-result-generic-named-type", map[string]string{
			"internal/x/env.go": genericResultEnv("type Box[T any] struct{ Value T }\n", "Box[T]", "Box[T]{}"),
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) Box[string] }\nfunc f(g Getter) { _ = g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V55-generic-wrapper-result-anonymous-struct-through-constraint", map[string]string{
			"internal/x/env.go": genericResultEnv("", "struct{ Value T }", "struct{ Value T }{}"),
			"internal/x/use.go": "package x\nfunc f[G interface{ Lookup(string) struct{ Value string } }](g G) { _ = g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V56-generic-wrapper-result-map-of-anonymous-struct", map[string]string{
			"internal/x/env.go": genericResultEnv("", "map[string]struct{ Value []T }", "nil"),
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) map[string]struct{ Value []string } }\nfunc f(g Getter) { _ = g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V57-generic-wrapper-result-struct-of-another-field-name", map[string]string{
			"internal/x/env.go": genericResultEnv("", "struct{ Value T }", "struct{ Value T }{}"),
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) struct{ Other string } }\nfunc f(g Getter) { _ = g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", false},
		{"V58-generic-wrapper-result-interface-of-another-method", map[string]string{
			"internal/x/env.go": genericResultEnv("", "interface{ Value() T }", "nil"),
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) interface{ Other() string } }\nfunc f(g Getter) { _ = g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", false},
		{"V59-generic-wrapper-result-struct-bound-twice-differently", map[string]string{
			"internal/x/env.go": genericResultEnv("", "struct{ A T; B T }", "struct{ A T; B T }{}"),
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) struct{ A string; B int } }\nfunc f(g Getter) { _ = g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", false},
		// A wrapper whose name parameter is a named string type or a string-constrained type parameter, passed bare or
		// through a conversion.
		{"V60-named-string-parameter-converted", map[string]string{
			"internal/x/env.go": "package x\nimport \"os\"\ntype key string\nfunc Read(name key) string { return os.Getenv(string(name)) }\n",
			"internal/x/use.go": "package x\nfunc f() string { return Read(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V61-type-parameter-of-a-generic-function-converted", map[string]string{
			"internal/x/env.go": "package x\nimport \"os\"\nfunc Read[T ~string](name T) string { return os.Getenv(string(name)) }\n",
			"internal/x/use.go": "package x\nfunc f() string { return Read(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V62-type-parameter-of-a-generic-method-converted", map[string]string{
			"internal/x/env.go": "package x\nimport \"os\"\ntype Env[T ~string] struct{}\nfunc (Env[T]) Lookup(name T) string { return os.Getenv(string(name)) }\n",
			"internal/x/use.go": "package x\nfunc f() string { return Env[string]{}.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V63-type-parameter-passed-bare-to-a-generic-wrapper", map[string]string{
			"internal/x/env.go": "package x\nimport \"os\"\nfunc inner[T ~string](name T) string { return os.Getenv(string(name)) }\nfunc Read[T ~string](name T) string { return inner(name) }\n",
			"internal/x/use.go": "package x\nfunc f() string { return Read(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V64-string-parameter-converted-to-string", map[string]string{
			"internal/x/env.go": "package x\nimport \"os\"\nfunc Read(name string) string { return os.Getenv(string(name)) }\n",
			"internal/x/use.go": "package x\nfunc f() string { return Read(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V65-type-parameter-method-through-interface", map[string]string{
			"internal/x/env.go": "package x\nimport \"os\"\ntype Env[T ~string] struct{}\nfunc (Env[T]) Lookup(name T) string { return os.Getenv(string(name)) }\n",
			"internal/x/use.go": "package x\ntype Getter interface{ Lookup(string) string }\nfunc f(g Getter) string { return g.Lookup(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", true},
		{"V66-named-string-parameter-of-another-package", map[string]string{
			"internal/envutil/envutil.go": "package envutil\nimport \"os\"\ntype Key string\nfunc Read(name Key) string { return os.Getenv(string(name)) }\n",
			"internal/x/x.go":             "package x\nimport \"" + mod + "/internal/envutil\"\nfunc f() string { return envutil.Read(\"SOME_KEY\") }\n",
		}, "internal/x/x.go", true},
		{"V67-conversion-of-a-fixed-name-is-no-wrapper", map[string]string{
			"internal/x/env.go": "package x\nimport \"os\"\ntype key string\nconst fixed key = \"FIXED\"\nfunc Read(name key) string { return os.Getenv(string(fixed)) + string(name) }\n",
			"internal/x/use.go": "package x\nfunc f() string { return Read(\"SOME_KEY\") }\n",
		}, "internal/x/use.go", false},
		{"V68-type-parameter-of-a-non-string-constraint-is-no-name", map[string]string{
			"internal/x/env.go": "package x\nimport \"os\"\nfunc Read[T ~int](n T) string { return os.Getenv(string(rune(n))) }\n",
			"internal/x/use.go": "package x\nfunc f() string { _ = \"SOME_KEY\"\nreturn Read(1) }\n",
		}, "internal/x/use.go", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			readers := scanEnvReaders(c.files)["SOME_KEY"]
			got := len(readers) > 0
			if c.wantFile != "" {
				got = slices.Contains(readers, c.wantFile)
			}
			if got != c.want {
				t.Errorf("%s: SOME_KEY read by %q = %v (readers %v), want %v", c.name, c.wantFile, got, readers, c.want)
			}
		})
	}
}

// The tree is loaded once per build variant (no tags, -tags=integration), and the same package path can declare a
// wrapper in one variant and a same-named non-wrapper in the other. Wrappers belong to their own variant; the readers
// of both variants are joined.
func TestWorkerSecretKeyReaderScanBuildVariants(t *testing.T) {
	iface := "type Getter interface{ Lookup(string) string }\n"
	cases := []struct {
		name                string
		wrapper, nonWrapper map[string]string
		wantFile            string
		want                bool
	}{
		{"B01-func-literal-holder-of-the-other-variant",
			map[string]string{"internal/x/wrapper.go": "package x\nimport \"os\"\nvar read = func(name, fallback string) string { return os.Getenv(name) }\n"},
			map[string]string{"internal/x/use.go": "package x\nvar read = func(name, fallback string) string { return fallback }\nfunc f() string { return read(\"SOME_KEY\", \"d\") }\n"},
			"", false},
		{"B02-function-of-the-other-variant",
			map[string]string{"internal/x/env.go": "package x\nimport \"os\"\nfunc Read(name string) string { return os.Getenv(name) }\n"},
			map[string]string{"internal/x/env.go": "package x\nfunc Read(name string) string { return name }\nfunc f() string { return Read(\"SOME_KEY\") }\n"},
			"", false},
		{"B03-method-of-the-other-variant-through-an-interface",
			map[string]string{"internal/x/env.go": "package x\nimport \"os\"\ntype Env struct{}\nfunc (Env) Lookup(name string) string { return os.Getenv(name) }\n"},
			map[string]string{
				"internal/x/env.go": "package x\ntype Env struct{}\nfunc (Env) Lookup(name string) string { return name }\n",
				"internal/x/use.go": "package x\n" + iface + "func f(g Getter) string { return g.Lookup(\"SOME_KEY\") }\n",
			},
			"", false},
		{"B04-reader-of-its-own-variant-is-kept",
			map[string]string{
				"internal/x/wrapper.go":         "package x\nimport \"os\"\nvar read = func(name, fallback string) string { return os.Getenv(name) }\n",
				"internal/x/use_integration.go": "package x\nfunc g() string { return read(\"SOME_KEY\", \"d\") }\n",
			},
			map[string]string{"internal/x/use.go": "package x\nvar read = func(name, fallback string) string { return fallback }\nfunc f() string { return read(\"SOME_KEY\", \"d\") }\n"},
			"internal/x/use_integration.go", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			readers := scanEnvUnits(append(checkSources(c.wrapper), checkSources(c.nonWrapper)...))["SOME_KEY"]
			got := len(readers) > 0
			if c.wantFile != "" {
				got = slices.Contains(readers, c.wantFile) && !slices.Contains(readers, "internal/x/use.go")
			}
			if got != c.want {
				t.Errorf("%s: SOME_KEY readers %v, want read by %q = %v", c.name, readers, c.wantFile, c.want)
			}
		})
	}
}

// genericEnv is a package with a generic type Env whose Lookup method, on the receiver recv, reads the environment.
func genericEnv(pkg, recv string) string {
	return "package " + pkg + "\nimport \"os\"\ntype Env[T any] struct{}\nfunc (" + recv + ") Lookup(name string) string { return os.Getenv(name) }\n"
}

// genericResultEnv is package x with a generic type Env whose Lookup method reads the environment and returns result
// (which may use T), after the declarations in prefix.
func genericResultEnv(prefix, result, zero string) string {
	return "package x\nimport \"os\"\n" + prefix + "type Env[T any] struct{}\nfunc (Env[T]) Lookup(name string) " + result +
		" {\n_ = os.Getenv(name)\nreturn " + zero + "\n}\n"
}

// literalEnv is a func(name, fallback string) string literal that reads the environment, after the given holder text.
func literalEnv(holder string) string {
	return holder + "func(name, fallback string) string {\nif v, ok := os.LookupEnv(name); ok {\nreturn v\n}\nreturn fallback\n}\n"
}

// An exemption of nonEnvLookupHolders belongs to its own package directory: the same holder in a package of the
// directory dir_test counts. Each entry is a package-level holder variable and a named function type.
func TestWorkerSecretKeyReaderScanExemptionsByRealDir(t *testing.T) {
	entries := make([]lookupHolder, 0, len(nonEnvLookupHolders))
	for entry := range nonEnvLookupHolders {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].dir+"/"+entries[i].name < entries[j].dir+"/"+entries[j].name
	})
	forms := map[string]string{
		"var":  "package p\nvar %[1]s func(string) string\nfunc f() string { return %[1]s(\"SOME_KEY\") }\n",
		"type": "package p\ntype %[1]s func(string) string\nfunc f(h %[1]s) string { return h(\"SOME_KEY\") }\n",
	}
	for _, entry := range entries {
		for _, form := range []string{"var", "type"} {
			for _, dir := range []string{entry.dir, entry.dir + "_test"} {
				want := dir != entry.dir
				t.Run(dir+"/"+entry.name+"/"+form, func(t *testing.T) {
					file := dir + "/p.go"
					readers := scanEnvReaders(map[string]string{file: fmt.Sprintf(forms[form], entry.name)})["SOME_KEY"]
					if got := slices.Contains(readers, file); got != want {
						t.Errorf("holder %s %s in %s: SOME_KEY read = %v (readers %v), want %v", form, entry.name, dir, got, readers, want)
					}
				})
			}
		}
	}
}

// Two limits of the DSN checks are accepted and pinned here, so a change to either is seen and decided:
//   - A DSN held under a name the chart does not know, in a ConfigMap or an operator-created Secret, is not seen by the
//     chart. The process start guard still refuses a worker process whose environment carries a PostgreSQL URL under any name.
//   - A libpq key=value connection string (not a URL) under an unknown name is not recognised by the start guard. Closing
//     this needs a change to dsnclass (production code), which is out of scope of this test-only leaf.
func dsnAliasLimits(g *guard, check func(class string, environ []string) error) {
	g.expect(check(dsnclass.River, []string{"UNKNOWN_ALIAS=postgresql://u:p@h:5432/db"}) != nil,
		"the start guard no longer refuses a DSN URL under an unknown name: the accepted limit text is stale")
	g.expect(check(dsnclass.River, []string{"UNKNOWN_ALIAS=host=h port=5432 dbname=d user=u"}) == nil,
		"the start guard now refuses a libpq key=value string under an unknown name: close the accepted limit and update its text")
}
