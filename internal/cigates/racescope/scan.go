// Package racescope holds the guard of ci/race_excluded_packages.tsv: the packages the -race legs of ci/check_go.sh do not run
// (CHAOS-11413). A package may leave -race only while nothing in it, and nothing it links from this module, can start
// concurrent work. The scan is static: go/parser over every .go file of the package and of its in-module import closure.
package racescope

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// Row is one line of ci/race_excluded_packages.tsv: package, class, witness, reason.
type Row struct {
	Package string
	Class   string
	Witness string
	Reason  string
}

// bannedImports start work the race detector would watch (goroutines, processes, signal handlers, sockets) or hold shared state.
var bannedImports = map[string]bool{
	"sync": true, "sync/atomic": true, "golang.org/x/sync/errgroup": true,
	"os/exec": true, "os/signal": true, "C": true, "plugin": true, "net": true, "net/http": true, "database/sql": true,
}

// bannedSelectors are calls that start a goroutine or a timer goroutine without a `go` statement.
var bannedSelectors = map[string]bool{"Parallel": true, "AfterFunc": true, "NewTicker": true, "NewTimer": true, "Tick": true, "RunParallel": true, "StartProcess": true}

type goPackage struct {
	ImportPath     string
	Dir            string
	GoFiles        []string
	CgoFiles       []string
	CFiles         []string
	CXXFiles       []string
	MFiles         []string
	HFiles         []string
	FFiles         []string
	SFiles         []string
	SwigFiles      []string
	SwigCXXFiles   []string
	SysoFiles      []string
	Error          *struct{ Err string }
	TestGoFiles    []string
	XTestGoFiles   []string
	IgnoredGoFiles []string
	Imports        []string
	TestImports    []string
	XTestImports   []string
	Standard       bool
	DepOnly        bool
}

// Parse reads a list file. Sorted, no duplicate, four columns.
func Parse(path string) ([]Row, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rows []Row
	seen := map[string]bool{}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 4 || f[0] == "" || f[1] == "" || f[2] == "" || f[3] == "" {
			return nil, fmt.Errorf("%s: row %q is not package<TAB>class<TAB>witness<TAB>reason", path, line)
		}
		if seen[f[0]] {
			return nil, fmt.Errorf("%s: %s is listed twice", path, f[0])
		}
		seen[f[0]] = true
		if f[1] != "none" || f[2] != "-" {
			return nil, fmt.Errorf("%s: %s has class %q and witness %q: only class none with witness - exists (covered-by was retired: every row is judged on its whole in-module closure)", path, f[0], f[1], f[2])
		}
		if len(rows) > 0 && rows[len(rows)-1].Package >= f[0] {
			return nil, fmt.Errorf("%s: %s is not in LC_ALL=C order", path, f[0])
		}
		rows = append(rows, Row{f[0], f[1], f[2], f[3]})
	}
	return rows, nil
}

func goList(root string) (modpath string, pkgs map[string]*goPackage, err error) {
	run := func(args ...string) ([]byte, error) {
		cmd := exec.Command("go", args...)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=readonly", "CGO_ENABLED=1")
		return cmd.Output()
	}
	out, err := run("list", "-m")
	if err != nil {
		return "", nil, fmt.Errorf("go list -m: %w", err)
	}
	modpath = strings.TrimSpace(string(out))
	out, err = run("list", "-e", "-tags", "race", "-json", "./...")
	if err != nil {
		return "", nil, fmt.Errorf("go list ./...: %w", err)
	}
	pkgs = map[string]*goPackage{}
	dec := json.NewDecoder(strings.NewReader(string(out)))
	for dec.More() {
		var p goPackage
		if err := dec.Decode(&p); err != nil {
			return "", nil, err
		}
		pkgs[p.ImportPath] = &p
	}
	return modpath, pkgs, nil
}

// scanFile names every construct of one file that starts or watches concurrent work.
func scanFile(path string, ts typeStore) []string {
	tf := ts[path]
	var untyped []string
	if tf == nil {
		// Not type-checked (cgo, a build constraint, a type error): say so, and still run every check that needs no types.
		untyped = []string{filepath.Base(path) + ": the file was not type-checked, so the scan cannot judge its function values"}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return append(untyped, path+": unparsable: "+err.Error())
		}
		tf = &typed{fset: fset, file: f, info: &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Uses: map[*ast.Ident]types.Object{}}}
	}
	fset, f := tf.fset, tf.file
	out := untyped
	at := func(p token.Pos, what string) {
		out = append(out, fmt.Sprintf("%s:%d: %s", filepath.Base(path), fset.Position(p).Line, what))
	}
	for _, cg := range f.Comments {
		for _, c := range cg.List {
			if strings.HasPrefix(c.Text, "//go:build") && strings.Contains(c.Text, "race") {
				at(c.Pos(), "build constraint names race")
			}
			if strings.HasPrefix(c.Text, "//go:linkname") {
				at(c.Pos(), "go:linkname directive: it reaches a symbol of another package the scan never reads")
			}
			if strings.HasPrefix(c.Text, "//export ") {
				at(c.Pos(), "//export directive: the function is callable from C threads")
			}
		}
	}
	for _, imp := range f.Imports {
		v := strings.Trim(imp.Path.Value, `"`)
		if bannedImports[v] {
			at(imp.Pos(), "imports "+v)
		}
		if v == "C" {
			at(imp.Pos(), "cgo file: Go and C code the scan cannot model")
		}
		if stdlibPath(v) {
			if _, ok := stdlibAllowed[v]; !ok {
				at(imp.Pos(), "imports "+v+", which is not on the stdlib allowlist (it may start goroutines or run user code on one)")
			}
		}
		if v == "reflect" && strings.HasSuffix(path, "_test.go") {
			at(imp.Pos(), "imports reflect in a test file (a method called by name, such as Parallel, is invisible to the scan)")
		}
		if imp.Name != nil && imp.Name.Name == "." {
			at(imp.Pos(), "dot import of "+v+" (its calls lose their package qualifier, so the scan cannot see them)")
		}
	}
	for _, pos := range tf.ifaceConversions() {
		at(pos, "function value converted to an interface type (it may be called later from another goroutine, e.g. runtime.SetFinalizer)")
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.GoStmt:
			at(x.Pos(), "go statement")
		case *ast.ChanType:
			at(x.Pos(), "channel type")
		case *ast.SelectStmt:
			at(x.Pos(), "select statement")
		case *ast.SelectorExpr:
			if bannedSelectors[x.Sel.Name] {
				at(x.Pos(), "use of ."+x.Sel.Name)
			}
		case *ast.CallExpr:
			idx, pure := pureCallees[calleeKey(x, tf.info)]
			for i, a := range x.Args {
				switch {
				case pure && i == idx:
					// A pure callee runs only a function whose body is scanned: a literal, or a function of this package.
					if !tf.samePackageFunc(a) {
						if _, lit := a.(*ast.FuncLit); !lit {
							at(a.Pos(), "function value passed to "+calleeName(x)+" is a body the scan cannot see (only a literal or a function of this package is scanned)")
						}
					}
				case tf.isFunc(a):
					at(a.Pos(), "function value passed as an argument: the callee may run it on another goroutine")
				}
			}
		}
		return true
	})
	return out
}

// pureCallees lists the callees that run a function-valued argument synchronously, on the calling goroutine only, by their documented
// contract. It starts empty (acp threat-model ruling, CHAOS-11413); an entry needs its reason on its line. The callback check of a
// covered-by row never consults it: a callback that is shared by several workers may pass no function value at all.
var pureCallees = map[string]int{ // callee (go/types full name) -> index of its function-valued argument
	"path/filepath.WalkDir": 1, // walks in lexical order and calls fn on the calling goroutine (documented contract)
	"path/filepath.Walk":    1, // same contract as WalkDir: fn is called on the calling goroutine
	"sort.Slice":            1, // sorts on the calling goroutine and calls less there (documented contract)
	"sort.SliceStable":      1, // same contract as sort.Slice: less is called on the calling goroutine
	"slices.SortFunc":       1, // sorts on the calling goroutine and calls cmp there (documented contract)
	"slices.SortStableFunc": 1, // same contract as slices.SortFunc: cmp is called on the calling goroutine
	"(*testing.T).Run":      1, // testing: waits for the subtest; only t.Parallel (refused here) lets it overlap its parent
}

func calleeName(c *ast.CallExpr) string {
	switch fn := c.Fun.(type) {
	case *ast.Ident:
		return fn.Name
	case *ast.SelectorExpr:
		if pkg, ok := fn.X.(*ast.Ident); ok {
			return pkg.Name + "." + fn.Sel.Name
		}
		return fn.Sel.Name
	}
	return ""
}

// typed is one parsed and type-checked file of the module (go/packages, export data for imports).
type typed struct {
	fset    *token.FileSet
	file    *ast.File
	info    *types.Info
	pkgPath string
}

// typeStore maps an absolute file name to its typed form.
type typeStore map[string]*typed

// funcish: the type is a function, or a slice, array or map of them.
func funcish(t types.Type) bool {
	if t == nil {
		return false
	}
	switch u := t.Underlying().(type) {
	case *types.Signature:
		return true
	case *types.Slice:
		return funcish(u.Elem())
	case *types.Array:
		return funcish(u.Elem())
	case *types.Map:
		return funcish(u.Elem())
	}
	return false
}

// isFunc: the expression, however it is spelled (literal, variable, field, method value, call result, conversion), has a function type.
func (tf *typed) isFunc(a ast.Expr) bool {
	tv, ok := tf.info.Types[a]
	return ok && !tv.IsType() && funcish(tv.Type)
}

// samePackageFunc: the expression names a package-level function of the file's own package.
func (tf *typed) samePackageFunc(a ast.Expr) bool {
	id, ok := a.(*ast.Ident)
	if !ok {
		return false
	}
	fn, ok := tf.info.Uses[id].(*types.Func)
	return ok && fn.Pkg() != nil && fn.Pkg().Path() == tf.pkgPath && fn.Parent() == fn.Pkg().Scope()
}

// calleeKey resolves the callee of a call by type: "path/filepath.WalkDir", "(*testing.T).Run".
func calleeKey(c *ast.CallExpr, info *types.Info) string {
	var id *ast.Ident
	switch fn := c.Fun.(type) {
	case *ast.Ident:
		id = fn
	case *ast.SelectorExpr:
		id = fn.Sel
	default:
		return ""
	}
	if f, ok := info.Uses[id].(*types.Func); ok {
		return f.FullName()
	}
	return ""
}

// loadTypes parses and type-checks the packages (with their tests) once; a package that does not type-check is a problem, because the
// scan then cannot tell a function value from anything else.
func loadTypes(root string, importPaths []string) (typeStore, map[string][]string, error) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedImports | packages.NeedTypes |
			packages.NeedTypesInfo | packages.NeedSyntax,
		Dir:        root,
		Tests:      true,
		Env:        append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=readonly", "CGO_ENABLED=1"),
		BuildFlags: []string{"-tags=race"},
		ParseFile: func(fset *token.FileSet, filename string, src []byte) (*ast.File, error) {
			return parser.ParseFile(fset, filename, src, parser.ParseComments)
		},
	}
	loaded, err := packages.Load(cfg, importPaths...)
	if err != nil {
		return nil, nil, err
	}
	store := typeStore{}
	errs := map[string][]string{}
	for _, lp := range loaded {
		base := strings.TrimSuffix(strings.Fields(lp.ID)[0], "_test")
		for _, e := range lp.Errors {
			errs[base] = append(errs[base], "does not type-check: "+e.Msg)
		}
		if lp.Types == nil || lp.TypesInfo == nil {
			continue
		}
		for _, f := range lp.Syntax {
			name := lp.Fset.File(f.Pos()).Name()
			store[name] = &typed{fset: lp.Fset, file: f, info: lp.TypesInfo, pkgPath: lp.Types.Path()}
		}
	}
	return store, errs, nil
}

// external: an import path that is neither the standard library nor this module (singleflight, otel and grpc run user code on goroutines of their own).
func external(modpath, path string) bool {
	first := strings.SplitN(path, "/", 2)[0]
	return strings.Contains(first, ".") && path != modpath && !strings.HasPrefix(path, modpath+"/")
}

func goFilesOf(p *goPackage, tests bool) []string {
	files := append(append([]string{}, p.GoFiles...), p.CgoFiles...)
	if tests {
		files = append(files, p.TestGoFiles...)
		files = append(files, p.XTestGoFiles...)
		files = append(files, p.IgnoredGoFiles...)
	}
	return files
}

func scanPackage(p *goPackage, tests bool, modpath string, ts typeStore, loadErrs map[string][]string) []string {
	var out []string
	if p.Error != nil {
		out = append(out, "go list reports an error for the package: "+strings.TrimSpace(p.Error.Err))
	}
	for _, e := range loadErrs[p.ImportPath] {
		out = append(out, e)
	}
	for _, group := range [][]string{p.CFiles, p.CXXFiles, p.MFiles, p.HFiles, p.FFiles, p.SFiles, p.SwigFiles, p.SwigCXXFiles, p.SysoFiles} {
		for _, f := range group {
			out = append(out, f+": non-Go source the scan cannot read")
		}
	}
	imports := append([]string{}, p.Imports...)
	if tests {
		imports = append(append(imports, p.TestImports...), p.XTestImports...)
	}
	for _, imp := range imports {
		if imp == modpath || strings.HasPrefix(imp, modpath+"/") {
			out = append(out, "imports "+imp+", an in-module package: a row may import nothing from this module (what it links can start goroutines the scan cannot soundly follow, and a path outside go list ./... is not scanned at all)")
		}
		if external(modpath, imp) {
			out = append(out, "imports out-of-module package "+imp+" (code outside the standard library and this module can run user code on goroutines of its own)")
		}
	}
	for _, f := range goFilesOf(p, tests) {
		out = append(out, scanFile(filepath.Join(p.Dir, f), ts)...)
	}
	return out
}

// Check returns every problem of rows against the module at root; the shipped call has root = the repository.
func Check(root string, rows []Row) ([]string, error) {
	modpath, pkgs, err := goList(root)
	if err != nil {
		return nil, err
	}
	var problems []string
	need := map[string]bool{}
	for _, r := range rows {
		if p := pkgs[modpath+"/"+r.Package]; p != nil {
			need[p.ImportPath] = true
		}
	}
	var patterns []string
	for k := range need {
		patterns = append(patterns, k)
	}
	sort.Strings(patterns)
	var ts typeStore
	loadErrs := map[string][]string{}
	if len(patterns) > 0 {
		if ts, loadErrs, err = loadTypes(root, patterns); err != nil {
			return nil, err
		}
	}
	for _, r := range rows {
		full := modpath + "/" + r.Package
		p := pkgs[full]
		switch {
		case r.Package == "internal/providersync":
			problems = append(problems, r.Package+": runs as test-name slices in every race leg and cannot be excluded")
			continue
		case p == nil:
			problems = append(problems, r.Package+": no such package")
			continue
		case len(p.TestGoFiles)+len(p.XTestGoFiles) == 0:
			problems = append(problems, r.Package+": has no test, so no race run to retire")
			continue
		}
		if r.Class != "none" || r.Witness != "-" {
			problems = append(problems, r.Package+": only class none with witness - exists (covered-by was retired: every row is judged on its whole in-module closure)")
		}
		for _, hit := range scanPackage(p, true, modpath, ts, loadErrs) {
			problems = append(problems, fmt.Sprintf("%s: own file: %s", r.Package, hit))
		}
	}
	sort.Strings(problems)
	return problems, nil
}

// stdlibAllowed is the deny-by-default allowlist of standard-library imports (acp, CHAOS-11413): a package that is not here is refused,
// so context (a custom parent calls the user's Err on a goroutine), net/rpc (a goroutine per request), runtime (SetFinalizer),
// reflect, sync, net/*, os/exec and os/signal are out until someone proves them. Admitted: only what the listed packages import
// today, each known to start no goroutine and to call user code (String, Error, MarshalJSON, Less ...) only on the calling goroutine.
var stdlibAllowed = map[string]string{
	"bytes":         "byte-slice helpers and Buffer: no goroutine; calls io.Reader and io.Writer methods on the calling goroutine only",
	"encoding/hex":  "pure encode and decode of hex text: no goroutine and no callback",
	"encoding/json": "Marshal and Unmarshal call user MarshalJSON and UnmarshalJSON on the calling goroutine; no goroutine started",
	"errors":        "error wrapping and matching: no goroutine; Is and As call user methods on the calling goroutine",
	"fmt":           "formatting calls user String, Error and Format methods on the calling goroutine; no goroutine started",
	"io":            "Reader and Writer helpers: no goroutine; io.Pipe only synchronises goroutines the caller already started",
	"io/fs":         "file-system interfaces and WalkDir: calls fn on the calling goroutine (documented); no goroutine started",
	"log/slog":      "structured logging: the Handler is called on the calling goroutine; no goroutine started",
	"math":          "pure numeric functions: no goroutine and no callback",
	"net/netip":     "value types for IP addresses: no goroutine, no I/O and no callback",
	"os":            "file and environment access: synchronous calls, no goroutine started (os.StartProcess is refused by selector)",
	"path/filepath": "path text helpers and Walk/WalkDir: fn is called on the calling goroutine (documented); no goroutine started",
	"regexp":        "compiled expressions: matching runs on the calling goroutine and calls no user code; no goroutine started",
	"slices":        "slice helpers: callbacks run on the calling goroutine (documented); no goroutine started",
	"sort":          "sorting: Less and Swap run on the calling goroutine (documented); no goroutine started",
	"strconv":       "number and quote conversion: no goroutine and no callback",
	"strings":       "string helpers: callbacks (Map, FieldsFunc) run on the calling goroutine; no goroutine started",
	"testing":       "runs tests on its own goroutines but t.Run waits for the subtest; t.Parallel and b.RunParallel are refused by selector",
	"time":          "clock and durations: Sleep blocks the calling goroutine; AfterFunc, NewTimer, NewTicker and Tick are refused by selector",
	"unicode":       "rune classification tables: no goroutine and no callback",
	"unicode/utf8":  "UTF-8 encode and decode: no goroutine and no callback",
}

// stdlibPath: the import path belongs to the standard library (or is the pseudo-package unsafe): its first element has no dot.
func stdlibPath(path string) bool {
	return path != "C" && !strings.Contains(strings.SplitN(path, "/", 2)[0], ".")
}

// typeOf: the type of an expression, also for the identifiers an assignment writes to.
func (tf *typed) typeOf(e ast.Expr) types.Type {
	if tv, ok := tf.info.Types[e]; ok {
		return tv.Type
	}
	if id, ok := e.(*ast.Ident); ok {
		if obj := tf.info.Uses[id]; obj != nil {
			return obj.Type()
		}
		if obj := tf.info.Defs[id]; obj != nil {
			return obj.Type()
		}
	}
	return nil
}

func isInterface(t types.Type) bool {
	if t == nil {
		return false
	}
	if _, ok := t.(*types.TypeParam); ok {
		return false
	}
	_, ok := t.Underlying().(*types.Interface)
	return ok
}

// ifaceConversions: the positions where a function-typed value meets an interface-typed target, whether the conversion is written
// (any(f)) or implied (an assignment, an argument, a return, a composite literal element).
func (tf *typed) ifaceConversions() []token.Pos {
	var out []token.Pos
	check := func(target types.Type, e ast.Expr) {
		if isInterface(target) && tf.isFunc(e) {
			out = append(out, e.Pos())
		}
	}
	ast.Inspect(tf.file, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			tv, ok := tf.info.Types[x.Fun]
			if !ok {
				return true
			}
			if tv.IsType() {
				if len(x.Args) == 1 {
					check(tv.Type, x.Args[0])
				}
				return true
			}
			sig, ok := tv.Type.Underlying().(*types.Signature)
			if !ok {
				return true
			}
			np := sig.Params().Len()
			for i, a := range x.Args {
				var pt types.Type
				switch {
				case sig.Variadic() && i >= np-1 && x.Ellipsis == token.NoPos:
					if sl, ok := sig.Params().At(np - 1).Type().(*types.Slice); ok {
						pt = sl.Elem()
					}
				case i < np:
					pt = sig.Params().At(i).Type()
				}
				check(pt, a)
			}
		case *ast.AssignStmt:
			if x.Tok != token.DEFINE && len(x.Lhs) == len(x.Rhs) {
				for i := range x.Lhs {
					check(tf.typeOf(x.Lhs[i]), x.Rhs[i])
				}
			}
		case *ast.ValueSpec:
			if x.Type != nil {
				for _, v := range x.Values {
					check(tf.typeOf(x.Type), v)
				}
			}
		case *ast.SendStmt:
			if ct := tf.typeOf(x.Chan); ct != nil {
				if ch, ok := ct.Underlying().(*types.Chan); ok {
					check(ch.Elem(), x.Value)
				}
			}
		case *ast.CompositeLit:
			t := tf.typeOf(x)
			if t == nil {
				return true
			}
			switch u := t.Underlying().(type) {
			case *types.Struct:
				for i, el := range x.Elts {
					if kv, ok := el.(*ast.KeyValueExpr); ok {
						if id, ok := kv.Key.(*ast.Ident); ok {
							for j := 0; j < u.NumFields(); j++ {
								if u.Field(j).Name() == id.Name {
									check(u.Field(j).Type(), kv.Value)
								}
							}
						}
					} else if i < u.NumFields() {
						check(u.Field(i).Type(), el)
					}
				}
			case *types.Slice, *types.Array, *types.Map:
				var key, elem types.Type
				switch m := u.(type) {
				case *types.Slice:
					elem = m.Elem()
				case *types.Array:
					elem = m.Elem()
				case *types.Map:
					key, elem = m.Key(), m.Elem()
				}
				for _, el := range x.Elts {
					if kv, ok := el.(*ast.KeyValueExpr); ok {
						check(key, kv.Key)
						check(elem, kv.Value)
					} else {
						check(elem, el)
					}
				}
			}
		}
		return true
	})
	var returns func(body *ast.BlockStmt, sig *types.Signature)
	returns = func(body *ast.BlockStmt, sig *types.Signature) {
		if body == nil || sig == nil {
			return
		}
		ast.Inspect(body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.FuncLit:
				s, _ := tf.typeOf(x).(*types.Signature)
				returns(x.Body, s)
				return false
			case *ast.ReturnStmt:
				if len(x.Results) == sig.Results().Len() {
					for i, r := range x.Results {
						check(sig.Results().At(i).Type(), r)
					}
				}
			}
			return true
		})
	}
	ast.Inspect(tf.file, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.FuncDecl:
			if fn, ok := tf.info.Defs[x.Name].(*types.Func); ok {
				s, _ := fn.Type().(*types.Signature)
				returns(x.Body, s)
			}
			return false
		case *ast.FuncLit:
			s, _ := tf.typeOf(x).(*types.Signature)
			returns(x.Body, s)
			return false
		}
		return true
	})
	return out
}
