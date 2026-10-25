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
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
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
	"os/exec": true, "os/signal": true, "net": true, "net/http": true, "database/sql": true,
}

// bannedSelectors are calls that start a goroutine or a timer goroutine without a `go` statement.
var bannedSelectors = map[string]bool{"Parallel": true, "AfterFunc": true, "NewTicker": true, "NewTimer": true, "Tick": true}

type goPackage struct {
	ImportPath     string
	Dir            string
	GoFiles        []string
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
		if f[1] != "none" && !strings.HasPrefix(f[1], "covered-by:") {
			return nil, fmt.Errorf("%s: %s has class %q, want none or covered-by:<package>", path, f[0], f[1])
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
		cmd.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=readonly")
		return cmd.Output()
	}
	out, err := run("list", "-m")
	if err != nil {
		return "", nil, fmt.Errorf("go list -m: %w", err)
	}
	modpath = strings.TrimSpace(string(out))
	out, err = run("list", "-tags", "race", "-json", "./...")
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
func scanFile(path string) []string {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		return []string{path + ": unparsable: " + err.Error()}
	}
	var out []string
	at := func(p token.Pos, what string) {
		out = append(out, fmt.Sprintf("%s:%d: %s", filepath.Base(path), fset.Position(p).Line, what))
	}
	for _, cg := range f.Comments {
		for _, c := range cg.List {
			if strings.HasPrefix(c.Text, "//go:build") && strings.Contains(c.Text, "race") {
				at(c.Pos(), "build constraint names race")
			}
		}
	}
	for _, imp := range f.Imports {
		v := strings.Trim(imp.Path.Value, `"`)
		if bannedImports[v] {
			at(imp.Pos(), "imports "+v)
		}
		if imp.Name != nil && imp.Name.Name == "." {
			at(imp.Pos(), "dot import of "+v+" (its calls lose their package qualifier, so the scan cannot see them)")
		}
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
		}
		return true
	})
	return out
}

func goFilesOf(p *goPackage, tests bool) []string {
	files := append([]string{}, p.GoFiles...)
	if tests {
		files = append(files, p.TestGoFiles...)
		files = append(files, p.XTestGoFiles...)
		files = append(files, p.IgnoredGoFiles...)
	}
	return files
}

func scanPackage(p *goPackage, tests bool) []string {
	var out []string
	for _, f := range goFilesOf(p, tests) {
		out = append(out, scanFile(filepath.Join(p.Dir, f))...)
	}
	return out
}

// Check returns every problem of rows against the module at root; the shipped call has root = the repository.
func Check(root string, rows []Row) ([]string, error) {
	modpath, pkgs, err := goList(root)
	if err != nil {
		return nil, err
	}
	excluded := map[string]bool{}
	for _, r := range rows {
		excluded[r.Package] = true
	}
	var problems []string
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
		covered := ""
		if strings.HasPrefix(r.Class, "covered-by:") {
			covered = modpath + "/" + strings.TrimPrefix(r.Class, "covered-by:")
			c := pkgs[covered]
			switch {
			case c == nil:
				problems = append(problems, r.Package+": covered-by package does not exist: "+r.Class)
				covered = ""
			case excluded[strings.TrimPrefix(r.Class, "covered-by:")]:
				problems = append(problems, r.Package+": the covered-by package is itself excluded from -race: "+r.Class)
				covered = ""
			default:
				problems = append(problems, witnessProblems(r, c)...)
			}
		} else if r.Witness != "-" {
			problems = append(problems, r.Package+": class none takes witness -")
		}
		for _, hit := range scanPackage(p, true) {
			problems = append(problems, fmt.Sprintf("%s: own file: %s", r.Package, hit))
		}
		if covered != "" {
			for _, f := range goFilesOf(p, true) {
				for _, hit := range callbackProblems(filepath.Join(p.Dir, f)) {
					problems = append(problems, fmt.Sprintf("%s: own file: %s", r.Package, hit))
				}
			}
		}
		for _, dep := range closure(modpath, pkgs, p) {
			if covered != "" {
				for _, f := range goFilesOf(pkgs[dep], false) {
					for _, hit := range callbackProblems(filepath.Join(pkgs[dep].Dir, f)) {
						problems = append(problems, fmt.Sprintf("%s: imports %s: %s", r.Package, strings.TrimPrefix(dep, modpath+"/"), hit))
					}
				}
			}
			for _, hit := range scanPackage(pkgs[dep], false) {
				if dep == covered {
					continue
				}
				problems = append(problems, fmt.Sprintf("%s: imports %s: %s", r.Package, strings.TrimPrefix(dep, modpath+"/"), hit))
			}
		}
	}
	sort.Strings(problems)
	return problems, nil
}

// closure: every in-module package the package, its tests and its external tests link, directly or not.
func closure(modpath string, pkgs map[string]*goPackage, p *goPackage) []string {
	seen := map[string]bool{}
	var stack []string
	push := func(imps []string) {
		for _, i := range imps {
			if strings.HasPrefix(i, modpath+"/") && !seen[i] && pkgs[i] != nil {
				seen[i] = true
				stack = append(stack, i)
			}
		}
	}
	push(p.Imports)
	push(p.TestImports)
	push(p.XTestImports)
	for len(stack) > 0 {
		x := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		push(pkgs[x].Imports)
	}
	var out []string
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// witnessProblems: the covered-by package keeps a Test function named by the row, in a file with no race constraint, so the race
// run that stands in for the excluded package cannot vanish.
func witnessProblems(r Row, covered *goPackage) []string {
	for _, f := range append(append(append([]string{}, covered.TestGoFiles...), covered.XTestGoFiles...), covered.IgnoredGoFiles...) {
		path := filepath.Join(covered.Dir, f)
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, raw, parser.ParseComments)
		if err != nil {
			continue
		}
		for _, d := range file.Decls {
			if fn, ok := d.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == r.Witness {
				for _, cg := range file.Comments {
					for _, c := range cg.List {
						if strings.HasPrefix(c.Text, "//go:build") && strings.Contains(c.Text, "!race") {
							return []string{r.Package + ": witness " + r.Witness + " is compiled out of -race by " + f}
						}
					}
				}
				return nil
			}
		}
	}
	return []string{r.Package + ": witness test " + r.Witness + " not found in " + r.Class}
}

// callbackCallees names the functions that call their callback from several goroutines, with the index of that argument. A
// covered-by row is sound only while the callback writes nothing shared: the race detector would have to see two workers write it.
var callbackCallees = map[string]int{"CheckTree": 1}

// callbackStdlib are the only packages a callback may call into: they hold no state the callback's caller shares.
var callbackStdlib = map[string]bool{"os": true, "filepath": true, "path": true, "strings": true, "bytes": true, "fmt": true, "errors": true, "strconv": true}

// callbackProblems: in the file, a callbackCallees function is only ever called directly (never aliased through a variable or
// passed on), and its callback is a function literal or a package-level function of the same file that (1) assigns to no variable
// it did not declare itself (assignment, op-assignment, ++/--, range-assign, index, field or pointer write) and (2) calls only
// builtins and the callbackStdlib packages, so no helper, method or function value can write a captured variable for it.
func callbackProblems(path string) []string {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return nil
	}
	top := map[string]*ast.FuncDecl{}
	for _, d := range f.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok && fn.Recv == nil {
			top[fn.Name.Name] = fn
		}
	}
	var out []string
	at := func(p token.Pos, what string) {
		out = append(out, fmt.Sprintf("%s:%d: %s", filepath.Base(path), fset.Position(p).Line, what))
	}
	direct := map[ast.Expr]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			direct[call.Fun] = true
		}
		return true
	})
	std := map[string]bool{}
	for _, imp := range f.Imports {
		v := strings.Trim(imp.Path.Value, `"`)
		name := v[strings.LastIndex(v, "/")+1:]
		if imp.Name != nil {
			name = imp.Name.Name
		}
		if callbackStdlib[v[strings.LastIndex(v, "/")+1:]] && !strings.Contains(v, ".") {
			std[name] = true
		}
	}
	checkBody := func(callee string, ft *ast.FuncType, body *ast.BlockStmt, pos token.Pos) {
		for _, w := range capturedWrites(ft, body) {
			at(w.pos, callee+" callback writes captured variable "+w.name)
		}
		ast.Inspect(body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fn := call.Fun.(type) {
			case *ast.Ident:
				if !builtinFuncs[fn.Name] {
					at(call.Pos(), callee+" callback calls "+fn.Name+", which is neither a builtin nor a standard-library function")
				}
			case *ast.SelectorExpr:
				if pkg, ok := fn.X.(*ast.Ident); !ok || !std[pkg.Name] {
					at(call.Pos(), callee+" callback calls ."+fn.Sel.Name+", which is not a function of the allowed standard-library packages")
				}
			case *ast.ArrayType, *ast.MapType, *ast.InterfaceType:
			default:
				at(call.Pos(), callee+" callback calls a function value")
			}
			return true
		})
	}
	notUse := map[*ast.Ident]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.SelectorExpr:
			notUse[x.Sel] = true
		case *ast.FuncDecl:
			notUse[x.Name] = true
		}
		return true
	})
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.Ident:
			if _, watched := callbackCallees[x.Name]; watched && !notUse[x] && !direct[x] {
				at(x.Pos(), x.Name+" is used other than as a direct call")
			}
		case *ast.SelectorExpr:
			if _, watched := callbackCallees[x.Sel.Name]; watched && !direct[x] {
				at(x.Sel.Pos(), x.Sel.Name+" is used other than as a direct call")
			}
		}
		return true
	})
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := ""
		switch fn := call.Fun.(type) {
		case *ast.Ident:
			name = fn.Name
		case *ast.SelectorExpr:
			name = fn.Sel.Name
		}
		idx, watched := callbackCallees[name]
		if !watched {
			return true
		}
		if len(call.Args) <= idx {
			at(call.Pos(), name+" is called without its callback argument")
			return true
		}
		switch cb := call.Args[idx].(type) {
		case *ast.FuncLit:
			checkBody(name, cb.Type, cb.Body, cb.Pos())
		case *ast.Ident:
			if fn := top[cb.Name]; fn != nil && fn.Body != nil {
				checkBody(name, fn.Type, fn.Body, cb.Pos())
			} else {
				at(cb.Pos(), name+" callback "+cb.Name+" is not a package-level function of this file")
			}
		default:
			at(call.Args[idx].Pos(), name+" callback is neither a package-level function nor a function literal")
		}
		return true
	})
	return out
}

var builtinFuncs = map[string]bool{"len": true, "cap": true, "append": true, "make": true, "new": true, "string": true, "int": true, "byte": true, "rune": true, "min": true, "max": true, "copy": true, "delete": true, "clear": true, "panic": true, "error": true, "bool": true, "float64": true, "int64": true, "uint64": true, "uint8": true}

type write struct {
	pos  token.Pos
	name string
}

// capturedWrites: assignments in body to a root variable that the function did not declare (parameter, result, :=, var, range).
func capturedWrites(ft *ast.FuncType, body *ast.BlockStmt) []write {
	local := map[string]bool{"_": true}
	declare := func(fl *ast.FieldList) {
		if fl == nil {
			return
		}
		for _, fld := range fl.List {
			for _, n := range fld.Names {
				local[n.Name] = true
			}
		}
	}
	declare(ft.Params)
	declare(ft.Results)
	ast.Inspect(body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			if x.Tok == token.DEFINE {
				for _, l := range x.Lhs {
					if id, ok := l.(*ast.Ident); ok {
						local[id.Name] = true
					}
				}
			}
		case *ast.ValueSpec:
			for _, id := range x.Names {
				local[id.Name] = true
			}
		case *ast.RangeStmt:
			if x.Tok == token.DEFINE {
				for _, e := range []ast.Expr{x.Key, x.Value} {
					if id, ok := e.(*ast.Ident); ok {
						local[id.Name] = true
					}
				}
			}
		case *ast.FuncLit:
			declare(x.Type.Params)
			declare(x.Type.Results)
		}
		return true
	})
	root := func(e ast.Expr) string {
		for {
			switch x := e.(type) {
			case *ast.Ident:
				return x.Name
			case *ast.IndexExpr:
				e = x.X
			case *ast.SelectorExpr:
				e = x.X
			case *ast.StarExpr:
				e = x.X
			case *ast.ParenExpr:
				e = x.X
			default:
				return ""
			}
		}
	}
	var out []write
	check := func(e ast.Expr, pos token.Pos) {
		if e == nil {
			return
		}
		if n := root(e); n != "" && !local[n] {
			out = append(out, write{pos, n})
		}
	}
	ast.Inspect(body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			if x.Tok != token.DEFINE {
				for _, l := range x.Lhs {
					check(l, x.Pos())
				}
			}
		case *ast.IncDecStmt:
			check(x.X, x.Pos())
		case *ast.RangeStmt:
			if x.Tok == token.ASSIGN {
				check(x.Key, x.Pos())
				check(x.Value, x.Pos())
			}
		case *ast.UnaryExpr:
			if x.Op == token.AND {
				check(x.X, x.Pos())
			}
		}
		return true
	})
	return out
}
