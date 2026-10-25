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
		if v := strings.Trim(imp.Path.Value, `"`); bannedImports[v] {
			at(imp.Pos(), "imports "+v)
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
		for _, dep := range closure(modpath, pkgs, p) {
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
