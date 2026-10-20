package affected

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// readCalls are the calls whose string arguments name a file or a directory the caller reads.
var readCalls = map[string]bool{
	"filepath.Join": true, "path.Join": true, "os.ReadFile": true, "os.ReadDir": true, "os.Open": true, "os.Stat": true,
	"os.Lstat": true, "os.DirFS": true, "filepath.WalkDir": true, "filepath.Walk": true, "filepath.Glob": true,
	"fs.WalkDir": true, "fs.Glob": true, "fs.ReadFile": true, "fs.ReadDir": true, "fs.Sub": true, "os.OpenFile": true,
}

// walkCalls list a directory tree or a directory.
var walkCalls = map[string]bool{
	"filepath.WalkDir": true, "filepath.Walk": true, "fs.WalkDir": true, "filepath.Glob": true, "fs.Glob": true, "os.ReadDir": true,
	"os.DirFS": true,
}

var rootLocator = regexp.MustCompile(`^(?i:(find)?(repo|module|git)_?root(dir)?|root|reporoot|moduleroot)$`)

// Read is one place a package names a repository path, or walks from the repository root.
type Read struct {
	Package string // directory of the package, relative to the repository root
	File    string // repository path of the Go file
	Line    int
	Test    bool   // the file is a _test.go file
	Path    string // the repository path named; empty for a walk from the repository root
	Literal string
}

func (r Read) String() string {
	what := fmt.Sprintf("names %s (literal %q)", r.Path, r.Literal)
	if r.Path == "" {
		what = "walks from the repository root"
	}
	return fmt.Sprintf("%s:%d %s", r.File, r.Line, what)
}

// topLevel is the repository's first-level entries, the only names a bare path segment is matched against.
func topLevel(root string) (map[string]bool, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, e := range entries {
		if e.Name() != ".git" {
			out[e.Name()] = true
		}
	}
	return out, nil
}

// normalizeRepoPath strips leading ./ and ../ and a trailing slash; "" when nothing is left.
func normalizeRepoPath(v string) string {
	for {
		switch {
		case strings.HasPrefix(v, "../"):
			v = v[3:]
		case strings.HasPrefix(v, "./"):
			v = v[2:]
		default:
			v = strings.TrimRight(v, "/")
			if v == ".." || v == "." || v == "" || strings.HasPrefix(v, "/") {
				return ""
			}
			return path.Clean(v)
		}
	}
}

// ScanReads returns every repository read of every Go package directory under root (testdata, vendor and dot
// directories skipped). A string literal is a read when it names a path under a first-level entry of the repository:
// a literal holding a slash whose first segment is such an entry, or a bare entry name passed to a read call.
// A walk call in a package that also locates the repository root is a read of the whole repository.
func ScanReads(root string) ([]Read, error) {
	top, err := topLevel(root)
	if err != nil {
		return nil, err
	}
	var reads []Read
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := d.Name()
		if d.IsDir() {
			if p != root && (strings.HasPrefix(name, ".") || name == "testdata" || name == "vendor" || name == "node_modules") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		got, err := scanFile(p, filepath.ToSlash(rel), top)
		if err != nil {
			return err
		}
		reads = append(reads, got...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	// A walk is a repository-root walk only when its package locates the root.
	locates := map[string]bool{}
	for _, r := range reads {
		if r.Literal == "\x00root" {
			locates[r.Package] = true
		}
	}
	var out []Read
	for _, r := range reads {
		switch r.Literal {
		case "\x00root":
			continue
		case "\x00walk":
			if !locates[r.Package] {
				continue
			}
			r.Literal = ""
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].File != out[j].File {
			return out[i].File < out[j].File
		}
		return out[i].Line < out[j].Line
	})
	return out, nil
}

func callName(e ast.Expr) string {
	switch f := e.(type) {
	case *ast.SelectorExpr:
		if x, ok := f.X.(*ast.Ident); ok {
			return x.Name + "." + f.Sel.Name
		}
		return f.Sel.Name
	case *ast.Ident:
		return f.Name
	}
	return ""
}

func scanFile(file, rel string, top map[string]bool) ([]Read, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	pkg := path.Dir(rel)
	test := strings.HasSuffix(rel, "_test.go")
	var out []Read
	add := func(pos token.Pos, p, lit string) {
		out = append(out, Read{Package: pkg, File: rel, Line: fset.Position(pos).Line, Test: test, Path: p, Literal: lit})
	}
	inReadCall := map[*ast.BasicLit]bool{}
	consumed := map[*ast.BasicLit]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := callName(call.Fun)
		short := name
		if i := strings.LastIndexByte(name, '.'); i >= 0 {
			short = name[i+1:]
		}
		if rootLocator.MatchString(short) {
			add(call.Pos(), "", "\x00root")
		}
		if walkCalls[name] {
			add(call.Pos(), "", "\x00walk")
		}
		if name == "filepath.Join" || name == "path.Join" {
			joinLiterals(call, top, consumed, add)
		}
		if readCalls[name] {
			for _, a := range call.Args {
				ast.Inspect(a, func(m ast.Node) bool {
					if lit, ok := m.(*ast.BasicLit); ok && lit.Kind == token.STRING {
						inReadCall[lit] = true
					}
					return true
				})
			}
		}
		return true
	})
	ast.Inspect(f, func(n ast.Node) bool {
		lit, ok := n.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING || consumed[lit] {
			return true
		}
		v, err := strconv.Unquote(lit.Value)
		if err != nil {
			return true
		}
		if v == "go.mod" && inReadCall[lit] {
			add(lit.Pos(), "", "\x00root")
			return true
		}
		p := normalizeRepoPath(v)
		if p == "" || strings.ContainsAny(p, " \t\n:*?[{$%") {
			return true
		}
		first := strings.SplitN(p, "/", 2)[0]
		if !top[first] {
			return true
		}
		if strings.Contains(p, "/") || inReadCall[lit] || (strings.Contains(first, ".") && !strings.HasPrefix(first, ".")) {
			add(lit.Pos(), p, v)
		}
		return true
	})
	return out, nil
}

// checkReads returns one line per defect: a read with no covering reads.tsv row, a code read declared only as test, a
// row for a package directory with no Go file. A read of a path in no class needs no row: a change there is FULL.
func checkReads(root string, t *tables, reads []Read) []string {
	declared := map[string]map[string]string{} // package -> class -> scope
	for _, r := range t.reads {
		if declared[r.pkg] == nil {
			declared[r.pkg] = map[string]string{}
		}
		declared[r.pkg][r.class] = r.scope
	}
	walked := map[string]map[string]string{} // package -> class -> scope, from walks.tsv
	for _, r := range t.walks {
		if walked[r.pkg] == nil {
			walked[r.pkg] = map[string]string{}
		}
		walked[r.pkg][r.class] = r.scope
	}
	walkers := map[string]bool{}
	var out []string
	seen := map[string]bool{}
	report := func(s string) {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	for _, r := range reads {
		scope := scopeTest
		if !r.Test {
			scope = scopeCode
		}
		if r.Path == "" {
			walkers[r.Package] = true
			switch {
			case len(walked[r.Package]) == 0:
				report(fmt.Sprintf("%s: undeclared walker: %s; add a walks.tsv row %s\t%s\t<class> for every class the walk reads (%s when it reads only Go package trees)", r.Package, r, r.Package, scope, goTrees))
			case scope == scopeCode && len(codeOnly(walked[r.Package])) == 0:
				report(fmt.Sprintf("%s: %s is a walk in non-test code, so its walks.tsv rows must be %s\t%s\t<class> (its importers' tests walk too)", r.Package, r, r.Package, scopeCode))
			}
			continue
		}
		class := t.suggest(r.Path)
		if class == "" {
			continue
		}
		switch {
		case !t.covers(r.Path, declared[r.Package]):
			report(fmt.Sprintf("%s: undeclared reader: %s; add the reads.tsv row %s\t%s\t%s", r.Package, r, r.Package, scope, class))
		case scope == scopeCode && !t.covers(r.Path, codeOnly(declared[r.Package])):
			report(fmt.Sprintf("%s: %s is read by non-test code, so the row must be %s\t%s\t%s (its importers' tests read it too)", r.Package, r, r.Package, scopeCode, class))
		}
	}
	for _, r := range t.walks {
		if !walkers[r.pkg] {
			report(fmt.Sprintf("walks.tsv:%d: package %s does not walk from the repository root (no walk call in a package that locates the root)", r.line, r.pkg))
		}
	}
	for _, r := range t.reads {
		entries, err := os.ReadDir(filepath.Join(root, filepath.FromSlash(r.pkg)))
		hasGo := false
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".go") {
				hasGo = true
			}
		}
		if err != nil || !hasGo {
			report(fmt.Sprintf("reads.tsv:%d: package %s has no Go file", r.line, r.pkg))
		}
	}
	return out
}

func codeOnly(classes map[string]string) map[string]string {
	out := map[string]string{}
	for c, scope := range classes {
		if scope == scopeCode {
			out[c] = scope
		}
	}
	return out
}

// PlanReads is the guard of the declared-read table: it fails, naming each one, when a Go package reads a path of a
// declared class (or walks from the repository root) without a reads.tsv row that covers it.
func PlanReads(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("plan-reads", flag.ContinueOnError)
	fs.SetOutput(stderr)
	root := fs.String("root", "", "repository root")
	tablesDir := fs.String("tables", "", "directory of classes.tsv and reads.tsv (default ROOT/internal/cigates/affected)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *root == "" {
		fmt.Fprintln(stderr, "plan-reads: --root is required")
		return 2
	}
	dir := *tablesDir
	if dir == "" {
		dir = filepath.Join(*root, filepath.FromSlash(plannerDir))
	}
	t, err := loadTables(dir)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	reads, err := ScanReads(*root)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	defects := checkReads(*root, t, reads)
	for _, d := range defects {
		fmt.Fprintln(stdout, d)
	}
	if len(defects) > 0 {
		fmt.Fprintf(stdout, "plan-reads: %d defect(s) in the declared-read table\n", len(defects))
		return 1
	}
	fmt.Fprintf(stdout, "plan-reads: %d read(s) covered by %d reads row(s) and %d walks row(s)\n", len(reads), len(t.reads), len(t.walks))
	return 0
}

// joinLiterals reads a Join whose string-literal arguments, from one naming a first-level entry on, spell one path:
// filepath.Join(root, "docs", "a.md") reads docs/a.md.
func joinLiterals(call *ast.CallExpr, top map[string]bool, consumed map[*ast.BasicLit]bool, add func(token.Pos, string, string)) {
	var parts []string
	var lits []*ast.BasicLit
	flush := func() {
		if len(parts) > 0 {
			p := normalizeRepoPath(path.Join(parts...))
			if p != "" && top[strings.SplitN(p, "/", 2)[0]] {
				for _, l := range lits {
					consumed[l] = true
				}
				add(lits[0].Pos(), p, strings.Join(parts, ", "))
				if p == "go.mod" {
					add(lits[0].Pos(), "", "\x00root")
				}
			}
		}
		parts, lits = nil, nil
	}
	for _, a := range call.Args {
		lit, ok := a.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			flush()
			continue
		}
		v, err := strconv.Unquote(lit.Value)
		if err != nil || v == "" {
			flush()
			continue
		}
		if len(parts) == 0 && (v == ".." || v == ".") {
			continue
		}
		parts = append(parts, v)
		lits = append(lits, lit)
	}
	flush()
}
