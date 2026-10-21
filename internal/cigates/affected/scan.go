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
	"os/exec"
	"path"
	"path/filepath"
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

// Read is one place a package names a repository path.
type Read struct {
	Package string // directory of the package, relative to the repository root
	File    string // repository path of the Go file
	Line    int
	Test    bool   // the file is a _test.go file
	Path    string // the repository path named
	Literal string
}

func (r Read) String() string {
	what := fmt.Sprintf("names %s (literal %q)", r.Path, r.Literal)
	return fmt.Sprintf("%s:%d %s", r.File, r.Line, what)
}

// topLevel is the repository's first-level entries, the only names a bare path segment is matched against.
func topLevel(root string, files []string) (map[string]bool, error) {
	out := map[string]bool{}
	if files != nil {
		for _, f := range files {
			out[strings.SplitN(f, "/", 2)[0]] = true
		}
		return out, nil
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.Name() != ".git" {
			out[e.Name()] = true
		}
	}
	return out, nil
}

// repoFiles is the tracked files of the git checkout at root (`git ls-files`), so a scan reads what CI checks out and
// never a local untracked or ignored file; nil when root is not a git checkout (a plain fixture tree is walked instead).
func repoFiles(root string) []string {
	if _, err := os.Stat(filepath.Join(root, ".git")); err != nil {
		return nil
	}
	out, err := exec.Command("git", "-C", root, "ls-files", "-z").Output()
	if err != nil {
		return nil
	}
	var files []string
	for _, f := range strings.Split(string(out), "\x00") {
		if f != "" {
			files = append(files, f)
		}
	}
	return files
}

// skippedGoDir: directories the source scan never enters below the root.
func skippedGoDir(name string) bool {
	return strings.HasPrefix(name, ".") || name == "testdata" || name == "vendor" || name == "node_modules"
}

// goSources is every Go file the scan reads, as repository paths.
func goSources(root string, files []string) ([]string, error) {
	var out []string
	if files != nil {
		for _, f := range files {
			if !strings.HasSuffix(f, ".go") {
				continue
			}
			segs := strings.Split(f, "/")
			skip := false
			for _, d := range segs[:len(segs)-1] {
				if skippedGoDir(d) {
					skip = true
				}
			}
			if !skip {
				if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(f))); err == nil {
					out = append(out, f)
				}
			}
		}
		return out, nil
	}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p != root && skippedGoDir(d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(d.Name(), ".go") {
			rel, err := filepath.Rel(root, p)
			if err != nil {
				return err
			}
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	return out, err
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
func ScanReads(root string) ([]Read, error) {
	tracked := repoFiles(root)
	top, err := topLevel(root, tracked)
	if err != nil {
		return nil, err
	}
	sources, err := goSources(root, tracked)
	if err != nil {
		return nil, err
	}
	var reads []Read
	for _, rel := range sources {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, filepath.Join(root, filepath.FromSlash(rel)), nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		reads = append(reads, scanFile(fset, f, rel, top)...)
	}
	out := reads
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

func scanFile(fset *token.FileSet, f *ast.File, rel string, top map[string]bool) []Read {
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
	return out
}

// checkReads returns one line per defect: a read with no covering reads.tsv row, a code read declared only as test, a
// row for a package directory with no Go file. A read of a path in no class needs no row.
func checkReads(root string, t *tables, reads []Read) (defects []string, add, replace map[string]bool) {
	add, replace = map[string]bool{}, map[string]bool{}
	declared := map[string]map[string]string{} // package -> class -> scope
	for _, r := range t.reads {
		if declared[r.pkg] == nil {
			declared[r.pkg] = map[string]string{}
		}
		declared[r.pkg][r.class] = r.scope
	}
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
		class := t.suggest(r.Path)
		if class == "" {
			continue
		}
		switch {
		case !t.covers(r.Path, declared[r.Package]):
			report(fmt.Sprintf("%s: undeclared reader: %s; add the reads.tsv row %s\t%s\t%s", r.Package, r, r.Package, scope, class))
			add[r.Package+"\t"+scope+"\t"+class] = true
		case scope == scopeCode && !t.covers(r.Path, codeOnly(declared[r.Package])):
			report(fmt.Sprintf("%s: %s is read by non-test code, so the row must be %s\t%s\t%s (its importers' tests read it too)", r.Package, r, r.Package, scopeCode, class))
			if declared[r.Package][class] == scopeTest {
				replace[r.Package+"\t"+scopeTest+"\t"+class+" -> "+r.Package+"\t"+scopeCode+"\t"+class] = true
			} else {
				add[r.Package+"\t"+scopeCode+"\t"+class] = true
			}
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
	for row := range add {
		if pkg, class, ok := strings.Cut(row, "\t"+scopeTest+"\t"); ok && add[pkg+"\t"+scopeCode+"\t"+class] {
			delete(add, row) // the code row covers the tests' reads too
		}
	}
	return out, add, replace
}

// printRows prints a heading and the rows under it, one exact TSV line each, sorted, ready to paste.
func printRows(w io.Writer, heading string, rows map[string]bool) {
	if len(rows) == 0 {
		return
	}
	sorted := make([]string, 0, len(rows))
	for r := range rows {
		sorted = append(sorted, r)
	}
	sort.Strings(sorted)
	fmt.Fprintln(w, heading)
	for _, r := range sorted {
		fmt.Fprintln(w, r)
	}
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
// declared class without a reads.tsv row that covers it.
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
		dir = filepath.Join(*root, "internal", "cigates", "affected")
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
	defects, add, replace := checkReads(*root, t, reads)
	for _, d := range defects {
		fmt.Fprintln(stdout, d)
	}
	if len(defects) > 0 {
		table := filepath.ToSlash(filepath.Join(dir, readsFile))
		if rel, err := filepath.Rel(*root, filepath.Join(dir, readsFile)); err == nil && !strings.HasPrefix(rel, "..") {
			table = filepath.ToSlash(rel)
		}
		printRows(stdout, "add these rows to "+table+":", add)
		printRows(stdout, "replace these rows in "+table+":", replace)
		fmt.Fprintf(stdout, "plan-reads: %d defect(s) in the declared-read table\n", len(defects))
		return 1
	}
	fmt.Fprintf(stdout, "plan-reads: %d read(s) covered by %d reads row(s)\n", len(reads), len(t.reads))
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
