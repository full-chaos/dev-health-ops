package affected

import (
	"bytes"
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
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// readCalls are the calls whose string arguments name a file or a directory the caller reads.
var readCalls = map[string]bool{
	"filepath.Join": true, "path.Join": true, "os.ReadFile": true, "os.ReadDir": true, "os.Open": true, "os.Stat": true,
	"os.Lstat": true, "os.DirFS": true, "filepath.WalkDir": true, "filepath.Walk": true, "filepath.Glob": true,
	"fs.WalkDir": true, "fs.Glob": true, "fs.ReadFile": true, "fs.ReadDir": true, "fs.Sub": true, "os.OpenFile": true,
}

// Kinds of Read: a path named or resolved (ReadPath), a read the tool cannot bound (ReadDyn), a walk of Root to Depth
// (ReadWalk), a run or load of the Go packages of Target (ReadEdge). dynreads.go finds the last three.
const (
	ReadPath = ""
	ReadDyn  = "dyn"
	ReadWalk = "walk"
	ReadEdge = "edge"
	// ReadOpaque: a file the call graph of reach.tsv cannot see through (go:linkname, cgo, plugin); its tables are
	// defective, so every plan is FULL.
	ReadOpaque = "opaque"
)

// Read is one place a package names a repository path, or reads, walks, runs or loads one.
type Read struct {
	Package string // directory of the package, relative to the repository root
	File    string // repository path of the Go file
	Line    int
	Test    bool   // the file is a _test.go file
	Kind    string // ReadPath, ReadDyn, ReadWalk or ReadEdge
	Path    string // ReadPath: the repository path named
	Root    string // ReadWalk: the directory walked, or unexpressed
	Depth   string // ReadWalk: tree or dir
	Target  string // ReadEdge: the packages run or loaded (dir, dir/..., **/dir), or unexpressed
	Literal string
}

func (r Read) String() string {
	what := fmt.Sprintf("names %s (literal %q)", r.Path, r.Literal)
	switch r.Kind {
	case ReadDyn:
		what = fmt.Sprintf("reads a path the tool cannot bound (%s)", r.Literal)
	case ReadWalk:
		what = fmt.Sprintf("walks %s (%s; %s)", r.Root, r.Depth, r.Literal)
	case ReadEdge:
		what = fmt.Sprintf("runs or loads %s (%s)", r.Target, r.Literal)
	}
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
	byDir := map[string][]*srcFile{}
	var dirs []string
	for _, rel := range sources {
		fset := token.NewFileSet()
		src, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			return nil, err
		}
		f, err := parser.ParseFile(fset, filepath.Join(root, filepath.FromSlash(rel)), src, parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		reads = append(reads, scanFile(fset, f, rel, top)...)
		reads = append(reads, opaqueReads(fset, f, rel, src)...)
		d := path.Dir(rel)
		if byDir[d] == nil {
			dirs = append(dirs, d)
		}
		byDir[d] = append(byDir[d], newSrcFile(rel, fset, f))
	}
	module := modulePath(root)
	for _, d := range dirs {
		reads = append(reads, newDirGroup(d, module, byDir[d]).analyze()...)
	}
	out := reads
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].File != out[j].File {
			return out[i].File < out[j].File
		}
		return out[i].Line < out[j].Line
	})
	return out, nil
}

var (
	linknameDirective = regexp.MustCompile(`(?m)^//go:linkname\s`)
	notIntegration    = regexp.MustCompile(`(?m)^//go:build .*!\s*integration\b`)
)

// opaqueReads names what in one file hides calls from the static call graph.
func opaqueReads(fset *token.FileSet, f *ast.File, rel string, src []byte) []Read {
	var out []Read
	add := func(line int, what string) {
		out = append(out, Read{Package: path.Dir(rel), File: rel, Line: line, Test: strings.HasSuffix(rel, "_test.go"), Kind: ReadOpaque, Literal: what})
	}
	if bytes.Contains(src, []byte("//go:linkname")) {
		if loc := linknameDirective.FindIndex(src); loc != nil {
			add(bytes.Count(src[:loc[0]], []byte("\n"))+1, "go:linkname")
		}
	}
	// reach.tsv loads the module with the integration tag only: an untagged build is a subset of that build unless a
	// file is built only without the tag.
	if bytes.Contains(src, []byte("integration")) {
		if loc := notIntegration.FindIndex(src); loc != nil {
			add(bytes.Count(src[:loc[0]], []byte("\n"))+1, "a build constraint on !integration")
		}
	}
	for _, im := range f.Imports {
		switch p, _ := strconv.Unquote(im.Path.Value); p {
		case "C":
			add(fset.Position(im.Pos()).Line, "cgo")
		case "plugin":
			add(fset.Position(im.Pos()).Line, "plugin")
		}
	}
	return out
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

// guardResult is what the guard of the read tables finds: one line per defect, and the rows that fix them.
type guardResult struct {
	defects []string
	add     map[string]map[string]bool // table file -> rows to add
	remove  map[string]map[string]bool // generated table file -> rows to remove
	replace map[string]bool            // reads.tsv rows to replace (`old -> new`)
}

// checkReads returns one line per defect: a read with no covering reads.tsv row, a code read declared only as test, a
// row for a package directory with no Go file, and every difference between the generated tables (walks.tsv,
// dynreads.tsv, edges.tsv) and the rows the code calls for. A read of a path in no class needs no reads.tsv row: a
// change there is FULL.
func checkReads(root string, t *tables, reads []Read) guardResult {
	g := guardResult{add: map[string]map[string]bool{readsFile: {}}, remove: map[string]map[string]bool{}, replace: map[string]bool{}}
	add := g.add[readsFile]
	declared := map[string]map[string]string{} // package -> class -> scope
	for _, r := range t.reads {
		if declared[r.pkg] == nil {
			declared[r.pkg] = map[string]string{}
		}
		declared[r.pkg][r.class] = r.scope
	}
	seen := map[string]bool{}
	report := func(s string) {
		if !seen[s] {
			seen[s] = true
			g.defects = append(g.defects, s)
		}
	}
	for _, r := range reads {
		if r.Kind != ReadPath {
			continue
		}
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
				g.replace[r.Package+"\t"+scopeTest+"\t"+class+" -> "+r.Package+"\t"+scopeCode+"\t"+class] = true
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
	gen := generatedRows(reads)
	have := t.generated()
	for _, file := range generatedFiles {
		g.add[file], g.remove[file] = map[string]bool{}, map[string]bool{}
		for _, row := range sortedKeys(gen[file]) {
			if _, ok := have[file][row]; !ok {
				pkg := strings.SplitN(row, "\t", 2)[0]
				report(fmt.Sprintf("%s: undeclared %s: %s; add the %s row %s", pkg, tableWhat[file], gen[file][row], file, row))
				g.add[file][row] = true
			}
		}
		for _, row := range sortedKeys(have[file]) {
			if _, ok := gen[file][row]; !ok {
				report(fmt.Sprintf("%s:%s: %s is a row the code does not call for (the code's rows come from its AST)", file, have[file][row], row))
				g.remove[file][row] = true
			}
		}
	}
	// reach.tsv places every code-scope site and no other; its reachers are checked by plan-reads --reach.
	sites := codeSites(reads)
	for _, k := range sortedKeys(sites) {
		if _, ok := t.reach[k]; !ok {
			report(fmt.Sprintf("%s: %s has no %s row; run cigate plan-reads --root . --write", strings.SplitN(k, "\t", 2)[0], sites[k][0], reachFile))
		}
	}
	for _, row := range sortedKeys(t.reachRows) {
		f := strings.SplitN(row, "\t", 3)
		if _, ok := sites[f[0]+"\t"+f[1]]; !ok {
			report(fmt.Sprintf("%s:%s: %s names no code-scope site the tool cannot bound; run cigate plan-reads --root . --write", reachFile, t.reachRows[row], row))
		}
	}
	for _, r := range reads {
		if r.Kind == ReadOpaque {
			report(fmt.Sprintf("%s:%d: %s hides calls from the call graph of %s: every plan is FULL", r.File, r.Line, r.Literal, reachFile))
		}
	}
	return g
}

// checkReach recomputes reach.tsv and returns one defect per differing row.
func checkReach(root string, t *tables, reads []Read) ([]string, reachStats, error) {
	want, st, err := reachRows(root, reads)
	if err != nil {
		return nil, st, err
	}
	var defects []string
	for _, row := range sortedKeys(want) {
		if _, ok := t.reachRows[row]; !ok {
			defects = append(defects, fmt.Sprintf("%s: missing row %s (run cigate plan-reads --root . --write)", reachFile, row))
		}
	}
	for _, row := range sortedKeys(t.reachRows) {
		if !want[row] {
			defects = append(defects, fmt.Sprintf("%s:%s: %s is a row the call graph does not call for (run cigate plan-reads --root . --write)", reachFile, t.reachRows[row], row))
		}
	}
	return defects, st, nil
}

var tableWhat = map[string]string{walksFile: "walk", dynReadsFile: "unbounded read", edgesFile: "run or load of Go packages"}

// generatedRows is the walks.tsv, dynreads.tsv and edges.tsv rows the reads call for, each with one sample read.
func generatedRows(reads []Read) map[string]map[string]string {
	out := map[string]map[string]string{walksFile: {}, dynReadsFile: {}, edgesFile: {}}
	for _, r := range reads {
		site := path.Base(r.File)
		var file, row string
		switch r.Kind {
		case ReadWalk:
			file, row = walksFile, r.Package+"\t"+site+"\t"+r.Root+"\t"+r.Depth
		case ReadDyn:
			file, row = dynReadsFile, r.Package+"\t"+site
		case ReadEdge:
			if r.Target == "" {
				continue
			}
			file, row = edgesFile, r.Package+"\t"+site+"\t"+r.Target
		default:
			continue
		}
		if _, ok := out[file][row]; !ok {
			out[file][row] = r.String()
		}
	}
	return out
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

// PlanReads is the guard of the read tables: it fails, naming each one, when a Go package reads a path of a declared
// class without a reads.tsv row that covers it, or when walks.tsv, dynreads.tsv or edges.tsv differ from the rows the
// code calls for. With --write it writes the three generated tables from the code instead.
func PlanReads(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("plan-reads", flag.ContinueOnError)
	fs.SetOutput(stderr)
	root := fs.String("root", "", "repository root")
	tablesDir := fs.String("tables", "", "directory of the read tables (default ROOT/internal/cigates/affected)")
	write := fs.Bool("write", false, "write walks.tsv, dynreads.tsv, edges.tsv and reach.tsv from the code, then check")
	checkCallGraph := fs.Bool("reach", false, "also recompute reach.tsv from the call graph (go/packages, SSA, RTA) and fail on any difference")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *root == "" {
		fmt.Fprintln(stderr, "plan-reads: --root is required")
		return 2
	}
	absRoot, err := filepath.Abs(*root)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	*root = absRoot
	dir := *tablesDir
	if dir == "" {
		dir = filepath.Join(*root, "internal", "cigates", "affected")
	}
	reads, err := ScanReads(*root)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if *write {
		if err := writeGenerated(dir, reads); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		rows, st, err := reachRows(*root, reads)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		if err := writeReach(dir, rows); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		printReachStats(stdout, st)
	}
	t, err := loadTables(dir)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	g := checkReads(*root, t, reads)
	if *checkCallGraph && !*write {
		defects, st, err := checkReach(*root, t, reads)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		printReachStats(stdout, st)
		g.defects = append(g.defects, defects...)
	}
	for _, d := range g.defects {
		fmt.Fprintln(stdout, d)
	}
	if len(g.defects) > 0 {
		table := func(file string) string {
			p := filepath.ToSlash(filepath.Join(dir, file))
			if rel, err := filepath.Rel(*root, filepath.Join(dir, file)); err == nil && !strings.HasPrefix(rel, "..") {
				p = filepath.ToSlash(rel)
			}
			return p
		}
		printRows(stdout, "add these rows to "+table(readsFile)+":", g.add[readsFile])
		printRows(stdout, "replace these rows in "+table(readsFile)+":", g.replace)
		for _, file := range generatedFiles {
			printRows(stdout, "add these rows to "+table(file)+" (or run cigate plan-reads --write):", g.add[file])
			printRows(stdout, "remove these rows from "+table(file)+" (or run cigate plan-reads --write):", g.remove[file])
		}
		fmt.Fprintf(stdout, "plan-reads: %d defect(s) in the read tables\n", len(g.defects))
		return 1
	}
	fmt.Fprintf(stdout, "plan-reads: %d read(s) covered by %d reads, %d walks, %d dynreads, %d edges and %d reach row(s)\n", len(reads),
		len(t.reads), len(t.walks), len(t.dynReads), len(t.edges), len(t.reachRows))
	return 0
}

func printReachStats(w io.Writer, st reachStats) {
	fmt.Fprintf(w, "plan-reads: reach.tsv over %d code-scope site file(s), %d program run(s) over %d tag set(s): %s wall, peak RSS %d MiB\n",
		st.sites, st.programs, len(reachTags), st.wall.Round(time.Second), st.peakRSSMiB)
}

// writeGenerated writes walks.tsv, dynreads.tsv and edges.tsv as the reads call for them.
func writeGenerated(dir string, reads []Read) error {
	gen := generatedRows(reads)
	for _, file := range generatedFiles {
		var b strings.Builder
		b.WriteString(generatedHeader[file])
		for _, row := range sortedKeys(gen[file]) {
			b.WriteString(row + "\n")
		}
		if err := os.WriteFile(filepath.Join(dir, file), []byte(b.String()), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// modulePath is the module path of ROOT/go.mod, "" when there is none.
func modulePath(root string) string {
	raw, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		return ""
	}
	for _, l := range strings.Split(string(raw), "\n") {
		if f := strings.Fields(l); len(f) >= 2 && f[0] == "module" {
			return strings.Trim(f[1], `"`)
		}
	}
	return ""
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
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
