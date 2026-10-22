package affected

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// The declared-read tables of `cigate plan-reads`. classes.tsv names repository path classes (`class<TAB>glob`, a class
// may hold several globs). reads.tsv says which package reads which class (`package<TAB>scope<TAB>class`): scope test =
// the package's own tests name it, scope code = its non-test code names it.
const (
	classesFile = "classes.tsv"
	readsFile   = "reads.tsv"
	walksFile   = "walks.tsv"
	scopeTest   = "test"
	scopeCode   = "code"
)

type classGlob struct{ class, glob string }

type readRow struct {
	pkg, scope, class string
	line              int
}

type tables struct {
	globs    []classGlob
	reads    []readRow
	walks    []walkRow
	dynReads []siteRow
	edges    []edgeRow

	generatedRows map[string]map[string]string
}

func readTSV(file string, fields int) ([][]string, []int, error) {
	raw, err := os.ReadFile(file)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var rows [][]string
	var lines []int
	sc := bufio.NewScanner(bytes.NewReader(raw))
	n := 0
	for sc.Scan() {
		n++
		l := sc.Text()
		if strings.TrimSpace(l) == "" || strings.HasPrefix(l, "#") {
			continue
		}
		f := strings.Split(l, "\t")
		if len(f) != fields {
			return nil, nil, fmt.Errorf("%s:%d: want %d tab-separated fields, got %d", file, n, fields, len(f))
		}
		for _, v := range f {
			if v == "" || strings.TrimSpace(v) != v {
				return nil, nil, fmt.Errorf("%s:%d: empty or padded field", file, n)
			}
		}
		rows = append(rows, f)
		lines = append(lines, n)
	}
	return rows, lines, sc.Err()
}

// loadTables reads classes.tsv and reads.tsv from dir. A missing file is an empty table.
func loadTables(dir string) (*tables, error) {
	t := &tables{}
	cls, _, err := readTSV(filepath.Join(dir, classesFile), 2)
	if err != nil {
		return nil, err
	}
	known := map[string]bool{}
	for _, r := range cls {
		if err := checkGlob(r[1]); err != nil {
			return nil, fmt.Errorf("%s: class %s: %w", classesFile, r[0], err)
		}
		t.globs = append(t.globs, classGlob{class: r[0], glob: r[1]})
		known[r[0]] = true
	}
	if t.reads, err = loadRows(dir, readsFile, known); err != nil {
		return nil, err
	}
	if err := loadGenerated(dir, t); err != nil {
		return nil, err
	}
	return t, nil
}

func loadRows(dir, file string, known map[string]bool) ([]readRow, error) {
	rows, lines, err := readTSV(filepath.Join(dir, file), 3)
	if err != nil {
		return nil, err
	}
	var out []readRow
	seen := map[string]bool{}
	for i, r := range rows {
		row := readRow{pkg: r[0], scope: r[1], class: r[2], line: lines[i]}
		if row.scope != scopeTest && row.scope != scopeCode {
			return nil, fmt.Errorf("%s:%d: scope %q is neither %s nor %s", file, row.line, row.scope, scopeTest, scopeCode)
		}
		if !known[row.class] {
			return nil, fmt.Errorf("%s:%d: class %q is not in %s", file, row.line, row.class, classesFile)
		}
		if path.Clean(row.pkg) != row.pkg || strings.HasPrefix(row.pkg, "/") || strings.HasPrefix(row.pkg, "../") {
			return nil, fmt.Errorf("%s:%d: package %q is not a clean repository path", file, row.line, row.pkg)
		}
		k := row.pkg + "\t" + row.class
		if seen[k] {
			return nil, fmt.Errorf("%s:%d: package %s already declares class %s", file, row.line, row.pkg, row.class)
		}
		seen[k] = true
		out = append(out, row)
	}
	return out, nil
}

func checkGlob(g string) error {
	if g == "" || strings.HasPrefix(g, "/") || strings.HasSuffix(g, "/") {
		return fmt.Errorf("glob %q must be a relative path pattern", g)
	}
	for _, seg := range strings.Split(g, "/") {
		if seg == "**" {
			continue
		}
		if strings.Contains(seg, "**") {
			return fmt.Errorf("glob %q: ** must be a whole segment", g)
		}
		if _, err := path.Match(seg, ""); err != nil {
			return fmt.Errorf("glob %q: %w", g, err)
		}
	}
	return nil
}

// globMatch matches a slash path against a glob whose `**` segment matches zero or more segments.
func globMatch(glob, p string) bool {
	return matchSegs(strings.Split(glob, "/"), strings.Split(p, "/"))
}

func matchSegs(g, p []string) bool {
	for len(g) > 0 {
		if g[0] == "**" {
			for i := 0; i <= len(p); i++ {
				if matchSegs(g[1:], p[i:]) {
					return true
				}
			}
			return false
		}
		if len(p) == 0 {
			return false
		}
		if ok, _ := path.Match(g[0], p[0]); !ok {
			return false
		}
		g, p = g[1:], p[1:]
	}
	return len(p) == 0
}

// covers reports whether a read of p (a file, or a directory the reader walks) is covered by one of the classes: a glob
// that matches p, or a glob X/** with p equal to X or under it.
func (t *tables) covers(p string, classes map[string]string) bool {
	for _, g := range t.globs {
		if _, ok := classes[g.class]; !ok {
			continue
		}
		if globMatch(g.glob, p) {
			return true
		}
		if dir, ok := strings.CutSuffix(g.glob, "/**"); ok && (globMatch(dir, p) || globMatch(dir+"/**", p)) {
			return true
		}
	}
	return false
}

// suggest is the anchored class of p's first-level entry, the row that always covers a read of p; "" when p is in no class.
func (t *tables) suggest(p string) string {
	first := strings.SplitN(p, "/", 2)[0]
	for _, g := range t.globs {
		if strings.HasPrefix(g.glob, "**") {
			continue
		}
		if globMatch(g.glob, p) || globMatch(g.glob, first) || strings.HasPrefix(g.glob, first+"/") {
			return g.class
		}
	}
	return ""
}

// The generated tables: `cigate plan-reads --write` writes them from the code (dynreads.go), and plan-reads and
// plan-verdict fail (FULL) on any difference, so a row is never narrower or wider than the code. site is the base name
// of the Go file: a _test.go site is a read of the package's own tests (the package alone is selected), any other site
// is a read of its non-test code (every test binary that links the package is selected).
//
//	walks.tsv     package<TAB>site<TAB>root<TAB>depth   the site walks root (`.` = the repository) to depth tree or dir;
//	                                                    root `unexpressed` (depth `-`): a walk the tool cannot bound
//	dynreads.tsv  package<TAB>site                      the site reads a path the tool cannot bound
//	edges.tsv     package<TAB>site<TAB>target           the site runs (go run/build/test/vet/list) or loads (go/packages)
//	                                                    the Go packages of target: dir, dir/... (a tree), ... (the
//	                                                    module), **/dir (any directory ending in dir: the command's
//	                                                    directory is not known), or unexpressed (any other program)
const (
	dynReadsFile = "dynreads.tsv"
	edgesFile    = "edges.tsv"
)

var generatedFiles = []string{walksFile, dynReadsFile, edgesFile}

var generatedHeader = map[string]string{
	walksFile:    "# GENERATED by `cigate plan-reads --root . --write` (tables.go): package<TAB>site<TAB>root<TAB>depth\n",
	dynReadsFile: "# GENERATED by `cigate plan-reads --root . --write` (tables.go): package<TAB>site\n",
	edgesFile:    "# GENERATED by `cigate plan-reads --root . --write` (tables.go): package<TAB>site<TAB>target\n",
}

type siteRow struct {
	pkg, site string
	line      int
}

func (r siteRow) test() bool { return strings.HasSuffix(r.site, "_test.go") }

type walkRow struct {
	siteRow
	root, depth string
}

type edgeRow struct {
	siteRow
	target string
}

func cleanRel(p string) bool {
	return p != "" && path.Clean(p) == p && !strings.HasPrefix(p, "/") && p != ".." && !strings.HasPrefix(p, "../")
}

func loadSite(file string, line int, pkg, site string) (siteRow, error) {
	if !cleanRel(pkg) {
		return siteRow{}, fmt.Errorf("%s:%d: package %q is not a clean repository path", file, line, pkg)
	}
	if !strings.HasSuffix(site, ".go") || strings.Contains(site, "/") {
		return siteRow{}, fmt.Errorf("%s:%d: site %q is not a Go file base name", file, line, site)
	}
	return siteRow{pkg: pkg, site: site, line: line}, nil
}

// validTarget: a directory, a tree, the module, a directory pattern, or unexpressed.
func validTarget(t string) bool {
	switch {
	case t == unexpressed || t == "...":
		return true
	case strings.HasPrefix(t, "**/"):
		t = strings.TrimPrefix(t, "**/")
	}
	t = strings.TrimSuffix(t, "/...")
	return cleanRel(t) && !strings.Contains(t, "*")
}

func loadGenerated(dir string, t *tables) error {
	rows, lines, err := readTSV(filepath.Join(dir, walksFile), 4)
	if err != nil {
		return err
	}
	t.generatedRows = map[string]map[string]string{walksFile: {}, dynReadsFile: {}, edgesFile: {}}
	for i, r := range rows {
		s, err := loadSite(walksFile, lines[i], r[0], r[1])
		if err != nil {
			return err
		}
		w := walkRow{siteRow: s, root: r[2], depth: r[3]}
		switch {
		case w.root == unexpressed:
			if w.depth != "-" {
				return fmt.Errorf("%s:%d: an unexpressed root has depth -", walksFile, s.line)
			}
		case w.root != "." && !cleanRel(w.root):
			return fmt.Errorf("%s:%d: root %q is not ., a clean repository directory or %s", walksFile, s.line, w.root, unexpressed)
		case w.depth != depthTree && w.depth != depthDir:
			return fmt.Errorf("%s:%d: depth %q is neither %s nor %s", walksFile, s.line, w.depth, depthTree, depthDir)
		}
		t.walks = append(t.walks, w)
		t.generatedRows[walksFile][strings.Join(r, "\t")] = fmt.Sprint(s.line)
	}
	rows, lines, err = readTSV(filepath.Join(dir, dynReadsFile), 2)
	if err != nil {
		return err
	}
	for i, r := range rows {
		s, err := loadSite(dynReadsFile, lines[i], r[0], r[1])
		if err != nil {
			return err
		}
		t.dynReads = append(t.dynReads, s)
		t.generatedRows[dynReadsFile][strings.Join(r, "\t")] = fmt.Sprint(s.line)
	}
	rows, lines, err = readTSV(filepath.Join(dir, edgesFile), 3)
	if err != nil {
		return err
	}
	for i, r := range rows {
		s, err := loadSite(edgesFile, lines[i], r[0], r[1])
		if err != nil {
			return err
		}
		if !validTarget(r[2]) {
			return fmt.Errorf("%s:%d: target %q is not a directory, dir/..., ..., **/dir or %s", edgesFile, s.line, r[2], unexpressed)
		}
		t.edges = append(t.edges, edgeRow{siteRow: s, target: r[2]})
		t.generatedRows[edgesFile][strings.Join(r, "\t")] = fmt.Sprint(s.line)
	}
	return nil
}

// generated is the checked-in generated rows: table file -> row -> its line number.
func (t *tables) generated() map[string]map[string]string { return t.generatedRows }

// covers reports whether the walk reads repository path p.
func (w walkRow) covers(p string) bool {
	if w.root == unexpressed || w.root == "." && w.depth == depthTree {
		return true
	}
	rel := p
	if w.root != "." {
		var ok bool
		if rel, ok = strings.CutPrefix(p, w.root+"/"); !ok {
			return false
		}
	}
	return w.depth == depthTree || !strings.Contains(rel, "/")
}

// matches reports whether an edge target names the package directory dir.
func (e edgeRow) matches(dir string) bool {
	t := e.target
	if t == "..." {
		return true
	}
	tree := strings.HasSuffix(t, "/...")
	t = strings.TrimSuffix(t, "/...")
	if globMatch(t, dir) {
		return true
	}
	return tree && globMatch(t+"/**", dir)
}

// classesOf is the classes of p, and whether one of them is anchored at a first-level entry (p is a known path).
func (t *tables) classesOf(p string) (map[string]bool, bool) {
	out := map[string]bool{}
	anchored := false
	for _, g := range t.globs {
		if globMatch(g.glob, p) {
			out[g.class] = true
			if !strings.HasPrefix(g.glob, "**") {
				anchored = true
			}
		}
	}
	return out, anchored
}
