package affected

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

// The declared-read tables. classes.tsv names the repository path classes the planner knows (`class<TAB>glob`, a class
// may hold several globs); a changed path in no class is FULL. reads.tsv says which package reads which class
// (`package<TAB>scope<TAB>class`): scope test = the package's own tests read it (the package alone is selected), scope
// code = its non-test code reads it (every test binary that links the package is selected). A class with no reads row
// is reader-less. walks.tsv has the same shape and declares what a package that walks from the repository root reads
// (a literal row never covers a walk: the walk reaches every class its filter lets through). A glob that starts with ** (a file kind anywhere, such as **/*.json) only adds readers: a path is known
// only through a glob anchored at a first-level entry. The reserved class go-trees declares a walker that reads no class
// (only Go package trees or FULL paths). `cigate plan-reads` is the guard that keeps reads.tsv complete.
const (
	classesFile = "classes.tsv"
	readsFile   = "reads.tsv"
	walksFile   = "walks.tsv"
	scopeTest   = "test"
	scopeCode   = "code"
	goTrees     = "go-trees"
)

type classGlob struct{ class, glob string }

type readRow struct {
	pkg, scope, class string
	line              int
}

type tables struct {
	globs []classGlob
	reads []readRow
	walks []readRow // walks.tsv: what a package that walks from the repository root reads, one row per class
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

// loadTables reads classes.tsv and reads.tsv from dir. A missing file is an empty table: with no class every non-Go
// path outside a package is FULL.
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
	if t.walks, err = loadRows(dir, walksFile, known); err != nil {
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
		if !known[row.class] && row.class != goTrees {
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

func (t *tables) readersOf(classes map[string]bool) []readRow {
	var out []readRow
	for _, r := range append(append([]readRow{}, t.reads...), t.walks...) {
		if classes[r.class] {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].pkg < out[j].pkg })
	return out
}

// logRootWalks writes, for every package with walks.tsv rows, the classes its walk is declared to read against the classes
// the change reaches, and whether that selected it: a false NONE that rests on a walk declaration shows in the job log.
func (t *tables) logRootWalks(w io.Writer, reached map[string]bool) {
	if w == nil || len(reached) == 0 {
		return
	}
	declared := map[string][]string{}
	for _, r := range t.walks {
		declared[r.pkg] = append(declared[r.pkg], r.class)
	}
	var reach []string
	for c := range reached {
		reach = append(reach, c)
	}
	sort.Strings(reach)
	var pkgs []string
	for p := range declared {
		pkgs = append(pkgs, p)
	}
	sort.Strings(pkgs)
	for _, p := range pkgs {
		classes := declared[p]
		sort.Strings(classes)
		state := "not selected"
		for _, c := range classes {
			if reached[c] {
				state = "selected"
			}
		}
		fmt.Fprintf(w, "root walk in %s: classes declared %v, reachable %v; %s\n", p, classes, reach, state)
	}
}
