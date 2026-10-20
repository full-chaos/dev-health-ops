package affected

import (
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

// walks.tsv: one row per walk call site of a package that walks the repository, with the walk measured, not named:
//
//	package<TAB>scope<TAB>site<TAB>root<TAB>skip<TAB>filter<TAB>classes
//
// site is the base name of the Go file that holds the walk. root is the repository directory the walk starts at (`.` for
// the repository root), `outside` for a walk that never reads the repository (a temp dir, /proc) or `unexpressed` when the
// root cannot be written down (the plan is then FULL for every non-Go change). skip is `-` or a comma list of directories
// the walk does not enter: a pattern without `/` matches a directory base name below the root, a pattern with `/` a
// repository directory, and `*` means the walk does not descend at all. filter is a comma list of the files the walk uses:
// a pattern without `/` matches a base name, a pattern with `/` a repository path (`**` = any segments). classes is `-` or
// the comma list of classes the walk reaches on the tree; plan-reads computes it and fails when it differs.
// The planner selects a walker for every changed non-Go path its root, skip and filter match.
const (
	walkOutside     = "outside"
	walkUnexpressed = "unexpressed"
	// scopeStatic: the walk runs only in the go-quality static leg, which runs on every change, so it selects nothing.
	// Only the planner's own guard (plan-reads, internal/cigates/affected) may carry it; plan-reads refuses it elsewhere.
	scopeStatic = "static"
)

type walkRow struct {
	pkg, scope, site, root string
	skip, filter, classes  []string
	line                   int
}

func splitList(v string) []string {
	if v == "-" {
		return nil
	}
	return strings.Split(v, ",")
}

func checkWalkPattern(p string) error {
	if strings.Contains(p, "/") {
		return checkGlob(p)
	}
	if _, err := path.Match(p, ""); err != nil || p == "" {
		return fmt.Errorf("pattern %q is not a valid base-name glob", p)
	}
	return nil
}

func loadWalks(dir string, known map[string]bool) ([]walkRow, error) {
	rows, lines, err := readTSV(filepath.Join(dir, walksFile), 7)
	if err != nil {
		return nil, err
	}
	var out []walkRow
	for i, r := range rows {
		w := walkRow{pkg: r[0], scope: r[1], site: r[2], root: r[3], skip: splitList(r[4]), filter: splitList(r[5]),
			classes: splitList(r[6]), line: lines[i]}
		where := fmt.Sprintf("%s:%d", walksFile, w.line)
		if w.scope != scopeTest && w.scope != scopeCode && w.scope != scopeStatic {
			return nil, fmt.Errorf("%s: scope %q is not %s, %s or %s", where, w.scope, scopeTest, scopeCode, scopeStatic)
		}
		if path.Clean(w.pkg) != w.pkg || strings.HasPrefix(w.pkg, "/") || strings.HasPrefix(w.pkg, "../") {
			return nil, fmt.Errorf("%s: package %q is not a clean repository path", where, w.pkg)
		}
		if !strings.HasSuffix(w.site, ".go") || strings.Contains(w.site, "/") {
			return nil, fmt.Errorf("%s: site %q is not a Go file base name", where, w.site)
		}
		if w.root != "." && w.root != walkOutside && w.root != walkUnexpressed &&
			(path.Clean(w.root) != w.root || strings.HasPrefix(w.root, "/") || strings.HasPrefix(w.root, "../")) {
			return nil, fmt.Errorf("%s: root %q is not `.`, a clean repository directory, %s or %s", where, w.root, walkOutside, walkUnexpressed)
		}
		if len(w.filter) == 0 && w.root != walkOutside {
			return nil, fmt.Errorf("%s: a walk inside the repository needs a filter (`*` for every file)", where)
		}
		for _, p := range append(append([]string{}, w.skip...), w.filter...) {
			if p == "*" {
				continue
			}
			if err := checkWalkPattern(p); err != nil {
				return nil, fmt.Errorf("%s: %w", where, err)
			}
		}
		for _, c := range w.classes {
			if !known[c] {
				return nil, fmt.Errorf("%s: class %q is not in %s", where, c, classesFile)
			}
		}
		out = append(out, w)
	}
	return out, nil
}

// matches reports whether the walk reads repository path p (a file).
func (w walkRow) matches(p string) bool {
	switch w.root {
	case walkOutside:
		return false
	case walkUnexpressed:
		return true
	}
	rel := p
	if w.root != "." {
		if !strings.HasPrefix(p, w.root+"/") {
			return false
		}
		rel = p[len(w.root)+1:]
	}
	segs := strings.Split(rel, "/")
	dirs := segs[:len(segs)-1]
	for i, d := range dirs {
		for _, s := range w.skip {
			if s == "*" {
				return false
			}
			if strings.Contains(s, "/") {
				if globMatch(s, path.Join(append([]string{w.rootPrefix()}, dirs[:i+1]...)...)) {
					return false
				}
			} else if ok, _ := path.Match(s, d); ok {
				return false
			}
		}
	}
	base := segs[len(segs)-1]
	for _, f := range w.filter {
		if f == "*" {
			return true
		}
		if strings.Contains(f, "/") {
			if globMatch(f, p) {
				return true
			}
		} else if ok, _ := path.Match(f, base); ok {
			return true
		}
	}
	return false
}

func (w walkRow) rootPrefix() string {
	if w.root == "." {
		return ""
	}
	return w.root
}

// reach is the classes of the files on the tree under root that the walk reads; only known (anchored) paths count, since
// a change anywhere else plans FULL.
func (t *tables) reach(root string, w walkRow) ([]string, error) {
	if w.root == walkOutside || w.root == walkUnexpressed {
		return nil, nil
	}
	start := root
	if w.root != "." {
		start = filepath.Join(root, filepath.FromSlash(w.root))
	}
	if _, err := os.Stat(start); errors.Is(err, fs.ErrNotExist) {
		return nil, nil // a root missing from the tree reaches nothing; a path added under it still matches the walk
	} else if err != nil {
		return nil, fmt.Errorf("root %s: %w", w.root, err)
	}
	got := map[string]bool{}
	err := filepath.WalkDir(start, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrPermission) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if !w.matches(rel) {
			return nil
		}
		if classes, known := t.classesOf(rel); known {
			for c := range classes {
				got[c] = true
			}
		}
		return nil
	})
	var out []string
	for c := range got {
		out = append(out, c)
	}
	sort.Strings(out)
	return out, err
}

// walkersOf is every walk row that reads p.
func (t *tables) walkersOf(p string) []walkRow {
	var out []walkRow
	for _, w := range t.walks {
		if w.scope != scopeStatic && w.matches(p) {
			out = append(out, w)
		}
	}
	return out
}

// logRootWalks writes, for every walking package, the classes its walks are declared to reach against the classes the
// change reaches, and whether the change selected it: a false NONE that rests on a walk shows in the job log.
func (t *tables) logRootWalks(w io.Writer, reached map[string]bool, selected map[string]bool) {
	if w == nil || len(reached) == 0 {
		return
	}
	declared := map[string]map[string]bool{}
	for _, r := range t.walks {
		if declared[r.pkg] == nil {
			declared[r.pkg] = map[string]bool{}
		}
		for _, c := range r.classes {
			declared[r.pkg][c] = true
		}
	}
	reach := sortedKeys(reached)
	var pkgs []string
	for p := range declared {
		pkgs = append(pkgs, p)
	}
	sort.Strings(pkgs)
	for _, p := range pkgs {
		state := "not selected"
		if selected[p] {
			state = "selected"
		}
		fmt.Fprintf(w, "root walk in %s: classes declared %v, reachable %v; %s\n", p, sortedKeys(declared[p]), reach, state)
	}
}

func sortedKeys(m map[string]bool) []string {
	out := []string{}
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
