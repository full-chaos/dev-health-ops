// Package affected decides which Go test packages a change set can affect: one verdict for the integration plan job of
// go.yml and for the go-quality legs. A pull request whose every changed path is a .go file plans AFFECTED: the changed
// packages' reverse import closure. A non-Go path plans the packages that read it (reads.tsv by class, walks.tsv, dynreads.tsv
// and edges.tsv, generated from the code and checked by cigate plan-reads), or NONE when no package reads it; a non-Go path
// in no declared class plans FULL, as do shared inputs and every push to main, merge group, schedule or manual run. Any
// doubt plans FULL.
package affected

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

var (
	sharedPrefixes = []string{"ci/", ".github/", "internal/testsupport/"}
	sharedFiles    = map[string]bool{"go.mod": true, "go.sum": true, "go.work": true, "go.work.sum": true}
	// The planner's code and data, and the workflow steps that build and run it: a change here can steer the verdict of
	// this very change set, so it is FULL. Test files (outside testdata) are not inputs.
	plannerDirs  = []string{"cmd/cigate/", "internal/cigates/affected/"}
	plannerFiles = map[string]bool{".github/workflows/go.yml": true, ".github/workflows/go-quality.yml": true}
	// Events that judge a change set. Every other event (push, merge_group, schedule, workflow_dispatch) runs everything.
	judgedEvents = map[string]bool{"pull_request": true, "range": true}
	knownEvents  = map[string]bool{"pull_request": true, "range": true, "push": true, "merge_group": true, "schedule": true,
		"workflow_dispatch": true, "release": true}
)

type goListEntry struct {
	ImportPath      string
	Dir             string
	ForTest         string
	Deps            []string
	EmbedFiles      []string
	TestEmbedFiles  []string
	XTestEmbedFiles []string
	Module          *struct{ Path string }
}

func readGoList(p string) ([]goListEntry, error) {
	raw, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	var out []goListEntry
	for {
		var e goListEntry
		if err := dec.Decode(&e); err == io.EOF {
			return out, nil
		} else if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
}

func readChanged(p string) ([]string, error) {
	raw, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	var out []string
	if bytes.IndexByte(raw, 0) >= 0 {
		for _, part := range bytes.Split(raw, []byte{0}) {
			if len(part) > 0 {
				out = append(out, string(part))
			}
		}
		return out, nil
	}
	// Newline mode trims ASCII blanks only: a path made of non-ASCII space (U+00A0) is a real path, never a blank line.
	for _, part := range bytes.Split(raw, []byte{'\n'}) {
		if len(bytes.Trim(part, " \t\r")) > 0 {
			out = append(out, string(part))
		}
	}
	return out, nil
}

func stripVariant(p string) string {
	if i := strings.Index(p, " ["); i >= 0 {
		return p[:i]
	}
	return p
}

func inTestdata(p string) bool {
	parts := strings.Split(p, "/")
	for _, part := range parts[:len(parts)-1] {
		if part == "testdata" {
			return true
		}
	}
	return false
}

func ownerOfDataFile(p string, packages map[string]goListEntry) (string, bool) {
	dir := path.Dir(p)
	for dir != "" && dir != "." {
		if owner, ok := packages[dir]; ok {
			rel, err := filepath.Rel(dir, p)
			if err != nil {
				return "", false
			}
			rel = filepath.ToSlash(rel)
			if inTestdata(rel) {
				return owner.ImportPath, true
			}
			for _, list := range [][]string{owner.EmbedFiles, owner.TestEmbedFiles, owner.XTestEmbedFiles} {
				for _, f := range list {
					if f == rel {
						return owner.ImportPath, true
					}
				}
			}
			return "", false
		}
		next := path.Dir(dir)
		if next == dir {
			break
		}
		dir = next
	}
	return "", false
}

// alwaysTSV lists the packages every AFFECTED plan selects, one per line: their tests read or run files the import
// closure cannot see. It is compiled into the planner, so the list that judges a change set is the merge base's.
//
//go:embed always.tsv
var alwaysTSV string

func alwaysList() []string {
	var out []string
	for _, line := range strings.Split(alwaysTSV, "\n") {
		if line = strings.Trim(line, " \t\r"); line != "" && !strings.HasPrefix(line, "#") {
			out = append(out, line)
		}
	}
	return out
}

// Verdict is the plan for one change set: NONE (no .go path and no planned package reads a changed path), AFFECTED
// (Keys: the planned packages the change reaches) or FULL (Reason).
type Verdict struct {
	Kind   string
	Reason string
	Keys   []string
}

func full(reason string) Verdict { return Verdict{Kind: "FULL", Reason: reason} }

type planInput struct {
	event       string
	changed     []string
	golist      []goListEntry
	keys        []string
	root        string
	goFilesFull bool
	always      []string
	tables      *tables   // the read tables; nil plans every non-Go path FULL
	guard       []string  // defects of the read tables against the code (plan-reads): any one plans FULL
	symlinks    []string  // repository paths tracked symlinks point at
	log         io.Writer // one line per selected package and why; nil discards
}

func hasAnyPrefix(p string, prefixes []string) bool {
	for _, pre := range prefixes {
		if strings.HasPrefix(p, pre) {
			return true
		}
	}
	return false
}

// selection is what the changed paths select: import paths whose reverse closure runs (changed), and import paths
// whose own tests run (direct), each with the first reason.
type selection struct {
	changed, direct map[string]string
}

func (s *selection) closure(imp, why string) {
	if _, ok := s.changed[imp]; !ok {
		s.changed[imp] = why
	}
}

func (s *selection) only(imp, why string) {
	if _, ok := s.direct[imp]; !ok {
		s.direct[imp] = why
	}
}

func (s *selection) reader(imp string, test bool, why string) {
	if test {
		s.only(imp, why)
	} else {
		s.closure(imp, why)
	}
}

func plan(in planInput) (Verdict, error) {
	if !judgedEvents[in.event] {
		return full("event " + in.event + ": main, merge groups, schedules and manual runs run everything"), nil
	}
	if len(in.changed) == 0 {
		return full("empty change set: a wrong base reads as no change"), nil
	}
	packages := map[string]goListEntry{}
	listed := map[string]bool{}
	binaries := map[string]map[string]bool{}
	deps := map[string]map[string]bool{}
	modulePath := ""
	for _, e := range in.golist {
		if strings.HasSuffix(e.ImportPath, ".test") && !strings.Contains(e.ImportPath, " ") {
			owner := strings.TrimSuffix(e.ImportPath, ".test")
			set := map[string]bool{owner: true}
			for _, d := range e.Deps {
				set[stripVariant(d)] = true
			}
			binaries[owner] = set
			continue
		}
		if strings.Contains(e.ImportPath, " ") || e.ForTest != "" || e.Dir == "" {
			continue
		}
		rel, err := filepath.Rel(in.root, e.Dir)
		if err != nil {
			return Verdict{}, err
		}
		packages[filepath.ToSlash(rel)] = e
		listed[e.ImportPath] = true
		set := map[string]bool{e.ImportPath: true}
		for _, d := range e.Deps {
			set[stripVariant(d)] = true
		}
		deps[e.ImportPath] = set
		if modulePath == "" && e.Module != nil {
			modulePath = e.Module.Path
		}
	}
	if len(packages) == 0 || modulePath == "" {
		return Verdict{}, errors.New("go list output holds no packages or no module path")
	}
	importPath := func(key string) string {
		if key == "." {
			return modulePath
		}
		return modulePath + "/" + key
	}
	t := in.tables
	sel := &selection{changed: map[string]string{}, direct: map[string]string{}}
	var goPaths, nonGo []string
	for _, raw := range in.changed {
		p := path.Clean(raw)
		if plannerFiles[p] || (hasAnyPrefix(p, plannerDirs) && !strings.HasSuffix(p, "_test.go") && !inTestdata(p)) {
			return full("planner input: " + p), nil
		}
		if sharedFiles[p] || hasAnyPrefix(p, sharedPrefixes) {
			return full("shared change: " + p), nil
		}
		if goModFiles[path.Base(p)] {
			return full("Go module file: " + p), nil // a read of a module file in an unbounded directory has no row
		}
		if strings.HasSuffix(p, ".go") {
			goPaths = append(goPaths, p)
			if inTestdata(p) {
				owner, ok := ownerOfDataFile(p, packages)
				if !ok {
					return full("Go fixture with no owning package: " + p), nil
				}
				sel.closure(owner, "its Go fixture "+p+" changed")
				continue
			}
			owner, ok := packages[path.Dir(p)]
			if !ok {
				return full("Go file outside any listed package: " + p), nil
			}
			sel.closure(owner.ImportPath, p+" changed")
			continue
		}
		if t == nil {
			return full("non-Go file: " + p), nil
		}
		if strings.HasSuffix(p, ".py") {
			return full("non-Go file is Python source: " + p), nil
		}
		classes, anchored := t.classesOf(p)
		if !anchored {
			// testdata, data files beside a package, and every path outside the classed first-level entries: the literal
			// scan does not resolve a read of them.
			return full("non-Go file in no declared class: " + p), nil
		}
		for _, target := range in.symlinks {
			if p == target || strings.HasPrefix(p, target+"/") || target == "." {
				return full("non-Go file a tracked symlink points at: " + p), nil
			}
		}
		nonGo = append(nonGo, p)
		for _, r := range t.reads {
			if classes[r.class] {
				sel.reader(importPath(r.pkg), r.scope == scopeTest, "reads.tsv "+r.pkg+" "+r.scope+" "+r.class+" ("+p+")")
			}
		}
		// The package directories that hold p: their own-directory reads, embeds and data files.
		for dir := path.Dir(p); ; dir = path.Dir(dir) {
			if e, ok := packages[dir]; ok {
				sel.closure(e.ImportPath, "its directory holds "+p)
			}
			if dir == "." {
				break
			}
		}
	}
	if in.goFilesFull && len(goPaths) > 0 {
		return full("Go source in the change set: every package runs"), nil
	}
	if len(in.guard) > 0 {
		return full("the read tables differ from the code (cigate plan-reads): " + in.guard[0]), nil
	}
	if t == nil {
		t = &tables{}
	}
	changedPaths := append(append([]string{}, goPaths...), nonGo...)
	for _, w := range t.walks {
		if w.root == unexpressed {
			sel.reader(importPath(w.pkg), w.test(), "walks.tsv "+w.pkg+" "+w.site+" walks an unbounded root")
			continue
		}
		for _, p := range changedPaths {
			if w.covers(p) {
				sel.reader(importPath(w.pkg), w.test(), "walks.tsv "+w.pkg+" "+w.site+" walks "+w.root+" ("+p+")")
				break
			}
		}
	}
	// The packages each expressed edge runs or loads; an edge that names no listed package is unbounded.
	type ran struct {
		row  edgeRow
		pkgs []string
	}
	var edges []ran
	for _, e := range t.edges {
		var pkgs []string
		if e.target != unexpressed {
			for dir, entry := range packages {
				if e.matches(dir) {
					pkgs = append(pkgs, entry.ImportPath)
				}
			}
			sort.Strings(pkgs)
		}
		if len(pkgs) == 0 {
			sel.reader(importPath(e.pkg), e.test(), "edges.tsv "+e.pkg+" "+e.site+" runs "+e.target)
			continue
		}
		edges = append(edges, ran{e, pkgs})
	}
	// An unbounded read may name a changed Go file as well as a data file: every change set selects its readers.
	for _, d := range t.dynReads {
		sel.reader(importPath(d.pkg), d.test(), "dynreads.tsv "+d.pkg+" "+d.site+" reads an unbounded path")
	}
	always := map[string]bool{}
	for _, k := range in.always {
		always[k] = true
	}
	why := map[string]string{}
	var affected []string
	for _, key := range in.keys {
		imp := importPath(key)
		closure, ok := binaries[imp]
		if !ok && !(in.goFilesFull && listed[imp]) {
			return full("integration package without a test binary: " + key), nil
		}
		if !ok {
			// a package with no test file: go test and go vet only build it
			closure = deps[imp]
		}
		// A binary that runs or loads other packages (edges.tsv) reads their source and runs their code: their closure
		// joins its own, and a package whose own tests a change selects selects the binary that runs it.
		cl := map[string]bool{}
		for p := range closure {
			cl[p] = true
		}
		runs := map[string]bool{}
		for grew := true; grew; {
			grew = false
			for _, e := range edges {
				owner := importPath(e.row.pkg)
				if !(e.row.test() && owner == imp) && !(!e.row.test() && cl[owner]) {
					continue
				}
				for _, p := range e.pkgs {
					if runs[p] {
						continue
					}
					runs[p], grew = true, true
					for d := range deps[p] {
						cl[d] = true
					}
					for d := range binaries[p] {
						cl[d] = true
					}
				}
			}
		}
		reason := ""
		if r, ok := sel.direct[imp]; ok {
			reason = r
		}
		for p := range runs {
			if r, ok := sel.direct[p]; ok && reason == "" {
				reason = "runs " + p + ": " + r
			}
		}
		if reason == "" {
			for _, p := range sortedKeys(cl) {
				if r, ok := sel.changed[p]; ok {
					reason = "links " + p + ": " + r
					break
				}
			}
		}
		if reason == "" && always[key] && (len(goPaths) > 0 || len(sel.changed)+len(sel.direct) > 0) {
			reason = "always.tsv"
		}
		if reason != "" {
			affected = append(affected, key)
			why[key] = reason
		}
	}
	sort.Strings(affected)
	if in.log != nil {
		for _, k := range affected {
			fmt.Fprintf(in.log, "plan: %s selected: %s\n", k, why[k])
		}
	}
	if len(goPaths) == 0 && len(affected) == 0 {
		return Verdict{Kind: "NONE"}, nil
	}
	return Verdict{Kind: "AFFECTED", Keys: affected}, nil
}

// PlanVerdict prints the plan verdict of a change set: NONE, `AFFECTED <n>` and one key per line, or `FULL <reason>`.
// It returns the exit code: 0 with a verdict, 1 on a read, table or event error, 2 on a usage error.
func PlanVerdict(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("plan-verdict", flag.ContinueOnError)
	fs.SetOutput(stderr)
	event := fs.String("event", "", "GitHub event name: pull_request (or range, a push-range check for an informational job) judges the change set; any other event is FULL")
	changedP := fs.String("changed", "", "changed-file list (NUL- or newline-separated)")
	golistP := fs.String("golist", "", "go list -test -json output")
	keysP := fs.String("keys", "", "planned package keys, one per line, relative to the module root")
	root := fs.String("root", "", "repository root (the head tree: its Go sources are scanned for reads)")
	fs.String("base", "", "base sha of the change set (kept for the callers; the verdict reads no file at the base)")
	goFilesFull := fs.Bool("go-files-full", false, "a change set with any .go file is FULL (the go-quality legs then run no package subset)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *event == "" || *changedP == "" || *golistP == "" || *keysP == "" || *root == "" {
		fmt.Fprintln(stderr, "plan-verdict: --event, --changed, --golist, --keys and --root are required")
		return 2
	}
	if !knownEvents[*event] {
		fmt.Fprintf(stderr, "plan-verdict: unknown event %q\n", *event)
		return 1
	}
	changed, err := readChanged(*changedP)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	keyRaw, err := os.ReadFile(*keysP)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	var keys []string
	for _, k := range strings.Split(string(keyRaw), "\n") {
		if k = strings.TrimSpace(k); k != "" {
			keys = append(keys, k)
		}
	}
	golist, err := readGoList(*golistP)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	in := planInput{event: *event, changed: changed, golist: golist, keys: keys, root: *root, goFilesFull: *goFilesFull,
		always: alwaysList(), log: stderr}
	// A root that does not exist (a synthetic go list) has no code to scan: no tables, so every non-Go path is FULL and
	// a .go plan is the import closure and always.tsv.
	if _, statErr := os.Stat(*root); judgedEvents[*event] && len(changed) > 0 && statErr == nil {
		// The read tables of the head tree and the guard over its code: the tables are planner inputs (a change to them
		// is FULL), so they are the merge base's whenever they matter.
		dir := filepath.Join(*root, filepath.FromSlash(plannerDirs[1]))
		tb, err := loadTables(dir)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		reads, err := ScanReads(*root)
		if err != nil {
			fmt.Fprintln(stderr, "scan the Go sources for reads:", err)
			return 1
		}
		in.tables, in.guard = tb, checkReads(*root, tb, reads).defects
		if in.symlinks, err = symlinkTargets(*root); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
	}
	v, err := plan(in)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	switch v.Kind {
	case "NONE":
		fmt.Fprintln(stdout, "NONE")
	case "AFFECTED":
		fmt.Fprintf(stdout, "AFFECTED %d\n", len(v.Keys))
		for _, k := range v.Keys {
			fmt.Fprintln(stdout, k)
		}
	default:
		// A path may hold a newline (git diff -z keeps it); the verdict is line-oriented, so the reason stays on one line.
		fmt.Fprintln(stdout, "FULL "+strings.NewReplacer("\n", `\n`, "\r", `\r`).Replace(v.Reason))
	}
	return 0
}

// symlinkTargets is the repository path every tracked symlink points at ("." for one that leaves the repository or
// cannot be read): a change there is a change to the link's path too.
func symlinkTargets(root string) ([]string, error) {
	var links []string
	if _, err := os.Stat(filepath.Join(root, ".git")); err == nil {
		out, err := exec.Command("git", "-C", root, "ls-files", "-s", "-z").Output()
		if err != nil {
			return nil, fmt.Errorf("git ls-files -s: %w", err)
		}
		for _, rec := range strings.Split(string(out), "\x00") {
			if meta, p, ok := strings.Cut(rec, "\t"); ok && strings.HasPrefix(meta, "120000 ") {
				links = append(links, p)
			}
		}
	} else {
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.Type()&fs.ModeSymlink != 0 {
				rel, err := filepath.Rel(root, p)
				if err != nil {
					return err
				}
				links = append(links, filepath.ToSlash(rel))
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	var out []string
	for _, l := range links {
		target, err := os.Readlink(filepath.Join(root, filepath.FromSlash(l)))
		r := "."
		if err == nil && !filepath.IsAbs(target) {
			r = path.Clean(path.Dir(l) + "/" + filepath.ToSlash(target))
			if r == ".." || strings.HasPrefix(r, "../") {
				r = "."
			}
		}
		out = append(out, r)
	}
	return out, nil
}
