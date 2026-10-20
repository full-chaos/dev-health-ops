// Package affected decides which Go test packages a change set can affect: one verdict, NONE / AFFECTED / FULL, for the
// integration plan job of go.yml and for the go-quality legs.
package affected

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

var (
	sharedPrefixes = []string{"internal/testsupport/"}
	sharedFiles    = map[string]bool{"go.mod": true, "go.sum": true, "go.work": true, "go.work.sum": true}
	// The planner's own code and tables: a change here changes the verdict of this very change set.
	plannerDir = "internal/cigates/affected/"
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

// Verdict is the plan for one change set. Kind is NONE (every changed path is in a class no Go test reads), AFFECTED
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
	root, base  string
	tables      *tables
	goFilesFull bool
	log         io.Writer // one line per root-walk decision; nil discards
}

// isPythonSource: a Python file outside a Go package's testdata or embed files has no declared reader; it plans FULL.
func isPythonSource(p string) bool { return strings.HasSuffix(p, ".py") }

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
	keySet := map[string]bool{}
	for _, k := range in.keys {
		keySet[k] = true
	}
	rules := &planRules{root: in.root, base: in.base, keys: keySet, quality: in.goFilesFull}

	changedPackages := map[string]bool{} // import paths whose reverse closure is selected
	direct := map[string]bool{}          // import paths selected alone
	hasGo := false
	reached := map[string]bool{} // classes the changed non-Go paths fall in
	for _, raw := range in.changed {
		p := path.Clean(raw)
		if strings.HasSuffix(p, ".go") {
			hasGo = true
		}
		if sharedFiles[p] || hasAnyPrefix(p, sharedPrefixes) {
			return full("shared change: " + p), nil
		}
		if strings.HasPrefix(p, plannerDir) && !strings.HasSuffix(p, "_test.go") && !inTestdata(p) {
			return full("planner input: " + p), nil
		}
		why, keys := rules.classifyInput(p)
		if why != "" {
			return full("shared change: " + p + ": " + why), nil
		}
		for _, k := range keys {
			direct[importPath(k)] = true
		}
		if strings.HasSuffix(p, ".go") && !inTestdata(p) {
			owner, ok := packages[path.Dir(p)]
			if !ok {
				return full("Go file outside any listed package: " + p), nil
			}
			changedPackages[owner.ImportPath] = true
			continue
		}
		if owner, ok := ownerOfDataFile(p, packages); ok {
			changedPackages[owner] = true
			continue
		}
		if isPythonSource(p) {
			return full("non-Go file is Python source: " + p), nil
		}
		classes, known := in.tables.classesOf(p)
		for c := range classes {
			reached[c] = true
		}
		if !known {
			if len(keys) > 0 {
				continue
			}
			return full("non-Go file in no declared class: " + p), nil
		}
		for _, row := range in.tables.readersOf(classes) {
			if row.scope == scopeCode {
				changedPackages[importPath(row.pkg)] = true
			} else {
				direct[importPath(row.pkg)] = true
			}
		}
	}
	if in.goFilesFull && hasGo {
		return full("Go source in the change set: every package runs"), nil
	}
	in.tables.logRootWalks(in.log, reached)
	if len(changedPackages) == 0 && len(direct) == 0 {
		return Verdict{Kind: "NONE"}, nil
	}
	var affected []string
	for _, key := range in.keys {
		imp := importPath(key)
		closure, ok := binaries[imp]
		if !ok && !(in.goFilesFull && listed[imp]) {
			return full("integration package without a test binary: " + key), nil
		}
		if !ok {
			// A package with no test file: go test only compiles it, so only a change to the package itself selects it.
			closure = map[string]bool{imp: true}
		}
		if direct[imp] {
			affected = append(affected, key)
			continue
		}
		for p := range closure {
			if changedPackages[p] {
				affected = append(affected, key)
				break
			}
		}
	}
	sort.Strings(affected)
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
	root := fs.String("root", "", "repository root")
	base := fs.String("base", "", "base sha; go.yml and ci/check_go.sh are compared at its merge base with HEAD")
	tablesDir := fs.String("tables", "", "directory of classes.tsv and reads.tsv (default ROOT/internal/cigates/affected)")
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
	dir := *tablesDir
	if dir == "" {
		dir = filepath.Join(*root, filepath.FromSlash(plannerDir))
	}
	tb, err := loadTables(dir)
	if err != nil {
		fmt.Fprintln(stderr, err)
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
	v, err := plan(planInput{event: *event, changed: changed, golist: golist, keys: keys, root: *root, base: *base, tables: tb, goFilesFull: *goFilesFull, log: stderr})
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
