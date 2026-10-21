// Package affected decides which Go test packages a change set can affect: one verdict for the integration plan job of
// go.yml and for the go-quality legs. A pull request whose every changed path is a .go file plans AFFECTED: the changed
// packages' reverse import closure. Any other path (data, tsv, yaml, docs, workflows, scripts, testdata) plans FULL, as do
// shared inputs and every push to main, merge group, schedule or manual run. NONE is part of the contract (check_go.sh and
// go-quality.yml accept it) but no change set plans it: bounding which tests read a non-Go file is separate work.
package affected

import (
	"bytes"
	_ "embed"
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

// Verdict is the plan for one change set: AFFECTED (Keys: the planned packages the change reaches) or FULL (Reason).
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
}

func hasAnyPrefix(p string, prefixes []string) bool {
	for _, pre := range prefixes {
		if strings.HasPrefix(p, pre) {
			return true
		}
	}
	return false
}

func plan(in planInput) (Verdict, error) {
	if !judgedEvents[in.event] {
		return full("event " + in.event + ": main, merge groups, schedules and manual runs run everything"), nil
	}
	if len(in.changed) == 0 {
		return full("empty change set: a wrong base reads as no change"), nil
	}
	packages := map[string]goListEntry{}
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

	changedPackages := map[string]bool{} // import paths whose reverse closure is selected
	for _, raw := range in.changed {
		p := path.Clean(raw)
		if plannerFiles[p] || (hasAnyPrefix(p, plannerDirs) && !strings.HasSuffix(p, "_test.go") && !inTestdata(p)) {
			return full("planner input: " + p), nil
		}
		if sharedFiles[p] || hasAnyPrefix(p, sharedPrefixes) {
			return full("shared change: " + p), nil
		}
		if !strings.HasSuffix(p, ".go") {
			return full("non-Go file: " + p), nil
		}
		if inTestdata(p) {
			owner, ok := ownerOfDataFile(p, packages)
			if !ok {
				return full("Go fixture with no owning package: " + p), nil
			}
			changedPackages[owner] = true
			continue
		}
		owner, ok := packages[path.Dir(p)]
		if !ok {
			return full("Go file outside any listed package: " + p), nil
		}
		changedPackages[owner.ImportPath] = true
	}
	if in.goFilesFull {
		return full("Go source in the change set: every package runs"), nil
	}
	always := map[string]bool{}
	for _, k := range in.always {
		always[k] = true
	}
	var affected []string
	for _, key := range in.keys {
		imp := importPath(key)
		closure, ok := binaries[imp]
		if !ok {
			return full("integration package without a test binary: " + key), nil
		}
		if always[key] {
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

// PlanVerdict prints the plan verdict of a change set: `AFFECTED <n>` and one key per line, or `FULL <reason>`.
// It returns the exit code: 0 with a verdict, 1 on a read or event error, 2 on a usage error.
func PlanVerdict(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("plan-verdict", flag.ContinueOnError)
	fs.SetOutput(stderr)
	event := fs.String("event", "", "GitHub event name: pull_request (or range, a push-range check for an informational job) judges the change set; any other event is FULL")
	changedP := fs.String("changed", "", "changed-file list (NUL- or newline-separated)")
	golistP := fs.String("golist", "", "go list -test -json output")
	keysP := fs.String("keys", "", "planned package keys, one per line, relative to the module root")
	root := fs.String("root", "", "repository root")
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
	v, err := plan(planInput{event: *event, changed: changed, golist: golist, keys: keys, root: *root, goFilesFull: *goFilesFull,
		always: alwaysList()})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if v.Kind == "AFFECTED" {
		fmt.Fprintf(stdout, "AFFECTED %d\n", len(v.Keys))
		for _, k := range v.Keys {
			fmt.Fprintln(stdout, k)
		}
		return 0
	}
	// A path may hold a newline (git diff -z keeps it); the verdict is line-oriented, so the reason stays on one line.
	fmt.Fprintln(stdout, "FULL "+strings.NewReplacer("\n", `\n`, "\r", `\r`).Replace(v.Reason))
	return 0
}
