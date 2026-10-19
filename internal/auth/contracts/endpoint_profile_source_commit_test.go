package contracts

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-4765. An inventory's top-level source_commit says which commit its
// file:line anchors were derived against. Two halves, and this file is honest
// about which is enforced:
//
//	half 1, unreachable: source_commit must name a commit that resolves in the
//	  repo and is an ancestor of HEAD. ENFORCED by problemsForSourceCommit.
//	half 2, stale but reachable: every anchor line must read the same at
//	  source_commit as in the tree under test. ENFORCED by the same function,
//	  for anchors in the repo that owns the inventory (path/line anchors and
//	  source{file,line}); an anchor naming another repo is not checkable here.
//
// NOT enforced: that the anchor's line is the RIGHT symbol (the rows gate in
// internal/queryapi/server owns that), and anchors in other repos' inventories.
// The fixtures below run on every build; the check over the real inventories
// is in endpoint_profile_source_commit_history_test.go behind the
// authhistory build tag, run only by the auth-test job (full history).

var fullSHA = regexp.MustCompile(`^[0-9a-f]{40}$`)

func gitOut(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	return strings.TrimRight(string(out), "\n"), err
}

type lineAnchor struct {
	file string
	line int
}

func collectAnchors(node any, out map[lineAnchor]bool) {
	switch value := node.(type) {
	case map[string]any:
		line, hasLine := value["line"].(float64)
		if hasLine && (value["repo"] == nil || value["repo"] == "ops") {
			if path, ok := value["path"].(string); ok {
				out[lineAnchor{path, int(line)}] = true
			}
			if file, ok := value["file"].(string); ok {
				out[lineAnchor{file, int(line)}] = true
			}
		}
		for _, child := range value {
			collectAnchors(child, out)
		}
	case []any:
		for _, child := range value {
			collectAnchors(child, out)
		}
	}
}

func lineOfText(text string, line int) (string, bool) {
	lines := strings.Split(text, "\n")
	if line < 1 || line > len(lines) {
		return "", false
	}
	return lines[line-1], true
}

// problemsForSourceCommit returns one message per defect; empty means both
// halves hold. repo is a git working tree whose HEAD is the tree under test.
func problemsForSourceCommit(repo, name string, document map[string]any) []string {
	commit, _ := document["source_commit"].(string)
	if !fullSHA.MatchString(commit) {
		return []string{fmt.Sprintf("%s: source_commit %q is not a 40-hex commit id", name, commit)}
	}
	if _, err := gitOut(repo, "cat-file", "-e", commit+"^{commit}"); err != nil {
		hint := "the commit does not exist in this repository (never pushed, or from another repo)"
		if shallow, _ := gitOut(repo, "rev-parse", "--is-shallow-repository"); shallow == "true" {
			hint = "this is a SHALLOW checkout, the one benign cause: fetch full history (actions/checkout fetch-depth: 0) before judging the inventory"
		}
		return []string{fmt.Sprintf("%s: source_commit %s does not resolve: %s", name, commit, hint)}
	}
	if _, err := gitOut(repo, "merge-base", "--is-ancestor", commit, "HEAD"); err != nil {
		return []string{fmt.Sprintf("%s: source_commit %s resolves but is not an ancestor of HEAD", name, commit)}
	}
	anchors := map[lineAnchor]bool{}
	collectAnchors(document["rows"], anchors)
	if len(anchors) == 0 {
		return []string{fmt.Sprintf("%s: no file:line anchor found in rows; the stale-base check would measure nothing", name)}
	}
	ordered := make([]lineAnchor, 0, len(anchors))
	for a := range anchors {
		ordered = append(ordered, a)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].file != ordered[j].file {
			return ordered[i].file < ordered[j].file
		}
		return ordered[i].line < ordered[j].line
	})
	var problems []string
	for _, a := range ordered {
		then, err := gitOut(repo, "show", commit+":"+a.file)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: %s does not exist at source_commit %s", name, a.file, commit))
			continue
		}
		raw, err := os.ReadFile(filepath.Join(repo, a.file))
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: anchor file %s: %v", name, a.file, err))
			continue
		}
		oldLine, okThen := lineOfText(then, a.line)
		newLine, okNow := lineOfText(string(raw), a.line)
		if !okThen || !okNow || oldLine != newLine {
			problems = append(problems, fmt.Sprintf("%s: %s:%d reads %q at source_commit %s but %q in the tree: the anchors were not derived against source_commit (stale base)", name, a.file, a.line, strings.TrimSpace(oldLine), commit, strings.TrimSpace(newLine)))
		}
	}
	return problems
}

func fixtureRepo(t *testing.T) (dir string, base, moved string) {
	t.Helper()
	dir = t.TempDir()
	run := func(args ...string) string {
		out, err := gitOut(dir, append([]string{"-c", "user.name=t", "-c", "user.email=t@example.invalid", "-c", "commit.gpgsign=false", "-c", "init.defaultBranch=main"}, args...)...)
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return out
	}
	run("init", "-q")
	write := func(content string) {
		if err := os.WriteFile(filepath.Join(dir, "routes.go"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("package x\n\nfunc a() {}\nmux.Handle(\"/a\", h)\n")
	run("add", "routes.go")
	run("commit", "-q", "-m", "base")
	base = run("rev-parse", "HEAD")
	write("package x\n\nfunc a() {}\nfunc b() {}\nmux.Handle(\"/a\", h)\n")
	run("commit", "-q", "-am", "moved")
	moved = run("rev-parse", "HEAD")
	return dir, base, moved
}

func inventoryAt(commit string, line int) map[string]any {
	raw := fmt.Sprintf(`{"source_commit": %q, "rows": [{"source": {"file": "routes.go", "line": %d}}]}`, commit, line)
	var document map[string]any
	if err := json.Unmarshal([]byte(raw), &document); err != nil {
		panic(err)
	}
	return document
}

// Green control: the tip commit with the anchor line as it is at the tip.
func TestSourceCommitFixtureAcceptsAnInventoryDerivedAtTheTip(t *testing.T) {
	dir, _, moved := fixtureRepo(t)
	if problems := problemsForSourceCommit(dir, "fixture", inventoryAt(moved, 5)); len(problems) != 0 {
		t.Fatalf("a correct inventory was refused: %v", problems)
	}
}

// The web case: a well-formed id that no reader can resolve.
func TestSourceCommitFixtureRejectsAnUnreachableCommit(t *testing.T) {
	dir, _, _ := fixtureRepo(t)
	problems := problemsForSourceCommit(dir, "fixture", inventoryAt(strings.Repeat("a", 40), 5))
	if len(problems) != 1 || !strings.Contains(problems[0], "does not resolve") {
		t.Fatalf("an unreachable source_commit was not refused: %v", problems)
	}
}

// The acr case: the base is reachable (an ancestor of HEAD, so an ancestry
// check alone passes) but the anchor line reads differently there.
func TestSourceCommitFixtureRejectsAStaleButReachableBase(t *testing.T) {
	dir, base, _ := fixtureRepo(t)
	if _, err := gitOut(dir, "merge-base", "--is-ancestor", base, "HEAD"); err != nil {
		t.Fatalf("fixture is wrong: the stale base must be an ancestor of HEAD: %v", err)
	}
	problems := problemsForSourceCommit(dir, "fixture", inventoryAt(base, 5))
	if len(problems) != 1 || !strings.Contains(problems[0], "stale base") {
		t.Fatalf("a stale-but-reachable source_commit was not refused: %v", problems)
	}
}

func TestSourceCommitFixtureNamesShallowCheckoutAsTheBenignCause(t *testing.T) {
	dir, base, _ := fixtureRepo(t)
	shallow := filepath.Join(t.TempDir(), "shallow")
	if out, err := gitOut(filepath.Dir(shallow), "clone", "-q", "--depth", "1", "file://"+dir, shallow); err != nil {
		t.Fatalf("shallow clone: %v: %s", err, out)
	}
	problems := problemsForSourceCommit(shallow, "fixture", inventoryAt(base, 4))
	if len(problems) != 1 || !strings.Contains(problems[0], "SHALLOW") {
		t.Fatalf("a shallow checkout did not name itself as the cause: %v", problems)
	}
}

func TestSourceCommitFixtureRejectsAMalformedIdAndAnInventoryWithNoAnchors(t *testing.T) {
	dir, _, moved := fixtureRepo(t)
	if problems := problemsForSourceCommit(dir, "fixture", inventoryAt("main", 5)); len(problems) != 1 {
		t.Fatalf("a branch name was accepted as source_commit: %v", problems)
	}
	empty := map[string]any{"source_commit": moved, "rows": []any{}}
	if problems := problemsForSourceCommit(dir, "fixture", empty); len(problems) != 1 || !strings.Contains(problems[0], "measure nothing") {
		t.Fatalf("an inventory with no anchors passed vacuously: %v", problems)
	}
}
