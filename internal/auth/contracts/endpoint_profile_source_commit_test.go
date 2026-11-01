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
//	half 2, stale but reachable: every anchor must hold at source_commit and
//	  in the tree under test. An anchor holds when the line reads the same at
//	  the same number in both, OR when the tree line's exact text occurs exactly
//	  once in that file at source_commit and exactly once in the tree (a pure
//	  line shift, which a squash merge can cause). Text absent at source_commit,
//	  or occurring twice or more on either side, fails. ENFORCED by the same
//	  function, for anchors in the repo that owns the inventory (path/line
//	  anchors and source{file,line}); an anchor naming another repo is not
//	  checkable here.
//
// A declared rewrite (the inventory's rewritten_anchors) lets a change rewrite the text of an anchored line on purpose;
// see the cells below. source_commit is still a commit that is an ancestor of HEAD, always.
//
// NOT enforced: that the anchor's line is the RIGHT symbol (the rows gate in
// internal/queryapi/server owns that), and anchors in other repos' inventories.
// The fixtures below run on every build; the check over the real inventories
// is in endpoint_profile_source_commit_history_test.go behind the
// authhistory build tag, run only by the auth-test job (full history).

var fullSHA = regexp.MustCompile(`^[0-9a-f]{40}$`)

func gitOut(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-c", "maintenance.auto=false", "-c", "gc.auto=0"}, args...)...)
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

func countLine(text, want string) int {
	n := 0
	for _, l := range strings.Split(text, "\n") {
		if l == want {
			n++
		}
	}
	return n
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
	rewrites, shapeProblems := rewrittenAnchors(name, document["rewritten_anchors"])
	problems = append(problems, shapeProblems...)
	matched := map[lineAnchor]bool{}
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
		if entry, declared := rewrites[a]; declared {
			matched[a] = true
			problems = append(problems, problemsForRewrite(name, commit, entry, then, string(raw))...)
			continue
		}
		oldLine, okThen := lineOfText(then, a.line)
		newLine, okNow := lineOfText(string(raw), a.line)
		shifted := okNow && countLine(then, newLine) == 1 && countLine(string(raw), newLine) == 1
		if !shifted && (!okThen || !okNow || oldLine != newLine) {
			problems = append(problems, fmt.Sprintf("%s: %s:%d reads %q at source_commit %s but %q in the tree: the anchors were not derived against source_commit (stale base)", name, a.file, a.line, strings.TrimSpace(oldLine), commit, strings.TrimSpace(newLine)))
		}
	}
	for _, a := range sortedAnchors(rewrites) {
		if !matched[a] {
			problems = append(problems, fmt.Sprintf("%s: rewritten anchor %s:%d matches no anchor of the rows", name, a.file, a.line))
		}
	}
	return problems
}

// rewrittenEntry is one declared rewrite: the line at file:line was text was at source_commit and reads now in the tree.
type rewrittenEntry struct {
	file, was, now string
	line           int
}

func sortedAnchors(entries map[lineAnchor]rewrittenEntry) []lineAnchor {
	out := make([]lineAnchor, 0, len(entries))
	for a := range entries {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].file != out[j].file {
			return out[i].file < out[j].file
		}
		return out[i].line < out[j].line
	})
	return out
}

// rewrittenAnchors reads the top-level rewritten_anchors: absent is none; a present value is a list whose entries are
// exactly {file, line, was, now}, each anchor at most once.
func rewrittenAnchors(name string, value any) (map[lineAnchor]rewrittenEntry, []string) {
	entries := map[lineAnchor]rewrittenEntry{}
	if value == nil {
		return entries, nil
	}
	list, ok := value.([]any)
	if !ok {
		return entries, []string{fmt.Sprintf("%s: rewritten_anchors is not a list", name)}
	}
	var problems []string
	for i, item := range list {
		object, ok := item.(map[string]any)
		file, okFile := object["file"].(string)
		was, okWas := object["was"].(string)
		now, okNow := object["now"].(string)
		number, okLine := object["line"].(float64)
		if !ok || len(object) != 4 || !okFile || !okWas || !okNow || !okLine || number != float64(int(number)) || number < 1 || file == "" || was == "" || now == "" {
			problems = append(problems, fmt.Sprintf("%s: rewritten_anchors[%d] is not exactly {file, line, was, now} with a file, a whole line number from 1 and a non-empty was and now", name, i))
			continue
		}
		key := lineAnchor{file, int(number)}
		if _, dup := entries[key]; dup {
			problems = append(problems, fmt.Sprintf("%s: rewritten_anchors lists %s:%d twice", name, file, key.line))
			continue
		}
		entries[key] = rewrittenEntry{file: file, line: key.line, was: was, now: now}
	}
	return entries, problems
}

// problemsForRewrite judges one declared rewrite against the file at source_commit (then) and in the tree (now).
func problemsForRewrite(name, commit string, entry rewrittenEntry, then, now string) []string {
	var problems []string
	where := fmt.Sprintf("%s: rewritten anchor %s:%d", name, entry.file, entry.line)
	treeLine, ok := lineOfText(now, entry.line)
	if !ok || treeLine != entry.now {
		problems = append(problems, fmt.Sprintf("%s: now %q is not the tree text %q", where, entry.now, strings.TrimSpace(treeLine)))
	}
	if n := countLine(then, entry.was); n != 1 {
		problems = append(problems, fmt.Sprintf("%s: was %q occurs %d times in the file at source_commit %s, want exactly 1", where, entry.was, n, commit))
	}
	if ok && countLine(then, treeLine) > 0 {
		problems = append(problems, fmt.Sprintf("%s rewrites nothing: the tree line %q also occurs at source_commit %s", where, strings.TrimSpace(treeLine), commit))
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

func fixtureRepoWith(t *testing.T, before, after string) (dir, base, tip string) {
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
	write(before)
	run("add", "routes.go")
	run("commit", "-q", "-m", "base")
	base = run("rev-parse", "HEAD")
	write(after)
	run("commit", "-q", "-am", "tip")
	return dir, base, run("rev-parse", "HEAD")
}

// A line shifted by an insertion, its text unique in both trees, passes.
func TestSourceCommitFixtureAcceptsAShiftedAnchorWithUniqueText(t *testing.T) {
	dir, base, _ := fixtureRepo(t)
	if problems := problemsForSourceCommit(dir, "fixture", inventoryAt(base, 5)); len(problems) != 0 {
		t.Fatalf("a pure line shift was refused: %v", problems)
	}
}

// Same line number, different text, and the new text did not exist before.
func TestSourceCommitFixtureRejectsChangedTextAtTheSameLine(t *testing.T) {
	dir, base, _ := fixtureRepoWith(t, "package x\n\nfunc a() {}\nmux.Handle(\"/a\", h)\n", "package x\n\nfunc a() {}\nmux.Handle(\"/b\", h)\n")
	problems := problemsForSourceCommit(dir, "fixture", inventoryAt(base, 4))
	if len(problems) != 1 || !strings.Contains(problems[0], "stale base") {
		t.Fatalf("changed text at the same line was not refused: %v", problems)
	}
}

// A new text, absent at source_commit, at a shifted line.
func TestSourceCommitFixtureRejectsATextAbsentAtSourceCommit(t *testing.T) {
	dir, base, _ := fixtureRepoWith(t, "package x\n\nfunc a() {}\nmux.Handle(\"/a\", h)\n", "package x\n\nfunc a() {}\nfunc b() {}\nmux.Handle(\"/a\", h)\nmux.Handle(\"/new\", h)\n")
	problems := problemsForSourceCommit(dir, "fixture", inventoryAt(base, 6))
	if len(problems) != 1 || !strings.Contains(problems[0], "stale base") {
		t.Fatalf("a text absent at source_commit was not refused: %v", problems)
	}
}

// A shifted text that occurs twice in the tree, then twice at source_commit.
func TestSourceCommitFixtureRejectsADuplicatedText(t *testing.T) {
	line := "mux.Handle(\"/a\", h)\n"
	dir, base, _ := fixtureRepoWith(t, "package x\n\nfunc a() {}\n"+line, "package x\n\nfunc a() {}\nfunc b() {}\n"+line+line)
	if problems := problemsForSourceCommit(dir, "fixture", inventoryAt(base, 5)); len(problems) != 1 {
		t.Fatalf("a text duplicated in the tree was accepted: %v", problems)
	}
	dir, base, _ = fixtureRepoWith(t, "package x\n"+line+line, "package x\nfunc b() {}\nfunc c() {}\n"+line)
	if problems := problemsForSourceCommit(dir, "fixture", inventoryAt(base, 4)); len(problems) != 1 {
		t.Fatalf("a text duplicated at source_commit was accepted: %v", problems)
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

// A declared rewrite: an inventory may list, in the top-level rewritten_anchors, an anchor whose line this change
// rewrote on purpose. The entry is {file, line, was, now}. For an anchor with an entry, now must be the tree text at that
// line and was must occur exactly once in the file at source_commit; every other anchor keeps the rule above. An entry
// fails when the tree line also occurs at source_commit (it rewrites nothing) and when it matches no anchor, so the list
// empties again at the re-pin and cannot rot.

const (
	rewriteBefore = "package x\n\nfunc a() {}\nmux.Handle(\"/a\", h)\n"
	rewriteAfter  = "package x\n\nfunc a() {}\nmux.Handle(\"/b\", h)\n"
	rewriteWas    = "mux.Handle(\"/a\", h)"
	rewriteNow    = "mux.Handle(\"/b\", h)"
)

// inventoryWith is an inventory with an anchor at each of lines and the rewritten_anchors JSON given (empty: none).
func inventoryWith(commit string, lines []int, entries string) map[string]any {
	rows := make([]string, 0, len(lines))
	for _, line := range lines {
		rows = append(rows, fmt.Sprintf(`{"source": {"file": "routes.go", "line": %d}}`, line))
	}
	raw := fmt.Sprintf(`{"source_commit": %q, "rows": [%s]`, commit, strings.Join(rows, ","))
	if entries != "" {
		raw += `, "rewritten_anchors": ` + entries
	}
	raw += "}"
	var document map[string]any
	if err := json.Unmarshal([]byte(raw), &document); err != nil {
		panic(err)
	}
	return document
}

func rewriteEntry(file string, line int, was, now string) string {
	return fmt.Sprintf(`[{"file": %q, "line": %d, "was": %q, "now": %q}]`, file, line, was, now)
}

func wantOneProblem(t *testing.T, problems []string, fragments ...string) {
	t.Helper()
	if len(problems) != 1 {
		t.Fatalf("want exactly one problem, got %v", problems)
	}
	for _, fragment := range fragments {
		if !strings.Contains(problems[0], fragment) {
			t.Fatalf("the problem %q does not say %q", problems[0], fragment)
		}
	}
}

// Green: the line's text was rewritten, the entry names the old and the new text, both hold.
func TestSourceCommitRewrittenAnchorAcceptsAValidEntry(t *testing.T) {
	dir, base, _ := fixtureRepoWith(t, rewriteBefore, rewriteAfter)
	if problems := problemsForSourceCommit(dir, "fixture", inventoryWith(base, []int{4}, rewriteEntry("routes.go", 4, rewriteWas, rewriteNow))); len(problems) != 0 {
		t.Fatalf("a valid rewritten anchor was refused: %v", problems)
	}
}

// was is not a line of the file at source_commit (a wrong text).
func TestSourceCommitRewrittenAnchorRejectsAWrongWas(t *testing.T) {
	dir, base, _ := fixtureRepoWith(t, rewriteBefore, rewriteAfter)
	problems := problemsForSourceCommit(dir, "fixture", inventoryWith(base, []int{4}, rewriteEntry("routes.go", 4, "mux.Handle(\"/zzz\", h)", rewriteNow)))
	wantOneProblem(t, problems, "rewritten anchor routes.go:4", "was", "0 times")
}

// was is the NEW text: it exists in the tree and not at source_commit.
func TestSourceCommitRewrittenAnchorRejectsAWasAbsentAtSourceCommit(t *testing.T) {
	dir, base, _ := fixtureRepoWith(t, rewriteBefore, rewriteAfter)
	problems := problemsForSourceCommit(dir, "fixture", inventoryWith(base, []int{4}, rewriteEntry("routes.go", 4, rewriteNow, rewriteNow)))
	wantOneProblem(t, problems, "rewritten anchor routes.go:4", "was", "0 times")
}

// was occurs twice at source_commit: it does not name one line.
func TestSourceCommitRewrittenAnchorRejectsAWasThatOccursTwice(t *testing.T) {
	line := "mux.Handle(\"/a\", h)\n"
	dir, base, _ := fixtureRepoWith(t, "package x\n"+line+line, "package x\n"+line+"mux.Handle(\"/b\", h)\n")
	problems := problemsForSourceCommit(dir, "fixture", inventoryWith(base, []int{3}, rewriteEntry("routes.go", 3, rewriteWas, rewriteNow)))
	wantOneProblem(t, problems, "rewritten anchor routes.go:3", "was", "2 times")
}

// now is not what the tree holds at that line.
func TestSourceCommitRewrittenAnchorRejectsANowThatIsNotTheTreeText(t *testing.T) {
	dir, base, _ := fixtureRepoWith(t, rewriteBefore, rewriteAfter)
	problems := problemsForSourceCommit(dir, "fixture", inventoryWith(base, []int{4}, rewriteEntry("routes.go", 4, rewriteWas, "mux.Handle(\"/c\", h)")))
	wantOneProblem(t, problems, "rewritten anchor routes.go:4", "now", "is not the tree text")
}

// The entry names a line no anchor of the rows is on.
func TestSourceCommitRewrittenAnchorRejectsAnEntryThatMatchesNoAnchor(t *testing.T) {
	dir, base, _ := fixtureRepoWith(t, rewriteBefore, rewriteAfter)
	problems := problemsForSourceCommit(dir, "fixture", inventoryWith(base, []int{3}, rewriteEntry("routes.go", 4, rewriteWas, rewriteNow)))
	wantOneProblem(t, problems, "rewritten anchor routes.go:4", "matches no anchor")
}

// The tree line also occurs at source_commit (a line shift): the entry rewrites nothing.
func TestSourceCommitRewrittenAnchorRejectsAnEntryThatRewritesNothing(t *testing.T) {
	dir, base, _ := fixtureRepo(t)
	problems := problemsForSourceCommit(dir, "fixture", inventoryWith(base, []int{5}, rewriteEntry("routes.go", 5, rewriteWas, rewriteWas)))
	wantOneProblem(t, problems, "rewritten anchor routes.go:5", "rewrites nothing")
}

// An anchor with no entry keeps today's rule, beside an anchor that has one.
func TestSourceCommitRewrittenAnchorLeavesAnAnchorWithoutAnEntryUnderTheOldRule(t *testing.T) {
	dir, base, _ := fixtureRepoWith(t, "package x\n\nfunc a() {}\nmux.Handle(\"/a\", h)\n", "package x\n\nfunc z() {}\nmux.Handle(\"/b\", h)\n")
	problems := problemsForSourceCommit(dir, "fixture", inventoryWith(base, []int{3, 4}, rewriteEntry("routes.go", 4, rewriteWas, rewriteNow)))
	wantOneProblem(t, problems, "routes.go:3", "stale base")
}

// The same anchor listed twice, and an entry that is not {file, line, was, now} with nothing else.
func TestSourceCommitRewrittenAnchorRejectsAMalformedEntryAndADuplicate(t *testing.T) {
	dir, base, _ := fixtureRepoWith(t, rewriteBefore, rewriteAfter)
	one := `{"file": "routes.go", "line": 4, "was": "mux.Handle(\"/a\", h)", "now": "mux.Handle(\"/b\", h)"}`
	for name, entries := range map[string]string{
		"listed twice":                      "[" + one + "," + one + "]",
		"a missing key":                     `[{"file": "routes.go", "line": 4, "was": "mux.Handle(\"/a\", h)"}]`,
		"an extra key":                      `[{"file": "routes.go", "line": 4, "was": "mux.Handle(\"/a\", h)", "now": "mux.Handle(\"/b\", h)", "why": "x"}]`,
		"a line that is not a whole number": `[{"file": "routes.go", "line": 4.5, "was": "mux.Handle(\"/a\", h)", "now": "mux.Handle(\"/b\", h)"}]`,
		"not a list":                        `{"file": "routes.go"}`,
	} {
		problems := problemsForSourceCommit(dir, "fixture", inventoryWith(base, []int{4}, entries))
		if len(problems) == 0 || !strings.Contains(strings.Join(problems, "\n"), "rewritten_anchors") {
			t.Errorf("%s: not refused as a rewritten_anchors defect: %v", name, problems)
		}
	}
}
