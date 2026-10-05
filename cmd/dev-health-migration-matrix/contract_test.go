package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/migrationmatrix"
)

// TestTheCommittedMatrixSatisfiesItsOwnContract runs the real -check against
// the real repository, so `go test ./...` fails the moment the committed page
// disagrees with the committed sources or a row claims a status its evidence
// column cannot support.
//
// This is separate from internal/migrationmatrix's unit tests on purpose:
// those prove the RULES work on synthetic rows, this proves the SHIPPED
// DOCUMENT obeys them. A repo can have a perfect validator and a lying page.
func TestTheCommittedMatrixSatisfiesItsOwnContract(t *testing.T) {
	root := repoRoot(t)
	if err := runCheck(root); err != nil {
		t.Fatalf("the committed migration matrix fails its own contract: %v\n\n"+
			"Re-render with:\n"+
			"  go run ./cmd/dev-health-migration-matrix -render -root . -fleet none\n", err)
	}
}

// TestCheckFailsWhenTheDocIsEditedByHand is the RED half. A generated block is
// only as good as the check that notices someone improved it in place -- which
// is precisely how the ic_finalize citation came to name a file that had been
// deleted weeks earlier.
func TestCheckFailsWhenTheDocIsEditedByHand(t *testing.T) {
	root := t.TempDir()
	realRoot := repoRoot(t)

	for _, relative := range []string{
		docRelative,
		statusRelative,
		renderRelative,
		nativeRelative,
		digestPinRelative,
		providerMatrixRelative,
		dailyFamiliesRelative,
		remainingFamiliesRel,
		jobDailyPyRelative,
		catalogRelative,
		frozenRoutesRelative,
	} {
		copyInto(t, filepath.Join(realRoot, relative), filepath.Join(root, relative))
	}

	// Sanity: the copy still passes, so a failure below is caused by the
	// edit and not by the copying.
	if err := runCheck(root); err != nil {
		t.Fatalf("the copied tree should pass before it is tampered with: %v", err)
	}

	docPath := filepath.Join(root, docRelative)
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read copied doc: %v", err)
	}
	// The most tempting hand edit there is: soften the honest cell. Tampering
	// with the PARITY column rather than the deployed one on purpose -- the
	// deployed cell's text legitimately changes when the fleet is rebuilt
	// (it read "**unknown**" until the Compose COMMIT build-arg landed,
	// and a real sha after), so a test anchored on it would fail
	// for a reason that has nothing to do with tampering.
	//
	// "UNVERIFIED" is present for as long as any family is unverified; once
	// every family carries real parity evidence there is none left to
	// soften in that direction, so the fallback tampers a VERIFIED cell's
	// citation instead -- either edit is a generated cell disagreeing with
	// its own ledger, which is exactly what the check exists to catch.
	tampered := replaceFirst(string(raw), "| UNVERIFIED |", "| VERIFIED |")
	if tampered == string(raw) {
		tampered = replaceFirst(string(raw), "| VERIFIED (", "| DIVERGED (")
	}
	if tampered == string(raw) {
		t.Fatal("expected the rendered doc to contain a parity cell to tamper with")
	}
	if err := os.WriteFile(docPath, []byte(tampered), 0o644); err != nil {
		t.Fatalf("write tampered doc: %v", err)
	}

	if err := runCheck(root); err == nil {
		t.Fatal("hand-editing a generated cell must fail the check; it passed")
	}
}

func replaceFirst(haystack, old, replacement string) string {
	index := indexOf(haystack, old)
	if index < 0 {
		return haystack
	}
	return haystack[:index] + replacement + haystack[index+len(old):]
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

func copyInto(t *testing.T, from, to string) {
	t.Helper()
	raw, err := os.ReadFile(from) //nolint:gosec // test-local path
	if err != nil {
		t.Fatalf("read %s: %v", from, err)
	}
	if err := os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(to), err)
	}
	if err := os.WriteFile(to, raw, 0o644); err != nil {
		t.Fatalf("write %s: %v", to, err)
	}
}

// repoRoot walks up from the test's working directory to the module root.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find the module root above the test's working directory")
		}
		dir = parent
	}
}

// The offline -routing path exists so an operator without a DSN can still
// render the page. Its instruction was once unfollowable: it
// named an unexported function, so "produce rows with the reader's SQL" meant
// retyping the SQL. These two tests pin the two halves of the fix -- that the
// statement is reachable, and that a snapshot missing the routing KEY is
// refused instead of silently collapsing two documents into one row.

// scratchGitConfig is the front of every git call that a test of this package
// makes in a scratch repository. Without it, `git commit` (and `git fetch`)
// start `git maintenance run --auto --quiet` as a child process. On a host
// whose git runs that step detached it can still write under `.git` when the
// test returns; Go then cannot remove the temp dir and FAILS the test with
// "TempDir RemoveAll cleanup: unlinkat .../.git: directory not empty", after
// the test's own assertion passed (CHAOS-8529: three such red runs in one
// hour). gc.auto=0 and maintenance.auto=false stop git from starting it.
var scratchGitConfig = []string{"-c", "gc.auto=0", "-c", "maintenance.auto=false"}

// scratchGit is the ONLY way a test of this package may start git:
// TestEveryTestGitCallGoesThroughScratchGit fails on a direct exec.Command.
func scratchGit(args ...string) *exec.Cmd {
	return exec.Command("git", append(append([]string{}, scratchGitConfig...), args...)...)
}

// The settings are part of the argument list of every command scratchGit
// builds, before the caller's own arguments (git reads -c only before the
// subcommand).
func TestScratchGitCarriesTheNoMaintenanceSettings(t *testing.T) {
	got := scratchGit("-C", "somewhere", "commit", "-qm", "x").Args
	want := []string{"git", "-c", "gc.auto=0", "-c", "maintenance.auto=false", "-C", "somewhere", "commit", "-qm", "x"}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("scratchGit args = %q, want %q", got, want)
	}
}

// A test file of this package that starts git directly would bring the
// maintenance child process back. The guard reads the test sources; it fails
// when it reads none, and when the one allowed call (inside scratchGit) is
// not found, so it cannot pass by reading nothing.
func TestEveryTestGitCallGoesThroughScratchGit(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	direct := "exec.Command(" + `"git"`
	read, allowed := 0, 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		source, err := os.ReadFile(entry.Name())
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		read++
		for number, line := range strings.Split(string(source), "\n") {
			if !strings.Contains(line, direct) {
				continue
			}
			if strings.Contains(line, "scratchGitConfig") {
				allowed++
				continue
			}
			t.Errorf("%s:%d starts git directly; use scratchGit so that no maintenance process outlives the test", entry.Name(), number+1)
		}
	}
	if read == 0 {
		t.Fatal("no _test.go file was read: the guard measured nothing")
	}
	if allowed != 1 {
		t.Fatalf("found %d git calls inside scratchGit, want exactly 1: the guard no longer recognises the helper", allowed)
	}
}

// copyContractTree copies every committed file -check reads into a fresh
// root, so a test can alter one of them and run the real -check on it.
func copyContractTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	realRoot := repoRoot(t)
	for _, relative := range []string{
		docRelative, statusRelative, renderRelative, nativeRelative, digestPinRelative,
		providerMatrixRelative, dailyFamiliesRelative, remainingFamiliesRel, jobDailyPyRelative,
		catalogRelative, frozenRoutesRelative,
	} {
		copyInto(t, filepath.Join(realRoot, relative), filepath.Join(root, relative))
	}
	return root
}

// runCheckCapturingViolations runs the real -check and returns its error and
// what it printed, so a test can say WHICH rule failed rather than only that
// something did.
func runCheckCapturingViolations(t *testing.T, root string) (error, string) {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	saved := os.Stderr
	os.Stderr = writer
	checkErr := runCheck(root)
	os.Stderr = saved
	_ = writer.Close()
	var captured bytes.Buffer
	_, _ = io.Copy(&captured, reader)
	return checkErr, captured.String()
}

// writeSnapshotAndPage commits `rows` as the render snapshot AND re-renders
// writeOpsShaAndPage commits `opsSha` as the render snapshot's ops_sha AND
// re-renders the ops block from it, exactly as -render would, so the page
// and its snapshot still agree (R13 passes) and any failure is about
// ops_sha's ancestry alone, not a doc/snapshot mismatch.
func writeOpsShaAndPage(t *testing.T, root, opsSha string) {
	t.Helper()
	snapshot, err := migrationmatrix.LoadRender(filepath.Join(root, renderRelative))
	if err != nil {
		t.Fatalf("load the copied snapshot: %v", err)
	}
	snapshot.OpsSha = opsSha
	if err := writeJSON(filepath.Join(root, renderRelative), snapshot); err != nil {
		t.Fatalf("write the snapshot: %v", err)
	}
	catalog, err := migrationmatrix.LoadCatalog(filepath.Join(root, catalogRelative))
	if err != nil {
		t.Fatalf("load the copied catalog: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(root, docRelative))
	if err != nil {
		t.Fatalf("read the copied doc: %v", err)
	}
	doc, err := migrationmatrix.ReplaceBlock(string(raw), migrationmatrix.OpsBlockBegin, migrationmatrix.OpsBlockEnd,
		migrationmatrix.RenderOpsBlock(snapshot, catalog))
	if err != nil {
		t.Fatalf("re-render the ops block: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, docRelative), []byte(doc), 0o644); err != nil {
		t.Fatalf("write the doc: %v", err)
	}
}

// TestCheckFailsWhenOpsShaIsNotAnAncestorOfHEAD is the RED half: a rebase
// (or a squash on main) can leave a shape-valid ops_sha in last-render.json
// that HEAD's own history no longer contains, with the rebase itself
// reporting no conflicts at all -- so -check must not pass with the wrong
// sha in the file. Reproduced with real git history, the same way
// TestRenderFailsOnALiveRowTheCatalogCannotDispatch does for R14, rather
// than asserted from the validator function alone.
func TestCheckFailsWhenOpsShaIsNotAnAncestorOfHEAD(t *testing.T) {
	root := copyContractTree(t)
	run := func(args ...string) string {
		t.Helper()
		full := append([]string{"-C", root, "-c", "user.email=t@example.com",
			"-c", "user.name=t", "-c", "commit.gpgsign=false"}, args...)
		out, err := scratchGit(full...).CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	run("init", "-q", "-b", "main")
	run("add", "-A")
	run("commit", "-qm", "c1")
	stale := run("rev-parse", "HEAD")

	// A commit with no history in common with the first one -- the shape a
	// rebase leaves behind when it replays this branch onto a base whose
	// own history no longer contains the commit ops_sha names (most often
	// because it was squashed into a different commit on main).
	run("checkout", "-q", "--orphan", "tmp")
	run("commit", "-q", "--allow-empty", "-m", "c2")
	run("branch", "-f", "main", "tmp")
	run("checkout", "-q", "main")
	run("branch", "-D", "tmp")
	head := run("rev-parse", "HEAD")
	if stale == head {
		t.Fatalf("test setup did not diverge HEAD from the stale commit")
	}

	// GREEN control: ops_sha == HEAD (what a fresh render just wrote) still
	// passes -- the rule is ancestry, not the incident shape, and this is
	// the ordinary case that must never false-positive.
	writeOpsShaAndPage(t, root, head)
	if err := runCheck(root); err != nil {
		t.Fatalf("ops_sha == HEAD must pass ancestry: %v", err)
	}

	// RED: a real, resolvable commit that is no longer an ancestor of HEAD.
	writeOpsShaAndPage(t, root, stale)
	err, printed := runCheckCapturingViolations(t, root)
	if err == nil {
		t.Fatal("-check must fail when ops_sha is not an ancestor of HEAD")
	}
	if !strings.Contains(printed, "R9-ops-sha-not-ancestor") {
		t.Fatalf("-check must name R9-ops-sha-not-ancestor, got:\n%s", printed)
	}
	// A checker that says only "mismatch" makes the next person do the diff
	// by hand -- the failure must name BOTH the stale recorded value and
	// HEAD it was checked against.
	if !strings.Contains(printed, stale) || !strings.Contains(printed, head) {
		t.Fatalf("the failure must name both the recorded ops_sha and HEAD, got:\n%s", printed)
	}
}

func gitRunFor(t *testing.T, dir string, args ...string) string {
	t.Helper()
	full := append([]string{"-C", dir, "-c", "user.email=t@example.com",
		"-c", "user.name=t", "-c", "commit.gpgsign=false"}, args...)
	out, err := scratchGit(full...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// TestOpsShaCheckFailsWhenHEADDoesNotResolve is the CI-shaped case a hosted
// runner's checkout can actually be in: a REAL git repository (unlike a
// synthetic fixture with no .git at all) whose HEAD does not resolve --
// unborn branch, detached-and-broken, or corrupt refs. This is the gate's
// own target environment misbehaving, so it must fail loudly rather than
// take the "no git here" skip a fixture with no .git legitimately gets.
func TestOpsShaCheckFailsWhenHEADDoesNotResolve(t *testing.T) {
	root := t.TempDir()
	gitRunFor(t, root, "init", "-q")

	violations, err := checkOpsShaAncestry(root, strings.Repeat("a", 40))
	if err != nil {
		t.Fatalf("checkOpsShaAncestry returned an error instead of a violation: %v", err)
	}
	found := false
	for _, v := range violations {
		if v.Rule == "R9-ops-sha-unverifiable" {
			found = true
		}
	}
	if !found {
		t.Fatalf("a git repository with no resolvable HEAD must fail R9-ops-sha-unverifiable, not skip silently; got: %v", violations)
	}
}

// TestOpsShaCheckDoesNotTrustAFalseNegativeFromAShallowClone reproduces,
// with real git plumbing, the actual mechanism a hosted-runner CI checkout
// hit: a LATER, unrelated `git fetch --depth=1` against a commit that is
// ALREADY fully present re-shallows the checkout anyway (`.git/shallow`
// records the fetched commit as having no parents, independent of whether
// the real history is still present as objects), and every ancestry check
// after it in the same checkout can then read a genuine ancestor as "not an
// ancestor". Verified directly before writing this test: `git
// merge-base --is-ancestor` on an ordinary full clone answers true for the
// first commit; after `git fetch --depth=1 origin <a later commit that is
// already present>` in that SAME clone, the identical command answers
// false, while `git cat-file -e` on the same sha still succeeds. A checker
// that trusted that negative would be the exact false pass this ticket
// exists to close, wearing the fix's own name.
func TestOpsShaCheckDoesNotTrustAFalseNegativeFromAShallowClone(t *testing.T) {
	origin := t.TempDir()
	gitRunFor(t, origin, "init", "-q", "-b", "main")
	writeCommit := func(content, message string) string {
		if err := os.WriteFile(filepath.Join(origin, "f"), []byte(content), 0o600); err != nil {
			t.Fatalf("write f: %v", err)
		}
		gitRunFor(t, origin, "add", "-A")
		gitRunFor(t, origin, "commit", "-qm", message)
		return gitRunFor(t, origin, "rev-parse", "HEAD")
	}
	ancestorSha := writeCommit("1", "c1")
	laterSha := writeCommit("2", "c2")
	writeCommit("3", "c3")

	clone := t.TempDir()
	if out, err := scratchGit("clone", "-q", origin, clone).CombinedOutput(); err != nil {
		t.Fatalf("git clone: %v %s", err, out)
	}
	// Sanity: on an ordinary full clone, ancestry is real and true.
	if isAncestor, err := gitIsAncestor(clone, ancestorSha, "HEAD"); err != nil || !isAncestor {
		t.Fatalf("sanity check failed: a full clone should show ancestorSha as an ancestor of HEAD (isAncestor=%v, err=%v)", isAncestor, err)
	}

	// The re-shallow: a fetch of a commit the clone ALREADY has in full,
	// exactly as the observed CI step did against its own already-fetched
	// base sha.
	if out, err := scratchGit("-C", clone, "fetch", "-q", "--depth=1", "origin", laterSha).CombinedOutput(); err != nil {
		t.Fatalf("git fetch --depth=1: %v %s", err, out)
	}
	if shallow, err := gitOutput(clone, "rev-parse", "--is-shallow-repository"); err != nil || shallow != "true" {
		t.Fatalf("test setup did not actually re-shallow the clone (shallow=%q, err=%v)", shallow, err)
	}
	// The object is still there -- only the graph's parent link was cut.
	if _, err := gitOutput(clone, "cat-file", "-e", ancestorSha+"^{commit}"); err != nil {
		t.Fatalf("test setup lost the ancestor object entirely, not just its parent link: %v", err)
	}

	violations, err := checkOpsShaAncestry(clone, ancestorSha)
	if err != nil {
		t.Fatalf("checkOpsShaAncestry returned an error instead of a violation: %v", err)
	}
	var gotShallowRule, gotPlainNotAncestorRule bool
	for _, v := range violations {
		switch v.Rule {
		case "R9-ops-sha-unverifiable-shallow":
			gotShallowRule = true
		case "R9-ops-sha-not-ancestor":
			gotPlainNotAncestorRule = true
		}
	}
	if !gotShallowRule {
		t.Fatalf("a shallow clone's false-negative ancestry must fail R9-ops-sha-unverifiable-shallow, got: %v", violations)
	}
	if gotPlainNotAncestorRule {
		t.Fatalf("a shallow clone's untrustworthy negative must not be reported as a real R9-ops-sha-not-ancestor violation (that message blames a rebase/squash, which did not happen here), got: %v", violations)
	}
}

func TestNoFlagIsSilentlyIgnored(t *testing.T) {
	type flags = map[string]bool
	for _, c := range []struct {
		name          string
		explicit      flags
		render, check bool
		fleet         string
		refuse        bool
	}{
		{"-check alone", flags{"check": true}, false, true, "docker", false},
		{"-check -root", flags{"check": true, "root": true}, false, true, "docker", false},
		{"-check -dsn", flags{"check": true, "dsn": true}, false, true, "docker", true},
		{"-check -fleet", flags{"check": true, "fleet": true}, false, true, "none", true},
		{"-check -containers", flags{"check": true, "containers": true}, false, true, "docker", true},
		{"-render -check=false (set, false: not -check)", flags{"render": true, "check": true}, true, false, "docker", false},
		{"-render -dsn", flags{"render": true, "dsn": true}, true, false, "docker", false},
		{"-render -fleet docker -containers", flags{"render": true, "fleet": true, "containers": true}, true, false, "docker", false},
		{"-render -fleet none -containers", flags{"render": true, "fleet": true, "containers": true}, true, false, "none", true},
		{"-render -fleet <file> -containers", flags{"render": true, "fleet": true, "containers": true}, true, false, "/tmp/fleet.json", true},
	} {
		err := checkFlagCombination(c.explicit, c.render, c.check, c.fleet)
		if (err != nil) != c.refuse {
			t.Fatalf("%s: refused=%v, want %v (err=%v)", c.name, err != nil, c.refuse, err)
		}
	}
}

// The ops block says what serves the operations -- and that no routing row does -- and -check agrees with a re-render.
func TestRenderSaysTheCatalogIsServedAndCheckAgrees(t *testing.T) {
	root := copyContractTree(t)
	for _, args := range [][]string{
		{"init", "-q"},
		{"-c", "user.email=t@example.com", "-c", "user.name=t", "-c", "commit.gpgsign=false", "add", "-A"},
		{"-c", "user.email=t@example.com", "-c", "user.name=t", "-c", "commit.gpgsign=false", "commit", "-qm", "scratch"},
	} {
		if out, err := scratchGit(append([]string{"-C", root}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
	}
	if err := runRender(root, "", "none", nil); err != nil {
		t.Fatalf("runRender: %v", err)
	}
	doc, err := os.ReadFile(filepath.Join(root, docRelative))
	if err != nil {
		t.Fatal(err)
	}
	block, err := migrationmatrix.ExtractBlock(string(doc), migrationmatrix.OpsBlockBegin, migrationmatrix.OpsBlockEnd)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := migrationmatrix.LoadCatalog(filepath.Join(root, catalogRelative))
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("serves all **%d** catalog operations", catalog.OperationCount())
	if !strings.Contains(block, want) || !strings.Contains(block, "reads no routing row") {
		t.Fatalf("the ops block must say what serves the catalog operations (want %q):\n%s", want, block)
	}
	if strings.Contains(block, "| Operation |") {
		t.Fatalf("the ops block still renders a per-operation table:\n%s", block)
	}
	if err := runCheck(root); err != nil {
		t.Fatalf("-check refused what -render just wrote: %v", err)
	}
}
