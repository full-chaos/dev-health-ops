package gqlgenguard

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The tests here are the executed claim: the real gqlgen configuration, the
// real generator, the real repository. Everything else in this package is a
// fixture, and a fixture cannot tell you whether the thing shipped works.

// TestCheckDriftOnThisRepositoryMatchesTheCommittedRecord runs exactly what CI
// runs, against this checkout, and asserts it passes and writes nothing.
//
// This is the test that fails when someone regenerates over a hand-edit, or
// deletes one, or changes gqlgen.yml, or bumps the gqlgen version.
func TestCheckDriftOnThisRepositoryMatchesTheCommittedRecord(t *testing.T) {
	goAvailable(t)
	root := repoRoot(t)

	before := repoDigests(t, root)

	res, err := CheckDrift(context.Background(), Options{
		ModuleDir:  root,
		TempParent: t.TempDir(),
		Report:     io.Discard,
	})
	if err != nil {
		t.Fatalf("the drift check CI runs does not pass on this checkout: %v", err)
	}
	if len(res.Changes) == 0 {
		t.Fatal("no declared outputs were enumerated, so this proves nothing")
	}
	// The generator really ran, proved by what it produced rather than by the
	// repository differing from it. Drift used to be that proof: every
	// checked-in output carried a hand-edit, so a run that produced nothing
	// looked identical to a run that produced the tree. Expressing those
	// edits through gqlgen's own configuration ended the drift and took the
	// signal with it -- so the proof is now the outputs themselves. A
	// generator that no-ops leaves every declared output absent in the copy,
	// which is an empty generated digest here (and a refusal one step later,
	// because a checked-in output vanished).
	produced := 0
	for _, c := range res.Changes {
		if c.TreeDigest == "" {
			continue // not checked in; the generator may legitimately create it
		}
		if c.GeneratedDigest == "" {
			t.Fatalf("%s is checked in but a fresh generation produced nothing for it, so the generator did not really run", c.Path)
		}
		produced++
	}
	if produced == 0 {
		t.Fatal("no checked-in declared output was produced, so this proves nothing about the generator")
	}
	t.Logf("CELL-OUTPUT: accepted: %d checked-in declared output(s) reproduced, %d drifted", produced, len(res.Changes))
	assertUnchanged(t, before, repoDigests(t, root), "CheckDrift against the real repository")
}

// TestGenerateOnACopyOfThisRepositoryProducesTheRecordedDigests runs the
// writing verb for real -- on a copy, so the checkout is never a casualty --
// and asserts the bytes it lands are exactly the ones the record says a fresh
// generation produces, and that go.mod and go.sum did not move.
func TestGenerateOnACopyOfThisRepositoryProducesTheRecordedDigests(t *testing.T) {
	goAvailable(t)
	root := repoRoot(t)

	copyDir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve the copy directory: %v", err)
	}
	src, err := os.OpenRoot(root)
	if err != nil {
		t.Fatalf("open the repository root: %v", err)
	}
	defer src.Close()
	dst, err := os.OpenRoot(copyDir)
	if err != nil {
		t.Fatalf("open the copy root: %v", err)
	}
	defer dst.Close()
	if _, _, err := CopyTree(context.Background(), src, dst, skipVCS); err != nil {
		t.Fatalf("copy the repository: %v", err)
	}

	// What the record says a fresh generation produces, taken from the guard's
	// own run against the copy rather than re-parsed from the file.
	checked, err := CheckDrift(context.Background(), Options{
		ModuleDir:  copyDir,
		TempParent: t.TempDir(),
		Report:     io.Discard,
	})
	if err != nil {
		t.Fatalf("check drift in the copy: %v", err)
	}

	beforeMod, err := os.ReadFile(filepath.Join(copyDir, "go.mod"))
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	beforeSum, err := os.ReadFile(filepath.Join(copyDir, "go.sum"))
	if err != nil {
		t.Fatalf("read go.sum: %v", err)
	}

	// The repository records no drift now, so `generate` has nothing to
	// revert: it must succeed, write nothing, and say so. What it MUST NOT do
	// is decide that an empty record is permission to overwrite -- the arm
	// below plants a difference the record does not describe and pins the
	// refusal without depending on this repository carrying hand-edits, which
	// is a property that has now gone away.
	before, err := TakeSnapshot(dst, skipVCS)
	if err != nil {
		t.Fatalf("snapshot before the no-op generate: %v", err)
	}
	var quiet strings.Builder
	noop, err := Generate(context.Background(), Options{
		ModuleDir:  copyDir,
		TempParent: t.TempDir(),
		Report:     &quiet,
	})
	if err != nil {
		t.Fatalf("generate refused a tree a fresh generation already reproduces: %v\n%s", err, quiet.String())
	}
	if len(noop.Applied) != 0 {
		t.Fatalf("generate wrote %v although nothing drifted", noop.Applied)
	}
	if !strings.Contains(quiet.String(), "no output changed") {
		t.Fatalf("generate did not say it changed nothing:\n%s", quiet.String())
	}
	t.Logf("CELL-OUTPUT: accepted: %s", firstLine(quiet.String()))
	afterNoop, err := TakeSnapshot(dst, skipVCS)
	if err != nil {
		t.Fatalf("snapshot after the no-op generate: %v", err)
	}
	for rel, e := range before {
		if afterNoop[rel].Digest != e.Digest {
			t.Fatalf("the no-op generate changed %s", rel)
		}
	}
	if len(afterNoop) != len(before) {
		t.Fatalf("the no-op generate changed the file set: %d -> %d", len(before), len(afterNoop))
	}

	// A difference the record does not describe: the fail-closed arm.
	handEdited := filepath.Join(copyDir, "cmd", "query-api", "internal", "graph", "model", "models_gen.go")
	original, err := os.ReadFile(handEdited)
	if err != nil {
		t.Fatalf("read a generated file to hand-edit: %v", err)
	}
	if err := os.WriteFile(handEdited, append(original, []byte("\n// a hand-edit the record does not describe\n")...), 0o644); err != nil {
		t.Fatalf("plant the hand-edit: %v", err)
	}
	var refusal strings.Builder
	if _, rerr := Generate(context.Background(), Options{
		ModuleDir:  copyDir,
		TempParent: t.TempDir(),
		Report:     &refusal,
	}); rerr == nil {
		t.Fatal("generate overwrote a difference the record says nothing about")
	} else {
		t.Logf("CELL-OUTPUT: refused: %v", rerr)
	}
	if err := os.WriteFile(handEdited, original, 0o644); err != nil {
		t.Fatalf("restore the hand-edited file: %v", err)
	}

	var applied strings.Builder
	res, err := Generate(context.Background(), Options{
		ModuleDir:      copyDir,
		TempParent:     t.TempDir(),
		RevertRecorded: true,
		Report:         &applied,
	})
	if err != nil {
		t.Fatalf("generate in the copy: %v", err)
	}
	// How many hand-edits this repository records is a property of the
	// repository, not of the guard, and it is zero now: everything the
	// generated files used to carry by hand is expressed through gqlgen's own
	// configuration. So the run names what it reverts only when there is
	// something to name, and writes nothing when there is not -- which is the
	// same statement, and the one that survives the count reaching zero.
	recordBytes, err := os.ReadFile(filepath.Join(copyDir, DefaultDriftPath))
	if err != nil {
		t.Fatalf("read the copy's expected-drift record: %v", err)
	}
	recorded := 0
	for line := range strings.SplitSeq(string(recordBytes), "\n") {
		if strings.HasPrefix(line, "digest drift ") {
			recorded++
		}
	}
	switch {
	case recorded > 0:
		want := fmt.Sprintf("reverting %d recorded hand-edit(s)", recorded)
		if !strings.Contains(applied.String(), want) {
			t.Fatalf("the applied run did not name the hand-edits it reverted (want %q):\n%s", want, applied.String())
		}
		if len(res.Applied) == 0 {
			t.Fatal("generate applied nothing although the record describes drift")
		}
	default:
		if strings.Contains(applied.String(), "reverting ") {
			t.Fatalf("the run named hand-edits the record does not describe:\n%s", applied.String())
		}
		if len(res.Applied) != 0 {
			t.Fatalf("generate wrote %v although the record describes no drift", res.Applied)
		}
	}
	t.Logf("CELL-OUTPUT: accepted under -revert-recorded, %d recorded: %s", recorded, firstLine(applied.String()))

	// Every file now on disk carries the digest the check said the generator
	// produces. Comparing digests rather than "it changed" is what makes this
	// an equality rather than an observation.
	after := repoDigests(t, copyDir)
	for _, c := range checked.Changes {
		if c.GeneratedDigest == "" {
			if _, present := after[c.Path]; present && c.TreeDigest == "" {
				t.Fatalf("%s was created but the generator never wrote it", c.Path)
			}
			continue
		}
		if got := after[c.Path]; got != c.GeneratedDigest {
			t.Fatalf("%s after generate has digest %s, want the generated %s", c.Path, got, c.GeneratedDigest)
		}
	}

	afterMod, err := os.ReadFile(filepath.Join(copyDir, "go.mod"))
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	if string(afterMod) != string(beforeMod) {
		t.Fatal("go.mod is not byte-identical after generate; the generator's tidy reached the module")
	}
	afterSum, err := os.ReadFile(filepath.Join(copyDir, "go.sum"))
	if err != nil {
		t.Fatalf("read go.sum: %v", err)
	}
	if string(afterSum) != string(beforeSum) {
		t.Fatal("go.sum is not byte-identical after generate; the generator's tidy reached the module")
	}

	// A second check against the now-regenerated copy: the tree it leaves
	// behind must be one a fresh generation reproduces exactly. Whether it
	// still MATCHES the committed record depends on what the record described
	// -- reverting recorded drift makes the record stale by construction, and
	// reverting nothing leaves it accurate -- so the invariant asserted here
	// is the one that holds either way, and it is the stronger of the two:
	// generating twice converges.
	regenerated, err := CheckDrift(context.Background(), Options{
		ModuleDir:  copyDir,
		TempParent: t.TempDir(),
		Report:     io.Discard,
	})
	if regenerated == nil {
		t.Fatalf("the second drift check produced no result: %v", err)
	}
	if regenerated.Drifted() {
		t.Fatal("a freshly regenerated tree still drifts from a fresh generation, so the generator is not deterministic")
	}
	if recorded > 0 && err == nil {
		t.Fatal("the record described drift that was just reverted, so the check should no longer match it")
	}
	if recorded == 0 && err != nil {
		t.Fatalf("nothing was reverted, so the record should still match the tree: %v", err)
	}
	t.Logf("CELL-OUTPUT: accepted: regenerating twice converges; the record %s",
		map[bool]string{true: "is now stale, as it must be", false: "still matches"}[recorded > 0])
}

// repoDigests digests a whole checkout through a root handle.
func repoDigests(t *testing.T, dir string) map[string]string {
	t.Helper()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	defer root.Close()
	snap, err := TakeSnapshot(root, skipVCS)
	if err != nil {
		t.Fatalf("snapshot %s: %v", dir, err)
	}
	out := make(map[string]string, len(snap))
	for rel, e := range snap {
		out[rel] = e.Digest
	}
	return out
}

// TestCheckDriftOnThisRepositoryIgnoresAnInheritedWorkspace is the pairwise
// cell for GOWORK against the private copy. The generator child inherits the
// guard's environment; with a workspace naming the REAL module (and another),
// the child's package loading resolved against the workspace instead of the
// copy it runs in, and the generation silently changed -- executed before the
// fix: schema.resolvers.go came out with its module imports pruned, so
// check-drift refused on a record mismatch and `generate` would have written
// that output into the tree. The child now runs with GOWORK=off.
func TestCheckDriftOnThisRepositoryIgnoresAnInheritedWorkspace(t *testing.T) {
	goAvailable(t)
	root := repoRoot(t)
	other := t.TempDir()
	if err := os.WriteFile(filepath.Join(other, "go.mod"), []byte("module example.com/other\n\ngo 1.25\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	work := filepath.Join(t.TempDir(), "go.work")
	if err := os.WriteFile(work, []byte("go 1.27.0\n\nuse (\n\t"+other+"\n\t"+root+"\n)\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOWORK", work)

	before := repoDigests(t, root)
	var report strings.Builder
	_, err := CheckDrift(context.Background(), Options{ModuleDir: root, TempParent: t.TempDir(), Report: &report})
	if err != nil {
		t.Fatalf("with GOWORK set, the drift check no longer matches the committed record: %v", err)
	}
	if !strings.Contains(report.String(), "GOWORK=off") {
		t.Fatalf("the report does not say the inherited workspace was switched off:\n%s", report.String())
	}
	assertUnchanged(t, before, repoDigests(t, root), "CheckDrift under an inherited GOWORK")
}

// firstLine is the head of a multi-line report, for a cell's output line.
func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
