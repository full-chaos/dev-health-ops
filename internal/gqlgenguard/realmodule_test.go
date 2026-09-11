package gqlgenguard

import (
	"context"
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
	if !res.Drifted() {
		t.Fatal("this repository's generated files carry deliberate hand-edits, so a fresh generation MUST differ from them; no drift means the generator did not really run")
	}
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

	// On this repository the record describes the checked-in hand-edits, so a
	// plain `generate` must refuse and write nothing rather than reverting
	// them silently.
	var refusal strings.Builder
	before, err := TakeSnapshot(dst, skipVCS)
	if err != nil {
		t.Fatalf("snapshot before the refused generate: %v", err)
	}
	_, err = Generate(context.Background(), Options{
		ModuleDir:  copyDir,
		TempParent: t.TempDir(),
		Report:     &refusal,
	})
	if err == nil || !strings.Contains(err.Error(), "records as deliberate") {
		t.Fatalf("generate did not refuse to revert the recorded hand-edits: %v", err)
	}
	t.Logf("CELL-OUTPUT: refused: %v", err)
	afterRefusal, err := TakeSnapshot(dst, skipVCS)
	if err != nil {
		t.Fatalf("snapshot after the refused generate: %v", err)
	}
	for rel, e := range before {
		if afterRefusal[rel].Digest != e.Digest {
			t.Fatalf("the refused generate changed %s", rel)
		}
	}
	if len(afterRefusal) != len(before) {
		t.Fatalf("the refused generate changed the file set: %d -> %d", len(before), len(afterRefusal))
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
	if !strings.Contains(applied.String(), "reverting 3 recorded hand-edit(s)") {
		t.Fatalf("the applied run did not name the hand-edits it reverted:\n%s", applied.String())
	}
	t.Logf("CELL-OUTPUT: accepted under -revert-recorded: %s", firstLine(applied.String()))
	if len(res.Applied) == 0 {
		t.Fatal("generate applied nothing, so this proves nothing about the write path")
	}

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

	// A second check against the now-regenerated copy must report NO drift and
	// therefore refuse against the committed record, which is the honest
	// consequence of having reverted every hand-edit.
	regenerated, err := CheckDrift(context.Background(), Options{
		ModuleDir:  copyDir,
		TempParent: t.TempDir(),
		Report:     io.Discard,
	})
	if err == nil {
		t.Fatal("after regenerating over every hand-edit, the drift check still matched the record; the record cannot be describing the hand-edits")
	}
	if regenerated != nil && regenerated.Drifted() {
		t.Fatal("a freshly regenerated tree still drifts from a fresh generation, so the generator is not deterministic")
	}
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
