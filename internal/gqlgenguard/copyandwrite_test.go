package gqlgenguard

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// mkfifo makes a named pipe at p: the commonest non-regular file to find in a
// working tree, left there by a dev server.
func mkfifo(t *testing.T, p string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(p, 0o644); err != nil {
		t.Skipf("this platform cannot create a FIFO: %v", err)
	}
}

// TestANonRegularFileOnlyMattersWhereTheConfigNamesIt: a socket or FIFO left in
// the checkout by something unrelated must not fail a gate that has nothing to
// do with it -- it is skipped, named, and stood in for by the same inert
// self-loop a dropped link gets. One the configuration DECLARES is a refusal:
// the generator cannot read or write it.
func TestANonRegularFileOnlyMattersWhereTheConfigNamesIt(t *testing.T) {
	cells := []struct {
		shape   string
		at      string
		wantErr string
	}{
		{"G24-01 a FIFO in an unrelated directory: skipped and named", "unrelated/devserver.sock", ""},
		{"G24-02 a FIFO beside a declared output: skipped and named", "gen/devserver.sock", ""},
		{"G24-03 a FIFO AS the schema: refused", "schema.graphql", "not a regular file"},
		{"G24-04 a FIFO AS a declared output: refused", "gen/generated.go", "not a regular file"},
		{"G24-05 a FIFO AS the config: refused", "gqlgen.yml", "not a regular file"},
	}
	for _, c := range cells {
		t.Run(c.shape, func(t *testing.T) {
			f := guardFixture(t).withModuleFiles()
			if c.wantErr != "" {
				if err := os.Remove(filepath.Join(f.dir, filepath.FromSlash(c.at))); err != nil {
					t.Fatal(err)
				}
			}
			mkfifo(t, filepath.Join(f.dir, filepath.FromSlash(c.at)))
			opts := guardOptions(f, &fakeGenerator{fn: rewriteOutputs("generated")})
			var report strings.Builder
			opts.Report = &report
			_, err := CheckDrift(context.Background(), opts)
			logCell(t, err, "accepted")
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("want a refusal naming the non-regular file, got %v\n%s", err, report.String())
				}
				if !strings.Contains(err.Error(), c.at) {
					t.Fatalf("the refusal does not name %s: %v", c.at, err)
				}
				return
			}
			if !strings.Contains(report.String(), "skipped "+c.at+" ") {
				t.Fatalf("the run did not name the skipped entry %s:\n%s", c.at, report.String())
			}
			// The run proceeded: the only refusal left is the missing record.
			if err == nil || !strings.Contains(err.Error(), "no expected-drift record") {
				t.Fatalf("an unrelated non-regular file changed the verdict: %v\n%s", err, report.String())
			}
		})
	}
}

// TestTheSnapshotRefusesANonRegularFileTheGeneratorCreated: the copy is
// snapshotted after generating, and a generator that creates a socket or FIFO
// there has produced something no digest describes. The copy's own entries were
// filtered on the way in, so this is the only route left.
func TestTheSnapshotRefusesANonRegularFileTheGeneratorCreated(t *testing.T) {
	f := guardFixture(t).withModuleFiles()
	opts := guardOptions(f, &fakeGenerator{fn: func(workDir string) error {
		return syscall.Mkfifo(filepath.Join(workDir, "gen", "leftover.sock"), 0o644)
	}})
	var report strings.Builder
	opts.Report = &report
	before := f.digests()
	_, err := CheckDrift(context.Background(), opts)
	logCell(t, err, "accepted")
	if err == nil || !strings.Contains(err.Error(), "refusing tree containing") {
		t.Fatalf("want the snapshot's refusal, got %v\n%s", err, report.String())
	}
	assertUnchanged(t, before, f.digests(), "a generator that created a FIFO in the copy")
}

// TestTheVersionControlDirectoryIsNeitherCopiedNorSnapshotted: .git is not part
// of the module for the generator's purposes, and copying it multiplies the
// cost of every run. The copy must not carry it and the snapshot must not
// digest it.
func TestTheVersionControlDirectoryIsNeitherCopiedNorSnapshotted(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	for _, p := range []string{".git/objects/ab/cdef", ".gitignore", "gen/keep.go"} {
		full := filepath.Join(src, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	srcRoot, err := os.OpenRoot(src)
	if err != nil {
		t.Fatal(err)
	}
	defer srcRoot.Close()
	dstRoot, err := os.OpenRoot(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer dstRoot.Close()
	if _, _, err := CopyTree(context.Background(), srcRoot, dstRoot, skipVCS); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(dst, ".git")); err == nil {
		t.Fatal(".git was copied into the private copy")
	}
	for _, want := range []string{".gitignore", "gen/keep.go"} {
		if _, err := os.Lstat(filepath.Join(dst, filepath.FromSlash(want))); err != nil {
			t.Fatalf("%s was not copied: %v", want, err)
		}
	}
	snap, err := TakeSnapshot(srcRoot, skipVCS)
	if err != nil {
		t.Fatal(err)
	}
	for rel := range snap {
		if rel == ".git" || strings.HasPrefix(rel, ".git/") {
			t.Fatalf("the snapshot digested %s", rel)
		}
	}
	if _, ok := snap[".gitignore"]; !ok {
		t.Fatal("the snapshot skipped .gitignore, which is not the version-control directory")
	}
	t.Logf("CELL-OUTPUT: accepted: copy and snapshot both carry %d entries, none under .git", len(snap))
}

// TestAFailedRenameIsAFailedWriteAndLeavesNoTemporary: AtomicWrite's whole
// contract is that a reader sees the old file or the new one. If the rename
// fails and the error is dropped, the caller is told the write succeeded while
// nothing moved -- and the temporary stays in the tree.
func TestAFailedRenameIsAFailedWriteAndLeavesNoTemporary(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	// A non-empty directory cannot be replaced by a file.
	if err := os.MkdirAll(filepath.Join(dir, "occupied", "child"), 0o755); err != nil {
		t.Fatal(err)
	}
	err = AtomicWrite(root, "occupied", []byte("new"), 0o644)
	logCell(t, err, "accepted")
	if err == nil {
		t.Fatal("AtomicWrite reported success although the rename could not happen")
	}
	if !strings.Contains(err.Error(), `replace "occupied"`) {
		t.Fatalf("the error does not name the replacement that failed: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".gqlgen-guard-") {
			t.Fatalf("the failed write left its temporary %s behind", e.Name())
		}
	}
}

// TestCopyingBackKeepsTheModeTheFileHas: regenerating is not a permission
// change. The mode comes from the file being copied back -- the copy carries
// the tree's own -- so an executable output stays executable.
func TestCopyingBackKeepsTheModeTheFileHas(t *testing.T) {
	// A mode the ordinary umask trims (0666 -> 0644 under 022), pinned so the
	// cell does not pass because of whichever umask the runner happened to
	// have: creating the replacement with the right mode is not enough, it has
	// to be SET.
	old := syscall.Umask(0o022)
	t.Cleanup(func() { syscall.Umask(old) })

	f := guardFixture(t).withModuleFiles()
	target := filepath.Join(f.dir, "gen", "generated.go")
	if err := os.Chmod(target, 0o666); err != nil {
		t.Fatal(err)
	}
	opts := writeOptions(f, &fakeGenerator{fn: rewriteOutputs("generated")})
	var report strings.Builder
	opts.Report = &report
	res, err := Generate(context.Background(), opts)
	if err != nil {
		t.Fatalf("generate: %v\n%s", err, report.String())
	}
	if len(res.Applied) == 0 {
		t.Fatal("nothing was applied, so the cell proves nothing")
	}
	info, err := os.Lstat(target)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o666 {
		t.Fatalf("the regenerated output's mode is %v, want 0666 (umask 022 would have made it 0644)", info.Mode().Perm())
	}
	t.Logf("CELL-OUTPUT: accepted: %s kept mode %v across a regeneration, under umask 022", "gen/generated.go", info.Mode().Perm())
}

// TestAKilledRunsPrivateCopyIsReportedAndRemovedByTheNextRun: a SIGKILL is the
// one way a run cannot clean up after itself, and what it leaves is a whole
// copy of the working tree. The next run says so and removes it -- but only
// directories carrying this guard's own marker whose process is gone.
func TestAKilledRunsPrivateCopyIsReportedAndRemovedByTheNextRun(t *testing.T) {
	f := guardFixture(t).withModuleFiles()
	opts := guardOptions(f, &fakeGenerator{fn: rewriteOutputs("generated")})
	parent := opts.TempParent

	// A dead run's leftovers, a live run's, and something that is not ours.
	stale := filepath.Join(parent, "gqlgen-guard-deadbeef0001")
	live := filepath.Join(parent, "gqlgen-guard-deadbeef0002")
	foreign := filepath.Join(parent, "gqlgen-guard-deadbeef0003")
	for _, d := range []string{stale, live, foreign} {
		if err := os.MkdirAll(filepath.Join(d, "payload"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// A pid that cannot be running: the kernel refuses to allocate 0, and a
	// process group leader's negative id is never a pid.
	if err := os.WriteFile(filepath.Join(parent, ownerMarkerOf(filepath.Base(stale))), []byte("2147483646\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parent, ownerMarkerOf(filepath.Base(live))), fmt.Appendf(nil, "%d\n", os.Getpid()), 0o600); err != nil {
		t.Fatal(err)
	}
	if processAlive(2147483646) {
		t.Skip("pid 2147483646 is running on this host, so it is not a dead run")
	}

	var report strings.Builder
	opts.Report = &report
	_, _ = CheckDrift(context.Background(), opts)
	logCell(t, nil, firstLine(report.String()))

	if _, err := os.Stat(stale); err == nil {
		t.Fatalf("the dead run's private copy %s was not removed", stale)
	}
	if !strings.Contains(report.String(), "removed the private copy pid 2147483646 left behind") {
		t.Fatalf("the run did not report the copy it removed:\n%s", report.String())
	}
	for _, keep := range []string{live, foreign} {
		if _, err := os.Stat(keep); err != nil {
			t.Fatalf("%s was removed although it is not a dead run's: %v", keep, err)
		}
	}
	// The marker is beside the copy, never in it: the copy the generator sees
	// must be byte-identical to the module.
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), "gqlgen-guard-") {
			inside, rerr := os.ReadDir(filepath.Join(parent, e.Name()))
			if rerr != nil {
				continue
			}
			for _, f := range inside {
				if strings.Contains(f.Name(), ".owner") {
					t.Fatalf("the owner marker was written inside %s", e.Name())
				}
			}
		}
	}
}

// TestARunLeavesNothingBesideItsPrivateCopyEither: the owner marker is a file
// in the operator's temporary directory. A run that removed its copy and left
// the marker would leak one small file per run for ever, and nothing would
// ever look at it again -- so removal takes both, and a marker whose directory
// is already gone is swept.
func TestARunLeavesNothingBesideItsPrivateCopyEither(t *testing.T) {
	f := guardFixture(t).withModuleFiles()
	opts := guardOptions(f, &fakeGenerator{fn: rewriteOutputs("generated")})
	parent := opts.TempParent

	// An orphan from an earlier run: the marker with no directory.
	orphan := ownerMarkerOf("gqlgen-guard-0badc0de0001")
	if err := os.WriteFile(filepath.Join(parent, orphan), []byte("2147483646\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var report strings.Builder
	opts.Report = &report
	_, _ = CheckDrift(context.Background(), opts)
	logCell(t, nil, firstLine(report.String()))

	if _, err := os.Stat(filepath.Join(parent, orphan)); err == nil {
		t.Fatalf("the orphan marker %s was not swept", orphan)
	}
	if !strings.Contains(report.String(), "already gone") {
		t.Fatalf("the run did not report the orphan marker it removed:\n%s", report.String())
	}
	left, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range left {
		if strings.Contains(e.Name(), "gqlgen-guard-") {
			t.Fatalf("the run left %s behind in %s", e.Name(), parent)
		}
	}
	t.Logf("CELL-OUTPUT: accepted: %d entries left in the temporary directory", len(left))
}

// TestACopyThatCannotBeRemovedIsReported: the removal used to discard its
// error, so a copy of the whole working tree could stay in the temporary
// directory with nothing in the output saying so.
func TestACopyThatCannotBeRemovedIsReported(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores the directory permissions this cell depends on")
	}
	f := guardFixture(t).withModuleFiles()
	var locked string
	opts := guardOptions(f, &fakeGenerator{fn: func(workDir string) error {
		if err := rewriteOutputs("generated")(workDir); err != nil {
			return err
		}
		// A directory inside the copy that cannot be emptied.
		locked = filepath.Join(workDir, "gen", "model")
		return os.Chmod(locked, 0o500)
	}})
	var report strings.Builder
	opts.Report = &report
	_, _ = CheckDrift(context.Background(), opts)
	if locked != "" {
		_ = os.Chmod(locked, 0o755)
	}
	logCell(t, nil, firstLine(report.String()))
	if !strings.Contains(report.String(), "could not be removed") {
		t.Fatalf("a private copy that could not be removed was not reported:\n%s", report.String())
	}
}

// TestGlobMetaIsExactlyWhatMatchTreatsAsSyntax: the literal prefix of a schema
// pattern is everything before its first pattern character, and the confinement
// decision is made about that prefix. A character filepath.Match treats as
// syntax but this constant omits would be resolved BY NAME instead, which is
// not where the glob will look.
func TestGlobMetaIsExactlyWhatMatchTreatsAsSyntax(t *testing.T) {
	if filepath.Separator != '/' {
		t.Skip("filepath.Match disables escaping where the separator is a backslash")
	}
	// A character is pattern syntax when a name containing it, used AS a
	// pattern, behaves differently from that name: it either matches some
	// other string, or fails to match itself, or is not a valid pattern.
	syntax := func(c string) bool {
		pat := "a" + c + "b"
		self, selfErr := filepath.Match(pat, pat)
		other, _ := filepath.Match(pat, "aQb")
		return selfErr != nil || !self || other
	}
	for _, c := range []string{"*", "?", "[", `\`} {
		if !syntax(c) {
			t.Errorf("%q is not pattern syntax after all", c)
		}
		if !strings.Contains(globMeta, c) {
			t.Errorf("globMeta %q omits %q, which filepath.Match treats as syntax", globMeta, c)
		}
	}
	for _, c := range []string{"-", ".", "_", "+", "@", "=", "]", "{", "}"} {
		if syntax(c) {
			t.Errorf("%q is pattern syntax after all", c)
		}
		if strings.Contains(globMeta, c) {
			t.Errorf("globMeta %q lists %q, which filepath.Match treats as a literal", globMeta, c)
		}
	}
	t.Logf("CELL-OUTPUT: accepted: globMeta = %q", globMeta)
}

var _ = time.Second
