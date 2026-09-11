package gqlgenguard

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TestEveryWriteGoesThroughARoot creates each escape AFTER the check that
// would have refused it has passed (testStageHook), so the only thing between
// the escape and a write into the working tree is the os.Root the write goes
// through. W1: the copy's parent swapped for a link into the module after its
// pre-check. W2: the generator's scratch HOME planted as a link into the module
// before the guard writes the telemetry mode file there. W3/W4: a directory or
// file in the private copy planted as a link into the module before the copy
// is written.
func TestEveryWriteGoesThroughARoot(t *testing.T) {
	cells := []struct {
		shape   string
		stage   string
		escape  func(t *testing.T, f *fixture, dir string)
		wantErr string
		// absent is a module path that must not exist afterwards: the digest
		// comparison sees files, and an escaped MkdirAll creates only
		// directories.
		absent string
	}{
		{"W1 the copy's parent replaced by a link into the module after its pre-check", "copy-parent-checked", func(t *testing.T, f *fixture, dir string) {
			if err := os.Rename(dir, dir+".moved"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(f.dir, "unrelated"), dir); err != nil {
				t.Fatal(err)
			}
		}, "physically inside the module", ""},
		{"W2 the scratch HOME planted as a link to a not-yet-existing directory in the module", "scratch-created", func(t *testing.T, f *fixture, dir string) {
			if err := os.Symlink(filepath.Join(f.dir, "unrelated", "home-target"), filepath.Join(dir, "home")); err != nil {
				t.Fatal(err)
			}
		}, "path escapes from parent", ""},
		{"W3 a directory of the copy planted as a link into the module before the copy is written", "copy-created", func(t *testing.T, f *fixture, dir string) {
			if err := os.Symlink(filepath.Join(f.dir, "gen"), filepath.Join(dir, "gen")); err != nil {
				t.Fatal(err)
			}
		}, "path escapes from parent", ""},
		{"W4 a file of the copy planted as a link into the module before the copy is written", "copy-created", func(t *testing.T, f *fixture, dir string) {
			if err := os.MkdirAll(filepath.Join(dir, "gen"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(f.dir, "gen", "generated.go"), filepath.Join(dir, "gen", "generated.go")); err != nil {
				t.Fatal(err)
			}
		}, "path escapes from parent", ""},
		{"W2b the scratch HOME's .config planted as a link into the module", "scratch-created", func(t *testing.T, f *fixture, dir string) {
			if err := os.MkdirAll(filepath.Join(dir, "home"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(f.dir, "unrelated"), filepath.Join(dir, "home", ".config")); err != nil {
				t.Fatal(err)
			}
		}, "path escapes from parent", "unrelated/go"},
		{"W2c the telemetry mode file planted as a link to a file in the module", "scratch-created", func(t *testing.T, f *fixture, dir string) {
			if err := os.MkdirAll(filepath.Join(dir, "home", ".config", "go", "telemetry"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(f.dir, "unrelated", "notes.md"), filepath.Join(dir, "home", ".config", "go", "telemetry", "mode")); err != nil {
				t.Fatal(err)
			}
		}, "path escapes from parent", ""},
		{"W5 the private copy's PATH swapped for a link to the module root after the copy was created", "copy-created", func(t *testing.T, f *fixture, dir string) {
			if err := os.Rename(dir, dir+".moved"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(f.dir, dir); err != nil {
				t.Fatal(err)
			}
		}, "no longer names the directory the guard created", ""},
		{"W6 the scratch directory's PATH swapped for a link into the module after it was created", "scratch-created", func(t *testing.T, f *fixture, dir string) {
			if err := os.Rename(dir, dir+".moved"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(f.dir, "unrelated"), dir); err != nil {
				t.Fatal(err)
			}
		}, "no longer names the directory the guard created", ""},
		{"W7 the private copy's PATH swapped for a link to the module root after every other check, before the generator", "before-generator", func(t *testing.T, f *fixture, dir string) {
			copyDir := strings.SplitN(dir, "\x00", 2)[0]
			if err := os.Rename(copyDir, copyDir+".moved"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(f.dir, copyDir); err != nil {
				t.Fatal(err)
			}
		}, "no longer names the directory the guard created", ""},
		{"W8 the scratch directory's PATH swapped for a link into the module before the generator", "before-generator", func(t *testing.T, f *fixture, dir string) {
			scratch := strings.SplitN(dir, "\x00", 2)[1]
			if err := os.Rename(scratch, scratch+".moved"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(f.dir, "unrelated"), scratch); err != nil {
				t.Fatal(err)
			}
		}, "no longer names the directory the guard created", ""},
	}
	for i, c := range cells {
		t.Run(cellID("G21", i)+" "+c.shape, func(t *testing.T) {
			f := guardFixture(t).withModuleFiles()
			opts := guardOptions(f, &fakeGenerator{fn: rewriteOutputs("generated")})
			if strings.Contains(f.dir, c.wantErr) {
				t.Fatalf("wantErr %q also occurs in the fixture path %s", c.wantErr, f.dir)
			}
			fired := false
			testStageHook = func(stage, dir string) {
				if stage == c.stage && !fired {
					fired = true
					c.escape(t, f, dir)
				}
			}
			t.Cleanup(func() { testStageHook = nil })
			before := f.digests()
			var report strings.Builder
			opts.Report = &report
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			_, err := Generate(ctx, opts)
			logCell(t, err, "accepted")
			if !fired {
				t.Fatalf("the %q stage never ran, so the cell proves nothing", c.stage)
			}
			// The tree first: an escape that wrote into it is the failure that
			// matters, whatever the guard then said.
			assertUnchanged(t, before, f.digests(), c.shape)
			if c.absent != "" && f.exists(c.absent) {
				t.Fatalf("%s was created in the module", c.absent)
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("want a refusal containing %q, got %v\n%s", c.wantErr, err, report.String())
			}
		})
	}
}

// TestTheParentsLocationIsReadFromTheOpenHandle: between opening the copy's
// parent and reading back where it is, the PATH it was opened by can be made
// to point somewhere else. Only an answer read from the open descriptor itself
// (/proc/self/fd) still describes the directory the guard actually holds --
// resolving the name again describes whatever the name now points at, and the
// in-module refusal is then made about the wrong directory.
//
// The two swaps are the whole point: the first makes the OPEN land on a
// directory inside the module, the second makes the NAME resolve outside it.
func TestTheParentsLocationIsReadFromTheOpenHandle(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the location is read from /proc/self/fd, which only Linux has")
	}
	f := guardFixture(t).withModuleFiles()
	outside, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	elsewhere, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	inModule := filepath.Join(f.dir, "unrelated")

	opts := guardOptions(f, &fakeGenerator{fn: rewriteOutputs("generated")})
	opts.TempParent = outside
	var report strings.Builder
	opts.Report = &report

	var opened, checked bool
	testStageHook = func(stage, dir string) {
		switch {
		case stage == "copy-parent-checked" && !checked:
			checked = true
			// The parent passed its check as a directory outside the module;
			// replace it with a link INTO the module, so the open below lands
			// there.
			if err := os.Rename(dir, dir+".moved"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(inModule, dir); err != nil {
				t.Fatal(err)
			}
		case stage == "copy-parent-opened" && !opened:
			opened = true
			// Now the directory is held. Point the PATH somewhere harmless, so
			// only a location read from the handle still says "in the module".
			if err := os.Remove(dir); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(elsewhere, dir); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Cleanup(func() { testStageHook = nil })

	before := f.digests()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err = Generate(ctx, opts)
	logCell(t, err, "accepted")
	if !checked || !opened {
		t.Fatalf("the stages never ran (checked=%v opened=%v), so the cell proves nothing", checked, opened)
	}
	assertUnchanged(t, before, f.digests(), "the parent's path repointed after it was opened")
	if entries, rerr := os.ReadDir(inModule); rerr == nil {
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), "gqlgen-guard-") {
				t.Fatalf("a private copy was created inside the module at %s", filepath.Join(inModule, e.Name()))
			}
		}
	}
	if err == nil || !strings.Contains(err.Error(), "physically inside the module") {
		t.Fatalf("want a refusal naming the in-module parent, got %v\n%s", err, report.String())
	}
}
