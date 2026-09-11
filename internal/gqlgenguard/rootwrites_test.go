package gqlgenguard

import (
	"context"
	"os"
	"path/filepath"
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
