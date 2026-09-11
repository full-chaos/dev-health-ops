package gqlgenguard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The tests in this file run the REAL gqlgen CLI, because the property they are
// about is what an ordinary child process can reach. A fake generator is
// in-process Go that the test wrote; it proves nothing about what `go run
// github.com/99designs/gqlgen` will open.

// TestTheRealGeneratorCannotReadASchemaOutsideTheModuleThroughALink is round
// 1's reproduction, kept verbatim in shape: a schema file symlinked from outside
// the module, named by the config. Before the fix the link was reproduced in
// the copy, the child followed it, and `leakedFromOutside` landed in the
// generated output in the working tree.
func TestTheRealGeneratorCannotReadASchemaOutsideTheModuleThroughALink(t *testing.T) {
	goAvailable(t)
	f := guardFixture(t).withModuleFiles()
	outside := t.TempDir()
	secretPath := filepath.Join(outside, "secret.graphql")
	if err := os.WriteFile(secretPath, []byte("type Query { leakedFromOutside: String! }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(secretPath, filepath.Join(f.dir, "schema-link.graphql")); err != nil {
		t.Fatal(err)
	}
	f.write("gqlgen.yml", strings.Replace(guardConfig, "schema: [schema.graphql]", "schema: [schema-link.graphql]", 1))
	before := f.digests()

	var report strings.Builder
	opts := guardOptions(f, nil)
	opts.Report = &report
	res, err := Generate(context.Background(), opts)

	got := ""
	if data, readErr := os.ReadFile(filepath.Join(f.dir, "gen", "generated.go")); readErr == nil {
		got = string(data)
	}
	var applied []string
	if res != nil {
		applied = res.Applied
	}
	t.Logf("Generate result=%v applied=%v generated-contains-outside-schema=%v", err, applied, strings.Contains(got, "leakedFromOutside"))

	if strings.Contains(got, "leakedFromOutside") {
		t.Fatal("the generator read a schema outside the module through a symbolic link and it reached the working tree")
	}
	if err == nil {
		t.Fatal("the guard accepted a generation whose only schema lives outside the module")
	}
	if !strings.Contains(report.String(), "dropped symbolic link schema-link.graphql (absolute target)") {
		t.Fatalf("the refusal's cause -- the dropped link -- was not reported; report:\n%s", report.String())
	}
	assertUnchanged(t, before, f.digests(), "a refused generation over an outside schema")
}

// TestTheRealGeneratorCannotWriteThroughAnEscapingDirectoryLink is round 1's
// second adversarial probe: an output directory that is a link out of the
// module. It was already refused before the fix -- the root handle refuses the
// Lstat -- and this pins that it still is, with the outside directory empty.
func TestTheRealGeneratorCannotWriteThroughAnEscapingDirectoryLink(t *testing.T) {
	goAvailable(t)
	f := guardFixture(t).withModuleFiles()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(f.dir, "link")); err != nil {
		t.Fatal(err)
	}
	f.write("gqlgen.yml", strings.Replace(guardConfig,
		"exec: {filename: gen/generated.go, package: gen}",
		"exec: {filename: link/generated.go, package: gen}", 1))
	before := f.digests()

	_, err := Generate(context.Background(), guardOptions(f, nil))
	if err == nil {
		t.Fatal("the guard accepted an output path that passes through a link out of the module")
	}
	entries, readErr := os.ReadDir(outside)
	if readErr != nil {
		t.Fatalf("read the outside directory: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("something was written outside the module: %v", entries)
	}
	assertUnchanged(t, before, f.digests(), "a refused generation through an escaping directory link")
}

// TestAVirtualEnvironmentShapedCheckoutStillGenerates is the usability half of
// the link policy, executed with the real CLI: a tree carrying the links a
// Python virtual environment carries -- an absolute interpreter link and a
// relative in-tree directory link -- still generates, the absolute link is
// dropped and named, and the relative one reaches the copy.
func TestAVirtualEnvironmentShapedCheckoutStillGenerates(t *testing.T) {
	goAvailable(t)
	f := guardFixture(t).withModuleFiles()
	interpreter := filepath.Join(t.TempDir(), "python3")
	if err := os.WriteFile(interpreter, []byte("#!/bin/false\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	f.write(".venv/lib/python3/site.py", "# in the module\n")
	if err := os.MkdirAll(filepath.Join(f.dir, ".venv", "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(interpreter, filepath.Join(f.dir, ".venv", "bin", "python")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("lib", filepath.Join(f.dir, ".venv", "lib64")); err != nil {
		t.Fatal(err)
	}

	var report strings.Builder
	opts := guardOptions(f, nil)
	opts.Report = &report
	res, err := Generate(context.Background(), opts)
	if err != nil {
		t.Fatalf("a virtual-environment-shaped checkout was refused: %v\nreport:\n%s", err, report.String())
	}
	if len(res.Applied) == 0 {
		t.Fatal("the real generator changed nothing, so this proves nothing about a successful run")
	}
	if !strings.Contains(report.String(), "dropped symbolic link .venv/bin/python (absolute target)") {
		t.Fatalf("the absolute interpreter link was not reported as dropped; report:\n%s", report.String())
	}
	if strings.Contains(report.String(), "dropped symbolic link .venv/lib64") {
		t.Fatalf("the in-tree relative link was dropped; report:\n%s", report.String())
	}
	want := []DroppedLink{{Path: ".venv/bin/python", Reason: droppedAbsolute}}
	if len(res.DroppedLinks) != len(want) || res.DroppedLinks[0] != want[0] {
		t.Fatalf("Result.DroppedLinks = %v, want %v", res.DroppedLinks, want)
	}
}

// TestSchemaInputConfinementInputDomain is the input-domain table for the
// schema half of the link policy: every way a configured schema can sit behind
// a link out of the module, and the one way a configuration can name no schema
// at all. Each row must REFUSE before the generator runs and name its cause,
// because gqlgen's own globbing swallows the errors that would otherwise
// explain a schema that silently went missing.
func TestSchemaInputConfinementInputDomain(t *testing.T) {
	type setup func(t *testing.T, f *fixture, outside string)
	linkOutsideDir := func(name string) setup {
		return func(t *testing.T, f *fixture, outside string) {
			if err := os.WriteFile(filepath.Join(outside, "schema.graphql"), []byte(fixtureSchema), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, filepath.Join(f.dir, name)); err != nil {
				t.Fatal(err)
			}
		}
	}
	cases := []struct {
		shape   string
		schema  string
		setup   setup
		wantErr string
	}{
		{shape: "canonical (a schema file inside the module)", schema: "schema.graphql"},
		{shape: "a literal schema path THROUGH a directory link out of the module", schema: "outdir/schema.graphql",
			setup: linkOutsideDir("outdir"), wantErr: `schema pattern "outdir/schema.graphql" cannot be resolved: directory "outdir"`},
		{shape: "a glob THROUGH a directory link out of the module", schema: "outdir/*.graphql",
			setup: linkOutsideDir("outdir"), wantErr: `schema pattern "outdir/*.graphql" cannot be resolved: directory "outdir"`},
		{shape: "a ** walk rooted AT a directory link out of the module", schema: "outdir/**/*.graphql",
			setup: linkOutsideDir("outdir"), wantErr: `schema pattern "outdir/**/*.graphql" cannot be resolved: directory "outdir"`},
		{shape: "a WILDCARD directory component matching a link out of the module, beside a real schema", schema: `schema.graphql, "out?ir/*.graphql"`,
			setup: linkOutsideDir("outdir"), wantErr: `schema pattern "out?ir/*.graphql" cannot be resolved: directory "outdir"`},
		{shape: "a literal schema path through a directory link out of the module at the SECOND level", schema: "sub/outdir/schema.graphql",
			setup: func(t *testing.T, f *fixture, outside string) {
				f.write("sub/keep.txt", "in the module\n")
				linkOutsideDir("sub/outdir")(t, f, outside)
			}, wantErr: `cannot be resolved: directory "sub/outdir"`},
		{shape: "a literal schema path that climbs BACK through a dropped link", schema: "outdir/../schema.graphql",
			setup: linkOutsideDir("outdir"), wantErr: `cannot be resolved: directory "outdir"`},
		{shape: "a glob matching one in-module schema AND one leaf link out of the module", schema: "sch/*.graphql",
			setup: func(t *testing.T, f *fixture, outside string) {
				f.write("sch/a.graphql", fixtureSchema)
				secret := filepath.Join(outside, "b.graphql")
				if err := os.WriteFile(secret, []byte("extend type Query { leakedFromOutside: String }\n"), 0o644); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(secret, filepath.Join(f.dir, "sch", "b.graphql")); err != nil {
					t.Fatal(err)
				}
			}, wantErr: "too many levels of symbolic links"},
		{shape: "a schema file that IS a link out of the module (round 1's shape)", schema: "schema-link.graphql",
			setup: func(t *testing.T, f *fixture, outside string) {
				secret := filepath.Join(outside, "secret.graphql")
				if err := os.WriteFile(secret, []byte("type Query { leakedFromOutside: String! }\n"), 0o644); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(secret, filepath.Join(f.dir, "schema-link.graphql")); err != nil {
					t.Fatal(err)
				}
			}, wantErr: "too many levels of symbolic links"},
		{shape: "a schema path through a REGULAR FILE (not a directory)", schema: "schema.graphql/x.graphql",
			wantErr: `"schema.graphql" is not a directory`},
		{shape: "a schema path that climbs OUT of the module with ../ (gqlgen would read it before any output check)", schema: "../outside-schema.graphql",
			setup: func(t *testing.T, f *fixture, outside string) {
				if err := os.WriteFile(filepath.Join(filepath.Dir(f.dir), "outside-schema.graphql"), []byte(fixtureSchema), 0o644); err != nil {
					t.Fatal(err)
				}
			}, wantErr: `schema pattern "../outside-schema.graphql" leaves the module`},
		{shape: "a glob whose literal directory climbs OUT of the module", schema: "../*.graphql",
			wantErr: `schema pattern "../*.graphql" leaves the module`},
		{shape: "a ../ that stays INSIDE the module (canonical-equivalent)", schema: "gen/../schema.graphql"},
		{shape: "zero (the only pattern matches no file)", schema: "missing.graphql",
			wantErr: "match no file"},
		{shape: "zero (a glob that matches no file)", schema: "nothing/*.graphql",
			wantErr: "match no file"},
		{shape: "an in-module directory link (reproduced, so it resolves in the copy)", schema: "inlink/schema.graphql",
			setup: func(t *testing.T, f *fixture, outside string) {
				f.write("real/schema.graphql", fixtureSchema)
				if err := os.Symlink("real", filepath.Join(f.dir, "inlink")); err != nil {
					t.Fatal(err)
				}
			}},
	}
	for i, tc := range cases {
		t.Run(cellID("G4", i)+" "+tc.shape, func(t *testing.T) {
			f := guardFixture(t)
			outside := t.TempDir()
			if tc.setup != nil {
				tc.setup(t, f, outside)
			}
			f.write("gqlgen.yml", strings.Replace(guardConfig, "schema: [schema.graphql]", "schema: ["+tc.schema+"]", 1))
			before := f.digests()

			gen := &fakeGenerator{fn: rewriteOutputs("generated")}
			var report strings.Builder
			opts := guardOptions(f, gen)
			opts.Report = &report
			res, err := UpdateDriftRecord(context.Background(), opts)
			accepted := ""
			if res != nil {
				accepted = fmt.Sprintf("schemas %v", res.Plan.Schemas)
			}
			logCell(t, err, accepted)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("cell REFUSED but its contract says accept: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("cell ACCEPTED but its contract says refuse (%q); report:\n%s", tc.wantErr, report.String())
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("refused for the wrong reason:\n  got  %v\n  want it to contain %q", err, tc.wantErr)
			}
			if gen.ran {
				t.Fatal("the generator ran over a configuration whose schema input was already a refusal")
			}
			assertUnchanged(t, before, f.digests(), tc.shape)
		})
	}
}

// TestThePrivateCopyIsNeverMadeInsideTheModule is the pairwise cell for the two
// inputs that name a location: the module root and the directory the private
// copy is made under (Options.TempParent, or TMPDIR when that is empty). When
// the second is inside the first, copying the module copies the copy into
// itself. Executed on the real binary before the fix: 91 nested levels and
// 11 GB in nine minutes, and the SIGTERM `timeout` sent was caught and ignored
// because the copy walk never looked at the context.
func TestThePrivateCopyIsNeverMadeInsideTheModule(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts func(f *fixture) Options
	}{
		{"TempParent inside the module", func(f *fixture) Options {
			o := guardOptions(f, nil)
			o.TempParent = filepath.Join(f.dir, "tmpx")
			return o
		}},
		{"TempParent IS the module root", func(f *fixture) Options {
			o := guardOptions(f, nil)
			o.TempParent = f.dir
			return o
		}},
		{"TempParent is a link OUTSIDE the module that resolves INSIDE it", func(f *fixture) Options {
			alias := filepath.Join(f.t.TempDir(), "alias")
			if err := os.Symlink(filepath.Join(f.dir, "tmpx"), alias); err != nil {
				f.t.Fatal(err)
			}
			o := guardOptions(f, nil)
			o.TempParent = alias
			return o
		}},
		{"TMPDIR inside the module, TempParent empty", func(f *fixture) Options {
			t.Setenv("TMPDIR", filepath.Join(f.dir, "tmpx"))
			o := guardOptions(f, nil)
			o.TempParent = ""
			return o
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := guardFixture(t)
			if err := os.MkdirAll(filepath.Join(f.dir, "tmpx"), 0o755); err != nil {
				t.Fatal(err)
			}
			before := f.digests()
			gen := &fakeGenerator{fn: rewriteOutputs("generated")}
			opts := tc.opts(f)
			opts.Generator = gen
			_, err := Generate(context.Background(), opts)
			if err == nil || !strings.Contains(err.Error(), "inside the module") {
				t.Fatalf("want a refusal naming the private copy inside the module, got %v", err)
			}
			if gen.ran {
				t.Fatal("the generator ran")
			}
			entries, _ := os.ReadDir(filepath.Join(f.dir, "tmpx"))
			if len(entries) != 0 {
				t.Fatalf("something was created in the in-module temp directory: %v", entries)
			}
			assertUnchanged(t, before, f.digests(), tc.name)
		})
	}
}

// TestCopyTreeStopsWhenCancelled pins that the copy walk observes its context:
// a cancelled context returns the cancellation and copies nothing, so a signal
// that arrives while a large module is being copied ends the run instead of
// being caught and then ignored until the walk finishes.
func TestCopyTreeStopsWhenCancelled(t *testing.T) {
	f := guardFixture(t)
	src, err := os.OpenRoot(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	dstDir := t.TempDir()
	dst, err := os.OpenRoot(dstDir)
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := CopyTree(ctx, src, dst, skipVCS); !errors.Is(err, context.Canceled) {
		t.Fatalf("CopyTree with a cancelled context returned %v, want context.Canceled", err)
	}
	entries, _ := os.ReadDir(dstDir)
	if len(entries) != 0 {
		t.Fatalf("CopyTree copied %d entries after cancellation", len(entries))
	}
}

// TestTheGuardsOwnInputFilesAreNeverReadThroughALinkOutOfTheModule is the
// sibling sweep of the link policy over the two files the GUARD reads by name:
// the gqlgen config (read from the private copy, by gqlgen) and the
// expected-drift record (read from the tree, through the module's root handle).
// A link out of the module is refused for both; an in-module link works for
// both.
func TestTheGuardsOwnInputFilesAreNeverReadThroughALinkOutOfTheModule(t *testing.T) {
	cases := []struct {
		name    string
		setup   func(t *testing.T, f *fixture, outside string)
		wantErr string
	}{
		{"the config is a link out of the module", func(t *testing.T, f *fixture, outside string) {
			cfg := filepath.Join(outside, "gqlgen.yml")
			if err := os.WriteFile(cfg, []byte(guardConfig), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(f.dir, "gqlgen.yml")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(cfg, filepath.Join(f.dir, "gqlgen.yml")); err != nil {
				t.Fatal(err)
			}
		}, "too many levels of symbolic links"},
		{"the config is an in-module link", func(t *testing.T, f *fixture, outside string) {
			f.write("conf/real.yml", guardConfig)
			if err := os.Remove(filepath.Join(f.dir, "gqlgen.yml")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("conf/real.yml", filepath.Join(f.dir, "gqlgen.yml")); err != nil {
				t.Fatal(err)
			}
		}, ""},
		{"the expected-drift record is a link out of the module", func(t *testing.T, f *fixture, outside string) {
			rec := filepath.Join(outside, "expected-drift.record")
			if err := os.WriteFile(rec, []byte(f.read("contracts/expected-drift.record")), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(f.dir, "contracts", "expected-drift.record")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(rec, filepath.Join(f.dir, "contracts", "expected-drift.record")); err != nil {
				t.Fatal(err)
			}
		}, "path escapes from parent"},
		{"the expected-drift record is an in-module link", func(t *testing.T, f *fixture, outside string) {
			f.write("contracts/real.record", f.read("contracts/expected-drift.record"))
			if err := os.Remove(filepath.Join(f.dir, "contracts", "expected-drift.record")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("real.record", filepath.Join(f.dir, "contracts", "expected-drift.record")); err != nil {
				t.Fatal(err)
			}
		}, ""},
	}
	for i, tc := range cases {
		t.Run(cellID("G7", i)+" "+tc.name, func(t *testing.T) {
			f := guardFixture(t)
			// Every case starts from a committed, matching record.
			if _, err := UpdateDriftRecord(context.Background(), guardOptions(f, &fakeGenerator{fn: rewriteOutputs("generated")})); err != nil {
				t.Fatalf("write the record: %v", err)
			}
			tc.setup(t, f, t.TempDir())
			before := f.digests()
			_, err := CheckDrift(context.Background(), guardOptions(f, &fakeGenerator{fn: rewriteOutputs("generated")}))
			logCell(t, err, "record matches byte for byte")
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("cell REFUSED but its contract says accept: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want a refusal containing %q, got %v", tc.wantErr, err)
			}
			assertUnchanged(t, before, f.digests(), tc.name)
		})
	}
}

// TestTakeSnapshotContextStopsWhenCancelled is TestCopyTreeStopsWhenCancelled's
// sibling: the two snapshots of the private copy are the other two walks a
// signal must be able to end.
func TestTakeSnapshotContextStopsWhenCancelled(t *testing.T) {
	f := guardFixture(t)
	root, err := os.OpenRoot(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	snap, err := TakeSnapshotContext(ctx, root, skipVCS)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("TakeSnapshotContext with a cancelled context returned %v (%d entries), want context.Canceled", err, len(snap))
	}
}
