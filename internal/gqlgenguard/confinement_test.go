package gqlgenguard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
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
			setup: linkOutsideDir("outdir"), wantErr: "has a `..` after a directory name"},
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
		{shape: "a ../ after a directory name, lexically inside the module (refused: it climbs from wherever the name resolves)", schema: "gen/../schema.graphql", wantErr: "has a `..` after a directory name"},
		{shape: "a ** walk passing a directory link out of the module at depth 2 (round 2's shape)", schema: `"sub/**/*.graphql"`,
			setup: func(t *testing.T, f *fixture, outside string) {
				f.write("sub/deep/inside.graphql", fixtureSchema)
				linkOutsideDir("sub/deep/outdir")(t, f, outside)
			}, wantErr: `schema pattern "sub/**/*.graphql" cannot be resolved: its ** walk passes "sub/deep/outdir"`},
		{shape: "a ** walk passing a directory link out of the module at depth 3", schema: `"sub/**/*.graphql"`,
			setup: func(t *testing.T, f *fixture, outside string) {
				f.write("sub/a/b/inside.graphql", fixtureSchema)
				linkOutsideDir("sub/a/b/outdir")(t, f, outside)
			}, wantErr: `its ** walk passes "sub/a/b/outdir"`},
		{shape: "a ** walk passing an absolute FILE link whose name does not match (a .venv-shaped interpreter)", schema: `"./**/*.graphql"`,
			setup: func(t *testing.T, f *fixture, outside string) {
				py := filepath.Join(outside, "python3")
				if err := os.WriteFile(py, []byte("x"), 0o755); err != nil {
					t.Fatal(err)
				}
				f.write(".venv/lib/keep", "x")
				if err := os.MkdirAll(filepath.Join(f.dir, ".venv", "bin"), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(py, filepath.Join(f.dir, ".venv", "bin", "python")); err != nil {
					t.Fatal(err)
				}
			}, wantErr: `its ** walk passes ".venv/bin/python"`},
		{shape: "a ** walk passing an IN-module directory link (reproduced; gqlgen does not follow it)", schema: `"sub/**/*.graphql"`,
			setup: func(t *testing.T, f *fixture, outside string) {
				f.write("sub/deep/inside.graphql", fixtureSchema)
				f.write("other/x.txt", "x")
				if err := os.Symlink("../../other", filepath.Join(f.dir, "sub", "deep", "inlink")); err != nil {
					t.Fatal(err)
				}
			}},
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

// TestTemplateInputConfinementInputDomain is the class sweep of the
// schema-input confinement over the other two configuration keys gqlgen READS
// as files (v0.17.66: codegen/config/package.go ModelTemplate,
// codegen/config/resolver.go ResolverTemplate; both read with os.ReadFile from
// the generator's working directory). Round 2 executed an absolute template
// outside the module reaching the generated output through `generate`; every
// row here is refused before the generator runs, except the canonical one.
func TestTemplateInputConfinementInputDomain(t *testing.T) {
	type knob struct{ name, section, key, from string }
	knobs := []knob{
		{"model.model_template", "model", "model_template", "model: {filename: gen/model/models_gen.go, package: model}"},
		{"resolver.resolver_template", "resolver", "resolver_template", "resolver: {layout: single-file, filename: gen/resolver.go, package: gen}"},
	}
	type shape struct {
		name    string
		value   func(t *testing.T, f *fixture, outside string) string
		wantErr string
	}
	shapes := []shape{
		{"canonical (a template file inside the module)", func(t *testing.T, f *fixture, outside string) string {
			f.write("tmpl/t.gotpl", "{{ reserveImport \"context\" }}\n")
			return "tmpl/t.gotpl"
		}, ""},
		{"absolute path outside the module", func(t *testing.T, f *fixture, outside string) string {
			p := filepath.Join(outside, "t.gotpl")
			if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
			return p
		}, "leaves the module"},
		{"relative ../ out of the module", func(t *testing.T, f *fixture, outside string) string {
			return "../t.gotpl"
		}, "leaves the module"},
		{"absolute path naming the REAL tree (outside the private copy)", func(t *testing.T, f *fixture, outside string) string {
			f.write("tmpl/t.gotpl", "x")
			return filepath.Join(f.dir, "tmpl", "t.gotpl")
		}, "leaves the module"},
		{"a link out of the module", func(t *testing.T, f *fixture, outside string) string {
			p := filepath.Join(outside, "t.gotpl")
			if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Join(f.dir, "tmpl"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(p, filepath.Join(f.dir, "tmpl", "t.gotpl")); err != nil {
				t.Fatal(err)
			}
			return "tmpl/t.gotpl"
		}, "too many levels of symbolic links"},
		{"a path through a directory link out of the module", func(t *testing.T, f *fixture, outside string) string {
			if err := os.WriteFile(filepath.Join(outside, "t.gotpl"), []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, filepath.Join(f.dir, "outdir")); err != nil {
				t.Fatal(err)
			}
			return "outdir/t.gotpl"
		}, "too many levels of symbolic links"},
		{"absent (names a file that does not exist)", func(t *testing.T, f *fixture, outside string) string {
			return "tmpl/missing.gotpl"
		}, "cannot be read"},
		{"wrong type (a directory)", func(t *testing.T, f *fixture, outside string) string {
			f.write("tmpl/dir.gotpl/keep", "x")
			return "tmpl/dir.gotpl"
		}, "not a regular file"},
		{"zero (empty string: gqlgen's built-in template)", func(t *testing.T, f *fixture, outside string) string {
			return ""
		}, ""},
	}
	n := 0
	for _, k := range knobs {
		for _, sh := range shapes {
			id := cellID("G8", n)
			n++
			t.Run(id+" "+k.name+"/"+sh.name, func(t *testing.T) {
				f := guardFixture(t)
				outside := t.TempDir()
				v := sh.value(t, f, outside)
				entry := strings.TrimSuffix(k.from, "}") + fmt.Sprintf(", %s: %q}", k.key, v)
				f.write("gqlgen.yml", strings.Replace(guardConfig, k.from, entry, 1))
				before := f.digests()
				gen := &fakeGenerator{fn: rewriteOutputs("generated")}
				_, err := UpdateDriftRecord(context.Background(), guardOptions(f, gen))
				logCell(t, err, "generated with the configured template")
				if sh.wantErr == "" {
					if err != nil {
						t.Fatalf("cell REFUSED but its contract says accept: %v", err)
					}
					return
				}
				if err == nil || !strings.Contains(err.Error(), sh.wantErr) {
					t.Fatalf("want a refusal containing %q, got %v", sh.wantErr, err)
				}
				if !strings.Contains(err.Error(), k.name) {
					t.Fatalf("the refusal does not name the knob %s: %v", k.name, err)
				}
				if gen.ran {
					t.Fatal("the generator ran over a template input that was already a refusal")
				}
				assertUnchanged(t, before, f.digests(), sh.name)
			})
		}
	}
}

// TestTheRealGeneratorUsesAnInModuleTemplate is the canonical template row run
// for real: an in-module model template is NOT refused, and its bytes reach the
// generated output -- so the confinement did not simply switch the knob off.
func TestTheRealGeneratorUsesAnInModuleTemplate(t *testing.T) {
	goAvailable(t)
	f := guardFixture(t).withModuleFiles()
	mod, err := os.ReadFile(filepath.Join(gqlgenModuleDir(t), "plugin", "modelgen", "models.gotpl"))
	if err != nil {
		t.Fatal(err)
	}
	f.write("tmpl/models.gotpl", string(mod)+"\n// in_module_template_marker\n")
	f.write("gqlgen.yml", strings.Replace(guardConfig,
		"model: {filename: gen/model/models_gen.go, package: model}",
		"model: {filename: gen/model/models_gen.go, package: model, model_template: tmpl/models.gotpl}", 1))
	if _, err := Generate(context.Background(), guardOptions(f, nil)); err != nil {
		t.Fatalf("an in-module template was refused: %v", err)
	}
	if !strings.Contains(f.read("gen/model/models_gen.go"), "in_module_template_marker") {
		t.Fatal("the in-module template did not reach the generated output")
	}
}

// TestARecursiveSchemaGlobRefusesALinkedDirectoryOutOfTheModule is round 2's
// second shape with the real generator: a `**` schema walk that passes a
// directory link out of the module. gqlgen's walk never follows a link -- the
// schema behind it would be left out, with generation and the record update
// both succeeding -- so the guard refuses the run instead, before anything is
// generated, naming the link.
func TestARecursiveSchemaGlobRefusesALinkedDirectoryOutOfTheModule(t *testing.T) {
	goAvailable(t)
	f := guardFixture(t).withModuleFiles()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.graphql"), []byte("type Query { leakedFromOutside: String! }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f.write("sub/deep/inside.graphql", fixtureSchema)
	if err := os.Symlink(outside, filepath.Join(f.dir, "sub", "deep", "outdir")); err != nil {
		t.Fatal(err)
	}
	f.write("gqlgen.yml", strings.Replace(guardConfig, "schema: [schema.graphql]", `schema: ["sub/**/*.graphql"]`, 1))
	before := f.digests()
	var report strings.Builder
	opts := guardOptions(f, nil)
	opts.Report = &report
	_, err := UpdateDriftRecord(context.Background(), opts)
	logCell(t, err, "")
	if err == nil || !strings.Contains(err.Error(), `its ** walk passes "sub/deep/outdir"`) {
		t.Fatalf("want a refusal naming the linked directory, got %v\n%s", err, report.String())
	}
	if strings.Contains(report.String(), "generator: ") {
		t.Fatalf("the generator was started before the refusal:\n%s", report.String())
	}
	assertUnchanged(t, before, f.digests(), "a recursive schema glob over a link out of the module")
}

// gqlgenModuleDir is where the gqlgen module this repository requires lives.
func gqlgenModuleDir(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/99designs/gqlgen").Output()
	if err != nil {
		t.Fatalf("locate the gqlgen module: %v", err)
	}
	return strings.TrimSpace(string(out))
}

// TestEveryConfigKeyThatNamesAPathInputDomain is the class, whole: every key
// in gqlgen v0.17.66's codegen/config structs whose value names a file, a
// directory or a Go package, one executed cell each, pointed OUTSIDE the
// module. Filesystem keys -- the outputs gqlgen writes and the inputs it reads
// -- are refused before generating. Go package keys (`models.*.model`,
// `autobind`) are not filesystem paths: the go command resolves them against
// go.mod/go.sum, so a DEPENDENCY package outside the module root is how the
// real config binds DateTime (github.com/99designs/gqlgen/graphql.Time) and
// must work; a package outside the module graph fails the generation.
func TestEveryConfigKeyThatNamesAPathInputDomain(t *testing.T) {
	const timeSchema = "scalar Time\n\ntype Query {\n  hello(name: String): String!\n  item: Item\n  at: Time\n}\n\ntype Item {\n  key: String!\n  value: Float\n}\n"
	cases := []struct {
		key     string
		edit    func(cfg string) string
		real    bool
		wantErr string
	}{
		// ---- outputs gqlgen writes ----
		{key: "exec.filename", edit: func(c string) string {
			return strings.Replace(c, "exec: {filename: gen/generated.go, package: gen}", "exec: {filename: ../x/generated.go, package: gen}", 1)
		}, wantErr: "resolves outside the module root"},
		{key: "exec.dir (follow-schema)", edit: func(c string) string {
			return strings.Replace(c, "exec: {filename: gen/generated.go, package: gen}", "exec: {layout: follow-schema, dir: ../outdir, package: gen}", 1)
		}, wantErr: "resolves outside the module root"},
		{key: "exec.filename_template (follow-schema)", edit: func(c string) string {
			return strings.Replace(c, "exec: {filename: gen/generated.go, package: gen}", `exec: {layout: follow-schema, dir: gen, package: gen, filename_template: "../../{name}.go"}`, 1)
		}, wantErr: "resolves outside the module root"},
		{key: "model.filename", edit: func(c string) string {
			return strings.Replace(c, "model: {filename: gen/model/models_gen.go, package: model}", "model: {filename: ../models_gen.go, package: model}", 1)
		}, wantErr: "resolves outside the module root"},
		{key: "federation.filename", edit: func(c string) string {
			return c + "federation: {filename: ../federation.go, package: gen}\n"
		}, wantErr: "resolves outside the module root"},
		{key: "resolver.filename (single-file)", edit: func(c string) string {
			return strings.Replace(c, "filename: gen/resolver.go", "filename: ../resolver.go", 1)
		}, wantErr: "resolves outside the module root"},
		{key: "resolver.dir (follow-schema)", edit: func(c string) string {
			return strings.Replace(c, "resolver: {layout: single-file, filename: gen/resolver.go, package: gen}", "resolver: {layout: follow-schema, dir: ../outdir, package: gen}", 1)
		}, wantErr: "resolves outside the module root"},
		{key: "resolver.filename_template (follow-schema)", edit: func(c string) string {
			return strings.Replace(c, "resolver: {layout: single-file, filename: gen/resolver.go, package: gen}", `resolver: {layout: follow-schema, dir: gen, package: gen, filename_template: "../../{name}.go"}`, 1)
		}, wantErr: "resolves outside the module root"},
		// ---- inputs gqlgen reads ----
		{key: "schema", edit: func(c string) string {
			return strings.Replace(c, "schema: [schema.graphql]", "schema: [../outside.graphql]", 1)
		}, wantErr: "leaves the module"},
		{key: "model.model_template", edit: func(c string) string {
			return strings.Replace(c, "package: model}", "package: model, model_template: /etc/hostname}", 1)
		}, wantErr: "leaves the module"},
		{key: "resolver.resolver_template", edit: func(c string) string {
			return strings.Replace(c, "filename: gen/resolver.go, package: gen}", "filename: gen/resolver.go, package: gen, resolver_template: /etc/hostname}", 1)
		}, wantErr: "leaves the module"},
		{key: "federation.model_template (parsed, never read by gqlgen)", edit: func(c string) string {
			return c + "federation: {filename: gen/federation.go, package: gen, model_template: /etc/hostname}\n"
		}, wantErr: "leaves the module"},
		// ---- Go package keys: resolved by the go command, real generator ----
		{key: "models.*.model -> a DEPENDENCY package (the real config's DateTime shape)", real: true, edit: func(c string) string {
			return c + "models:\n  Time:\n    model:\n      - github.com/99designs/gqlgen/graphql.Time\n"
		}},
		{key: "models.*.model -> a package outside the module graph", real: true, edit: func(c string) string {
			return c + "models:\n  Time:\n    model:\n      - example.com/not/in/graph.Time\n"
		}, wantErr: "the generator failed"},
		{key: "autobind -> a DEPENDENCY package", real: true, edit: func(c string) string {
			return c + "autobind:\n  - github.com/99designs/gqlgen/graphql\nmodels:\n  Time:\n    model:\n      - github.com/99designs/gqlgen/graphql.Time\n"
		}},
		{key: "autobind -> a package outside the module graph", real: true, edit: func(c string) string {
			return c + "autobind:\n  - example.com/not/in/graph\nmodels:\n  Time:\n    model:\n      - github.com/99designs/gqlgen/graphql.Time\n"
		}, wantErr: "the generator failed"},
	}
	for i, tc := range cases {
		t.Run(cellID("G9", i)+" "+tc.key, func(t *testing.T) {
			f := guardFixture(t)
			var gen Generator
			fake := &fakeGenerator{fn: rewriteOutputs("generated")}
			if tc.real {
				goAvailable(t)
				f.withModuleFiles()
				f.write("schema.graphql", timeSchema)
			} else {
				gen = fake
			}
			f.write("gqlgen.yml", tc.edit(guardConfig))
			before := f.digests()
			var report strings.Builder
			opts := guardOptions(f, gen)
			opts.Report = &report
			var err error
			if tc.real {
				_, err = Generate(context.Background(), opts)
			} else {
				_, err = UpdateDriftRecord(context.Background(), opts)
			}
			logCell(t, err, "generated")
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("cell REFUSED but its contract says accept: %v\n%s", err, report.String())
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want a refusal containing %q, got %v", tc.wantErr, err)
			}
			if !tc.real && fake.ran {
				t.Fatal("the generator ran over a configuration that was already a refusal")
			}
			assertUnchanged(t, before, f.digests(), tc.key)
		})
	}
}
