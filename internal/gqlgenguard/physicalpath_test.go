package gqlgenguard

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// shallowConfig is guardConfig as it reads from x/, one directory below the
// module root: the generator runs in x/, so every path climbs one level first.
const shallowConfig = `
schema: [../schema.graphql]
exec: {filename: ../gen/generated.go, package: gen}
model: {filename: ../gen/model/models_gen.go, package: model}
resolver: {layout: single-file, filename: ../gen/resolver.go, package: gen}
`

// TestEveryInputPathIsJudgedWhereItPhysicallyResolves is the input domain of
// the one rule every path the config names is held to: it is judged where the
// operating system resolves it, never where filepath.Clean says. Through an
// in-module link a schema, a template or a config directory can otherwise be
// read from OUTSIDE the module: after `a/b/L -> ../..` (the root), `a/b/L/..`
// is the root's parent to the kernel and `a/b` to Clean.
//
// Shapes, per key: (S1) an in-module link to an ANCESTOR followed by `..`
// (the escape above); (S2) no `..` after any name, but the config's directory
// reached through an in-module link whose target sits SHALLOWER than its name
// -- `a/b/c/cfg -> ../../../x` -- so a leading `../..` climbs out of the
// module physically while staying inside it lexically; (S3) an in-module link
// to a non-ancestor directory (accepted: the physical path is what the plan
// and the report name); (S4) a `..` after a name with no link at all.
// "outside" is the copy's parent, where every escape from the copy lands.
func TestEveryInputPathIsJudgedWhereItPhysicallyResolves(t *testing.T) {
	type cell struct {
		shape   string
		setup   func(t *testing.T, f *fixture, outside string) (config string)
		wantErr string
		// wantInput, for an accepted cell, is a "generator input:" line the
		// report must carry: the value as written and its physical path.
		wantInput string
	}
	link := func(t *testing.T, f *fixture, target, name string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(filepath.Join(f.dir, name)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(f.dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	outsideFile := func(t *testing.T, outside, rel, body string) {
		t.Helper()
		p := filepath.Join(outside, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	const evil = "extend type Query { outsideModuleMarker: String }\n"
	// ancestor: a/b/L -> ../.. (the module root), and the value under test.
	ancestor := func(key, value string) func(*testing.T, *fixture, string) string {
		return func(t *testing.T, f *fixture, outside string) string {
			link(t, f, "../..", "a/b/L")
			outsideFile(t, outside, "outside/evil.graphql", evil)
			outsideFile(t, outside, "outside/t.gotpl", "x")
			return withKey(guardConfig, key, value)
		}
	}
	// shallow: a/b/c/cfg -> ../../../x, the config in x/, reached as
	// a/b/c/cfg/gqlgen.yml. From x, `../..` is the copy's parent.
	shallow := func(key, value string) func(*testing.T, *fixture, string) string {
		return func(t *testing.T, f *fixture, outside string) string {
			f.write("x/gqlgen.yml", withKey(shallowConfig, key, value))
			f.remove("gqlgen.yml") // a config above x/ would be one gqlgen's discovery finds
			link(t, f, "../../../x", "a/b/c/cfg")
			outsideFile(t, outside, "outside/evil.graphql", evil)
			outsideFile(t, outside, "outside/t.gotpl", "x")
			return "a/b/c/cfg/gqlgen.yml"
		}
	}
	plain := func(key, value string) func(*testing.T, *fixture, string) string {
		return func(t *testing.T, f *fixture, outside string) string {
			f.write("tmpl/t.gotpl", "{{ reserveImport \"context\" }}\n")
			return withKey(guardConfig, key, value)
		}
	}
	const dotdot = "has a `..` after a directory name"
	cells := []cell{
		{shape: "S1 schema literal: a/b/L/../../outside/evil.graphql (L -> the root)", setup: ancestor("schema", "[a/b/L/../../outside/evil.graphql]"), wantErr: dotdot},
		{shape: "S1 schema glob: a/b/L/../../outside/*.graphql", setup: ancestor("schema", `["a/b/L/../../outside/*.graphql"]`), wantErr: dotdot},
		{shape: "S1 schema **: a/b/L/../../outside/**/*.graphql", setup: ancestor("schema", `["a/b/L/../../outside/**/*.graphql"]`), wantErr: dotdot},
		{shape: "S1 model.model_template: a/b/L/../../outside/t.gotpl", setup: ancestor("model_template", "a/b/L/../../outside/t.gotpl"), wantErr: dotdot},
		{shape: "S1 resolver.resolver_template: a/b/L/../../outside/t.gotpl", setup: ancestor("resolver_template", "a/b/L/../../outside/t.gotpl"), wantErr: dotdot},
		{shape: "S1 exec.filename (an output): a/b/L/../../outside/generated.go", setup: ancestor("exec.filename", "a/b/L/../../outside/generated.go"), wantErr: dotdot},
		{shape: "S1 the ancestor link alone (schema untouched): dropped, generation proceeds", setup: ancestor("", "")},
		{shape: "S2 canonical: the config dir through the shallower link, every path inside", setup: shallow("", ""), wantInput: "generator input: schema ../schema.graphql -> schema.graphql"},
		{shape: "S2 schema literal ../../outside/evil.graphql (lexically a/b/outside/..., physically the copy's parent)", setup: shallow("schema", "[../../outside/evil.graphql]"), wantErr: "outside the module"},
		{shape: "S2 schema glob ../../outside/*.graphql", setup: shallow("schema", `["../../outside/*.graphql"]`), wantErr: "outside the module"},
		{shape: "S2 schema ** ../../outside/**/*.graphql", setup: shallow("schema", `["../../outside/**/*.graphql"]`), wantErr: "outside the module"},
		{shape: "S2 model.model_template ../../outside/t.gotpl", setup: shallow("model_template", "../../outside/t.gotpl"), wantErr: "outside the module"},
		{shape: "S2 resolver.resolver_template ../../outside/t.gotpl", setup: shallow("resolver_template", "../../outside/t.gotpl"), wantErr: "outside the module"},
		{shape: "S2 federation.model_template ../../outside/t.gotpl", setup: shallow("federation.model_template", "../../outside/t.gotpl"), wantErr: "outside the module"},
		{shape: "S2 exec.filename (an output) ../../outside/generated.go", setup: shallow("exec.filename", "../../outside/generated.go"), wantErr: "outside the module"},
		{shape: "S3 schema through a link to an in-module directory (accepted; the physical path is named)", setup: func(t *testing.T, f *fixture, outside string) string {
			f.write("sub/real/schema.graphql", fixtureSchema)
			link(t, f, "sub/real", "slink")
			return withKey(guardConfig, "schema", "[slink/schema.graphql]")
		}, wantInput: "generator input: schema slink/schema.graphql -> sub/real/schema.graphql"},
		{shape: "S3 model.model_template through a link to an in-module directory (accepted)", setup: func(t *testing.T, f *fixture, outside string) string {
			f.write("tmpl/t.gotpl", "{{ reserveImport \"context\" }}\n")
			link(t, f, "tmpl", "tlink")
			return withKey(guardConfig, "model_template", "tlink/t.gotpl")
		}, wantInput: "generator input: model.model_template tlink/t.gotpl -> tmpl/t.gotpl"},
		{shape: "S4 schema gen/../schema.graphql (no link)", setup: plain("schema", "[gen/../schema.graphql]"), wantErr: dotdot},
		{shape: "S4 model.model_template tmpl/../tmpl/t.gotpl", setup: plain("model_template", "tmpl/../tmpl/t.gotpl"), wantErr: dotdot},
		{shape: "S4 resolver.resolver_template tmpl/../tmpl/t.gotpl", setup: plain("resolver_template", "tmpl/../tmpl/t.gotpl"), wantErr: dotdot},
		{shape: "S4 exec.filename gen/../gen/generated.go", setup: plain("exec.filename", "gen/../gen/generated.go"), wantErr: dotdot},
		{shape: "S4 resolver.filename gen/../gen/resolver.go", setup: plain("resolver.filename", "gen/../gen/resolver.go"), wantErr: dotdot},
		{shape: "S4 leading ../ only, from a config one level down (accepted)", setup: func(t *testing.T, f *fixture, outside string) string {
			f.write("x/gqlgen.yml", shallowConfig)
			f.remove("gqlgen.yml")
			return "x/gqlgen.yml"
		}, wantInput: "generator input: schema ../schema.graphql -> schema.graphql"},
	}
	for i, c := range cells {
		t.Run(cellID("G17", i)+" "+c.shape, func(t *testing.T) {
			f := guardFixture(t)
			opts := guardOptions(f, nil)
			outside := opts.TempParent
			cfg := c.setup(t, f, outside)
			if strings.HasSuffix(cfg, ".yml") {
				opts.ConfigPath = cfg
			} else {
				f.write("gqlgen.yml", cfg)
			}
			if c.wantErr != "" && strings.Contains(f.dir, c.wantErr) {
				t.Fatalf("wantErr %q also occurs in the fixture path %s", c.wantErr, f.dir)
			}
			before := f.digests()
			gen := &fakeGenerator{fn: rewriteOutputs("generated")}
			if opts.ConfigPath != "gqlgen.yml" {
				// The config is one level down (x/): its outputs are ../gen/...
				gen.fn = func(workDir string) error { return rewriteOutputs("generated")(filepath.Join(workDir, "..")) }
			}
			opts.Generator = gen
			var report strings.Builder
			opts.Report = &report
			_, err := Generate(context.Background(), opts)
			logCell(t, err, lineWith(report.String(), c.wantInput))
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("want a refusal containing %q, got %v\n%s", c.wantErr, err, report.String())
				}
				if gen.ran {
					t.Fatal("the generator ran")
				}
				assertUnchanged(t, before, f.digests(), c.shape)
				return
			}
			if err != nil {
				t.Fatalf("cell REFUSED but its contract says accept: %v\n%s", err, report.String())
			}
			if c.wantInput != "" && !strings.Contains(report.String(), c.wantInput+"\n") {
				t.Fatalf("the report does not name the input where it physically resolves (%q):\n%s", c.wantInput, report.String())
			}
		})
	}
}

// withKey sets one key of a guard config: "" leaves it as is; "schema" replaces
// the schema line; "model_template" / "resolver_template" add the template to
// that section; "federation.model_template" adds a federation section holding
// only the template; "exec.filename" / "resolver.filename" replace that value.
func withKey(cfg, key, value string) string {
	switch key {
	case "":
		return cfg
	case "schema":
		i := strings.Index(cfg, "schema: [")
		j := strings.Index(cfg[i:], "\n")
		return cfg[:i] + "schema: " + value + cfg[i+j:]
	case "model_template":
		return strings.Replace(cfg, "package: model}", "package: model, model_template: "+value+"}", 1)
	case "resolver_template":
		return strings.Replace(cfg, "resolver.go, package: gen}", "resolver.go, package: gen, resolver_template: "+value+"}", 1)
	case "federation.model_template":
		return cfg + "federation: {model_template: " + value + "}\n"
	case "exec.filename":
		i := strings.Index(cfg, "exec: {filename: ")
		j := strings.Index(cfg[i:], ",")
		return cfg[:i] + "exec: {filename: " + value + cfg[i+j:]
	case "resolver.filename":
		i := strings.Index(cfg, "resolver: {layout: single-file, filename: ")
		j := strings.Index(cfg[i:], ", package")
		return cfg[:i] + "resolver: {layout: single-file, filename: " + value + cfg[i+j:]
	}
	panic("withKey: unknown key " + key)
}

func (f *fixture) remove(rel string) {
	f.t.Helper()
	if err := os.Remove(filepath.Join(f.dir, filepath.FromSlash(rel))); err != nil {
		f.t.Fatal(err)
	}
}

// lineWith returns the report line starting with prefix, or the config line.
func lineWith(report, prefix string) string {
	for _, l := range strings.Split(report, "\n") {
		if prefix != "" && strings.HasPrefix(l, prefix) {
			return l
		}
	}
	for _, l := range strings.Split(report, "\n") {
		if strings.HasPrefix(l, "config: ") {
			return l
		}
	}
	return ""
}

// TestTheRealGeneratorRunsWhereTheConfigPhysicallyIs executes S2's canonical
// cell with the real generator: the config reached through a link whose target
// is shallower than its name. The plan's outputs, judged from the physical
// directory, are exactly the files gqlgen writes there; judged from the
// lexical directory they would be two levels deeper.
func TestTheRealGeneratorRunsWhereTheConfigPhysicallyIs(t *testing.T) {
	goAvailable(t)
	f := guardFixture(t).withModuleFiles()
	f.write("x/gqlgen.yml", shallowConfig)
	f.remove("gqlgen.yml")
	if err := os.MkdirAll(filepath.Join(f.dir, "a", "b", "c"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../../../x", filepath.Join(f.dir, "a", "b", "c", "cfg")); err != nil {
		t.Fatal(err)
	}
	opts := guardOptions(f, nil)
	opts.ConfigPath = "a/b/c/cfg/gqlgen.yml"
	var report strings.Builder
	opts.Report = &report
	res, err := Generate(context.Background(), opts)
	t.Logf("CELL-OUTPUT: %v; %s", err, lineWith(report.String(), "config: "))
	if err != nil {
		t.Fatalf("generation through the shallower config link failed: %v\n%s", err, report.String())
	}
	if !strings.Contains(report.String(), "config: x/gqlgen.yml ") {
		t.Fatalf("the plan does not name the config where it physically is:\n%s", report.String())
	}
	if !strings.Contains(f.read("gen/generated.go"), "func (ec *executionContext)") {
		t.Fatalf("gen/generated.go was not regenerated (applied %v)", res)
	}
}

// TestLocationInputsAreJudgedWhereTheyPhysicallyResolve: the copy's parent and the go command's locations, each named as
// `<a link OUTSIDE the module to an in-module dir>/../<dir>` -- lexically
// outside, physically inside. Each refuses before anything is created in the
// tree.
func TestLocationInputsAreJudgedWhereTheyPhysicallyResolve(t *testing.T) {
	goAvailable(t)
	cells := []struct {
		shape   string
		env     string
		wantErr string
	}{
		{"TMPDIR (the copy's parent) = <L>/../gen, L -> <module>/unrelated", "TMPDIR", "would be made inside the module"},
		{"GOCACHE = <L>/../.gocache-in-tree", "GOCACHE", `GOCACHE="`},
		{"GOMODCACHE = <L>/../.modcache-in-tree", "GOMODCACHE", `GOMODCACHE="`},
		{"GOPATH = <L>/../.gopath-in-tree", "GOPATH", `GOPATH="`},
	}
	for i, c := range cells {
		t.Run(cellID("G18", i)+" "+c.shape, func(t *testing.T) {
			f := guardFixture(t).withModuleFiles()
			x := t.TempDir()
			if err := os.Symlink(filepath.Join(f.dir, "unrelated"), filepath.Join(x, "L")); err != nil {
				t.Fatal(err)
			}
			opts := guardOptions(f, &fakeGenerator{fn: rewriteOutputs("generated")})
			switch c.env {
			case "TMPDIR":
				opts.TempParent = filepath.Join(x, "L") + "/../gen"
			case "GOPATH":
				t.Setenv("GOMODCACHE", "")
				t.Setenv("GOPATH", filepath.Join(x, "L")+"/../.gopath-in-tree")
			default:
				t.Setenv(c.env, filepath.Join(x, "L")+"/../."+strings.ToLower(strings.TrimPrefix(c.env, "GO"))+"-in-tree")
			}
			if strings.Contains(f.dir, c.wantErr) {
				t.Fatalf("wantErr %q also occurs in the fixture path %s", c.wantErr, f.dir)
			}
			before := f.digests()
			var report strings.Builder
			opts.Report = &report
			// Bounded: a copy made inside the module copies itself without end,
			// and that must fail this cell, not hang the suite.
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			_, err := CheckDrift(ctx, opts)
			logCell(t, err, "accepted")
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("want a refusal containing %q, got %v\n%s", c.wantErr, err, report.String())
			}
			assertUnchanged(t, before, f.digests(), c.shape)
		})
	}
}

// TestPhysicalPathInputDomain executes physicalPath -- the resolver every
// confinement decision now goes through -- over each shape of path it can be
// handed, against the answer the operating system gives where one exists.
func TestPhysicalPathInputDomain(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	mk := func(rel string) {
		if err := os.MkdirAll(filepath.Join(root, rel), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	ln := func(target, rel string) {
		if err := os.Symlink(target, filepath.Join(root, rel)); err != nil {
			t.Fatal(err)
		}
	}
	mk("m/a/b")
	mk("m/x")
	mk("out")
	if err := os.WriteFile(filepath.Join(root, "m", "file.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	ln("../..", "m/a/b/L")        // an ancestor (m)
	ln("../../x", "m/a/b/S")      // shallower than its name
	ln(root+"/out", "m/a/abs")    // absolute
	ln("S", "m/a/b/chain")        // a link to a link
	ln("loop", "m/a/loop")        // a self-loop
	ln("nowhere/x", "m/a/dangle") // dangling
	cells := []struct {
		shape, in, want, wantErr string
	}{
		{"canonical (no link, exists)", root + "/m/a/b", root + "/m/a/b", ""},
		{"a missing tail (kept as written: where a create lands)", root + "/m/a/new/file", root + "/m/a/new/file", ""},
		{"`..` after an ANCESTOR link climbs from the target", root + "/m/a/b/L/../out", root + "/out", ""},
		{"`..` after a SHALLOWER link climbs from the target", root + "/m/a/b/S/../../out", root + "/out", ""},
		{"an absolute link restarts at /", root + "/m/a/abs/f", root + "/out/f", ""},
		{"a chain of links", root + "/m/a/b/chain", root + "/m/x", ""},
		{"a self-loop", root + "/m/a/loop/f", "", "too many levels of symbolic links"},
		{"a dangling link (its target kept as written)", root + "/m/a/dangle", root + "/m/a/nowhere/x", ""},
		{"`..` past / stays at /", "/../../" + strings.TrimPrefix(root, "/") + "/m", root + "/m", ""},
		{"`.` and doubled separators", root + "/./m//a/./b", root + "/m/a/b", ""},
		{"a link, then a name inside its target", root + "/m/a/b/L/x/f", root + "/m/x/f", ""},
		{"a regular file where a directory is named", root + "/m/file.txt/f", "", "not a directory"},
	}
	for i, c := range cells {
		t.Run(cellID("G19", i)+" "+c.shape, func(t *testing.T) {
			got, err := physicalPath(c.in)
			logCell(t, err, got)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("want an error containing %q, got %q, %v", c.wantErr, got, err)
				}
				return
			}
			if err != nil || got != c.want {
				t.Fatalf("physicalPath(%q) = %q, %v; want %q", c.in, got, err, c.want)
			}
			// Where the path exists, the operating system agrees.
			if real, rerr := filepath.EvalSymlinks(c.in); rerr == nil && real != got {
				t.Fatalf("physicalPath(%q) = %q but the operating system resolves %q", c.in, got, real)
			}
		})
	}
	t.Run(cellID("G19", len(cells))+" a relative path is taken from the working directory", func(t *testing.T) {
		err := withWorkingDir(filepath.Join(root, "m", "a", "b"), func() error {
			got, err := physicalPath("L/../out")
			logCell(t, err, got)
			if err != nil || got != root+"/out" {
				t.Fatalf("physicalPath(L/../out) from m/a/b = %q, %v; want %q", got, err, root+"/out")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}

// TestEachPhysicalCheckRefusesOnItsOwn drives each physical confinement check
// directly, handed the config directory as the guard used to compute it --
// the LEXICAL path through a link whose target is shallower than its name.
// In the running guard the config directory is already physical, links to
// ancestors are not reproduced and `..` after a name is refused, so no input
// reaches these checks with a lexical directory; without this table each
// check could be disabled and nothing would fail.
func TestEachPhysicalCheckRefusesOnItsOwn(t *testing.T) {
	module, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Dir(module)
	for _, d := range []string{"a/b/c", "x"} {
		if err := os.MkdirAll(filepath.Join(module, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("../../../x", filepath.Join(module, "a", "b", "c", "cfg")); err != nil {
		t.Fatal(err)
	}
	name := "gqlgen-guard-escape-" + filepath.Base(module)
	if err := os.MkdirAll(filepath.Join(outside, name), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(filepath.Join(outside, name)) })
	for _, f := range []string{"evil.graphql", "t.gotpl"} {
		if err := os.WriteFile(filepath.Join(outside, name, f), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	lexical := filepath.Join(module, "a", "b", "c", "cfg") // physically module/x
	up := "../../" + name                                  // lexically a/b/<name>, physically beside the module
	cells := []struct {
		shape string
		run   func() error
	}{
		{"refuseTemplateInput: " + up + "/t.gotpl", func() error {
			_, err := refuseTemplateInput("model.model_template", up+"/t.gotpl", module, lexical)
			return err
		}},
		{"refuseSchemaPatternsLeavingTheModule: " + up + "/*.graphql", func() error {
			return refuseSchemaPatternsLeavingTheModule([]string{up + "/*.graphql"}, module, lexical)
		}},
		{"schemaTraversalErr (from the lexical dir): " + up + "/*.graphql", func() error {
			return withWorkingDir(lexical, func() error { return schemaTraversalErr(up+"/*.graphql", module) })
		}},
		{"physicalSchemaInputs: " + up + "/evil.graphql", func() error {
			_, err := physicalSchemaInputs(module, lexical, []string{up + "/evil.graphql"})
			return err
		}},
	}
	for i, c := range cells {
		t.Run(cellID("G20", i)+" "+c.shape, func(t *testing.T) {
			err := c.run()
			logCell(t, err, "accepted")
			if err == nil || !strings.Contains(err.Error(), "physically") {
				t.Fatalf("want a refusal naming where the path physically resolves, got %v", err)
			}
		})
	}
}
