package gqlgenguard

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// gqlgenDiscoveryNames are the file names gqlgen's own config discovery tries,
// in order, in the working directory and every parent
// (codegen/config/config.go cfgFilenames + findCfg). TestTheDiscoveryNames
// AreGqlgens reads them back out of the gqlgen source this module requires, so
// a version that renames or adds one fails a test instead of reopening a shadow.
var gqlgenDiscoveryNames = []string{".gqlgen.yml", "gqlgen.yml", "gqlgen.yaml"}

func TestTheDiscoveryNamesAreGqlgens(t *testing.T) {
	src, err := os.ReadFile(filepath.Join(gqlgenModuleDir(t), "codegen", "config", "config.go"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`var cfgFilenames = \[\]string\{([^}]*)\}`).FindSubmatch(src)
	if m == nil {
		t.Fatal("gqlgen's cfgFilenames declaration was not found; its discovery may have changed")
	}
	var got []string
	for _, q := range regexp.MustCompile(`"([^"]+)"`).FindAllSubmatch(m[1], -1) {
		got = append(got, string(q[1]))
	}
	if strings.Join(got, ",") != strings.Join(gqlgenDiscoveryNames, ",") || strings.Join(got, ",") != strings.Join(discoveryConfigNames, ",") {
		t.Fatalf("gqlgen discovers %v; this package assumes %v (tests) / %v (guard)", got, gqlgenDiscoveryNames, discoveryConfigNames)
	}
}

// TestConfigDiscoveryInputDomain: the generator is told the validated config
// explicitly, and the private copy may hold no other file gqlgen's discovery
// would pick up -- beside the validated config or in any directory above it
// inside the module: a shadow `.gqlgen.yml` (discovery's FIRST name) would
// otherwise drive the generator past every check.
func TestConfigDiscoveryInputDomain(t *testing.T) {
	sub := strings.ReplaceAll(guardConfig, "gen/", "../gen/")
	sub = strings.Replace(sub, "schema: [schema.graphql]", "schema: [../schema.graphql]", 1)
	cases := []struct {
		shape   string
		config  string // module-relative path of the validated config
		plant   map[string]string
		gen     func(workDir string) error
		wantErr string
	}{
		{shape: "canonical (only the validated config)", config: "gqlgen.yml"},
		{shape: ".gqlgen.yml beside the validated config", config: "gqlgen.yml", plant: map[string]string{".gqlgen.yml": guardConfig}, wantErr: `".gqlgen.yml"`},
		{shape: "gqlgen.yaml beside the validated config", config: "gqlgen.yml", plant: map[string]string{"gqlgen.yaml": guardConfig}, wantErr: `"gqlgen.yaml"`},
		{shape: "the validated config is gqlgen.yaml, a gqlgen.yml beside it", config: "gqlgen.yaml", plant: map[string]string{"gqlgen.yaml": guardConfig, "gqlgen.yml": guardConfig}, wantErr: `"gqlgen.yml"`},
		{shape: "the validated config is conf/gqlgen.yml, gqlgen.yml in the PARENT (module root)", config: "conf/gqlgen.yml", plant: map[string]string{"conf/gqlgen.yml": sub, "gqlgen.yml": guardConfig}, wantErr: `"gqlgen.yml"`},
		{shape: "the validated config is conf/gqlgen.yml, .gqlgen.yml in the parent", config: "conf/gqlgen.yml", plant: map[string]string{"conf/gqlgen.yml": sub, ".gqlgen.yml": guardConfig}, wantErr: `".gqlgen.yml"`},
		{shape: "a discovery name in an UNRELATED subdirectory (not on the discovery path)", config: "gqlgen.yml", plant: map[string]string{"elsewhere/.gqlgen.yml": guardConfig}},
		{shape: "the validated config is conf/gqlgen.yml and nothing else is on the path (canonical, nested)", config: "conf/gqlgen.yml", plant: map[string]string{"conf/gqlgen.yml": sub},
			gen: func(workDir string) error {
				for _, rel := range []string{"gen/generated.go", "gen/model/models_gen.go"} {
					if err := writeIn(workDir, "../"+rel, strings.Replace(bodyFor(rel), "tree", "generated", 1)); err != nil {
						return err
					}
				}
				return nil
			}},
		{shape: "a custom config name with no discovery file anywhere", config: "custom.yml", plant: map[string]string{"custom.yml": guardConfig}},
		{shape: "a DIRECTORY named .gqlgen.yml beside the config (gqlgen's discovery only stats)", config: "gqlgen.yml", plant: map[string]string{".gqlgen.yml/keep": "x"}, wantErr: `".gqlgen.yml"`},
	}
	for i, tc := range cases {
		t.Run(cellID("G10", i)+" "+tc.shape, func(t *testing.T) {
			f := guardFixture(t)
			if tc.config != "gqlgen.yml" {
				if err := os.Remove(filepath.Join(f.dir, "gqlgen.yml")); err != nil {
					t.Fatal(err)
				}
			}
			for rel, body := range tc.plant {
				f.write(rel, body)
			}
			before := f.digests()
			gen := &fakeGenerator{fn: rewriteOutputs("generated")}
			if tc.gen != nil {
				gen.fn = tc.gen
			}
			opts := guardOptions(f, gen)
			opts.ConfigPath = tc.config
			_, err := UpdateDriftRecord(context.Background(), opts)
			logCell(t, err, "generated from "+tc.config)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("cell REFUSED but its contract says accept: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) || !strings.Contains(err.Error(), "discover") {
				t.Fatalf("want a refusal naming the discoverable config %s, got %v", tc.wantErr, err)
			}
			if gen.ran {
				t.Fatal("the generator ran beside a config gqlgen could discover instead")
			}
			assertUnchanged(t, before, f.digests(), tc.shape)
		})
	}
}

// TestAShadowConfigCannotWriteIntoTheTree, with the real generator: a `.gqlgen.yml` whose exec output is an ABSOLUTE path
// into the working tree. Before the fix gqlgen discovered it instead of the
// validated gqlgen.yml, and even a refused check-drift left the file behind.
func TestAShadowConfigCannotWriteIntoTheTree(t *testing.T) {
	goAvailable(t)
	f := guardFixture(t).withModuleFiles()
	f.write(".gqlgen.yml", strings.Replace(guardConfig, "exec: {filename: gen/generated.go, package: gen}",
		"exec: {filename: "+filepath.Join(f.dir, "shadow", "generated.go")+", package: gen}", 1))
	before := f.digests()
	_, err := CheckDrift(context.Background(), guardOptions(f, nil))
	logCell(t, err, "")
	if f.exists("shadow/generated.go") {
		t.Fatal("the generator wrote into the working tree through a shadow config")
	}
	if err == nil {
		t.Fatal("check-drift accepted a module carrying a config gqlgen would discover")
	}
	assertUnchanged(t, before, f.digests(), "a shadow .gqlgen.yml")
}

// TestTheRealGeneratorReadsTheValidatedConfigByName pins the other half: with
// a custom config name, gqlgen's discovery finds NOTHING and falls back to its
// built-in default config -- so the generator must be handed the validated file
// explicitly. The real run writes the outputs custom.yml declares.
func TestTheRealGeneratorReadsTheValidatedConfigByName(t *testing.T) {
	goAvailable(t)
	f := guardFixture(t).withModuleFiles()
	if err := os.Rename(filepath.Join(f.dir, "gqlgen.yml"), filepath.Join(f.dir, "custom.yml")); err != nil {
		t.Fatal(err)
	}
	var report strings.Builder
	opts := guardOptions(f, nil)
	opts.ConfigPath = "custom.yml"
	opts.Report = &report
	res, err := Generate(context.Background(), opts)
	if err != nil {
		t.Fatalf("generate with a custom config name: %v\n%s", err, report.String())
	}
	if len(res.Applied) == 0 || f.read("gen/generated.go") == markedExec {
		t.Fatalf("the generator did not produce custom.yml's outputs (applied %v)", res.Applied)
	}
	if !strings.Contains(report.String(), "--config custom.yml") {
		t.Fatalf("the report does not show the config passed to the generator:\n%s", report.String())
	}
}

// TestCopyBackInputDomain: every destination is re-verified against the state
// the generation was checked against, and ANY difference refuses the whole
// apply with nothing written, so an edit made while the generator runs is
// never overwritten.
func TestCopyBackInputDomain(t *testing.T) {
	cases := []struct {
		shape   string
		setup   func(t *testing.T, f *fixture)
		during  func(t *testing.T, f *fixture)
		wantErr string
	}{
		{shape: "canonical (the tree does not move during generation)"},
		{shape: "a declared output EDITED during generation (the edit would be overwritten)", during: func(t *testing.T, f *fixture) {
			f.write("gen/generated.go", headerLine+"\n\npackage gen\n// HAND EDIT DURING GENERATION\n")
		}, wantErr: `"gen/generated.go" changed in the working tree`},
		{shape: "a declared output CREATED during generation where it was absent", setup: func(t *testing.T, f *fixture) {
			if err := os.Remove(filepath.Join(f.dir, "gen", "model", "models_gen.go")); err != nil {
				t.Fatal(err)
			}
		}, during: func(t *testing.T, f *fixture) {
			f.write("gen/model/models_gen.go", markedModel+"// created during generation\n")
		}, wantErr: `"gen/model/models_gen.go" changed in the working tree`},
		{shape: "a declared output DELETED during generation", during: func(t *testing.T, f *fixture) {
			if err := os.Remove(filepath.Join(f.dir, "gen", "generated.go")); err != nil {
				t.Fatal(err)
			}
		}, wantErr: `"gen/generated.go" changed in the working tree`},
		{shape: "a declared output replaced by a SYMBOLIC LINK during generation", during: func(t *testing.T, f *fixture) {
			full := filepath.Join(f.dir, "gen", "generated.go")
			if err := os.Remove(full); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("model/models_gen.go", full); err != nil {
				t.Fatal(err)
			}
		}, wantErr: `"gen/generated.go" changed in the working tree`},
		{shape: "a declared output replaced by a link to an IDENTICAL copy (same bytes, different kind)", during: func(t *testing.T, f *fixture) {
			f.write("elsewhere/same.go", markedExec)
			full := filepath.Join(f.dir, "gen", "generated.go")
			if err := os.Remove(full); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("../elsewhere/same.go", full); err != nil {
				t.Fatal(err)
			}
		}, wantErr: `"gen/generated.go" changed in the working tree`},
		{shape: "an output the generator does NOT change, edited during generation (still refused: the apply is all or nothing)", during: func(t *testing.T, f *fixture) {
			f.write("gen/resolver.go", markedResolver+"// edited\n")
		}, wantErr: `"gen/resolver.go" changed in the working tree`},
		{shape: "an unrelated file edited during generation (not a destination)", during: func(t *testing.T, f *fixture) {
			f.write("unrelated/notes.md", "edited\n")
		}},
		{shape: "an ABSENT declared output that becomes a symbolic link during generation", setup: func(t *testing.T, f *fixture) {
			if err := os.Remove(filepath.Join(f.dir, "gen", "model", "models_gen.go")); err != nil {
				t.Fatal(err)
			}
		}, during: func(t *testing.T, f *fixture) {
			if err := os.Symlink("../../unrelated/notes.md", filepath.Join(f.dir, "gen", "model", "models_gen.go")); err != nil {
				t.Fatal(err)
			}
		}, wantErr: `"gen/model/models_gen.go" changed in the working tree`},
	}
	for i, tc := range cases {
		t.Run(cellID("G11", i)+" "+tc.shape, func(t *testing.T) {
			f := guardFixture(t)
			if tc.setup != nil {
				tc.setup(t, f)
			}
			var mid map[string]string
			gen := &fakeGenerator{fn: func(workDir string) error {
				if tc.during != nil {
					tc.during(t, f)
				}
				mid = f.digests()
				return rewriteOutputs("generated")(workDir)
			}}
			_, err := Generate(context.Background(), guardOptions(f, gen))
			logCell(t, err, "applied")
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("cell REFUSED but its contract says accept: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want a refusal containing %q, got %v", tc.wantErr, err)
			}
			// Nothing written after the refusal: the tree is exactly what the
			// concurrent edit left.
			assertUnchanged(t, mid, f.digests(), tc.shape)
		})
	}
}

// TestUpdateDriftRecordRefusesWhenATrackedOutputMovesDuringGeneration is the
// copy-back rule's sibling on the other verb that writes: the record describes
// the tree the generation was checked against, so a declared output edited
// while the generator runs refuses the record update, and no record is written.
func TestUpdateDriftRecordRefusesWhenATrackedOutputMovesDuringGeneration(t *testing.T) {
	f := guardFixture(t)
	gen := &fakeGenerator{fn: func(workDir string) error {
		f.write("gen/generated.go", headerLine+"\n\npackage gen\n// edited during -update\n")
		return rewriteOutputs("generated")(workDir)
	}}
	_, err := UpdateDriftRecord(context.Background(), guardOptions(f, gen))
	logCell(t, err, "record written")
	if err == nil || !strings.Contains(err.Error(), `"gen/generated.go" changed in the working tree`) {
		t.Fatalf("want a refusal naming the moved output, got %v", err)
	}
	if f.exists("contracts/expected-drift.record") {
		t.Fatal("a record was written for a tree that no longer exists")
	}
}
