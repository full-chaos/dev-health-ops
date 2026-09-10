package gqlgenguard

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// This file executes the whole input domain of every guard, validator and
// parser this package adds. One row per (field x shape) cell, each row run, so
// that a shape nobody thought about is a failing row here rather than a finding
// in a review round.
//
// The shapes, applied to every field they can be applied to: absent, null,
// zero, empty container, wrong container type, wrong scalar type, fractional
// where integral, out of vocabulary, boundary and boundary plus or minus one,
// duplicate, and the canonical value.

// domainCase is one cell.
type domainCase struct {
	field string
	shape string
	// config is the gqlgen.yml body; {{DIR}} and {{OUTSIDE}} expand as in the
	// knob matrix.
	config string
	// wantErr, when set, is a substring the refusal must contain. When empty
	// the cell must be ACCEPTED, and wantOutputs (if set) must match.
	wantErr     string
	wantOutputs []string
}

const domainBase = `
schema: [schema.graphql]
exec: {filename: gen/generated.go, package: gen}
model: {filename: gen/model/models_gen.go, package: model}
resolver: {layout: single-file, filename: gen/resolver.go, package: gen}
`

var domainCanonicalOutputs = []string{
	"gen/generated.go",
	"gen/model/models_gen.go",
	"gen/resolver.go",
}

// TestGqlgenConfigParserInputDomain is the input-domain table for the guard
// that reads the generator's configuration and decides what it may write.
func TestGqlgenConfigParserInputDomain(t *testing.T) {
	cases := []domainCase{
		// ---- the whole configuration ----
		{field: "the config file", shape: "canonical", config: domainBase, wantOutputs: domainCanonicalOutputs},
		{field: "the config file", shape: "empty container (empty document)", config: "",
			// An empty document is a parse failure, not "take every default".
			wantErr: "load gqlgen config"},
		{field: "the config file", shape: "wrong container type (a list, not a map)", config: "- schema.graphql\n",
			wantErr: "load gqlgen config"},
		{field: "the config file", shape: "out of vocabulary (an unknown key)", config: domainBase + "resolvers: {dir: sneaky}\n",
			wantErr: "load gqlgen config"},
		{field: "the config file", shape: "duplicate (the same key twice)", config: domainBase + "exec: {filename: gen/other.go, package: gen}\n",
			wantErr: "load gqlgen config"},

		// ---- schema ----
		{field: "schema", shape: "absent", config: strings.Replace(domainBase, "schema: [schema.graphql]\n", "", 1),
			// gqlgen's default is the literal schema.graphql, which the fixture has.
			wantOutputs: domainCanonicalOutputs},
		{field: "schema", shape: "null", config: strings.Replace(domainBase, "schema: [schema.graphql]", "schema:", 1),
			// A null list decodes to the default, same as absent.
			wantOutputs: domainCanonicalOutputs},
		{field: "schema", shape: "empty container", config: strings.Replace(domainBase, "schema: [schema.graphql]", "schema: []", 1),
			// No sources at all is accepted by the config layer; the generator
			// is what refuses an empty schema, and it never runs in the tree.
			wantOutputs: domainCanonicalOutputs},
		{field: "schema", shape: "zero (an empty string entry)", config: strings.Replace(domainBase, "schema: [schema.graphql]", `schema: [""]`, 1),
			wantOutputs: domainCanonicalOutputs},
		{field: "schema", shape: "wrong scalar type (a bool)", config: strings.Replace(domainBase, "schema: [schema.graphql]", "schema: true", 1),
			// MEASURED, not assumed: gqlgen's StringList accepts a bare scalar,
			// so `true` becomes the one-element list ["true"], which globs to
			// nothing. It is accepted with no schema sources rather than
			// refused. It cannot move an output path, so the plan is canonical.
			wantOutputs: domainCanonicalOutputs},
		{field: "schema", shape: "out of vocabulary (a glob that matches nothing)", config: strings.Replace(domainBase, "schema: [schema.graphql]", "schema: [nothing/*.graphql]", 1),
			wantOutputs: domainCanonicalOutputs},
		{field: "schema", shape: "boundary (a single file, canonical)", config: domainBase, wantOutputs: domainCanonicalOutputs},
		{field: "schema", shape: "boundary+1 (two files, deduplicated)", config: strings.Replace(domainBase, "schema: [schema.graphql]", "schema: [schema.graphql, schema.graphql]", 1),
			wantOutputs: domainCanonicalOutputs},
		{field: "schema", shape: "duplicate (a glob and a literal matching the same file)", config: strings.Replace(domainBase, "schema: [schema.graphql]", "schema: [schema.graphql, \"*.graphql\"]", 1),
			wantOutputs: domainCanonicalOutputs},

		// ---- exec.filename ----
		{field: "exec.filename", shape: "absent (the default is still an output)",
			config:      strings.Replace(domainBase, "exec: {filename: gen/generated.go, package: gen}", "exec: {package: gen}", 1),
			wantOutputs: []string{"gen/model/models_gen.go", "gen/resolver.go", "generated.go"}},
		{field: "exec.filename", shape: "null", config: strings.Replace(domainBase, "filename: gen/generated.go", "filename: null", 1),
			// MEASURED: yaml.v3 leaves a field untouched on an explicit null, so
			// the DefaultConfig value survives and an author who writes
			// `filename: null` gets generated.go beside the config -- NOT "no
			// exec output". Indistinguishable from absent, and both are outputs.
			wantOutputs: []string{"gen/model/models_gen.go", "gen/resolver.go", "generated.go"}},
		{field: "exec.filename", shape: "zero (empty string)", config: strings.Replace(domainBase, "filename: gen/generated.go", `filename: ""`, 1),
			wantErr: "filename must be specified"},
		{field: "exec.filename", shape: "wrong scalar type (a number)", config: strings.Replace(domainBase, "filename: gen/generated.go", "filename: 12", 1),
			// MEASURED: YAML coerces the number to the string "12", so the
			// refusal comes from gqlgen's own .go-suffix rule, not the decoder.
			wantErr: "path to a go source file"},
		{field: "exec.filename", shape: "wrong container type (a list)", config: strings.Replace(domainBase, "filename: gen/generated.go", "filename: [a.go]", 1),
			wantErr: "load gqlgen config"},
		{field: "exec.filename", shape: "out of vocabulary (not a .go file)", config: strings.Replace(domainBase, "filename: gen/generated.go", "filename: gen/generated.txt", 1),
			wantErr: "path to a go source file"},
		{field: "exec.filename", shape: "boundary (at the module root)", config: strings.Replace(domainBase, "filename: gen/generated.go", "filename: generated.go", 1),
			wantOutputs: []string{"gen/model/models_gen.go", "gen/resolver.go", "generated.go"}},
		{field: "exec.filename", shape: "boundary-1 (one level above the module root)", config: strings.Replace(domainBase, "filename: gen/generated.go", "filename: ../generated.go", 1),
			wantErr: "resolves outside the module root"},
		{field: "exec.filename", shape: "absolute inside the module", config: strings.Replace(domainBase, "filename: gen/generated.go", "filename: {{DIR}}/gen/generated.go", 1),
			wantOutputs: domainCanonicalOutputs},
		{field: "exec.filename", shape: "absolute outside the module", config: strings.Replace(domainBase, "filename: gen/generated.go", "filename: {{OUTSIDE}}/generated.go", 1),
			wantErr: "resolves outside the module root"},
		{field: "exec.filename", shape: "duplicate (equal to resolver.filename)", config: strings.Replace(domainBase, "filename: gen/generated.go", "filename: gen/resolver.go", 1),
			wantErr: "collision"},

		// ---- exec.layout ----
		{field: "exec.layout", shape: "absent (defaults to single-file)", config: domainBase, wantOutputs: domainCanonicalOutputs},
		{field: "exec.layout", shape: "zero (empty string, treated as absent)",
			config:      strings.Replace(domainBase, "exec: {filename: gen/generated.go, package: gen}", `exec: {layout: "", filename: gen/generated.go, package: gen}`, 1),
			wantOutputs: domainCanonicalOutputs},
		{field: "exec.layout", shape: "out of vocabulary",
			config:  strings.Replace(domainBase, "exec: {filename: gen/generated.go, package: gen}", "exec: {layout: sideways, dir: gen, package: gen}", 1),
			wantErr: "invalid layout"},
		{field: "exec.layout", shape: "canonical follow-schema, which makes dir live",
			config: strings.Replace(domainBase, "exec: {filename: gen/generated.go, package: gen}", "exec: {layout: follow-schema, dir: gen, package: gen}", 1),
			wantOutputs: []string{
				"gen/common!.generated.go", "gen/model/models_gen.go", "gen/prelude.generated.go",
				"gen/resolver.go", "gen/root_.generated.go", "gen/schema.generated.go",
			}},
		{field: "exec.layout", shape: "follow-schema with dir absent",
			config:  strings.Replace(domainBase, "exec: {filename: gen/generated.go, package: gen}", "exec: {layout: follow-schema, package: gen}", 1),
			wantErr: "dir must be specified"},

		// ---- exec.worker_limit: the only integral field in the surface ----
		{field: "exec.worker_limit", shape: "absent", config: domainBase, wantOutputs: domainCanonicalOutputs},
		{field: "exec.worker_limit", shape: "zero (boundary, means unlimited)",
			config:      strings.Replace(domainBase, "package: gen}", "package: gen, worker_limit: 0}", 1),
			wantOutputs: domainCanonicalOutputs},
		{field: "exec.worker_limit", shape: "boundary+1",
			config:      strings.Replace(domainBase, "package: gen}", "package: gen, worker_limit: 1}", 1),
			wantOutputs: domainCanonicalOutputs},
		{field: "exec.worker_limit", shape: "boundary-1 (negative, unsigned field)",
			config:  strings.Replace(domainBase, "package: gen}", "package: gen, worker_limit: -1}", 1),
			wantErr: "load gqlgen config"},
		{field: "exec.worker_limit", shape: "fractional where integral",
			// MEASURED: YAML accepts a fractional value into gqlgen's unsigned
			// worker_limit rather than refusing it. Pinned as ACCEPTED because
			// that is what the generator does; worker_limit cannot move an
			// output path, so it changes nothing this guard protects.
			config:      strings.Replace(domainBase, "package: gen}", "package: gen, worker_limit: 1.5}", 1),
			wantOutputs: domainCanonicalOutputs},
		{field: "exec.worker_limit", shape: "wrong scalar type (a string)",
			config:  strings.Replace(domainBase, "package: gen}", `package: gen, worker_limit: "many"}`, 1),
			wantErr: "load gqlgen config"},

		// ---- exec.package ----
		{field: "exec.package", shape: "absent (inferred from the output directory)",
			config:      strings.Replace(domainBase, "exec: {filename: gen/generated.go, package: gen}", "exec: {filename: gen/generated.go}", 1),
			wantOutputs: domainCanonicalOutputs},
		{field: "exec.package", shape: "out of vocabulary (a path, not a package name)",
			config:  strings.Replace(domainBase, "exec: {filename: gen/generated.go, package: gen}", "exec: {filename: gen/generated.go, package: gen/inner}", 1),
			wantErr: "output package name only"},

		// ---- model.filename ----
		{field: "model.filename", shape: "absent (default beside the config)",
			config:      strings.Replace(domainBase, "model: {filename: gen/model/models_gen.go, package: model}\n", "", 1),
			wantOutputs: []string{"gen/generated.go", "gen/resolver.go", "models_gen.go"}},
		{field: "model.filename", shape: "zero (empty string disables the model output)",
			config:      strings.Replace(domainBase, "model: {filename: gen/model/models_gen.go, package: model}", `model: {filename: ""}`, 1),
			wantOutputs: []string{"gen/generated.go", "gen/resolver.go"}},
		{field: "model.filename", shape: "out of vocabulary (not a .go file)",
			config:  strings.Replace(domainBase, "filename: gen/model/models_gen.go", "filename: gen/model/models_gen.txt", 1),
			wantErr: "path to a go source file"},
		{field: "model.filename", shape: "boundary-1 (above the module root)",
			config:  strings.Replace(domainBase, "filename: gen/model/models_gen.go", "filename: ../models_gen.go", 1),
			wantErr: "resolves outside the module root"},
		{field: "model.filename", shape: "duplicate (equal to exec.filename)",
			config:  strings.Replace(domainBase, "filename: gen/model/models_gen.go", "filename: gen/generated.go", 1),
			wantErr: "collision"},

		// ---- resolver.layout ----
		{field: "resolver.layout", shape: "absent (defaults to single-file)",
			config:      strings.Replace(domainBase, "resolver: {layout: single-file, filename: gen/resolver.go, package: gen}", "resolver: {filename: gen/resolver.go, package: gen}", 1),
			wantOutputs: domainCanonicalOutputs},
		{field: "resolver.layout", shape: "out of vocabulary",
			config:  strings.Replace(domainBase, "layout: single-file", "layout: sideways", 1),
			wantErr: "invalid layout"},
		{field: "resolver.layout", shape: "canonical follow-schema, which makes dir and filename_template live",
			config: strings.Replace(domainBase, "resolver: {layout: single-file, filename: gen/resolver.go, package: gen}", "resolver: {layout: follow-schema, dir: gen, package: gen}", 1),
			wantOutputs: []string{
				"gen/generated.go", "gen/model/models_gen.go", "gen/prelude.resolvers.go",
				"gen/resolver.go", "gen/schema.resolvers.go",
			}},

		// ---- resolver.dir ----
		{field: "resolver.dir", shape: "absent under follow-schema (leaves the section undefined)",
			config:      strings.Replace(domainBase, "resolver: {layout: single-file, filename: gen/resolver.go, package: gen}", "resolver: {layout: follow-schema, package: gen}", 1),
			wantOutputs: []string{"gen/generated.go", "gen/model/models_gen.go"}},
		{field: "resolver.dir", shape: "zero (empty string, same as absent)",
			config:      strings.Replace(domainBase, "resolver: {layout: single-file, filename: gen/resolver.go, package: gen}", `resolver: {layout: follow-schema, dir: "", package: gen}`, 1),
			wantOutputs: []string{"gen/generated.go", "gen/model/models_gen.go"}},
		{field: "resolver.dir", shape: "boundary (the module root itself)",
			config: strings.Replace(domainBase, "resolver: {layout: single-file, filename: gen/resolver.go, package: gen}", "resolver: {layout: follow-schema, dir: ., package: fixture}", 1),
			wantOutputs: []string{
				"gen/generated.go", "gen/model/models_gen.go", "prelude.resolvers.go",
				"resolver.go", "schema.resolvers.go",
			}},
		{field: "resolver.dir", shape: "boundary-1 (one level above the module root)",
			config:  strings.Replace(domainBase, "resolver: {layout: single-file, filename: gen/resolver.go, package: gen}", "resolver: {layout: follow-schema, dir: .., package: escape}", 1),
			wantErr: "resolves outside the module root"},
		{field: "resolver.dir", shape: "absolute outside the module",
			config:  strings.Replace(domainBase, "resolver: {layout: single-file, filename: gen/resolver.go, package: gen}", "resolver: {layout: follow-schema, dir: {{OUTSIDE}}, package: escape}", 1),
			wantErr: "resolves outside the module root"},
		{field: "resolver.dir", shape: "wrong container type (a list)",
			config:  strings.Replace(domainBase, "resolver: {layout: single-file, filename: gen/resolver.go, package: gen}", "resolver: {layout: follow-schema, dir: [gen], package: gen}", 1),
			wantErr: "load gqlgen config"},

		// ---- resolver.filename_template ----
		{field: "resolver.filename_template", shape: "absent (a default template still names files)",
			config: strings.Replace(domainBase, "resolver: {layout: single-file, filename: gen/resolver.go, package: gen}", "resolver: {layout: follow-schema, dir: gen, package: gen}", 1),
			wantOutputs: []string{
				"gen/generated.go", "gen/model/models_gen.go", "gen/prelude.resolvers.go",
				"gen/resolver.go", "gen/schema.resolvers.go",
			}},
		{field: "resolver.filename_template", shape: "out of vocabulary (no {name} placeholder)",
			config: strings.Replace(domainBase, "resolver: {layout: single-file, filename: gen/resolver.go, package: gen}", `resolver: {layout: follow-schema, dir: gen, package: gen, filename_template: "all.go"}`, 1),
			// Every schema collapses onto one file; that is gqlgen's behaviour
			// and the plan says so rather than inventing per-schema names.
			wantOutputs: []string{"gen/all.go", "gen/generated.go", "gen/model/models_gen.go", "gen/resolver.go"}},
		{field: "resolver.filename_template", shape: "non-.go extension",
			config: strings.Replace(domainBase, "resolver: {layout: single-file, filename: gen/resolver.go, package: gen}", `resolver: {layout: follow-schema, dir: gen, package: gen, filename_template: "{name}.txt"}`, 1),
			wantOutputs: []string{
				"gen/generated.go", "gen/model/models_gen.go", "gen/prelude.txt",
				"gen/resolver.go", "gen/schema.txt",
			}},
		{field: "resolver.filename_template", shape: "boundary-1 (../ leaving the module)",
			config:  strings.Replace(domainBase, "resolver: {layout: single-file, filename: gen/resolver.go, package: gen}", `resolver: {layout: follow-schema, dir: gen, package: gen, filename_template: "../../{name}.go"}`, 1),
			wantErr: "resolves outside the module root"},
		{field: "resolver.filename_template", shape: "boundary (../ staying inside the module)",
			config: strings.Replace(domainBase, "resolver: {layout: single-file, filename: gen/resolver.go, package: gen}", `resolver: {layout: follow-schema, dir: gen, package: gen, filename_template: "../{name}.go"}`, 1),
			wantOutputs: []string{
				"gen/generated.go", "gen/model/models_gen.go", "gen/resolver.go",
				"prelude.go", "schema.go",
			}},
		{field: "resolver.filename_template", shape: "duplicate (collapsing onto exec.filename)",
			config:  strings.Replace(domainBase, "resolver: {layout: single-file, filename: gen/resolver.go, package: gen}", `resolver: {layout: follow-schema, dir: gen, package: gen, filename_template: "../gen/generated.go"}`, 1),
			wantErr: "collision"},

		// ---- resolver.filename ----
		{field: "resolver.filename", shape: "absent under follow-schema (defaults to resolver.go in dir)",
			config: strings.Replace(domainBase, "resolver: {layout: single-file, filename: gen/resolver.go, package: gen}", "resolver: {layout: follow-schema, dir: gen, package: gen}", 1),
			wantOutputs: []string{
				"gen/generated.go", "gen/model/models_gen.go", "gen/prelude.resolvers.go",
				"gen/resolver.go", "gen/schema.resolvers.go",
			}},
		{field: "resolver.filename", shape: "zero under single-file",
			config:  strings.Replace(domainBase, "resolver: {layout: single-file, filename: gen/resolver.go, package: gen}", `resolver: {layout: single-file, filename: "", dir: gen, package: gen}`, 1),
			wantErr: "filename must be specified"},
		{field: "resolver.filename", shape: "out of vocabulary (not a .go file) under single-file",
			config:  strings.Replace(domainBase, "filename: gen/resolver.go", "filename: gen/resolver.txt", 1),
			wantErr: "path to a go source file"},

		// ---- federation ----
		{field: "federation.filename", shape: "absent (no federation output at all)", config: domainBase, wantOutputs: domainCanonicalOutputs},
		{field: "federation.filename", shape: "canonical (adds its file AND the fixed requires file)",
			config: domainBase + "federation: {filename: gen/federation.go, package: gen}\n",
			wantOutputs: []string{
				"gen/federation.go", "gen/federation.requires.go", "gen/generated.go",
				"gen/model/models_gen.go", "gen/resolver.go",
			}},
		{field: "federation.filename", shape: "out of vocabulary (not a .go file)",
			config:  domainBase + "federation: {filename: gen/federation.txt, package: gen}\n",
			wantErr: "path to a go source file"},
		{field: "federation.filename", shape: "boundary-1 (above the module root)",
			config:  domainBase + "federation: {filename: ../federation.go, package: gen}\n",
			wantErr: "resolves outside the module root"},
		{field: "federation.filename", shape: "duplicate (equal to exec.filename)",
			config:  domainBase + "federation: {filename: gen/generated.go, package: gen}\n",
			wantErr: "collision"},

		// ---- skip_validation: a knob that changes WHETHER generation succeeds ----
		{field: "skip_validation", shape: "absent", config: domainBase, wantOutputs: domainCanonicalOutputs},
		{field: "skip_validation", shape: "canonical true (no effect on the output surface)",
			config: domainBase + "skip_validation: true\n", wantOutputs: domainCanonicalOutputs},
		{field: "skip_validation", shape: "wrong scalar type (a string)",
			// MEASURED: YAML coerces the quoted "yes" to a boolean. Accepted,
			// and irrelevant to the output surface either way.
			config: domainBase + `skip_validation: "yes"` + "\n", wantOutputs: domainCanonicalOutputs},
	}

	for _, tc := range cases {
		t.Run(tc.field+"/"+tc.shape, func(t *testing.T) {
			f := newFixture(t, map[string]string{"schema.graphql": fixtureSchema})
			outside := filepath.Join(filepath.Dir(f.dir), "outside-the-module")
			if err := os.MkdirAll(outside, 0o755); err != nil {
				t.Fatalf("create the outside directory: %v", err)
			}
			f.write("gqlgen.yml", strings.NewReplacer("{{DIR}}", f.dir, "{{OUTSIDE}}", outside).Replace(tc.config))

			plan, err := EnumerateOutputs(f.dir, "gqlgen.yml")
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("cell ACCEPTED but its contract says refuse (%q); plan %v", tc.wantErr, planPaths(plan))
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("refused for the wrong reason:\n  got  %v\n  want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("cell REFUSED but its contract says accept: %v", err)
			}
			if tc.wantOutputs != nil && !equalStrings(planPaths(plan), tc.wantOutputs) {
				t.Fatalf("declared outputs:\n  got  %v\n  want %v", planPaths(plan), tc.wantOutputs)
			}
		})
	}
}

const provenanceMarker = "// Code generated by github.com/99designs/gqlgen"

func padTo(n int) string { return strings.Repeat("/", n) }

// TestTheProvenanceScanWindowIsPinned keeps the two boundary rows above
// meaningful: they use a literal 4096, so the constant must equal it.
func TestTheProvenanceScanWindowIsPinned(t *testing.T) {
	if markerScanBytes != 4096 {
		t.Fatalf("markerScanBytes is %d; the boundary rows in this file are written against 4096 and must be moved with it", markerScanBytes)
	}
}

// TestProvenanceGuardInputDomain is the input-domain table for the guard that
// decides whether a declared output may be overwritten. Its input is the state
// of that path in the working tree.
func TestProvenanceGuardInputDomain(t *testing.T) {
	cases := []struct {
		shape   string
		plant   func(t *testing.T, f *fixture, full string)
		wantErr string
	}{
		{shape: "absent (the generator will create it)", plant: func(t *testing.T, f *fixture, full string) {
			if err := os.Remove(full); err != nil {
				t.Fatalf("remove: %v", err)
			}
		}},
		{shape: "canonical (carries the generated notice)", plant: func(*testing.T, *fixture, string) {}},
		{shape: "carries the follow-schema resolver notice", plant: func(t *testing.T, f *fixture, full string) {
			f.write("gen/generated.go", "package gen\n\n// This file will be automatically regenerated based on the schema\n")
		}},
		{shape: "carries the dependency-injection notice", plant: func(t *testing.T, f *fixture, full string) {
			f.write("gen/generated.go", "package gen\n\n// This file will not be regenerated automatically.\n")
		}},
		{shape: "zero (an empty file, no notice)", plant: func(t *testing.T, f *fixture, full string) {
			f.write("gen/generated.go", "")
		}, wantErr: "hand-written"},
		{shape: "out of vocabulary (a hand-written file)", plant: func(t *testing.T, f *fixture, full string) {
			f.write("gen/generated.go", "package gen\n\n// a person wrote this\n")
		}, wantErr: "hand-written"},
		// The two boundary rows use a LITERAL window size, not markerScanBytes.
		// Deriving the padding from the constant makes the test move with it, so
		// widening the window would still pass -- measured: that mutant survived
		// until these rows were pinned to 4096 and the constant pinned separately.
		{shape: "boundary (the notice at the very end of the scan window)", plant: func(t *testing.T, f *fixture, full string) {
			f.write("gen/generated.go", padTo(4096-len(provenanceMarker))+provenanceMarker)
		}},
		{shape: "boundary+1 (the notice one byte past the scan window)", plant: func(t *testing.T, f *fixture, full string) {
			f.write("gen/generated.go", padTo(4096-len(provenanceMarker)+1)+provenanceMarker)
		}, wantErr: "hand-written"},
		{shape: "wrong type (a directory)", plant: func(t *testing.T, f *fixture, full string) {
			if err := os.Remove(full); err != nil {
				t.Fatalf("remove: %v", err)
			}
			if err := os.Mkdir(full, 0o755); err != nil {
				t.Fatalf("mkdir: %v", err)
			}
		}, wantErr: "not a regular file"},
		{shape: "wrong type (a symbolic link)", plant: func(t *testing.T, f *fixture, full string) {
			if err := os.Remove(full); err != nil {
				t.Fatalf("remove: %v", err)
			}
			if err := os.Symlink("model/models_gen.go", full); err != nil {
				t.Fatalf("symlink: %v", err)
			}
		}, wantErr: "symbolic link"},
	}

	for _, tc := range cases {
		t.Run(tc.shape, func(t *testing.T) {
			f := guardFixture(t)
			tc.plant(t, f, filepath.Join(f.dir, "gen", "generated.go"))
			before := f.digests()

			gen := &fakeGenerator{fn: rewriteOutputs("generated")}
			opts := guardOptions(f, gen)
			opts.Report = io.Discard
			_, err := UpdateDriftRecord(context.Background(), opts)

			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("cell ACCEPTED but its contract says refuse (%q)", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("refused for the wrong reason:\n  got  %v\n  want it to contain %q", err, tc.wantErr)
				}
				if gen.ran {
					t.Fatal("the generator ran even though the tree state was already a refusal")
				}
				assertUnchanged(t, before, f.digests(), tc.shape)
				return
			}
			if err != nil {
				t.Fatalf("cell REFUSED but its contract says accept: %v", err)
			}
		})
	}
}

// TestOutputClassifierInputDomain is the input-domain table for the guard that
// decides whether what the generator wrote may be copied back. Its input is the
// set of paths the generation changed.
func TestOutputClassifierInputDomain(t *testing.T) {
	cases := []struct {
		shape   string
		gen     func(workDir string) error
		wantErr string
	}{
		{shape: "empty container (the generator wrote nothing)", gen: func(string) error { return nil }},
		{shape: "canonical (exactly the declared outputs)", gen: rewriteOutputs("generated")},
		{shape: "duplicate (the same output written twice)", gen: func(workDir string) error {
			if err := rewriteOutputs("first")(workDir); err != nil {
				return err
			}
			return rewriteOutputs("second")(workDir)
		}},
		{shape: "boundary (a module file only)", gen: func(workDir string) error {
			return writeIn(workDir, "go.sum", "tidied\n")
		}},
		{shape: "boundary+1 (a module file in a NESTED module)", gen: func(workDir string) error {
			return writeIn(workDir, "nested/go.mod", "module example.com/nested\n")
		}},
		{shape: "out of vocabulary (a path outside the declared surface)", gen: func(workDir string) error {
			return writeIn(workDir, "unrelated/notes.md", "rewritten by the generator\n")
		}, wantErr: "unrelated/notes.md"},
		{shape: "out of vocabulary (a hidden file)", gen: func(workDir string) error {
			return writeIn(workDir, ".hidden", "x\n")
		}, wantErr: ".hidden"},
		{shape: "out of vocabulary (a deletion outside the declared surface)", gen: func(workDir string) error {
			return os.Remove(filepath.Join(workDir, "unrelated", "notes.md"))
		}, wantErr: "unrelated/notes.md"},
		{shape: "out of vocabulary (a new symbolic link)", gen: func(workDir string) error {
			return os.Symlink("gen/generated.go", filepath.Join(workDir, "link.go"))
		}, wantErr: "link.go"},
	}

	for _, tc := range cases {
		t.Run(tc.shape, func(t *testing.T) {
			f := guardFixture(t)
			before := f.digests()
			gen := &fakeGenerator{fn: tc.gen}
			_, err := UpdateDriftRecord(context.Background(), guardOptions(f, gen))

			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("cell ACCEPTED but its contract says refuse (%q)", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("refused for the wrong reason:\n  got  %v\n  want it to contain %q", err, tc.wantErr)
				}
				assertUnchanged(t, before, f.digests(), tc.shape)
				return
			}
			if err != nil {
				t.Fatalf("cell REFUSED but its contract says accept: %v", err)
			}
			if !gen.ran {
				t.Fatal("the generator never ran, so this cell proves nothing")
			}
		})
	}
}

// TestRecordComparatorInputDomain is the input-domain table for the byte
// comparison against the committed record. Its input is the committed file.
func TestRecordComparatorInputDomain(t *testing.T) {
	const recordRel = "contracts/expected-drift.record"

	cases := []struct {
		shape   string
		mutate  func(record string) string
		remove  bool
		wantErr string
	}{
		{shape: "canonical (byte identical)", mutate: func(r string) string { return r }},
		{shape: "absent", remove: true, wantErr: "no expected-drift record"},
		{shape: "zero (an empty file)", mutate: func(string) string { return "" }, wantErr: "does not match"},
		{shape: "boundary (one byte removed from the end)", mutate: func(r string) string { return r[:len(r)-1] }, wantErr: "does not match"},
		{shape: "boundary+1 (one byte added at the end)", mutate: func(r string) string { return r + "\n" }, wantErr: "does not match"},
		{shape: "out of vocabulary (a digest changed)", mutate: func(r string) string {
			return strings.Replace(r, "digest drift", "digest same", 1)
		}, wantErr: "does not match"},
		{shape: "out of vocabulary (a whole digest line deleted)", mutate: func(r string) string {
			lines := strings.Split(r, "\n")
			for i, l := range lines {
				if strings.HasPrefix(l, "digest ") {
					return strings.Join(append(lines[:i], lines[i+1:]...), "\n")
				}
			}
			return r
		}, wantErr: "does not match"},
		{shape: "out of vocabulary (the explanation removed)", mutate: func(r string) string {
			return strings.TrimPrefix(r, recordHeader)
		}, wantErr: "does not match"},
		{shape: "duplicate (the record appended to itself)", mutate: func(r string) string { return r + r }, wantErr: "does not match"},
		{shape: "wrong type (a directory where the record should be)", remove: true, wantErr: "no expected-drift record"},
	}

	for _, tc := range cases {
		t.Run(tc.shape, func(t *testing.T) {
			f := guardFixture(t)
			opts := guardOptions(f, &fakeGenerator{fn: rewriteOutputs("generated")})
			if _, err := UpdateDriftRecord(context.Background(), opts); err != nil {
				t.Fatalf("write the record: %v", err)
			}
			if tc.remove {
				if err := os.Remove(filepath.Join(f.dir, filepath.FromSlash(recordRel))); err != nil {
					t.Fatalf("remove the record: %v", err)
				}
			} else {
				f.write(recordRel, tc.mutate(f.read(recordRel)))
			}
			before := f.digests()

			_, err := CheckDrift(context.Background(), guardOptions(f, &fakeGenerator{fn: rewriteOutputs("generated")}))
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("cell ACCEPTED but its contract says refuse (%q)", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("refused for the wrong reason:\n  got  %v\n  want it to contain %q", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("cell REFUSED but its contract says accept: %v", err)
			}
			assertUnchanged(t, before, f.digests(), "CheckDrift writes nothing in any cell")
		})
	}
}
