package gqlgenguard

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// TestEnumerateOutputsCoversEveryKnobThatDecidesAnOutputPath is the knob
// matrix. Every configuration setting that can move, add, remove or rename an
// output has a row here, including the ones that are only live under a
// particular layout and the ones whose effect comes from being ABSENT.
//
// Rows marked runReal are then generated for real, with the gqlgen CLI, and
// every file the generator actually wrote is checked to be inside the
// enumerated plan. That is the assertion that makes this a test of gqlgen's
// behaviour rather than a test of this package's opinion of it: a knob whose
// effect the enumeration models wrongly produces a file outside the plan and
// fails here, not in production.
func TestEnumerateOutputsCoversEveryKnobThatDecidesAnOutputPath(t *testing.T) {
	cases := []struct {
		name string
		// config is the gqlgen.yml body. {{DIR}} expands to the fixture root
		// and {{OUTSIDE}} to a directory beside it.
		config string
		// extraSchemas adds schema files beyond schema.graphql.
		extraSchemas map[string]string
		// wantOutputs is the exact set of declared output paths.
		wantOutputs []string
		// wantErr, when set, is a substring the refusal must contain.
		wantErr string
		// runReal also runs the gqlgen CLI and asserts every file it wrote is
		// in the plan.
		runReal bool
	}{
		{
			name: "single-file exec and single-file resolver, all filenames explicit",
			config: `
schema: [schema.graphql]
exec: {filename: gen/generated.go, package: gen}
model: {filename: gen/model/models_gen.go, package: model}
resolver: {layout: single-file, filename: gen/resolver.go, package: gen}
`,
			wantOutputs: []string{
				"gen/generated.go",
				"gen/model/models_gen.go",
				"gen/resolver.go",
			},
			runReal: true,
		},
		{
			name: "exec.filename OMITTED still writes: the default is an output",
			config: `
schema: [schema.graphql]
exec: {package: fixture}
model: {filename: model/models_gen.go, package: model}
resolver: {layout: single-file, filename: resolver.go, package: fixture}
`,
			// generated.go is gqlgen's DefaultConfig value, not something the
			// file says. An enumeration that read only the keys present would
			// miss it entirely.
			wantOutputs: []string{
				"generated.go",
				"model/models_gen.go",
				"resolver.go",
			},
			runReal: true,
		},
		{
			name: "model.filename OMITTED defaults to models_gen.go beside the config",
			config: `
schema: [schema.graphql]
exec: {filename: gen/generated.go, package: gen}
resolver: {layout: single-file, filename: gen/resolver.go, package: gen}
`,
			wantOutputs: []string{
				"gen/generated.go",
				"gen/resolver.go",
				"models_gen.go",
			},
			runReal: true,
		},
		{
			name: "model DISABLED with an empty filename declares no model output",
			config: `
schema: [schema.graphql]
exec: {filename: gen/generated.go, package: gen}
model: {filename: ""}
resolver: {layout: single-file, filename: gen/resolver.go, package: gen}
`,
			wantOutputs: []string{
				"gen/generated.go",
				"gen/resolver.go",
			},
		},
		{
			name: "resolver layout follow-schema makes dir and filename_template live",
			config: `
schema: [schema.graphql]
exec: {filename: gen/generated.go, package: gen}
model: {filename: gen/model/models_gen.go, package: model}
resolver: {layout: follow-schema, dir: gen, package: gen, filename_template: "{name}.resolvers.go"}
`,
			wantOutputs: []string{
				"gen/generated.go",
				"gen/model/models_gen.go",
				"gen/prelude.resolvers.go",
				"gen/resolver.go",
				"gen/schema.resolvers.go",
			},
			runReal: true,
		},
		{
			name: "resolver filename_template with a NON-.go extension is still an output",
			config: `
schema: [schema.graphql]
exec: {filename: gen/generated.go, package: gen}
model: {filename: gen/model/models_gen.go, package: model}
resolver: {layout: follow-schema, dir: gen, package: gen, filename_template: "{name}.txt"}
`,
			// A guard scoped to *.go would not see this file at all, which is
			// how a resolver could be written somewhere nothing scanned.
			wantOutputs: []string{
				"gen/generated.go",
				"gen/model/models_gen.go",
				"gen/prelude.txt",
				"gen/resolver.go",
				"gen/schema.txt",
			},
			runReal: true,
		},
		{
			name: "exec layout follow-schema writes one file per schema plus a root file",
			config: `
schema: [schema.graphql, extra.graphql]
exec: {layout: follow-schema, dir: gen, package: gen}
model: {filename: gen/model/models_gen.go, package: model}
resolver: {layout: follow-schema, dir: gen, package: gen}
`,
			extraSchemas: map[string]string{"extra.graphql": fixtureSchemaExtra},
			wantOutputs: []string{
				"gen/common!.generated.go",
				"gen/extra.generated.go",
				"gen/extra.resolvers.go",
				"gen/model/models_gen.go",
				"gen/prelude.generated.go",
				"gen/prelude.resolvers.go",
				"gen/resolver.go",
				"gen/root_.generated.go",
				"gen/schema.generated.go",
				"gen/schema.resolvers.go",
			},
			runReal: true,
		},
		{
			name: "exec follow-schema honours exec.filename_template, extension included",
			config: `
schema: [schema.graphql]
exec: {layout: follow-schema, dir: gen, package: gen, filename_template: "{name}_exec.go"}
model: {filename: gen/model/models_gen.go, package: model}
resolver: {layout: follow-schema, dir: gen, package: gen}
`,
			wantOutputs: []string{
				"gen/common!_exec.go",
				"gen/model/models_gen.go",
				"gen/prelude.resolvers.go",
				"gen/prelude_exec.go",
				"gen/resolver.go",
				"gen/root_.generated.go",
				"gen/schema.resolvers.go",
				"gen/schema_exec.go",
			},
			runReal: true,
		},
		{
			name: "an ABSOLUTE resolver.dir inside the module resolves to a path inside it",
			config: `
schema: [schema.graphql]
exec: {filename: gen/generated.go, package: gen}
model: {filename: gen/model/models_gen.go, package: model}
resolver: {layout: follow-schema, dir: {{DIR}}/elsewhere, package: elsewhere}
`,
			wantOutputs: []string{
				"elsewhere/prelude.resolvers.go",
				"elsewhere/resolver.go",
				"elsewhere/schema.resolvers.go",
				"gen/generated.go",
				"gen/model/models_gen.go",
			},
			runReal: true,
		},
		{
			name: "an ABSOLUTE resolver.dir OUTSIDE the module is refused",
			config: `
schema: [schema.graphql]
exec: {filename: gen/generated.go, package: gen}
model: {filename: gen/model/models_gen.go, package: model}
resolver: {layout: follow-schema, dir: {{OUTSIDE}}, package: escape}
`,
			wantErr: "resolves outside the module root",
		},
		{
			name: "an ABSOLUTE exec.filename outside the module is refused",
			config: `
schema: [schema.graphql]
exec: {filename: {{OUTSIDE}}/generated.go, package: gen}
model: {filename: gen/model/models_gen.go, package: model}
resolver: {layout: single-file, filename: gen/resolver.go, package: gen}
`,
			wantErr: "resolves outside the module root",
		},
		{
			name: "a resolver filename_template carrying ../ out of the module is refused",
			config: `
schema: [schema.graphql]
exec: {filename: gen/generated.go, package: gen}
model: {filename: gen/model/models_gen.go, package: model}
resolver: {layout: follow-schema, dir: gen, package: gen, filename_template: "../../{name}.go"}
`,
			wantErr: "resolves outside the module root",
		},
		{
			name: "an exec filename_template carrying ../ out of the module is refused",
			config: `
schema: [schema.graphql]
exec: {layout: follow-schema, dir: gen, package: gen, filename_template: "../../{name}.go"}
model: {filename: gen/model/models_gen.go, package: model}
resolver: {layout: follow-schema, dir: gen, package: gen}
`,
			wantErr: "resolves outside the module root",
		},
		{
			name: "two sections resolving to the same file are refused as a collision",
			config: `
schema: [schema.graphql]
exec: {filename: gen/resolver.go, package: gen}
model: {filename: gen/model/models_gen.go, package: model}
resolver: {layout: single-file, filename: gen/resolver.go, package: gen}
`,
			wantErr: "collision",
		},
		{
			name: "federation adds its own file AND the fixed requires file beside it",
			config: `
schema: [schema.graphql]
exec: {filename: gen/generated.go, package: gen}
model: {filename: gen/model/models_gen.go, package: model}
federation: {filename: gen/federation.go, package: gen}
resolver: {layout: single-file, filename: gen/resolver.go, package: gen}
`,
			wantOutputs: []string{
				"gen/federation.go",
				"gen/federation.requires.go",
				"gen/generated.go",
				"gen/model/models_gen.go",
				"gen/resolver.go",
			},
			runReal: true,
		},
		{
			name: "a schema GLOB decides how many resolver files the template produces",
			config: `
schema: ["graphql/*.graphql"]
exec: {filename: gen/generated.go, package: gen}
model: {filename: gen/model/models_gen.go, package: model}
resolver: {layout: follow-schema, dir: gen, package: gen}
`,
			extraSchemas: map[string]string{
				"graphql/a.graphql": fixtureSchema,
				"graphql/b.graphql": fixtureSchemaExtra,
			},
			wantOutputs: []string{
				"gen/a.resolvers.go",
				"gen/b.resolvers.go",
				"gen/generated.go",
				"gen/model/models_gen.go",
				"gen/prelude.resolvers.go",
				"gen/resolver.go",
			},
			runReal: true,
		},
		{
			// A layout with neither dir nor filename leaves the resolver
			// section UNDEFINED, and gqlgen's own resolvergen returns early for
			// an undefined section rather than erroring. The plan says the same
			// thing, and the real generator below proves it writes no resolver.
			name: "resolver layout with neither dir nor filename declares no resolver output",
			config: `
schema: [schema.graphql]
exec: {filename: gen/generated.go, package: gen}
model: {filename: gen/model/models_gen.go, package: model}
resolver: {layout: follow-schema, package: gen}
`,
			wantOutputs: []string{
				"gen/generated.go",
				"gen/model/models_gen.go",
			},
			runReal: true,
		},
		{
			name: "resolver dir set but layout single-file with no filename is refused by gqlgen itself",
			config: `
schema: [schema.graphql]
exec: {filename: gen/generated.go, package: gen}
model: {filename: gen/model/models_gen.go, package: model}
resolver: {layout: single-file, dir: gen, package: gen}
`,
			wantErr: "filename must be specified",
		},
		{
			name: "an out-of-vocabulary resolver layout is refused by gqlgen itself",
			config: `
schema: [schema.graphql]
exec: {filename: gen/generated.go, package: gen}
model: {filename: gen/model/models_gen.go, package: model}
resolver: {layout: sideways, dir: gen, package: gen}
`,
			wantErr: "invalid layout",
		},
		{
			name: "an unknown configuration key is refused rather than ignored",
			config: `
schema: [schema.graphql]
exec: {filename: gen/generated.go, package: gen}
resolver: {layout: single-file, filename: gen/resolver.go, package: gen}
resolvers: {dir: sneaky}
`,
			wantErr: "load gqlgen config",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			files := map[string]string{"schema.graphql": fixtureSchema}
			for rel, body := range tc.extraSchemas {
				files[rel] = body
			}
			f := newFixture(t, files)
			outside := filepath.Join(filepath.Dir(f.dir), "outside-the-module")
			if err := os.MkdirAll(outside, 0o755); err != nil {
				t.Fatalf("create the outside directory: %v", err)
			}
			cfg := strings.NewReplacer("{{DIR}}", f.dir, "{{OUTSIDE}}", outside).Replace(tc.config)
			f.write("gqlgen.yml", cfg)

			plan, err := EnumerateOutputs(f.dir, "gqlgen.yml")
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected a refusal containing %q, got plan %v", tc.wantErr, planPaths(plan))
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("refusal %q does not contain %q", err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("enumerate: %v", err)
			}
			got := planPaths(plan)
			if !equalStrings(got, tc.wantOutputs) {
				t.Fatalf("declared outputs:\n  got  %v\n  want %v", got, tc.wantOutputs)
			}

			if !tc.runReal {
				return
			}
			assertRealGenerationStaysInsideThePlan(t, f, plan)
		})
	}
}

// assertRealGenerationStaysInsideThePlan runs the gqlgen CLI in the fixture
// and fails if it wrote anything the plan did not declare.
//
// This is what stops the enumeration from being an opinion: a knob whose
// effect the enumeration models wrongly produces a file outside the plan and
// fails here rather than in production. The generator runs in the fixture
// itself, not a copy, because a config may name an ABSOLUTE output directory,
// and an absolute path is not relocated by copying -- generating in a copy
// would silently test a different configuration from the one enumerated.
func assertRealGenerationStaysInsideThePlan(t *testing.T, f *fixture, plan *Plan) {
	t.Helper()
	goAvailable(t)
	f.withModuleFiles()

	root, err := os.OpenRoot(f.dir)
	if err != nil {
		t.Fatalf("open fixture root: %v", err)
	}
	defer root.Close()

	before, err := TakeSnapshot(root, skipVCS)
	if err != nil {
		t.Fatalf("snapshot fixture: %v", err)
	}

	var out strings.Builder
	gen := NewGoRunGenerator(&out, &out)
	if err := gen.Generate(context.Background(), f.dir); err != nil {
		t.Fatalf("the real generator failed in the fixture: %v\ngenerator output:\n%s", err, out.String())
	}

	after, err := TakeSnapshot(root, skipVCS)
	if err != nil {
		t.Fatalf("snapshot fixture after generating: %v", err)
	}

	declared := plan.PathSet()
	var outside, wrote []string
	for _, rel := range changedPaths(before, after) {
		if moduleFileNames[filepath.Base(rel)] {
			continue
		}
		wrote = append(wrote, rel)
		if _, ok := declared[rel]; !ok {
			outside = append(outside, rel)
		}
	}
	if len(outside) > 0 {
		t.Fatalf("the real generator wrote %d path(s) the plan does not declare: %v\nplan: %v\nall written: %v",
			len(outside), outside, planPaths(plan), wrote)
	}
	if len(wrote) == 0 {
		t.Fatalf("the real generator wrote nothing, so this row proves nothing about the plan")
	}
	t.Logf("the real generator wrote %d path(s), every one inside the plan: %v", len(wrote), wrote)

	// Every file gqlgen just wrote must pass the guard's own provenance check on
	// the NEXT run. This is what keeps the anchored shapes honest: they are
	// typed out in the guard, and a gqlgen that wrote a notice in a shape the
	// guard does not know -- a layout it forgot, a version that moved a line --
	// fails here, against the real generator, instead of refusing a real
	// checkout's own generated files as hand-written.
	var decisions strings.Builder
	if err := refuseHandWrittenCollisions(root, plan, &decisions); err != nil {
		t.Fatalf("a file the REAL generator wrote fails the guard's own provenance check: %v", err)
	}
	for _, line := range strings.Split(strings.TrimSpace(decisions.String()), "\n") {
		t.Logf("PROVENANCE-ROUNDTRIP: %s", line)
	}
}

func planPaths(p *Plan) []string {
	if p == nil {
		return nil
	}
	out := make([]string, 0, len(p.Outputs))
	for _, o := range p.Outputs {
		out = append(out, o.Path)
	}
	sort.Strings(out)
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
