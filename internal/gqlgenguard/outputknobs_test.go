package gqlgenguard

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestEveryOutputKnobIsSubjectToTheDotDotRule pins the CLASS the RISK-NOTES
// claim describes: the `..`-after-a-name rule holds for every output path a
// gqlgen config can name, not for the two the other tables happen to use.
// Each cell asserts the refusal NAMES ITS KNOB, so dropping one knob from the
// list the rule is applied to fails that cell alone.
func TestEveryOutputKnobIsSubjectToTheDotDotRule(t *testing.T) {
	const schema = "schema: [schema.graphql]\n"
	const model = "model: {filename: gen/model/models_gen.go, package: model}\n"
	const execSingle = "exec: {filename: gen/generated.go, package: gen}\n"
	const resolverSingle = "resolver: {layout: single-file, filename: gen/resolver.go, package: gen}\n"

	cells := []struct{ knob, config string }{
		{"exec.filename", schema + "exec: {filename: gen/../gen/generated.go, package: gen}\n" + model + resolverSingle},
		{"exec.dir", schema + "exec: {layout: follow-schema, dir: gen/../execdir, package: execdir}\n" + model + resolverSingle},
		{"exec.filename_template", schema + "exec: {filename: gen/generated.go, package: gen, filename_template: gen/../tmpl/exec.gotpl}\n" + model + resolverSingle},
		{"model.filename", schema + execSingle + "model: {filename: gen/../gen/model/models_gen.go, package: model}\n" + resolverSingle},
		{"federation.filename", schema + execSingle + model + resolverSingle + "federation: {filename: gen/../gen/federation.go, package: gen}\n"},
		{"resolver.filename", schema + execSingle + model + "resolver: {layout: single-file, filename: gen/../gen/resolver.go, package: gen}\n"},
		{"resolver.dir", schema + execSingle + model + "resolver: {layout: follow-schema, dir: gen/../resolvers, package: resolvers}\n"},
		{"resolver.filename_template", schema + execSingle + model + "resolver: {layout: single-file, filename: gen/resolver.go, package: gen, filename_template: gen/../tmpl/resolver.gotpl}\n"},
	}
	for i, c := range cells {
		t.Run(cellID("G23", i)+" "+c.knob, func(t *testing.T) {
			f := guardFixture(t)
			f.write("gqlgen.yml", c.config)
			_, err := EnumerateOutputs(f.dir, "gqlgen.yml")
			logCell(t, err, "accepted")
			if err == nil || !strings.Contains(err.Error(), "has a `..` after a directory name") {
				t.Fatalf("%s: want the `..` refusal, got %v", c.knob, err)
			}
			if !strings.Contains(err.Error(), c.knob) {
				t.Fatalf("the refusal does not name the knob %q: %v", c.knob, err)
			}
		})
	}
}

// TestTwoOutputsThatResolveToOneFileCollideThroughALinkToo: the collision check
// exists because "one would silently overwrite the other". Judged on the path
// as written, an in-module link makes two declarees name one file and the check
// says nothing — the copy reproduces in-module links, so the generator really
// does write both through it.
func TestTwoOutputsThatResolveToOneFileCollideThroughALink(t *testing.T) {
	f := guardFixture(t)
	if err := os.Symlink("gen", filepath.Join(f.dir, "genlink")); err != nil {
		t.Fatal(err)
	}
	f.write("gqlgen.yml", "schema: [schema.graphql]\n"+
		"exec: {filename: gen/generated.go, package: gen}\n"+
		"model: {filename: gen/model/models_gen.go, package: model}\n"+
		"resolver: {layout: single-file, filename: genlink/generated.go, package: gen}\n")
	_, err := EnumerateOutputs(f.dir, "gqlgen.yml")
	logCell(t, err, "accepted")
	if err == nil || !strings.Contains(err.Error(), "collision") {
		t.Fatalf("two declarees naming one file through a link were accepted: %v", err)
	}
	if !strings.Contains(err.Error(), "silently overwrite") {
		t.Fatalf("the refusal does not say what is at stake: %v", err)
	}
}
