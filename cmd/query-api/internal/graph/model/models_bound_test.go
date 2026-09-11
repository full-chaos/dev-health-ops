package model

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

// boundValueTypes are the four types gqlgen.yml binds by name, and the field on
// each whose tag is the reason for the binding.
var boundValueTypes = []struct {
	name  string
	value any
	field string
}{
	{"BreakdownItem", BreakdownItem{}, "Value"},
	{"SankeyEdge", SankeyEdge{}, "Value"},
	{"SankeyNode", SankeyNode{}, "Value"},
	{"TimeseriesBucket", TimeseriesBucket{}, "Value"},
	{"SankeyCoverage", SankeyCoverage{}, "DirectRepoCoverage"},
	{"SankeyCoverage", SankeyCoverage{}, "TeamFallbackRepoCoverage"},
	{"SankeyCoverage", SankeyCoverage{}, "RepoFanoutReposPerUnit"},
}

// boundTypes are the type names gqlgen.yml binds, which is a different list
// from the fields above: one type can carry several bound value fields.
var boundTypes = []string{"BreakdownItem", "SankeyEdge", "SankeyNode", "TimeseriesBucket", "SankeyCoverage"}

// TestABoundNullableValueRendersAsNullNotAbsent is the reason these four types
// are bound rather than generated, executed rather than described: gqlgen's
// model generator writes `,omitempty` for a nullable field, and that makes a
// nil DISAPPEAR from an encoding/json rendering instead of appearing as null.
func TestABoundNullableValueRendersAsNullNotAbsent(t *testing.T) {
	// The same struct twice: the tag this package declares, and the tag a
	// regeneration would write if the binding were dropped.
	type asBound struct {
		Key   string   `json:"key"`
		Value *float64 `json:"value"`
	}
	type asRegenerated struct {
		Key   string   `json:"key"`
		Value *float64 `json:"value,omitempty"`
	}

	bound, err := json.Marshal(asBound{Key: "k"})
	if err != nil {
		t.Fatal(err)
	}
	regenerated, err := json.Marshal(asRegenerated{Key: "k"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("CELL-OUTPUT: bound tag `json:\"value\"` -> %s", bound)
	t.Logf("CELL-OUTPUT: regenerated tag `json:\"value,omitempty\"` -> %s", regenerated)

	if got, want := string(bound), `{"key":"k","value":null}`; got != want {
		t.Fatalf("the bound tag rendered %s, want %s", got, want)
	}
	if got, want := string(regenerated), `{"key":"k"}`; got != want {
		t.Fatalf("the regenerated tag rendered %s, want %s", got, want)
	}
	if string(bound) == string(regenerated) {
		t.Fatal("the two tags render the same, so this cell proves nothing")
	}
}

// TestTheBoundTypesKeepTheirNullableValueTag pins what the binding is FOR. A
// regeneration that stopped honouring it would put `,omitempty` back on these
// fields, which is the edit this package exists to stop being re-made.
func TestTheBoundTypesKeepTheirNullableValueTag(t *testing.T) {
	for _, b := range boundValueTypes {
		t.Run(b.name+"."+b.field, func(t *testing.T) {
			f, ok := reflect.TypeOf(b.value).FieldByName(b.field)
			if !ok {
				t.Fatalf("%s has no field %s", b.name, b.field)
			}
			tag := f.Tag.Get("json")
			t.Logf("CELL-OUTPUT: %s.%s json tag = %q", b.name, b.field, tag)
			if strings.Contains(tag, ",") {
				t.Fatalf("%s.%s carries json tag %q, want the bare field name with no option "+
					"(an omitempty here turns an explicit null into an absent key)", b.name, b.field, tag)
			}
			if tag == "" {
				t.Fatalf("%s.%s carries no json tag at all", b.name, b.field)
			}
			if f.Type.Kind() != reflect.Pointer {
				t.Fatalf("%s.%s is %s, want a pointer so an unmeasured value stays distinguishable from zero",
					b.name, b.field, f.Type)
			}
		})
	}
}

// TestTheBoundTypesAreNotAlsoGenerated: the binding is what keeps these
// declarations out of the generated file. If gqlgen.yml lost it, gqlgen would
// emit its own copy and this package would not compile -- but a change that
// dropped the binding AND this file would compile and silently reintroduce
// `,omitempty`, so the two halves are asserted here as well as by the build.
func TestTheBoundTypesAreNotAlsoGenerated(t *testing.T) {
	generated, err := os.ReadFile("models_gen.go")
	if err != nil {
		t.Fatal(err)
	}
	bound, err := os.ReadFile("models_bound.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range boundTypes {
		decl := "type " + name + " struct {"
		if strings.Contains(string(generated), decl) {
			t.Errorf("%s is declared in models_gen.go: the gqlgen.yml binding was lost, and a "+
				"regeneration now decides this type's tags", name)
		}
		if !strings.Contains(string(bound), decl) {
			t.Errorf("%s is not declared in models_bound.go, so nothing in this package fixes its tags", name)
		}
	}
}

// TestTheCoverageSplitRendersItsUnmeasuredFieldsAsNull is the stated
// consequence of binding this type, executed: the three fields the coverage
// split adds used to carry `,omitempty` -- gqlgen's default, transcribed when
// they were hand-added rather than chosen -- so an unmeasured value left the
// key out. They render as explicit null now, which is the same rule the four
// value fields beside them already follow: a value the Go side computed as "no
// honest answer" reaches the wire as null in every rendering path.
func TestTheCoverageSplitRendersItsUnmeasuredFieldsAsNull(t *testing.T) {
	// The same five fields under the tags a regeneration would have written.
	type asRegenerated struct {
		TeamCoverage             float64  `json:"teamCoverage"`
		RepoCoverage             float64  `json:"repoCoverage"`
		DirectRepoCoverage       *float64 `json:"directRepoCoverage,omitempty"`
		TeamFallbackRepoCoverage *float64 `json:"teamFallbackRepoCoverage,omitempty"`
		RepoFanoutReposPerUnit   *float64 `json:"repoFanoutReposPerUnit,omitempty"`
	}

	bound, err := json.Marshal(SankeyCoverage{TeamCoverage: 0.5, RepoCoverage: 0.25})
	if err != nil {
		t.Fatal(err)
	}
	regenerated, err := json.Marshal(asRegenerated{TeamCoverage: 0.5, RepoCoverage: 0.25})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("CELL-OUTPUT: bound       -> %s", bound)
	t.Logf("CELL-OUTPUT: regenerated -> %s", regenerated)

	for _, key := range []string{"directRepoCoverage", "teamFallbackRepoCoverage", "repoFanoutReposPerUnit"} {
		if !strings.Contains(string(bound), `"`+key+`":null`) {
			t.Fatalf("the bound type omitted %s instead of rendering it null: %s", key, bound)
		}
		if strings.Contains(string(regenerated), key) {
			t.Fatalf("the regenerated tags kept %s, so this cell no longer shows the difference: %s", key, regenerated)
		}
	}
}
