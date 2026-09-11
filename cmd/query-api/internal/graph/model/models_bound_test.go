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
}

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
		t.Run(b.name, func(t *testing.T) {
			f, ok := reflect.TypeOf(b.value).FieldByName(b.field)
			if !ok {
				t.Fatalf("%s has no field %s", b.name, b.field)
			}
			tag := f.Tag.Get("json")
			t.Logf("CELL-OUTPUT: %s.%s json tag = %q", b.name, b.field, tag)
			if tag != "value" {
				t.Fatalf("%s.%s carries json tag %q, want exactly \"value\" (an omitempty here "+
					"turns an explicit null into an absent key)", b.name, b.field, tag)
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
	for _, b := range boundValueTypes {
		decl := "type " + b.name + " struct {"
		if strings.Contains(string(generated), decl) {
			t.Errorf("%s is declared in models_gen.go: the gqlgen.yml binding was lost, and a "+
				"regeneration now decides this type's tags", b.name)
		}
		if !strings.Contains(string(bound), decl) {
			t.Errorf("%s is not declared in models_bound.go, so nothing in this package fixes its tags", b.name)
		}
	}
}
