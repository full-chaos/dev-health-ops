package pybody

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// TestNewValidatorsMatchPydantic pins RequiredString/OptionalStringList/
// OptionalStringArrayDict/OptionalBoundedInt against real pydantic 2.13
// error shapes, captured live against `dev_health_ops.api`'s own installed
// pydantic (never hand-typed): list_type/dict_type/string_type per-element,
// int_type, greater_than_equal/less_than_equal, and the "missing" error's
// Input being the WHOLE parent object, not the field.
func TestNewValidatorsMatchPydantic(t *testing.T) {
	var errs Errors
	object, ok := errs.Object(Body{Value: mustDecode(t,
		`{"a":"not-a-list","b":[1,2],"c":"not-a-dict","d":{"k":"not-a-list"},"e":5,"f":-1,"g":"bad","h":1}`)})
	if !ok {
		t.Fatal("object refused")
	}
	errs.OptionalStringList(object, "a")
	errs.OptionalStringList(object, "b")
	errs.OptionalStringArrayDict(object, "c")
	errs.OptionalStringArrayDict(object, "d")
	errs.OptionalBoundedInt(object, "e", 0, 2)
	errs.OptionalBoundedInt(object, "f", 0, 2)
	errs.OptionalBoundedInt(object, "g", 0, 2)
	if _, ok := errs.RequiredString(object, "missing_field", 1, 0); ok {
		t.Error("missing_field should not validate")
	}

	rendered, _ := pyjson.Marshal(Detail(errs))
	want := `{"detail":[` +
		`{"type":"list_type","loc":["body","a"],"msg":"Input should be a valid list","input":"not-a-list"},` +
		`{"type":"string_type","loc":["body","b",0],"msg":"Input should be a valid string","input":1},` +
		`{"type":"string_type","loc":["body","b",1],"msg":"Input should be a valid string","input":2},` +
		`{"type":"dict_type","loc":["body","c"],"msg":"Input should be a valid dictionary","input":"not-a-dict"},` +
		`{"type":"list_type","loc":["body","d","k"],"msg":"Input should be a valid list","input":"not-a-list"},` +
		`{"type":"less_than_equal","loc":["body","e"],"msg":"Input should be less than or equal to 2","input":5,"ctx":{"le":2}},` +
		`{"type":"greater_than_equal","loc":["body","f"],"msg":"Input should be greater than or equal to 0","input":-1,"ctx":{"ge":0}},` +
		`{"type":"int_type","loc":["body","g"],"msg":"Input should be a valid integer","input":"bad"},` +
		`{"type":"missing","loc":["body","missing_field"],"msg":"Field required","input":` + mustRenderObject(t, object) + `}` +
		`]}`
	if string(rendered) != want {
		t.Fatalf("%s", rendered)
	}
}

// TestNewValidatorsHappyPath proves every helper returns the resolved Go
// value on valid input, and (present=false) on an absent/null field with
// zero errors recorded.
func TestNewValidatorsHappyPath(t *testing.T) {
	var errs Errors
	object, ok := errs.Object(Body{Value: mustDecode(t,
		`{"a":["x","y"],"b":{"github":["gh1","gh2"]},"c":1,"d":"team-1"}`)})
	if !ok {
		t.Fatal("object refused")
	}
	list, present := errs.OptionalStringList(object, "a")
	if !present || len(list) != 2 || list[0] != "x" || list[1] != "y" {
		t.Errorf("OptionalStringList: %v %v", list, present)
	}
	dict, present := errs.OptionalStringArrayDict(object, "b")
	if !present || len(dict["github"]) != 2 {
		t.Errorf("OptionalStringArrayDict: %v %v", dict, present)
	}
	value, present := errs.OptionalBoundedInt(object, "c", 0, 2)
	if !present || value != 1 {
		t.Errorf("OptionalBoundedInt: %v %v", value, present)
	}
	text, ok := errs.RequiredString(object, "d", 1, 0)
	if !ok || text != "team-1" {
		t.Errorf("RequiredString: %q %v", text, ok)
	}

	_, present = errs.OptionalStringList(object, "absent")
	if present {
		t.Error("OptionalStringList on an absent field must return present=false")
	}
	_, present = errs.OptionalStringArrayDict(object, "absent")
	if present {
		t.Error("OptionalStringArrayDict on an absent field must return present=false")
	}
	_, present = errs.OptionalBoundedInt(object, "absent", 0, 2)
	if present {
		t.Error("OptionalBoundedInt on an absent field must return present=false")
	}
	if len(errs) != 0 {
		t.Fatalf("happy path recorded errors: %+v", errs)
	}
}

func mustRenderObject(t *testing.T, object *pyjson.Object) string {
	t.Helper()
	rendered, err := pyjson.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	return string(rendered)
}
