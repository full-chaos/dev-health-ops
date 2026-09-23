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
		`{"type":"int_parsing","loc":["body","g"],"msg":"Input should be a valid integer, unable to parse string as an integer","input":"bad"},` +
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
	if !present || len(dict.Values["github"]) != 2 {
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

// TestDefaultedVariantsRejectNullMatchPydantic pins DefaultedStringList/
// DefaultedStringArrayDict/DefaultedAnyDict/DefaultedBoundedInt (the
// `Field(default_factory=...)`/`Field(default=...)` shape, WITHOUT `| None`
// in the pydantic model) against real pydantic 2.13, captured live: an
// explicit JSON null is itself a validation error for these fields, unlike
// the Optional* variants' `X | None` fields where null means "not provided".
func TestDefaultedVariantsRejectNullMatchPydantic(t *testing.T) {
	var errs Errors
	object, ok := errs.Object(Body{Value: mustDecode(t,
		`{"a":null,"b":null,"c":null,"d":null,"e":"not-a-dict"}`)})
	if !ok {
		t.Fatal("object refused")
	}
	errs.DefaultedStringList(object, "a")
	errs.DefaultedStringArrayDict(object, "b")
	errs.DefaultedAnyDict(object, "c")
	errs.DefaultedBoundedInt(object, "d", 0, 2)
	errs.DefaultedAnyDict(object, "e")

	rendered, _ := pyjson.Marshal(Detail(errs))
	want := `{"detail":[` +
		`{"type":"list_type","loc":["body","a"],"msg":"Input should be a valid list","input":null},` +
		`{"type":"dict_type","loc":["body","b"],"msg":"Input should be a valid dictionary","input":null},` +
		`{"type":"dict_type","loc":["body","c"],"msg":"Input should be a valid dictionary","input":null},` +
		`{"type":"int_type","loc":["body","d"],"msg":"Input should be a valid integer","input":null},` +
		`{"type":"dict_type","loc":["body","e"],"msg":"Input should be a valid dictionary","input":"not-a-dict"}` +
		`]}`
	if string(rendered) != want {
		t.Fatalf("%s", rendered)
	}
}

// TestDefaultedVariantsAbsentUsesDefault proves absent (not null) resolves
// to present=false with zero errors, for every Defaulted* variant -- the
// field's default applies, matching Field(default_factory=...)'s own
// behavior on a field FastAPI never received at all.
func TestDefaultedVariantsAbsentUsesDefault(t *testing.T) {
	var errs Errors
	object, ok := errs.Object(Body{Value: mustDecode(t, `{}`)})
	if !ok {
		t.Fatal("object refused")
	}
	if _, present := errs.DefaultedStringList(object, "a"); present {
		t.Error("DefaultedStringList on an absent field must return present=false")
	}
	if _, present := errs.DefaultedStringArrayDict(object, "b"); present {
		t.Error("DefaultedStringArrayDict on an absent field must return present=false")
	}
	if _, present := errs.DefaultedAnyDict(object, "c"); present {
		t.Error("DefaultedAnyDict on an absent field must return present=false")
	}
	if _, present := errs.DefaultedBoundedInt(object, "d", 0, 2); present {
		t.Error("DefaultedBoundedInt on an absent field must return present=false")
	}
	if len(errs) != 0 {
		t.Fatalf("absent-field pass recorded errors: %+v", errs)
	}
}

// TestBoundedIntCoercionMatchesPydantic pins OptionalBoundedInt's lax-mode
// coercion against real pydantic 2.13, captured live: a clean numeric
// string coerces, a whole-number float coerces, a fractional float is
// "int_from_float", and a string that does not parse as an integer
// (including a fractional one) is "int_parsing" -- not the same error as a
// wrong JSON type ("int_type", reserved for e.g. a bool/list/object).
func TestBoundedIntCoercionMatchesPydantic(t *testing.T) {
	var errs Errors
	object, ok := errs.Object(Body{Value: mustDecode(t,
		`{"a":"1","b":1.0,"c":1.5,"d":"1.5","e":"abc"}`)})
	if !ok {
		t.Fatal("object refused")
	}
	a, presentA := errs.OptionalBoundedInt(object, "a", 0, 2)
	b, presentB := errs.OptionalBoundedInt(object, "b", 0, 2)
	errs.OptionalBoundedInt(object, "c", 0, 2)
	errs.OptionalBoundedInt(object, "d", 0, 2)
	errs.OptionalBoundedInt(object, "e", 0, 2)
	if !presentA || a != 1 {
		t.Errorf(`OptionalBoundedInt("1"): value=%d present=%v, want 1/true`, a, presentA)
	}
	if !presentB || b != 1 {
		t.Errorf("OptionalBoundedInt(1.0): value=%d present=%v, want 1/true", b, presentB)
	}
	rendered, _ := pyjson.Marshal(Detail(errs))
	want := `{"detail":[` +
		`{"type":"int_from_float","loc":["body","c"],"msg":"Input should be a valid integer, got a number with a fractional part","input":1.5},` +
		`{"type":"int_parsing","loc":["body","d"],"msg":"Input should be a valid integer, unable to parse string as an integer","input":"1.5"},` +
		`{"type":"int_parsing","loc":["body","e"],"msg":"Input should be a valid integer, unable to parse string as an integer","input":"abc"}` +
		`]}`
	if string(rendered) != want {
		t.Fatalf("%s", rendered)
	}
}

// TestBoundedIntRejectsValuesBeyondInt64 is CHAOS-6310 r2 finding #1's
// direct reproduction: a live venue run sent sync_policy=2**64+1 (an
// ordinary JSON integer, unbounded in Python) and Go accepted it as team
// row "overflow-policy" (200) while Python answered 422 "less_than_equal".
// pyjson.Int's underlying *big.Int carries 2**64+1 exactly; the bug was
// converting it to int64 (which wraps, undefined per math/big's own docs)
// BEFORE the bounds check, so the wrapped value (1) passed a `le: 2` bound
// meant to reject it. Also covers the symmetric case (far below minValue)
// and a JSON-float and a decimal-string carrying the same out-of-range
// magnitude, since the fix (compare with math/big before ever narrowing to
// int64) is one code path for all three input shapes.
func TestBoundedIntRejectsValuesBeyondInt64(t *testing.T) {
	huge := "18446744073709551617" // 2**64 + 1
	veryNegative := "-18446744073709551617"
	body := `{"json_int":` + huge + `,"json_int_neg":` + veryNegative +
		`,"json_float":1.8446744073709552e19,"str_int":"` + huge + `"}`
	var errs Errors
	object, ok := errs.Object(Body{Value: mustDecode(t, body)})
	if !ok {
		t.Fatal("object refused")
	}
	if _, present := errs.OptionalBoundedInt(object, "json_int", 0, 2); present {
		t.Error(`OptionalBoundedInt(2**64+1, le=2): present=true, want a rejected greater-than-max value`)
	}
	if _, present := errs.OptionalBoundedInt(object, "json_int_neg", 0, 2); present {
		t.Error(`OptionalBoundedInt(-(2**64+1), ge=0): present=true, want a rejected less-than-min value`)
	}
	if _, present := errs.OptionalBoundedInt(object, "json_float", 0, 2); present {
		t.Error(`OptionalBoundedInt(1.8446744073709552e19, le=2): present=true, want a rejected out-of-range value`)
	}
	if _, present := errs.OptionalBoundedInt(object, "str_int", 0, 2); present {
		t.Error(`OptionalBoundedInt("2**64+1", le=2): present=true, want a rejected out-of-range value`)
	}
	for _, e := range errs {
		if e.Type != "less_than_equal" && e.Type != "greater_than_equal" {
			t.Errorf("unexpected error type %q for %v (loc %v) -- an out-of-range magnitude must fail the BOUNDS check, never int_parsing/int_type/int_from_float",
				e.Type, e.Input, e.Loc)
		}
	}
	if len(errs) != 4 {
		t.Fatalf("got %d errors, want 4: %+v", len(errs), errs)
	}
}

// TestEveryOptionalHelperTreatsNullAndAbsentAlike is the shared-package
// table test CHAOS-6310 r1 finding #1 asked for (R299): every `Optional*`
// helper's field is declared `X | None` in the pydantic model it mirrors,
// so an absent key and an explicit JSON null both mean "not provided" --
// present=false, zero errors -- and only a genuinely present, validly
// shaped value is present=true. One row per helper; every consumer of
// these helpers inherits this proof, not just teamsidentity.
func TestEveryOptionalHelperTreatsNullAndAbsentAlike(t *testing.T) {
	object, ok := (&Errors{}).Object(Body{Value: mustDecode(t,
		`{"str":"x","bool":true,"int":1,"list":["x"],"dict":{"p":["v"]},"bint":1,"any":{"k":1},`+
			`"str_null":null,"bool_null":null,"int_null":null,"list_null":null,"dict_null":null,"bint_null":null,"any_null":null}`)})
	if !ok {
		t.Fatal("object refused")
	}

	t.Run("OptionalString", func(t *testing.T) {
		var errs Errors
		if _, present := errs.OptionalString(object, "absent", 0, 0); present {
			t.Error("absent must be present=false")
		}
		if _, present := errs.OptionalString(object, "str_null", 0, 0); present {
			t.Error("null must be present=false")
		}
		if v, present := errs.OptionalString(object, "str", 0, 0); !present || v != "x" {
			t.Errorf("present value: got %q/%v", v, present)
		}
		if len(errs) != 0 {
			t.Errorf("recorded errors on valid input: %+v", errs)
		}
	})
	t.Run("OptionalBool", func(t *testing.T) {
		var errs Errors
		if _, present := errs.OptionalBool(object, "absent"); present {
			t.Error("absent must be present=false")
		}
		if _, present := errs.OptionalBool(object, "bool_null"); present {
			t.Error("null must be present=false")
		}
		if v, present := errs.OptionalBool(object, "bool"); !present || !v {
			t.Errorf("present value: got %v/%v", v, present)
		}
		if len(errs) != 0 {
			t.Errorf("recorded errors on valid input: %+v", errs)
		}
	})
	t.Run("OptionalInt", func(t *testing.T) {
		var errs Errors
		if _, present := errs.OptionalInt(object, "absent"); present {
			t.Error("absent must be present=false")
		}
		if _, present := errs.OptionalInt(object, "int_null"); present {
			t.Error("null must be present=false")
		}
		if v, present := errs.OptionalInt(object, "int"); !present || v != 1 {
			t.Errorf("present value: got %d/%v", v, present)
		}
		if len(errs) != 0 {
			t.Errorf("recorded errors on valid input: %+v", errs)
		}
	})
	t.Run("OptionalStringList", func(t *testing.T) {
		var errs Errors
		if _, present := errs.OptionalStringList(object, "absent"); present {
			t.Error("absent must be present=false")
		}
		if _, present := errs.OptionalStringList(object, "list_null"); present {
			t.Error("null must be present=false")
		}
		if v, present := errs.OptionalStringList(object, "list"); !present || len(v) != 1 || v[0] != "x" {
			t.Errorf("present value: got %v/%v", v, present)
		}
		if len(errs) != 0 {
			t.Errorf("recorded errors on valid input: %+v", errs)
		}
	})
	t.Run("OptionalStringArrayDict", func(t *testing.T) {
		var errs Errors
		if _, present := errs.OptionalStringArrayDict(object, "absent"); present {
			t.Error("absent must be present=false")
		}
		if _, present := errs.OptionalStringArrayDict(object, "dict_null"); present {
			t.Error("null must be present=false")
		}
		if v, present := errs.OptionalStringArrayDict(object, "dict"); !present || len(v.Values["p"]) != 1 {
			t.Errorf("present value: got %v/%v", v, present)
		}
		if len(errs) != 0 {
			t.Errorf("recorded errors on valid input: %+v", errs)
		}
	})
	t.Run("OptionalBoundedInt", func(t *testing.T) {
		var errs Errors
		if _, present := errs.OptionalBoundedInt(object, "absent", 0, 2); present {
			t.Error("absent must be present=false")
		}
		if _, present := errs.OptionalBoundedInt(object, "bint_null", 0, 2); present {
			t.Error("null must be present=false")
		}
		if v, present := errs.OptionalBoundedInt(object, "bint", 0, 2); !present || v != 1 {
			t.Errorf("present value: got %d/%v", v, present)
		}
		if len(errs) != 0 {
			t.Errorf("recorded errors on valid input: %+v", errs)
		}
	})
	t.Run("OptionalAnyDict", func(t *testing.T) {
		var errs Errors
		if _, present := errs.OptionalAnyDict(object, "absent"); present {
			t.Error("absent must be present=false")
		}
		if _, present := errs.OptionalAnyDict(object, "any_null"); present {
			t.Error("null must be present=false")
		}
		if v, present := errs.OptionalAnyDict(object, "any"); !present || v == nil {
			t.Errorf("present value: got %v/%v", v, present)
		}
		if len(errs) != 0 {
			t.Errorf("recorded errors on valid input: %+v", errs)
		}
	})
}

func mustRenderObject(t *testing.T, object *pyjson.Object) string {
	t.Helper()
	rendered, err := pyjson.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	return string(rendered)
}
