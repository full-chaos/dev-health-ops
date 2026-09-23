package pybody

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func read(t *testing.T, body, contentType string) (Body, Outcome, *Error) {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	got, outcome, failure, err := Read(request)
	if err != nil {
		t.Fatal(err)
	}
	return got, outcome, failure
}

func TestReadIsFastAPIsBodyStep(t *testing.T) {
	cases := []struct {
		name, body, contentType string
		outcome                 Outcome
		missing, raw            bool
		failure                 string
	}{
		{"empty", "", "application/json", Ready, true, false, ""},
		{"null", "null", "application/json", Ready, true, false, ""},
		{"object", `{"a":1}`, "application/json", Ready, false, false, ""},
		{"no content type is JSON", `{"a":1}`, "", Ready, false, false, ""},
		{"+json", `{"a":1}`, "application/merge-patch+json", Ready, false, false, ""},
		{"text/plain is raw", `{"a":1}`, "text/plain", Ready, false, true, ""},
		{"unparsable type is raw", `{"a":1}`, "application/", Ready, false, true, ""},
		{"bad JSON", `{"a":`, "application/json", DecodeFailed, false, false, `{"type":"json_invalid","loc":["body",5],"msg":"JSON decode error","input":{},"ctx":{"error":"Expecting value"}}`},
		{"not UTF-8", "\"\xff\"", "application/json", ParseFailed, false, false, ""},
		{"UTF-16LE with BOM", "\xff\xfe{\x00}\x00", "application/json", Ready, false, false, ""},
	}
	for _, test := range cases {
		body, outcome, failure := read(t, test.body, test.contentType)
		if outcome != test.outcome || body.Missing != test.missing || (body.Raw != nil) != test.raw {
			t.Errorf("%s: outcome %d missing %v raw %v", test.name, outcome, body.Missing, body.Raw != nil)
		}
		if test.failure != "" {
			rendered, _ := pyjson.Marshal(failure.json())
			if string(rendered) != test.failure {
				t.Errorf("%s: %s", test.name, rendered)
			}
		}
	}
}

func TestErrorsMatchPydantic(t *testing.T) {
	var errs Errors
	object, ok := errs.Object(Body{Value: mustDecode(t, `{"name":"","d":5,"s":"\ud800"}`)})
	if !ok {
		t.Fatal("object refused")
	}
	errs.OptionalString(object, "name", 1, 255)
	errs.OptionalString(object, "d", 0, 0)
	if value, ok := errs.OptionalString(object, "s", 1, 1); !ok || pyjson.Len(value) != 1 {
		t.Errorf("a lone surrogate is one character: %q %v", value, ok)
	}
	errs.Object(Body{Missing: true})
	errs.Object(Body{Value: []pyjson.Value{}})
	rendered, _ := pyjson.Marshal(Detail(errs))
	want := `{"detail":[{"type":"string_too_short","loc":["body","name"],"msg":"String should have at least 1 character","input":"","ctx":{"min_length":1}},` +
		`{"type":"string_type","loc":["body","d"],"msg":"Input should be a valid string","input":5},` +
		`{"type":"missing","loc":["body"],"msg":"Field required","input":null},` +
		`{"type":"model_attributes_type","loc":["body"],"msg":"Input should be a valid dictionary or object to extract fields from","input":[]}]}`
	if string(rendered) != want {
		t.Fatalf("%s", rendered)
	}
	raw := "\xff"
	var bad Errors
	bad.Object(Body{Raw: &raw})
	if _, err := pyjson.Marshal(Detail(bad)); err == nil {
		t.Fatal("echoing bytes that are not UTF-8 must fail the render (a 500), as jsonable_encoder does")
	}
}

// TestRequiredStringMatchesPydantic pins the three shapes verified against
// a live pydantic model for a required (no-default) str field: absent
// (type "missing", input the WHOLE object -- not null, unlike a missing
// body), present-but-null (type "string_type", input null), and
// present-with-the-wrong-JSON-type (type "string_type", input the raw
// value). See RequiredString's own doc comment for why the absent case
// differs from OptionalString's.
func TestRequiredStringMatchesPydantic(t *testing.T) {
	var errs Errors
	object, ok := errs.Object(Body{Value: mustDecode(t, `{"target_user_id":null}`)})
	if !ok {
		t.Fatal("object refused")
	}
	errs.RequiredString(object, "missing_field", 0, 0)
	errs.RequiredString(object, "target_user_id", 0, 0)

	object2, ok := errs.Object(Body{Value: mustDecode(t, `{"target_user_id":5}`)})
	if !ok {
		t.Fatal("object refused")
	}
	errs.RequiredString(object2, "target_user_id", 0, 0)

	rendered, _ := pyjson.Marshal(Detail(errs))
	want := `{"detail":[` +
		`{"type":"missing","loc":["body","missing_field"],"msg":"Field required","input":{"target_user_id":null}},` +
		`{"type":"string_type","loc":["body","target_user_id"],"msg":"Input should be a valid string","input":null},` +
		`{"type":"string_type","loc":["body","target_user_id"],"msg":"Input should be a valid string","input":5}]}`
	if string(rendered) != want {
		t.Fatalf("%s", rendered)
	}

	var happy Errors
	object3, _ := happy.Object(Body{Value: mustDecode(t, `{"target_user_id":"abc"}`)})
	value, ok := happy.RequiredString(object3, "target_user_id", 0, 0)
	if !ok || value != "abc" || len(happy) != 0 {
		t.Fatalf("value=%q ok=%v errs=%v", value, ok, happy)
	}
}

func mustDecode(t *testing.T, text string) pyjson.Value {
	t.Helper()
	value, err := pyjson.DecodeString(text)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

// TestDefaultedStringMatchesPydantic pins DefaultedString against a
// pydantic `field: str = "default"` (not `str | None`): absent means
// "apply the default yourself" (present=false, no error); a present null
// or wrong-type value is a type error, not a fall-back to the default --
// the live round finding this pins: `class M(BaseModel): s: str =
// "local"` on `{"s": null}` raises string_type, it does not set s="local".
func TestDefaultedStringMatchesPydantic(t *testing.T) {
	var errs Errors
	object, ok := errs.Object(Body{Value: mustDecode(t, `{"s":null,"n":5,"e":""}`)})
	if !ok {
		t.Fatal("object refused")
	}
	if _, present := errs.DefaultedString(object, "absent", 0, 0); present {
		t.Fatal("absent must report present=false with no error")
	}
	errs.DefaultedString(object, "s", 0, 0)
	errs.DefaultedString(object, "n", 0, 0)
	if value, present := errs.DefaultedString(object, "e", 0, 0); !present || value != "" {
		t.Fatalf("explicit empty string must be preserved: value=%q present=%v", value, present)
	}
	rendered, _ := pyjson.Marshal(Detail(errs))
	want := `{"detail":[` +
		`{"type":"string_type","loc":["body","s"],"msg":"Input should be a valid string","input":null},` +
		`{"type":"string_type","loc":["body","n"],"msg":"Input should be a valid string","input":5}]}`
	if string(rendered) != want {
		t.Fatalf("%s", rendered)
	}
}

// TestDefaultedBoolMatchesPydantic is DefaultedString's bool counterpart,
// same absent/null/wrong-type shape, for a `field: bool = False` schema.
func TestDefaultedBoolMatchesPydantic(t *testing.T) {
	var errs Errors
	object, ok := errs.Object(Body{Value: mustDecode(t, `{"b":null,"w":[],"t":true}`)})
	if !ok {
		t.Fatal("object refused")
	}
	if _, present := errs.DefaultedBool(object, "absent"); present {
		t.Fatal("absent must report present=false with no error")
	}
	errs.DefaultedBool(object, "b")
	errs.DefaultedBool(object, "w")
	if value, present := errs.DefaultedBool(object, "t"); !present || !value {
		t.Fatalf("true must be preserved: value=%v present=%v", value, present)
	}
	rendered, _ := pyjson.Marshal(Detail(errs))
	want := `{"detail":[` +
		`{"type":"bool_type","loc":["body","b"],"msg":"Input should be a valid boolean","input":null},` +
		`{"type":"bool_type","loc":["body","w"],"msg":"Input should be a valid boolean","input":[]}]}`
	if string(rendered) != want {
		t.Fatalf("%s", rendered)
	}
}

// TestOptionalBoolLaxCoercionMatchesPydantic pins the live round finding:
// pydantic's bool validator is LAX by default, coercing more than native
// JSON booleans -- the int/float 0/1, and a fixed case-insensitive string
// vocabulary. A stricter Go-only `raw.(bool)` assertion answered 422 where
// Python answers 200 for these. Verified live against the installed
// pydantic (2.13.4); see PydanticBool's own doc comment for the full rule.
func TestOptionalBoolLaxCoercionMatchesPydantic(t *testing.T) {
	accept := []struct {
		json string
		want bool
	}{
		{`true`, true}, {`false`, false},
		{`1`, true}, {`0`, false},
		{`1.0`, true}, {`0.0`, false},
		{`"yes"`, true}, {`"no"`, false},
		{`"Y"`, true}, {`"N"`, false},
		{`"on"`, true}, {`"off"`, false},
		{`"TRUE"`, true}, {`"False"`, false},
		{`"1"`, true}, {`"0"`, false},
	}
	for _, c := range accept {
		var errs Errors
		object, ok := errs.Object(Body{Value: mustDecode(t, `{"b":`+c.json+`}`)})
		if !ok {
			t.Fatalf("%s: object refused", c.json)
		}
		value, present := errs.OptionalBool(object, "b")
		if !present || value != c.want || len(errs) != 0 {
			t.Errorf("%s: value=%v present=%v errs=%v, want %v with no error", c.json, value, present, errs, c.want)
		}
	}
	// pydantic reports bool_parsing for a str, int or integral float it
	// cannot interpret, and bool_type for any other value.
	reject := []struct{ json, want string }{
		{`2`, "bool_parsing"}, {`-1`, "bool_parsing"}, {`2.0`, "bool_parsing"}, {`"yep"`, "bool_parsing"},
		{`""`, "bool_parsing"}, {`" true "`, "bool_parsing"}, {`"1.0"`, "bool_parsing"},
		{`0.5`, "bool_type"}, {`1e300`, "bool_type"}, {`1000000000000000000000000000000`, "bool_type"},
		{`[]`, "bool_type"}, {`{}`, "bool_type"},
	}
	for _, c := range reject {
		var errs Errors
		object, ok := errs.Object(Body{Value: mustDecode(t, `{"b":`+c.json+`}`)})
		if !ok {
			t.Fatalf("%s: object refused", c.json)
		}
		if _, present := errs.OptionalBool(object, "b"); present || len(errs) != 1 || errs[0].Type != c.want {
			t.Errorf("%s: present=%v errs=%v, want one %s error", c.json, present, errs, c.want)
		}
	}
}
