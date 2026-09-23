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

func mustDecode(t *testing.T, text string) pyjson.Value {
	t.Helper()
	value, err := pyjson.DecodeString(text)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
