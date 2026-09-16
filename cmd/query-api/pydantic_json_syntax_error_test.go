package main

import "testing"

// TestClassifyJSONSyntaxErrorMatchesLiveCapture pins classifyJSONSyntaxError
// against a live FastAPI TestClient capture of POST /api/v1/drilldown/prs's
// real json_invalid ctx.error/position for each malformed body shape
// (uv run python3, get_current_user dependency-overridden; bodies quoted
// in this PR's TEST-EVIDENCE).
func TestClassifyJSONSyntaxErrorMatchesLiveCapture(t *testing.T) {
	cases := []struct {
		name string
		body string
		want pyJSONError
	}{
		{"invalid start token", "not json", pyJSONError{Msg: "Expecting value", Pos: 0}},
		{"missing property quote", "{not json", pyJSONError{Msg: "Expecting property name enclosed in double quotes", Pos: 1}},
		{"empty body", "", pyJSONError{Msg: "Expecting value", Pos: 0}},
		{"trailing comma object", `{"a":1,}`, pyJSONError{Msg: "Illegal trailing comma before end of object", Pos: 6}},
		{"trailing comma array", "[1,2,]", pyJSONError{Msg: "Illegal trailing comma before end of array", Pos: 4}},
		{"missing colon", `{"a"`, pyJSONError{Msg: "Expecting ':' delimiter", Pos: 4}},
		{"missing value after colon", `{"a":}`, pyJSONError{Msg: "Expecting value", Pos: 5}},
		{"missing comma object", `{"a":1`, pyJSONError{Msg: "Expecting ',' delimiter", Pos: 6}},
		{"missing comma array", "[1,2", pyJSONError{Msg: "Expecting ',' delimiter", Pos: 4}},
		{"unterminated string", `"unterminated`, pyJSONError{Msg: "Unterminated string starting at", Pos: 0}},
		{"two values no comma", `{"a": "b" "c"}`, pyJSONError{Msg: "Expecting ',' delimiter", Pos: 10}},
		{"extra data", `{"a":1} extra`, pyJSONError{Msg: "Extra data", Pos: 8}},
		{"whitespace only", "  ", pyJSONError{Msg: "Expecting value", Pos: 2}},
		{"invalid keyword", `{"a": tru}`, pyJSONError{Msg: "Expecting value", Pos: 6}},
		{"leading zero number", `{"a": 01}`, pyJSONError{Msg: "Expecting ',' delimiter", Pos: 7}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyJSONSyntaxError([]byte(tc.body))
			if got != tc.want {
				t.Fatalf("classifyJSONSyntaxError(%q) = %+v, want %+v", tc.body, got, tc.want)
			}
		})
	}
}

// TestJsonSyntaxErrorDetailShape pins jsonSyntaxErrorDetail's full
// envelope (not just the classifier) at a given loc.
func TestJsonSyntaxErrorDetailShape(t *testing.T) {
	got := jsonSyntaxErrorDetail([]any{"body"}, []byte("not json"))
	want := pydanticErrorDetail{
		Type:  "json_invalid",
		Loc:   []any{"body", 0},
		Msg:   "JSON decode error",
		Input: map[string]any{},
		Ctx:   map[string]string{"error": "Expecting value"},
	}
	if got.Type != want.Type || got.Msg != want.Msg || got.Ctx["error"] != want.Ctx["error"] {
		t.Fatalf("jsonSyntaxErrorDetail = %+v, want %+v", got, want)
	}
	if len(got.Loc) != 2 || got.Loc[0] != "body" || got.Loc[1] != 0 {
		t.Fatalf("Loc = %v, want [\"body\", 0]", got.Loc)
	}
}
