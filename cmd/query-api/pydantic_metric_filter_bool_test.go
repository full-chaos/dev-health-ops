package main

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// TestCoerceBoolBodyFieldIsPydantics pins the shared bool rule on this
// route family's decoded values (pyjson numbers, raw strings), with each
// expectation from pydantic's TypeAdapter(bool | None).
func TestCoerceBoolBodyFieldIsPydantics(t *testing.T) {
	cases := []struct {
		value any
		want  string // "" for a valid bool
	}{
		{true, ""}, {"yes", ""}, {"OFF", ""}, {pyjson.IntOf(1), ""}, {pyjson.IntOf(0), ""}, {pyjson.Float(1), ""},
		{" true ", "bool_parsing"}, {"maybe", "bool_parsing"}, {"1.0", "bool_parsing"}, {pyjson.IntOf(2), "bool_parsing"},
		{pyjson.Float(2), "bool_parsing"}, {pyjson.Float(-9.2e18), "bool_parsing"}, {pyjson.IntOf(-9223372036854775808), "bool_parsing"},
		{pyjson.Float(1.5), "bool_type"}, {pyjson.Float(1e300), "bool_type"}, {pyjson.Float(-9.223372036854776e18), "bool_type"},
		{[]pyjson.Value{}, "bool_type"}, {pyjson.NewObject(), "bool_type"},
	}
	for _, c := range cases {
		_, present, detail := coerceBoolBodyField([]any{"body", "b"}, c.value)
		got := ""
		if detail != nil {
			got = detail.Type
		}
		if !present || got != c.want {
			t.Errorf("%#v: present=%v type=%q, want %q", c.value, present, got, c.want)
		}
	}
	if _, present, detail := coerceBoolBodyField([]any{"body", "b"}, nil); present || detail != nil {
		t.Errorf("nil: present=%v detail=%v, want absent", present, detail)
	}
}
