package main

import "testing"

// TestCoerceBoolBodyFieldIsPydantics pins the shared bool rule on this
// route family's decoded values (float64 numbers, raw strings), with each
// expectation from pydantic's TypeAdapter(bool | None).
func TestCoerceBoolBodyFieldIsPydantics(t *testing.T) {
	cases := []struct {
		value any
		want  string // "" for a valid bool
	}{
		{true, ""}, {"yes", ""}, {"OFF", ""}, {float64(1), ""}, {float64(0), ""},
		{" true ", "bool_parsing"}, {"maybe", "bool_parsing"}, {"1.0", "bool_parsing"}, {float64(2), "bool_parsing"},
		{float64(-9.2e18), "bool_parsing"},
		{1.5, "bool_type"}, {1e300, "bool_type"}, {-9.223372036854776e18, "bool_type"}, {[]any{}, "bool_type"},
		{map[string]any{}, "bool_type"},
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
