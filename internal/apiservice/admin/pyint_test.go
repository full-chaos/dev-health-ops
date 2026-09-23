package admin

import "testing"

// TestParsePyIntMatchesLivePython pins int(str)'s underscore-separator
// grammar (PEP 515), each case verified against a live python3
// (system /usr/bin/python3, 2026-09-23): "10"->10, "1_0"->10, "_10" raises,
// "10_" raises, "1__0" raises, "-10"->-10, "-1_0"->-10, "+10"->10,
// " 10"->10, "10 "->10, "1_0_0"->100, "01_0"->10. Caught by codex round
// pr2842-r1 (P1): strconv.Atoi alone rejects every underscore case Python
// accepts.
func TestParsePyIntMatchesLivePython(t *testing.T) {
	cases := []struct {
		raw  string
		want int
		ok   bool
	}{
		{"10", 10, true},
		{"1_0", 10, true},
		{"_10", 0, false},
		{"10_", 0, false},
		{"1__0", 0, false},
		{"-10", -10, true},
		{"-1_0", -10, true},
		{"+10", 10, true},
		{" 10", 10, true},
		{"10 ", 10, true},
		{"1_0_0", 100, true},
		{"01_0", 10, true},
		{"", 0, false},
		{"-", 0, false},
		{"_", 0, false},
		{"1_", 0, false},
		{"_1", 0, false},
		{"1.0", 0, false},
		{"abc", 0, false},
	}
	for _, c := range cases {
		got, ok := parsePyInt(c.raw)
		if got != c.want || ok != c.ok {
			t.Errorf("parsePyInt(%q) = (%d, %v), want (%d, %v)", c.raw, got, ok, c.want, c.ok)
		}
	}
}
