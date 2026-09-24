package reports

import (
	"strings"
	"testing"
)

// Expected values come from running Python's uuid.UUID(text) on each input
// (CPython 3.14.7); ok=false marks a ValueError.
var reportIDCells = []struct {
	input string
	want  string
	ok    bool
}{
	{"0123456789abcdef0123456789abcdef", "01234567-89ab-cdef-0123-456789abcdef", true},
	{"0123456789ABCDEF0123456789ABCDEF", "01234567-89ab-cdef-0123-456789abcdef", true},
	{"01234567-89ab-cdef-0123-456789abcdef", "01234567-89ab-cdef-0123-456789abcdef", true},
	{"{01234567-89ab-cdef-0123-456789abcdef}", "01234567-89ab-cdef-0123-456789abcdef", true},
	{"urn:uuid:01234567-89ab-cdef-0123-456789abcdef", "01234567-89ab-cdef-0123-456789abcdef", true},
	{"{{0123456789abcdef0123456789abcdef}}", "01234567-89ab-cdef-0123-456789abcdef", true},
	{"---0123456789abcdef0123456789abcdef--", "01234567-89ab-cdef-0123-456789abcdef", true},
	{"", "", false},
	{"not-a-uuid", "", false},
	{"0123456789abcdef0123456789abcde", "", false},
	{"0123456789abcdef0123456789abcdef0", "", false},
	{"0x0123456789abcdef0123456789abcd", "00012345-6789-abcd-ef01-23456789abcd", true},
	{"0X0123456789abcdef0123456789abcd", "00012345-6789-abcd-ef01-23456789abcd", true},
	{"0x_0123456789abcdef0123456789abc", "00001234-5678-9abc-def0-123456789abc", true},
	{"0x0123456789abcdef0123456789ab_1", "00001234-5678-9abc-def0-123456789ab1", true},
	{"_0123456789abcdef0123456789abcde", "", false},
	{"0123456789abcdef0123456789abcde_", "", false},
	{"0123456789abcde__123456789abcdef", "", false},
	{" 0123456789abcdef0123456789abcde", "00123456-789a-bcde-f012-3456789abcde", true},
	{"0123456789abcdef0123456789abcde ", "00123456-789a-bcde-f012-3456789abcde", true},
	{"\t0123456789abcdef0123456789abcd\n", "00012345-6789-abcd-ef01-23456789abcd", true},
	{"+0123456789abcdef0123456789abcde", "00123456-789a-bcde-f012-3456789abcde", true},
	{"-0123456789abcdef0123456789abcde", "", false},
	{"-0000000000000000000000000000000", "", false},
	{"+fffffffffffffffffffffffffffffff", "0fffffff-ffff-ffff-ffff-ffffffffffff", true},
	{"ffffffffffffffffffffffffffffffff", "ffffffff-ffff-ffff-ffff-ffffffffffff", true},
	{"g123456789abcdef0123456789abcdef", "", false},
	{"\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660\u0660", "00000000-0000-0000-0000-000000000000", true},
	{"\u0661\u0662\u0663\u0664\u0665\u0666\u0667\u0668\u0669\u0660\u0661\u0662\u0663\u0664\u0665\u0666\u0667\u0668\u0669\u0660\u0661\u0662\u0663\u0664\u0665\u0666\u0667\u0668\u0669\u0660\u0661\u0662", "12345678-9012-3456-7890-123456789012", true},
	{"\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10\uff10", "00000000-0000-0000-0000-000000000000", true},
	{"00000000000000000000000000000000", "00000000-0000-0000-0000-000000000000", true},
	{"00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000001", true},
	{"urn:0123456789abcdef0123456789abcdef", "01234567-89ab-cdef-0123-456789abcdef", true},
	{"uuid:0123456789abcdef0123456789abcdef", "01234567-89ab-cdef-0123-456789abcdef", true},
	{"uuid:urn:0123456789abcdef0123456789abcdef", "01234567-89ab-cdef-0123-456789abcdef", true},
	{"0xffffffffffffffffffffffffffffff", "00ffffff-ffff-ffff-ffff-ffffffffffff", true},
	{"a_111111111111111111111111111111", "0a111111-1111-1111-1111-111111111111", true},
	{"1_1_1_1_1_1_1_1_1_1_1_1_1_1_1_1_", "", false},
	{" 0x11111111111111111111111111111", "00011111-1111-1111-1111-111111111111", true},
	{"0123456789abcdef 123456789abcdef", "", false},
	{"\u00a00123456789abcdef0123456789abcd\u00a0", "00012345-6789-abcd-ef01-23456789abcd", true},
	{"\u20030123456789abcdef0123456789abcd\u2003", "00012345-6789-abcd-ef01-23456789abcd", true},
	{"\u001c0123456789abcdef0123456789abcd\u001f", "", false},
	{"\u00850123456789abcdef0123456789abcd\u0085", "00012345-6789-abcd-ef01-23456789abcd", true},
}

func TestParseReportID_MatchesPythonCells(t *testing.T) {
	for _, c := range reportIDCells {
		got, err := ParseReportID(c.input)
		if (err == nil) != c.ok {
			t.Errorf("ParseReportID(%q): err=%v, want ok=%v", c.input, err, c.ok)
			continue
		}
		if c.ok && got != c.want {
			t.Errorf("ParseReportID(%q) = %q, want %q", c.input, got, c.want)
		}
	}
}

// The base-16 error text is CPython's repr of the normalised string:
// backslashes doubled, quotes chosen by content, controls escaped.
func TestParseReportID_InvalidLiteralUsesCPythonRepr(t *testing.T) {
	cases := []struct{ input, want string }{
		{strings.Repeat("z", 32), "'" + strings.Repeat("z", 32) + "'"},
		{strings.Repeat(`\`, 32), "'" + strings.Repeat(`\\`, 32) + "'"},
		{strings.Repeat("'", 32), `"` + strings.Repeat("'", 32) + `"`},
		{strings.Repeat("\t", 32), "'" + strings.Repeat(`\t`, 32) + "'"},
		{"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz\u00e9", "'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz\u00e9'"},
		{"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz\u0085", `'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz\x85'`},
	}
	for _, c := range cases {
		_, err := ParseReportID(c.input)
		want := "invalid literal for int() with base 16: " + c.want
		if err == nil || err.Error() != want {
			t.Errorf("ParseReportID(%q) error = %v, want %q", c.input, err, want)
		}
	}
}
