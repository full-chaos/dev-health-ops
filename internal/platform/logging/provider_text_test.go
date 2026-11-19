package logging

import (
	"strings"
	"testing"
)

func TestProviderText(t *testing.T) {
	long := strings.Repeat("a", 5000)
	cases := []struct{ name, in, want string }{
		{"plain", "acme/api", "acme/api"},
		{"empty", "", ""},
		{"newline and escape", "acme/api\n\x1b[31m", `acme/api\u000a\u001b[31m`},
		{"bidi override and zero width", "acme/\u202eipa\u200b", `acme/\u202eipa\u200b`},
		{"line separator", "a\u2028b\u2029c", `a\u2028b\u2029c`},
		{"invalid utf-8", "a\xffb", `a\xffb`},
		{"backslash", `a\u000ab`, `a\\u000ab`},
		{"non-ascii letters kept", "acme/über", "acme/über"},
		{"exact bound", strings.Repeat("a", 128), strings.Repeat("a", 128)},
		{"over bound", long, strings.Repeat("a", 128) + "...[cut]"},
		{"escapes count against the bound", strings.Repeat("\n", 128), strings.Repeat(`\u000a`, 21) + "...[cut]"},
		{"cut lands on an escape boundary", strings.Repeat("a", 125) + "\n", strings.Repeat("a", 125) + "...[cut]"},
		{"cut counts runes", strings.Repeat("ü", 129), strings.Repeat("ü", 128) + "...[cut]"},
	}
	for _, c := range cases {
		if got := ProviderText(c.in); got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}
