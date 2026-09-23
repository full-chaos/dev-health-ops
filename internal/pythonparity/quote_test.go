package pythonparity

import "testing"

func TestQuoteIsURLLibQuote(t *testing.T) {
	const safe = ":/%#?=@[]!$&'()*+,;"
	for input, want := range map[string]string{
		"http://h/a b":     "http://h/a%20b",
		"http://h/café":    "http://h/caf%C3%A9",
		"http://h/x?a=1&b": "http://h/x?a=1&b",
		"~_.-AZaz09":       "~_.-AZaz09",
		"\"<>\\^`{|}":      "%22%3C%3E%5C%5E%60%7B%7C%7D",
		"%2F%zz":           "%2F%zz",
		"\x00\x7f":         "%00%7F",
	} {
		if got := Quote(input, safe); got != want {
			t.Errorf("Quote(%q) = %q, want %q", input, got, want)
		}
	}
}
