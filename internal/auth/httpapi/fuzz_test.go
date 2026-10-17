package httpapi

import (
	"strings"
	"testing"
)

// FuzzParseLimit drives the rate-limit text parser. Properties: no panic; an
// accepted limit has a positive count and window; and the multi-limit
// separators are never accepted (a limit set from the environment is read at
// startup, so a mis-parse serves with a different limit).
func FuzzParseLimit(f *testing.F) {
	for _, seed := range []string{"20/second", "1/minute", "5 per 2 hours", "1/second;5/minute", "0/second", "-1/day", "99999999999999999999/second", "1/2147483648 seconds", "1/fortnight", ""} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, text string) {
		limit, err := ParseLimit("fuzz", text)
		if err != nil {
			return
		}
		if limit.Count <= 0 || limit.Window <= 0 {
			t.Fatalf("accepted %q as a non-positive limit %+v", text, limit)
		}
		if strings.ContainsAny(text, ",;|") {
			t.Fatalf("accepted a multi-limit text %q", text)
		}
	})
}
