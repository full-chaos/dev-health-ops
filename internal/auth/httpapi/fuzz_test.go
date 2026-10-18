package httpapi

import (
	"net/netip"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
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

// FuzzParseForwardedTrust drives the FORWARDED_ALLOW_IPS parser, an operator
// configuration parser that decides which peers may set forwarded headers.
// Properties: no panic on any value or peer; "*" trusts every peer; and every
// plain IP address entry of the list trusts itself.
func FuzzParseForwardedTrust(f *testing.F) {
	for _, seed := range []struct{ value, host string }{
		{"*", "203.0.113.9"}, {"127.0.0.1", "127.0.0.1"}, {"10.0.0.0/8, 192.0.2.1", "10.1.2.3"},
		{"10.0.0.1/8", "10.0.0.1"}, {"::1,fe80::/10", "::1"}, {"unix:/run/app.sock", "unix:/run/app.sock"}, {"", ""}, {",,", " "},
	} {
		f.Add(seed.value, seed.host)
	}
	f.Fuzz(func(t *testing.T, value, host string) {
		trust := ParseForwardedTrust(value)
		got := trust.trusts(host)
		if value == "*" && !got {
			t.Fatalf("\"*\" did not trust %q", host)
		}
		for _, raw := range strings.Split(value, ",") {
			entry := strings.TrimFunc(raw, pythonparity.IsSpace)
			if addr, err := netip.ParseAddr(entry); err == nil && !trust.trusts(entry) {
				t.Fatalf("list %q does not trust its own address entry %q (%s)", value, entry, addr)
			}
		}
	})
}
