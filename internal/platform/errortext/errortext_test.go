package errortext

import (
	"strings"
	"testing"
)

// TestSanitizeIsTheCharacterizedFunction pins CHAOS-7943: the moved Sanitize
// returns, for every input of this corpus, exactly what the function returned
// in internal/syncdispatchruntime before the move (expected values were taken
// from that function at e609eb0e8c, before any line moved).
func TestSanitizeIsTheCharacterizedFunction(t *testing.T) {
	long := strings.Repeat("é", 4100)
	inputs := []string{
		"", "plain text", "Authorization: Bearer abc.def.ghi", "authorization=Basic dXNlcjpwYXNz", "proxy-authorization: tok extra",
		"bearer xyz123", "Basic dXNlcjpwYXNzd29yZA==", "ghp_" + strings.Repeat("a", 24), "gho_" + strings.Repeat("B", 22), "ghu_" + strings.Repeat("c", 20), "ghs_" + strings.Repeat("d", 30), "ghr_" + strings.Repeat("e", 21),
		"github_pat_" + strings.Repeat("f", 30), "glpat-" + strings.Repeat("g", 25), "xoxb-1234567890-abcdef", "token=abc123 secret: s3cr3t api_key=K client_secret=Z private_token=P access_token=A apikey=Q",
		"postgres://user:pass@host/db and https://tok@host", "no secret here token", long, "tail " + strings.Repeat("x", 3990), "mixed Bearer\tx and\nsecret=1",
	}
	expected := []string{
		"",
		"plain text",
		"[REDACTED]",
		"[REDACTED]",
		"[REDACTED]",
		"[REDACTED]",
		"[REDACTED]==",
		"[REDACTED]",
		"[REDACTED]",
		"[REDACTED]",
		"[REDACTED]",
		"[REDACTED]",
		"[REDACTED]",
		"[REDACTED]",
		"[REDACTED]",
		"[REDACTED] [REDACTED] [REDACTED] [REDACTED] [REDACTED] [REDACTED] [REDACTED]",
		"[REDACTED]host/db and [REDACTED]host",
		"no secret here token",
		"SKIP",
		"SKIP",
		"mixed [REDACTED] and\n[REDACTED]",
	}
	for index, input := range inputs {
		if expected[index] == "SKIP" {
			continue
		}
		if got := Sanitize(input); got != expected[index] {
			t.Errorf("Sanitize(%q) = %q; want %q", input, got, expected[index])
		}
	}
	// the two cap cases, by property: 4100 runes -> exactly 4000 runes ending in the suffix; text of 3995 runes stays whole.
	if got := Sanitize(long); len([]rune(got)) != 4000 || !strings.HasSuffix(got, "...[truncated]") {
		t.Errorf("long input: %d runes, suffix %v", len([]rune(got)), strings.HasSuffix(got, "...[truncated]"))
	}
	if got := Sanitize("tail " + strings.Repeat("x", 3990)); len([]rune(got)) != 3995 || strings.Contains(got, "truncated") {
		t.Errorf("3995-rune input changed: %d runes", len([]rune(got)))
	}
}
