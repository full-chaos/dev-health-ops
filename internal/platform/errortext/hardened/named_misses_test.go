package hardened

import "testing"

// CHAOS-7947: the sync writers' sanitizer used to be an RE2 port whose `\s`, `\b` and case folding were ASCII-only, while the
// recorded Python answer is Unicode-aware. Each row is a named miss of that port (a credential left in the text), taken from the
// differential run against the frozen Python answers (pythonparity TestSanitizeErrorTextMatchesFrozenPython); every row was RED on
// the RE2 port and must stay green on the sync writers' path (SyncWriters: main's chain with the Python pass appended).
func TestNamedMissesOfTheFormerRE2Port(t *testing.T) {
	rows := []struct{ name, in, want string }{
		{"no-break space after the scheme word", "bearer\u00a0Kghp_1", "[REDACTED]"},
		{"line separator after the scheme word", "Bearer\u2028x", "[REDACTED]"},
		{"paragraph separator after the scheme word", "Bearer\u2029x", "[REDACTED]"},
		{"ideographic space after the key name", "token=\u3000value", "[REDACTED]"},
		{"long s folds onto s in a key name", "\u017fecret=1", "[REDACTED]"},
		{"kelvin sign folds onto k in a prefixed token", "ghp_KKKKKKKKKKKKKKKKKKKK", "[REDACTED]"},
		{"kelvin sign folds onto k in a key name", "\u212aey token=1", "\u212aey [REDACTED]"},
		{"no-break space after a header colon", "Authorization:\u00a0Bearer\u00a0tok", "[REDACTED]"},
	}
	for _, row := range rows {
		if got := SyncWriters(row.in); got != row.want {
			t.Errorf("%s: SyncWriters(%q) = %q, want %q", row.name, row.in, got, row.want)
		}
	}
}
