package errortext

import "testing"

// CHAOS-7947: the sync writers' sanitizer used to be an RE2 port whose `\s`, `\b` and case folding were ASCII-only, while the
// recorded Python answer is Unicode-aware. Each row is a named miss of that port (a credential left in the text), taken from
// the differential run against the frozen Python answers (pythonparity TestSanitizeErrorTextMatchesFrozenPython); every row
// was RED on the RE2 port and must stay green on the one sanitizer.
func TestNamedMissesOfTheFormerRE2Port(t *testing.T) {
	// the wanted values are the answers the frozen Python oracle confirms for these exact corpus inputs
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
		if got := Sanitize(row.in); got != row.want {
			t.Errorf("%s: Sanitize(%q) = %q, want %q", row.name, row.in, got, row.want)
		}
	}
}

// CHAOS-7947: the hardened composition is NEVER weaker than the former RE2 port on a string it redacted. Python's Unicode `\b`
// does not see a boundary between a non-ASCII letter and a key name; the ASCII pass of SanitizeHardened does (as RE2 did), so
// these still redact. Each wanted value is the former port's own answer, taken from main at beec298d34.
func TestHardenedKeepsWhatTheFormerPortRedacted(t *testing.T) {
	rows := []struct{ in, want string }{
		{"İtoken=1", "İ[REDACTED]"},
		{"ıtoken=1", "ı[REDACTED]"},
		{"ıapi_key=ı", "ı[REDACTED]"},
		{"_secretapikeyghr_\u212aclient_secret://secret\u0130", "_secretapikeyghr_\u212a[REDACTED]"},
	}
	for _, row := range rows {
		if got := SanitizeHardened(row.in, 4000); got != row.want {
			t.Errorf("SanitizeHardened(%q) = %q, want %q", row.in, got, row.want)
		}
	}
}
