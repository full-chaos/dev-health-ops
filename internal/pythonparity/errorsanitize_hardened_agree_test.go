package pythonparity_test

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/errortext/hardened"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

// CHAOS-7947: each production entry point that stores, logs or returns error text is a thin wrapper of ONE named composition
// of the ONE engine: pythonparity.SanitizeErrorTextHardened = hardened.Parity (patterns, shapes, cap, userinfo last), syncdispatchruntime.SanitizeErrorText = hardened.SyncWriters (shapes first, then
// errortext.Sanitize, userinfo last: as that path always ran). A wrapper that drifts to the other order, or to its own, is RED on the corpus.
func TestEveryEntryPointIsItsNamedComposition(t *testing.T) {
	disagree := 0
	for _, pair := range sanitizeCorpus() {
		text, cap := pair[0].(string), pair[1].(int)
		if got, want := pythonparity.SanitizeErrorTextHardened(text, cap), hardened.Parity(text, cap); got != want {
			disagree++
			t.Errorf("pythonparity.SanitizeErrorTextHardened(%q, %d) = %q, hardened.Parity = %q", text, cap, got, want)
		}
		if got, want := syncdispatchruntime.SanitizeErrorText(text), hardened.SyncWriters(text); got != want {
			disagree++
			t.Errorf("syncdispatchruntime.SanitizeErrorText(%q) = %q, hardened.SyncWriters = %q", text, got, want)
		}
		if disagree > 12 {
			t.Fatal("stopping after 12 disagreements")
		}
	}
}
