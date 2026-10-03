package pythonparity_test

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/errortext"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

// CHAOS-7947: each production entry point that stores, logs or returns error text is a thin wrapper of ONE named composition
// of the ONE engine: pythonparity.SanitizeErrorTextHardened = errortext.SanitizeHardened (patterns, shapes, cap; always its
// order), syncdispatchruntime.SanitizeErrorText = errortext.SanitizeHardenedShapesFirst at 4000 (shapes first, as that path
// always ran). A wrapper that drifts to the other order, or to its own, is RED on the corpus.
func TestEveryEntryPointIsItsNamedComposition(t *testing.T) {
	disagree := 0
	for _, pair := range sanitizeCorpus() {
		text, cap := pair[0].(string), pair[1].(int)
		if got, want := pythonparity.SanitizeErrorTextHardened(text, cap), errortext.SanitizeHardened(text, cap); got != want {
			disagree++
			t.Errorf("pythonparity.SanitizeErrorTextHardened(%q, %d) = %q, errortext.SanitizeHardened = %q", text, cap, got, want)
		}
		if got, want := syncdispatchruntime.SanitizeErrorText(text), errortext.SanitizeHardenedShapesFirst(text, 4000); got != want {
			disagree++
			t.Errorf("syncdispatchruntime.SanitizeErrorText(%q) = %q, errortext.SanitizeHardenedShapesFirst = %q", text, got, want)
		}
		if disagree > 12 {
			t.Fatal("stopping after 12 disagreements")
		}
	}
}
