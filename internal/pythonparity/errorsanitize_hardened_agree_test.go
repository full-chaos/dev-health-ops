package pythonparity_test

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/errortext"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

// CHAOS-7947: every production entry point that stores, logs or returns error text is the ONE composition
// errortext.SanitizeHardened. Before the unification the two entry points applied the credential-shape pass in a different
// order and disagreed on 30 of the corpus strings (one left a value behind, one a trailing fragment); this pins that they
// agree on every corpus string, with and without a cap.
func TestEveryHardenedEntryPointIsTheOneComposition(t *testing.T) {
	disagree := 0
	for _, pair := range sanitizeCorpus() {
		text, cap := pair[0].(string), pair[1].(int)
		want := errortext.SanitizeHardened(text, cap)
		if got := pythonparity.SanitizeErrorTextHardened(text, cap); got != want {
			disagree++
			t.Errorf("pythonparity.SanitizeErrorTextHardened(%q, %d) = %q, errortext.SanitizeHardened = %q", text, cap, got, want)
		}
		if cap == 0 {
			if got, wantSync := syncdispatchruntime.SanitizeErrorText(text), errortext.SanitizeHardened(text, 4000); got != wantSync {
				disagree++
				t.Errorf("syncdispatchruntime.SanitizeErrorText(%q) = %q, errortext.SanitizeHardened(.., 4000) = %q", text, got, wantSync)
			}
		}
		if disagree > 12 {
			t.Fatal("stopping after 12 disagreements")
		}
	}
}
