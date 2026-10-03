package pythonparity_test

import (
	"strings"
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

// The sync writers' result is final: a second call over it changes nothing, and the parity sanitizer (the scrub's step over rows the
// sync writer stored) leaves it as it is. The case that broke the appended pass before it ran inside the last cut: text over the cap
// whose tail, after the appended pass's own cut, ends inside a userinfo-looking run.
func TestSyncEntryPointResultIsFinal(t *testing.T) {
	var texts []string
	for _, pair := range sanitizeCorpus() {
		texts = append(texts, pair[0].(string))
	}
	texts = append(texts, gateTexts()...)
	for pad := 0; pad <= 24; pad++ {
		texts = append(texts, strings.Repeat("a", pad)+" "+strings.Repeat("Bearer x 10.0.0.1:5432 ", 200))
		texts = append(texts, strings.Repeat("a", pad)+" "+strings.Repeat("Bearer x 10.0.0.1:5432 ", 200))
		texts = append(texts, strings.Repeat("a", pad)+" "+strings.Repeat("token= abc host:5432@ ", 200))
	}
	bad := 0
	for _, text := range texts {
		once := syncdispatchruntime.SanitizeErrorText(text)
		if twice := syncdispatchruntime.SanitizeErrorText(once); twice != once {
			bad++
			if bad <= 6 {
				t.Errorf("a second SanitizeErrorText call changes the result of %.60q: %d runes then %d", text, len([]rune(once)), len([]rune(twice)))
			}
		}
		if again := pythonparity.SanitizeErrorTextHardened(once, 4000); again != once {
			bad++
			if bad <= 6 {
				t.Errorf("the parity sanitizer rewrites a sync-written row of %.60q: %d runes then %d", text, len([]rune(once)), len([]rune(again)))
			}
		}
	}
	t.Logf("%d texts, %d not final", len(texts), bad)
}
