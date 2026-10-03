package syncdispatchruntime

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/credentialshapes"
)

// CHAOS-7937: the one shape list x every context for the sanitizer of the persisted sync error columns.
func TestSanitizeErrorTextRedactsEveryShapeInEveryContext(t *testing.T) {
	for _, shape := range credentialshapes.Shapes() {
		for _, value := range shape.Values {
			for _, context := range credentialshapes.Contexts() {
				if shape.Opaque && !context.NeedsWord {
					continue
				}
				got := sanitizeErrorText(credentialshapes.Expand(context.Template, value))
				if strings.Contains(got, credentialshapes.Tail(value)) || !strings.Contains(got, "[REDACTED]") {
					t.Fatalf("%s %q in %q: %s", shape.ID, value, context.Template, got)
				}
			}
		}
		if shape.Short != "" {
			if got := sanitizeErrorText(shape.Short); got != shape.Short {
				t.Fatalf("%s one byte under the minimum was changed: %q", shape.ID, got)
			}
		}
	}
}

func TestSanitizeErrorTextLeavesTheNegativeSentencesAlone(t *testing.T) {
	for _, text := range credentialshapes.Negatives() {
		if got := sanitizeErrorText(text); got != text {
			t.Fatalf("sanitizeErrorText(%q) = %q", text, got)
		}
	}
}

// A key that straddles the 4000-rune cap is redacted whole: the shape pass runs before the cap (a cut key is a fragment below the
// shape's minimum length that no later pass can see).
func TestAKeyAtTheCapIsRedactedWholeNotCutToAFragment(t *testing.T) {
	key := credentialshapes.Shapes()[5].Values[0] // an sk-admin key shape
	for _, filler := range []int{3900, 3950, 3963, 3980, 3990, 3995} {
		text := strings.Repeat("x ", filler/2) + key + " and more text after the key"
		got := sanitizeErrorText(text)
		if strings.Contains(got, key[:12]) {
			t.Fatalf("filler %d: part of the key survives the cap: ...%q", filler, got[len(got)-60:])
		}
	}
}
