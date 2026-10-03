package pythonparity_test

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/credentialshapes"
)

// CHAOS-7937: the one shape list x every context for SanitizeErrorTextHardened (the persisted error columns). The exact
// function stays the Python port the frozen oracle pins: it is NOT asked to redact the new shapes, and the test says so.

func TestHardenedSanitizerRedactsEveryShapeInEveryContext(t *testing.T) {
	for _, shape := range credentialshapes.Shapes() {
		for _, value := range shape.Values {
			for _, context := range credentialshapes.Contexts() {
				if shape.Opaque && !context.NeedsWord {
					continue
				}
				got := pythonparity.SanitizeErrorTextHardened(credentialshapes.Expand(context.Template, value), 0)
				if strings.Contains(got, credentialshapes.Tail(value)) || !strings.Contains(got, "[REDACTED]") {
					t.Fatalf("%s %q in %q: %s", shape.ID, value, context.Template, got)
				}
			}
		}
		if shape.Short != "" {
			if got := pythonparity.SanitizeErrorTextHardened(shape.Short, 0); got != shape.Short {
				t.Fatalf("%s one byte under the minimum was changed: %q", shape.ID, got)
			}
		}
	}
}

func TestHardenedSanitizerLeavesTheNegativeSentencesAlone(t *testing.T) {
	for _, text := range credentialshapes.Negatives() {
		if got := pythonparity.SanitizeErrorTextHardened(text, 0); got != text {
			t.Fatalf("SanitizeErrorTextHardened(%q) = %q", text, got)
		}
	}
}

// The exact (Python-parity) function does not redact an LLM-provider key bare: the named difference the hardened one closes.
func TestTheExactPythonPortKeepsItsRecordedAnswerForANewShape(t *testing.T) {
	shape := credentialshapes.Shapes()[0]
	if got := pythonparity.SanitizeErrorText(shape.Value(), 0); got != shape.Value() {
		t.Fatalf("the exact port changed its recorded answer: %q", got)
	}
}
