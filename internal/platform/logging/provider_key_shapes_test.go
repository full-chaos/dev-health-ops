package logging

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/credentialshapes"
)

// CHAOS-7937: every shape of the one list (internal/testsupport/credentialshapes) x every sentence context, for RedactText and
// for RedactCredentialShapes (the part the persisted error columns use). A shape with a prefix is redacted in EVERY context;
// an Opaque shape (no prefix: nothing but its alphabet) only behind a credential word, which is the stated limit.

func survivors(token, got string) bool { return strings.Contains(got, credentialshapes.Tail(token)) }

func TestEveryShapeValueInEveryContextForBothEntryPoints(t *testing.T) {
	for name, redact := range map[string]func(string) string{"RedactText": RedactText, "RedactCredentialShapes": RedactCredentialShapes} {
		for _, shape := range credentialshapes.Shapes() {
			for _, value := range shape.Values {
				for _, context := range credentialshapes.Contexts() {
					if shape.Opaque && !context.NeedsWord {
						continue
					}
					got := redact(credentialshapes.Expand(context.Template, value))
					if survivors(value, got) || !strings.Contains(got, redacted) {
						t.Fatalf("%s: %s %q in %q: %s", name, shape.ID, value, context.Template, got)
					}
				}
			}
		}
	}
}

// One byte under the minimum length is NOT redacted (the bound is part of the shape).
func TestEveryShapeOneByteUnderItsMinimumIsLeftAlone(t *testing.T) {
	for name, redact := range map[string]func(string) string{"RedactText": RedactText, "RedactCredentialShapes": RedactCredentialShapes} {
		for _, shape := range credentialshapes.Shapes() {
			if shape.Short == "" {
				continue
			}
			if got := redact(shape.Short); got != shape.Short {
				t.Fatalf("%s(%q) = %q, want it unchanged (one byte under the minimum of %s)", name, shape.Short, got, shape.ID)
			}
		}
	}
}

func TestEveryNegativeSentenceIsUnchanged(t *testing.T) {
	for name, redact := range map[string]func(string) string{"RedactText": RedactText, "RedactCredentialShapes": RedactCredentialShapes} {
		for _, text := range credentialshapes.Negatives() {
			if got := redact(text); got != text {
				t.Fatalf("%s(%q) = %q, want it unchanged", name, text, got)
			}
		}
	}
}

// DERIVED gate: every alternation of the provider and vendor patterns must be matched by at least one shape of the list, alone:
// a new alternation without a row fails here.
func TestEveryAlternationOfThePatternsHasAShape(t *testing.T) {
	for name, pattern := range map[string]*regexp.Regexp{"providerTokenPattern": providerTokenPattern, "vendorKeyPattern": vendorKeyPattern} {
		source := pattern.String()
		source = strings.TrimSuffix(strings.TrimPrefix(source, `(?:`), ")")
		for _, alternation := range splitTopLevel(source) {
			compiled := regexp.MustCompile(`(?:` + alternation + `)`)
			matched := false
			for _, exempt := range credentialshapes.ShapesWithoutPositiveFixture() {
				if alternation == exempt {
					matched = true // named exemption: no positive fixture can be committed (push protection)
				}
			}
			for _, shape := range credentialshapes.Shapes() {
				if !shape.Opaque && compiled.MatchString(shape.Value()) {
					matched = true
					break
				}
			}
			if !matched {
				t.Fatalf("%s: the alternation %q matches no shape of the list: add its row to credentialshapes.Shapes()", name, alternation)
			}
		}
	}
}

// The exemption list is pinned to EXACTLY the shapes push protection forbids (Stripe keys, Stripe webhook secrets,
// LaunchDarkly uuid keys): exempting another alternation and dropping its row must fail.
func TestExemptionListIsExactlyTheForbiddenShapes(t *testing.T) {
	want := []string{
		`(?:sk|rk)_(?:live|test)_[A-Za-z0-9]{16,}`,
		`whsec_[A-Za-z0-9]{16,}`,
		`(?:api|sdk|mob)-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`,
	}
	got := credentialshapes.ShapesWithoutPositiveFixture()
	if len(got) != len(want) {
		t.Fatalf("exemptions = %q, want exactly %q", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("exemption %d = %q, want %q", index, got[index], want[index])
		}
	}
}

// A named exemption must still be an alternation of the vendor pattern: a stale name fails here.
func TestExemptionsNameRealAlternations(t *testing.T) {
	have := map[string]bool{}
	for _, pattern := range []*regexp.Regexp{providerTokenPattern, vendorKeyPattern} {
		source := strings.TrimSuffix(strings.TrimPrefix(pattern.String(), `(?:`), ")")
		for _, alternation := range splitTopLevel(source) {
			have[alternation] = true
		}
	}
	for _, exempt := range credentialshapes.ShapesWithoutPositiveFixture() {
		if !have[exempt] {
			t.Fatalf("exemption %q is not an alternation of the provider or vendor pattern", exempt)
		}
	}
}

// splitTopLevel splits on "|" outside parentheses and brackets.
func splitTopLevel(source string) []string {
	var parts []string
	depth, start, inClass := 0, 0, false
	for index := 0; index < len(source); index++ {
		switch c := source[index]; {
		case c == '\\':
			index++
		case inClass:
			if c == ']' {
				inClass = false
			}
		case c == '[':
			inClass = true
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == '|' && depth == 0:
			parts = append(parts, source[start:index])
			start = index + 1
		}
	}
	return append(parts, source[start:])
}

// The handler path: the same shapes in an error attribute and in a string attribute of a JSON logger.
func TestHandlerRedactsEveryPrefixedShapeInErrorAndStringAttributes(t *testing.T) {
	var out strings.Builder
	logger := NewJSON(&out, -4)
	for _, shape := range credentialshapes.Shapes() {
		out.Reset()
		logger.Error("provider call failed", "detail", "Incorrect API key provided: "+shape.Value(), "err", fmt.Errorf("401 for key %s", shape.Value()))
		if survivors(shape.Value(), out.String()) {
			t.Fatalf("%s reached the log line: %s", shape.ID, out.String())
		}
	}
}

// The accepted costs of matching a prefix wherever it stands: redacted on purpose (named in RISK-NOTES), pinned so the cost is
// visible and a narrowing is a deliberate change.
func TestAcceptedCostsAreRedactedOnPurpose(t *testing.T) {
	if len(credentialshapes.AcceptedCosts()) < 6 {
		t.Fatalf("AcceptedCosts lists %d classes, want the 6 named in the RISK-NOTES", len(credentialshapes.AcceptedCosts()))
	}
	for _, cost := range credentialshapes.AcceptedCosts() {
		if cost.Class == "" || cost.Example == "" {
			t.Fatalf("an accepted cost without a class or an example: %#v", cost)
		}
		if RedactText(cost.Example) == cost.Example || RedactCredentialShapes(cost.Example) == cost.Example {
			t.Fatalf("redacted on purpose, class %q: %q is no longer redacted: update AcceptedCosts and the RISK-NOTES together", cost.Class, cost.Example)
		}
	}
}

// The measured limit of the credential-word rule (no skip: these forms are NOT redacted today; a widening shows as a failure here
// and is a deliberate change, D4203). An unprefixed value that is not right behind a credential word and at most three filler words
// cannot be found by shape.
func TestOpaqueValuesOutsideTheCredentialWordRuleAreNotRedacted(t *testing.T) {
	value := "a1a1a1a1a1a1a1a1a1a1a1a1"
	for name, template := range map[string]string{
		"four filler words":           "key is invalid for the " + "%s",
		"a word that is not a filler": "key unusual %s",
		"a comma after the word":      "key, %s",
		"an arrow after the word":     "key -> %s",
		"a word after the value":      "%s is the key",
	} {
		text := credentialshapes.Expand(template, value)
		if got := RedactText(text); got != text {
			t.Fatalf("%s: %q is now redacted (%q): update the RISK-NOTES and this test together", name, text, got)
		}
		if got := RedactCredentialShapes(text); got != text {
			t.Fatalf("%s: RedactCredentialShapes now redacts %q", name, text)
		}
	}
}
