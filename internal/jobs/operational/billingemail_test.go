package operational

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// goldenCase mirrors one testdata/billing_email/*.json fixture.
//
// These fixtures were produced by running the REAL Python renderer
// (`billing_emails.send_*` -> `EmailService.render_template`) once, before it
// was deleted in this same change. They are the only surviving evidence of
// what that renderer emitted, so they are frozen: a diff to an expected_html
// or expected_subject value is a behavior change to a customer-visible email
// and must be argued for, never regenerated to make a test pass.
type goldenCase struct {
	EmailType  string         `json:"email_type"`
	Case       string         `json:"case"`
	AppBaseURL string         `json:"app_base_url"`
	Owner      goldenOwner    `json:"owner"`
	Attributes map[string]any `json:"attributes"`
	ToAddress  string         `json:"expected_to_address"`
	Subject    string         `json:"expected_subject"`
	HTML       string         `json:"expected_html"`
}

type goldenOwner struct {
	Email    string `json:"email"`
	FullName string `json:"full_name"`
	OrgName  string `json:"org_name"`
}

func loadGoldenCases(t *testing.T) []goldenCase {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("testdata", "billing_email", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no billing email golden fixtures found")
	}
	cases := make([]goldenCase, 0, len(paths))
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var golden goldenCase
		if err := json.Unmarshal(raw, &golden); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		cases = append(cases, golden)
	}
	return cases
}

// TestBillingEmailRenderingMatchesTheFrozenPythonGoldens is the parity proof
// for the whole port: every byte of subject and body for every one of the
// seven types, against output captured from the Python implementation.
func TestBillingEmailRenderingMatchesTheFrozenPythonGoldens(t *testing.T) {
	for _, golden := range loadGoldenCases(t) {
		t.Run(golden.EmailType+"/"+golden.Case, func(t *testing.T) {
			attributes := attributesFromGolden(t, golden.Attributes)
			owner := OwnerContact{
				Email:    golden.Owner.Email,
				FullName: golden.Owner.FullName,
				OrgName:  golden.Owner.OrgName,
			}
			rendered, err := RenderBillingEmail(
				golden.EmailType, attributes, owner, golden.AppBaseURL)
			if err != nil {
				t.Fatalf("render failed: %v", err)
			}
			if rendered.Subject != golden.Subject {
				t.Errorf("subject diverged from the Python golden\n got: %q\nwant: %q",
					rendered.Subject, golden.Subject)
			}
			if rendered.HTML != golden.HTML {
				t.Errorf("body diverged from the Python golden\n got: %q\nwant: %q",
					rendered.HTML, golden.HTML)
			}
		})
	}
}

// TestGoldensCoverEverySupportedEmailType keeps the parity proof exhaustive:
// a newly supported type with no frozen fixture would otherwise ship
// unverified, and the golden suite above would still pass.
func TestGoldensCoverEverySupportedEmailType(t *testing.T) {
	covered := map[string]int{}
	for _, golden := range loadGoldenCases(t) {
		covered[golden.EmailType]++
	}
	for _, emailType := range []string{
		EmailTypeInvoiceReceipt, EmailTypePaymentFailed, EmailTypeSubscriptionChanged,
		EmailTypeSubscriptionCanceled, EmailTypeTrialStarted, EmailTypeTrialExpiring,
		EmailTypeTrialExpired,
	} {
		if covered[emailType] < 2 {
			t.Errorf("%s has %d golden fixtures; want at least 2 (one ASCII, one non-ASCII)",
				emailType, covered[emailType])
		}
	}
	if len(covered) != 7 {
		t.Errorf("golden fixtures cover %d types, want exactly the 7 supported ones: %v",
			len(covered), covered)
	}
}

// TestGoldensExerciseNonASCIIAndFractionalAmounts guards the two properties
// the fixtures were chosen for. Without this, someone could quietly replace
// the unicode fixtures with ASCII ones and the parity suite would still pass
// while proving strictly less.
func TestGoldensExerciseNonASCIIAndFractionalAmounts(t *testing.T) {
	sawNonASCII, sawFractionalAmount := false, false
	for _, golden := range loadGoldenCases(t) {
		for index := 0; index < len(golden.HTML); index++ {
			if golden.HTML[index] > 127 {
				sawNonASCII = true
				break
			}
		}
		if cents, ok := golden.Attributes["amount_cents"]; ok {
			if value, isNumber := cents.(float64); isNumber && int64(value)%100 != 0 {
				sawFractionalAmount = true
			}
		}
	}
	if !sawNonASCII {
		t.Error("no golden fixture renders a non-ASCII body")
	}
	if !sawFractionalAmount {
		t.Error("no golden fixture uses an amount with a non-zero cents component")
	}
}

func attributesFromGolden(t *testing.T, raw map[string]any) BillingAttributes {
	t.Helper()
	// Round-trip through the production decoder rather than assigning fields
	// directly, so the fixtures exercise the same JSONB coercion a stored row
	// goes through instead of a test-only shortcut.
	encoded, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	attributes, err := DecodeBillingAttributes(encoded)
	if err != nil {
		t.Fatalf("golden attributes did not decode: %v", err)
	}
	return attributes
}

func TestFormatAmountCentsMatchesPythonDecimalFormatting(t *testing.T) {
	// Expected values are `f"{Decimal(cents) / 100:.2f}"`.
	for _, test := range []struct {
		cents int64
		want  string
	}{
		{0, "0.00"},
		{7, "0.07"},
		{70, "0.70"},
		{100, "1.00"},
		{4999, "49.99"},
		{123456, "1234.56"},
		{-5, "-0.05"},
		{-123456, "-1234.56"},
		{9223372036854775807, "92233720368547758.07"},
		{-9223372036854775808, "-92233720368547758.08"},
	} {
		if got := formatAmountCents(test.cents); got != test.want {
			t.Errorf("formatAmountCents(%d) = %q, want %q", test.cents, got, test.want)
		}
	}
}

func TestDisplayTierFallsBackLikePython(t *testing.T) {
	for input, want := range map[string]string{
		"":         "Team",
		"   ":      "Team",
		"\t\n":     "Team",
		"Team":     "Team",
		"  Équipe": "Équipe",
	} {
		if got := displayTier(input); got != want {
			t.Errorf("displayTier(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestNormalizeAppBaseURLStripsTrailingSlashesAndDefaults(t *testing.T) {
	for input, want := range map[string]string{
		"":                           "https://example.com",
		"https://app.example.test/":  "https://app.example.test",
		"https://app.example.test":   "https://app.example.test",
		"https://app.example.test//": "https://app.example.test",
	} {
		if got := normalizeAppBaseURL(input); got != want {
			t.Errorf("normalizeAppBaseURL(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestRenderBillingEmailRejectsUnknownType(t *testing.T) {
	_, err := RenderBillingEmail("not_a_billing_email", BillingAttributes{},
		OwnerContact{Email: "a@b.test", FullName: "A", OrgName: "B"}, "https://x.test")
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("want an unsupported-type error, got %v", err)
	}
}

// TestFormatTemplateRefusesConstructsItCannotReproduce is the guard that keeps
// the golden fixtures meaningful: the renderer models a SUBSET of Python's
// str.format, so a template growing anything richer must fail loudly rather
// than render something the frozen goldens never checked.
func TestFormatTemplateRefusesConstructsItCannotReproduce(t *testing.T) {
	context := map[string]string{"name": "value"}
	for _, template := range []string{
		"{name:>10}",    // format spec
		"{name!r}",      // conversion
		"{0}",           // positional index
		"{obj.attr}",    // attribute access
		"{missing}",     // absent key: Python raised KeyError
		"{unterminated", // unterminated field
		"a } b",         // unescaped closing brace
	} {
		if _, err := formatTemplate(template, context); err == nil {
			t.Errorf("formatTemplate(%q) succeeded; want a refusal", template)
		}
	}
}

func TestFormatTemplateHandlesEscapedBraces(t *testing.T) {
	got, err := formatTemplate("{{literal}} {name} {{}}", map[string]string{"name": "x"})
	if err != nil {
		t.Fatal(err)
	}
	if want := "{literal} x {}"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
