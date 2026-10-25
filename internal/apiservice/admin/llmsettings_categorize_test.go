package admin

import (
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
)

func boolPtr(v bool) *bool { return &v }

// Ruling A, one test for each branch: the categorize purpose keeps its 400
// unless the stored opt-in is "true" or the same PUT sets it true.
func TestCategorizePurposeIsAcceptedOnlyForAnOptedInOrg(t *testing.T) {
	stored := func(value bool) func() (bool, error) { return func() (bool, error) { return value, nil } }
	for name, tc := range map[string]struct {
		body   *bool
		stored func() (bool, error)
		want   bool
	}{
		"no body field, stored off":           {nil, stored(false), false},
		"no body field, stored on":            {nil, stored(true), true},
		"body true, stored off":               {boolPtr(true), stored(false), true},
		"body true, stored on":                {boolPtr(true), stored(true), true},
		"body false, stored off":              {boolPtr(false), stored(false), false},
		"body false, stored on (opt-out PUT)": {boolPtr(false), stored(true), true},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := categorizeOptedIn(tc.body, tc.stored)
			if err != nil || got != tc.want {
				t.Fatalf("categorizeOptedIn = %v, %v; want %v", got, err, tc.want)
			}
		})
	}
	if _, err := categorizeOptedIn(nil, func() (bool, error) { return false, errors.New("down") }); err == nil {
		t.Fatal("an unreadable opt-in must be an error, never read as off or on")
	}
	read := false
	if on, _ := categorizeOptedIn(boolPtr(true), func() (bool, error) { read = true; return false, nil }); !on || read {
		t.Fatal("a request that sets the opt-in true must not need the stored value")
	}
}

func TestCategorizeIsNotAListedPurposeOfTheAdminAPI(t *testing.T) {
	if _, valid := llmorgsettings.ParsePurpose("categorize"); valid {
		t.Fatal("categorize must not parse as an ordinary purpose")
	}
}

func strPtrClef(s string) *string { return &s }

const clefAdminBase = "https://api.cloudflare.com/client/v4/accounts/0123456789abcdef0123456789abcdef/ai"

// clef is a decision model with no text route: only the categorize override may
// name it, with a model clef serves and the org's exact account URL.
func TestClefIsSavableOnlyAsTheCategorizeOverrideWithItsExactTarget(t *testing.T) {
	code := func(refusal *llmRefusal) string {
		if refusal == nil {
			return ""
		}
		value, _ := refusal.detail.(*pyjson.Object).Get("error")
		text, _ := value.(string)
		return text
	}
	for name, tc := range map[string]struct {
		provider   string
		purpose    llmorgsettings.Purpose
		hasPurpose bool
		model      string
		baseURL    *string
		want       string
	}{
		"default row":          {"clef", "", false, "clef", strPtrClef(clefAdminBase), "provider_not_for_purpose"},
		"default row, upper":   {" CLEF ", "", false, "clef", strPtrClef(clefAdminBase), "provider_not_for_purpose"},
		"investigate override": {"clef", llmorgsettings.PurposeInvestigate, true, "clef", strPtrClef(clefAdminBase), "provider_not_for_purpose"},
		"explain override":     {"clef", llmorgsettings.PurposeExplain, true, "clef", strPtrClef(clefAdminBase), "provider_not_for_purpose"},
		"categorize ok":        {"clef", llmorgsettings.PurposeCategorize, true, "clef", strPtrClef(clefAdminBase), ""},
		"categorize flash ok":  {"clef", llmorgsettings.PurposeCategorize, true, "clef-flash", strPtrClef(clefAdminBase + "/"), ""},
		"categorize model":     {"clef", llmorgsettings.PurposeCategorize, true, "clef-pro", strPtrClef(clefAdminBase), llmorgsettings.CodeCategorizeBYOModelUnsupported},
		"categorize traversal": {"clef", llmorgsettings.PurposeCategorize, true, "clef/../x", strPtrClef(clefAdminBase), llmorgsettings.CodeCategorizeBYOModelUnsupported},
		"categorize no url":    {"clef", llmorgsettings.PurposeCategorize, true, "clef", nil, "invalid_base_url"},
		"categorize jev url":   {"clef", llmorgsettings.PurposeCategorize, true, "clef", strPtrClef("https://api.typesafe.ai"), "invalid_base_url"},
		"categorize other url": {"clef", llmorgsettings.PurposeCategorize, true, "clef", strPtrClef("https://example.com/client/v4/accounts/0123456789abcdef0123456789abcdef/ai"), "invalid_base_url"},
		"other provider":       {"openai", llmorgsettings.PurposeCategorize, true, "gpt-x", nil, ""},
		"other provider, row":  {"anthropic", "", false, "m", nil, ""},
	} {
		if got := code(clefRefusal(tc.provider, tc.purpose, tc.hasPurpose, tc.model, tc.baseURL)); got != tc.want {
			t.Errorf("%s: refusal %q, want %q", name, got, tc.want)
		}
	}
}
