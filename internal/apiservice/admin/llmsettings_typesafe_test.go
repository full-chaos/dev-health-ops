package admin

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
)

// TypeSafe is an org's own decision model: only the categorize override may name
// it, with a listed versioned model and no base_url (the endpoint is fixed).
func TestTypeSafeIsSavableOnlyAsTheCategorizeOverrideWithAListedModelAndNoBaseURL(t *testing.T) {
	code := func(refusal *llmRefusal) string {
		if refusal == nil {
			return ""
		}
		value, _ := refusal.detail.(*pyjson.Object).Get("error")
		text, _ := value.(string)
		return text
	}
	url := func(s string) *string { return &s }
	cat := llmorgsettings.PurposeCategorize
	for name, tc := range map[string]struct {
		provider   string
		purpose    llmorgsettings.Purpose
		hasPurpose bool
		model      string
		baseURL    *string
		want       string
	}{
		"default row":            {"typesafe", "", false, "jev-1.13.0", nil, "provider_not_for_purpose"},
		"default row, spelled":   {" TypeSafe ", "", false, "jev-1.13.0", nil, "provider_not_for_purpose"},
		"investigate override":   {"typesafe", llmorgsettings.PurposeInvestigate, true, "jev-1.13.0", nil, "provider_not_for_purpose"},
		"explain override":       {"typesafe", llmorgsettings.PurposeExplain, true, "jev-1.13.0", nil, "provider_not_for_purpose"},
		"categorize ok":          {"typesafe", cat, true, "jev-1.13.0", nil, ""},
		"categorize empty url":   {"typesafe", cat, true, "jev-1.13.0", url(""), ""},
		"categorize spelled":     {" TYPESAFE ", cat, true, "jev-1.13.0", nil, ""},
		"categorize latest":      {"typesafe", cat, true, "jev-latest", nil, llmorgsettings.CodeCategorizeBYOModelUnsupported},
		"categorize preview":     {"typesafe", cat, true, "jev-preview", nil, llmorgsettings.CodeCategorizeBYOModelUnsupported},
		"categorize unlisted":    {"typesafe", cat, true, "jev-1.14.0", nil, llmorgsettings.CodeCategorizeBYOModelUnsupported},
		"categorize clef model":  {"typesafe", cat, true, "clef", nil, llmorgsettings.CodeCategorizeBYOModelUnsupported},
		"categorize jev url":     {"typesafe", cat, true, "jev-1.13.0", url("https://api.typesafe.ai"), "invalid_base_url"},
		"categorize post url":    {"typesafe", cat, true, "jev-1.13.0", url(llmorgsettings.TypeSafePostURL), "invalid_base_url"},
		"categorize other url":   {"typesafe", cat, true, "jev-1.13.0", url("https://example.com/v1"), "invalid_base_url"},
		"categorize blank url":   {"typesafe", cat, true, "jev-1.13.0", url(" "), "invalid_base_url"},
		"clef still ruled":       {"clef", cat, true, "jev-1.13.0", url(clefAdminBase), llmorgsettings.CodeCategorizeBYOModelUnsupported},
		"clef ok":                {"clef", cat, true, "clef", url(clefAdminBase), ""},
		"other provider":         {"openai", cat, true, "gpt-x", nil, ""},
		"other provider, row":    {"anthropic", "", false, "m", nil, ""},
		"jev model, other prov.": {"openai", cat, true, "jev-latest", nil, ""},
	} {
		if got := code(decisionProviderRefusal(tc.provider, tc.purpose, tc.hasPurpose, tc.model, tc.baseURL)); got != tc.want {
			t.Errorf("%s: refusal %q, want %q", name, got, tc.want)
		}
	}
}
