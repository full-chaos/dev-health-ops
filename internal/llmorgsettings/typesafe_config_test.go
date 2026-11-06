package llmorgsettings

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// The planted key is a marker, never a real credential.
const testTypeSafeKey = "tsk_PLANTED0123456789abcdefKEYmark"

// TypeSafe (jev) is an org's own decision model for the categorize purpose only.
// It needs its own key and takes no base_url: the endpoint is fixed in code, so
// any saved base_url is refused before a lookup.
func TestTypeSafeIsACategorizeOnlyProviderWithAKeyAndNoBaseURL(t *testing.T) {
	key := secrets.NewHidden(testTypeSafeKey)
	categorize := func(mutate func(*OrgConfig)) OrgConfig {
		cfg := OrgConfig{Purpose: PurposeCategorize, Provider: "typesafe", Model: "jev-1.13.0", APIKey: key}
		if mutate != nil {
			mutate(&cfg)
		}
		return cfg
	}
	for name, tc := range map[string]struct {
		cfg  OrgConfig
		want ConfigReason
	}{
		"categorize":            {categorize(nil), ConfigActive},
		"categorize, spelled":   {categorize(func(c *OrgConfig) { c.Provider = " TypeSafe " }), ConfigActive},
		"categorize, no key":    {categorize(func(c *OrgConfig) { c.APIKey = secrets.NewHidden("") }), ConfigMissingCredentials},
		"categorize, jev url":   {categorize(func(c *OrgConfig) { c.BaseURL = "https://api.typesafe.ai" }), ConfigInvalidBaseURL},
		"categorize, post url":  {categorize(func(c *OrgConfig) { c.BaseURL = "https://api.typesafe.ai/v1/systemone" }), ConfigInvalidBaseURL},
		"categorize, other url": {categorize(func(c *OrgConfig) { c.BaseURL = "https://example.com/v1" }), ConfigInvalidBaseURL},
		"categorize, blank url": {categorize(func(c *OrgConfig) { c.BaseURL = " " }), ConfigInvalidBaseURL},
		"default row":           {categorize(func(c *OrgConfig) { c.Purpose = "" }), ConfigUnknownProvider},
		"investigate":           {categorize(func(c *OrgConfig) { c.Purpose = PurposeInvestigate }), ConfigUnknownProvider},
		"explain":               {categorize(func(c *OrgConfig) { c.Purpose = PurposeExplain }), ConfigUnknownProvider},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := CheckOrgConfig(context.Background(), tc.cfg)
			if err != nil || got != tc.want {
				t.Fatalf("CheckOrgConfig = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}
