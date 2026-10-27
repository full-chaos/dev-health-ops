package llmorgsettings

import (
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const (
	testClefAccount = "0123456789abcdef0123456789abcdef"
	testClefBase    = "https://api.cloudflare.com/client/v4/accounts/" + testClefAccount + "/ai"
)

func TestParseClefBaseURLAcceptsOnlyTheExactAccountURL(t *testing.T) {
	for _, ok := range []string{testClefBase, testClefBase + "/"} {
		if account, accepted := ParseClefBaseURL(ok); !accepted || account != testClefAccount {
			t.Errorf("%q = %q, %v; want the account id", ok, account, accepted)
		}
	}
	for name, raw := range map[string]string{
		"http":          "http://api.cloudflare.com/client/v4/accounts/" + testClefAccount + "/ai",
		"other host":    "https://api.typesafe.ai/client/v4/accounts/" + testClefAccount + "/ai",
		"uppercase":     "https://API.CLOUDFLARE.COM/client/v4/accounts/" + testClefAccount + "/ai",
		"trailing dot":  "https://api.cloudflare.com./client/v4/accounts/" + testClefAccount + "/ai",
		"port":          "https://api.cloudflare.com:8443/client/v4/accounts/" + testClefAccount + "/ai",
		"user info":     "https://a:b@api.cloudflare.com/client/v4/accounts/" + testClefAccount + "/ai",
		"ip literal":    "https://[::1]/client/v4/accounts/" + testClefAccount + "/ai",
		"idn lookalike": "https://api.cloudflаre.com/client/v4/accounts/" + testClefAccount + "/ai",
		"escaped slash": "https://api.cloudflare.com/client/v4/accounts/" + testClefAccount + "%2Fai",
		"double slash":  "https://api.cloudflare.com//client/v4/accounts/" + testClefAccount + "/ai",
		"query":         testClefBase + "?a=b",
		"empty query":   testClefBase + "?",
		"fragment":      testClefBase + "#x",
		"other path":    "https://api.cloudflare.com/client/v4/accounts/" + testClefAccount + "/ai/v1",
		"short account": "https://api.cloudflare.com/client/v4/accounts/abc/ai",
		"upper account": "https://api.cloudflare.com/client/v4/accounts/" + strings.ToUpper(testClefAccount) + "/ai",
		"padded":        " " + testClefBase,
		"empty":         "",
	} {
		if account, accepted := ParseClefBaseURL(raw); accepted || account != "" {
			t.Errorf("%s: %q was accepted as %q", name, raw, account)
		}
	}
}

func TestClefModelsAreAClosedMapAndNeverJoinedIntoAPath(t *testing.T) {
	for _, model := range []string{"clef", "clef-flash"} {
		url, ok := ClefPostURL(testClefBase, model)
		want := "https://api.cloudflare.com/client/v4/accounts/" + testClefAccount + "/ai/run/@cf/cloudflare/" + model
		if !ok || url != want {
			t.Errorf("%s: %q %v, want %q", model, url, ok, want)
		}
	}
	for _, model := range []string{"", "CLEF", "clef/../x", "clef?x", "clef-flash/", " clef", "@cf/cloudflare/clef", "clef-pro", "jev-1.13.0"} {
		if url, ok := ClefPostURL(testClefBase, model); ok || url != "" {
			t.Errorf("model %q was accepted: %q", model, url)
		}
	}
}

// clef writes no text, so it is known to the categorize purpose only.
func TestClefIsKnownToTheCategorizePurposeOnly(t *testing.T) {
	for purpose, want := range map[Purpose]ConfigReason{
		PurposeCategorize:  ConfigActive,
		"":                 ConfigUnknownProvider,
		PurposeInvestigate: ConfigUnknownProvider,
		PurposeExplain:     ConfigUnknownProvider,
	} {
		if _, reason := CheckProviderFor(purpose, " Clef "); reason != want {
			t.Errorf("purpose %q: %s, want %s", purpose, reason, want)
		}
	}
	if IsKnownProvider(ProviderClef) {
		t.Error("clef is a known provider of every purpose")
	}
	if _, reason := CheckProvider(ProviderClef); reason != ConfigUnknownProvider {
		t.Errorf("CheckProvider(clef) = %s: the default row must not know clef", reason)
	}
}

func TestACategorizeClefConfigNeedsTheTokenAndTheExactAccountURL(t *testing.T) {
	cfg := func(token, base string) OrgConfig {
		return OrgConfig{Purpose: PurposeCategorize, Provider: ProviderClef, Model: "clef", APIKey: secrets.NewHidden(token), BaseURL: base}
	}
	for name, tc := range map[string]struct {
		cfg  OrgConfig
		want ConfigReason
	}{
		"ok":                {cfg("tok-0123456789abcdef", testClefBase), ConfigActive},
		"no token":          {cfg("", testClefBase), ConfigMissingCredentials},
		"no base url":       {cfg("tok-0123456789abcdef", ""), ConfigInvalidBaseURL},
		"other host":        {cfg("tok-0123456789abcdef", "https://example.com/client/v4/accounts/"+testClefAccount+"/ai"), ConfigInvalidBaseURL},
		"token in base url": {cfg("tok-0123456789abcdef", testClefBase+"?k=tok-0123456789abcdef"), ConfigInvalidBaseURL},
		"key marked in url": {func() OrgConfig { c := cfg("tok-0123456789abcdef", testClefBase); c.KeyInBaseURL = true; return c }(), ConfigInvalidBaseURL},
		"default row":       {func() OrgConfig { c := cfg("tok-0123456789abcdef", testClefBase); c.Purpose = ""; return c }(), ConfigUnknownProvider},
		"investigate row": {func() OrgConfig {
			c := cfg("tok-0123456789abcdef", testClefBase)
			c.Purpose = PurposeInvestigate
			return c
		}(), ConfigUnknownProvider},
	} {
		got, err := CheckOrgConfig(context.Background(), tc.cfg)
		if err != nil || got != tc.want {
			t.Errorf("%s: %s, %v; want %s", name, got, err, tc.want)
		}
	}
}

func TestClefEndpointIsDerivedFromTheValidatedBaseURL(t *testing.T) {
	if EndpointTypeWorkersAI != "workers_ai" {
		t.Fatalf("EndpointTypeWorkersAI = %q", EndpointTypeWorkersAI)
	}
	for _, raw := range []string{testClefBase, testClefBase + "/"} {
		endpointType, gatewayID, ok := ClefEndpoint(raw)
		if !ok || endpointType != EndpointTypeWorkersAI || gatewayID != "" {
			t.Errorf("%q = %q, %q, %v; want workers_ai with no gateway id", raw, endpointType, gatewayID, ok)
		}
	}
	for name, raw := range map[string]string{
		"gateway host": "https://gateway.ai.cloudflare.com/v1/" + testClefAccount + "/my-gateway/workers-ai",
		"http":         "http://api.cloudflare.com/client/v4/accounts/" + testClefAccount + "/ai",
		"query":        testClefBase + "?a=b",
		"empty":        "",
	} {
		if endpointType, gatewayID, ok := ClefEndpoint(raw); ok || endpointType != "" || gatewayID != "" {
			t.Errorf("%s: %q = %q, %q, %v; want a refusal with empty values", name, raw, endpointType, gatewayID, ok)
		}
	}
}
