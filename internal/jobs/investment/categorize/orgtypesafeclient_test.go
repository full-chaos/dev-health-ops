package categorize

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const orgTypeSafeKey = "tsk_PLANTED0123456789abcdefKEYmark"

// The org client sends to the one fixed URL with the org key only, whatever the
// platform TYPESAFE_* variables say.
func TestTheOrgTypeSafeClientSendsToTheFixedURLWithTheOrgKeyOnly(t *testing.T) {
	t.Setenv("TYPESAFE_API_KEY", "tsk_PLATFORMplanted0123456789abcdef")
	t.Setenv("TYPESAFE_BASE_URL", "https://platform.example.invalid")
	t.Setenv("TYPESAFE_MODEL", "jev-1.14.0")
	transport := &clefTransport{answer: func(int, *http.Request) (*http.Response, error) {
		return clefAnswer(http.StatusOK, `{"model":"jev-1.13.0","answers":{},"usage":{"input_tokens":10,"output_tokens":1}}`)
	}}
	client, err := NewOrgTypeSafeClient(OrgTypeSafeClientConfig{APIKey: secrets.NewHidden(orgTypeSafeKey), Model: "jev-1.13.0",
		HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	if client.Model() != "jev-1.13.0" {
		t.Fatalf("model = %q", client.Model())
	}
	if _, err := client.PostSystemOneDetailed(context.Background(), []byte(`{"model":"jev-1.13.0","state":"x","questions":{}}`)); err != nil {
		t.Fatal(err)
	}
	req := transport.requests[0]
	if req.URL.String() != llmorgsettings.TypeSafePostURL || req.Header.Get("Authorization") != "Bearer "+orgTypeSafeKey {
		t.Fatalf("request %s, bearer is the org key: %v", req.URL, req.Header.Get("Authorization") == "Bearer "+orgTypeSafeKey)
	}
	if DefaultTypeSafeBaseURL+typeSafeSystemOnePath != llmorgsettings.TypeSafePostURL {
		t.Fatalf("the platform URL %q and the org URL %q differ", DefaultTypeSafeBaseURL+typeSafeSystemOnePath, llmorgsettings.TypeSafePostURL)
	}
}

// Only a model on the closed list and a key the echo check can protect are
// accepted; nothing about the key is in the error.
func TestTheOrgTypeSafeClientRefusesAnUnlistedModelAndAShortKey(t *testing.T) {
	for name, cfg := range map[string]OrgTypeSafeClientConfig{
		"latest":    {APIKey: secrets.NewHidden(orgTypeSafeKey), Model: "jev-latest"},
		"preview":   {APIKey: secrets.NewHidden(orgTypeSafeKey), Model: "jev-preview"},
		"unlisted":  {APIKey: secrets.NewHidden(orgTypeSafeKey), Model: "jev-1.14.0"},
		"empty":     {APIKey: secrets.NewHidden(orgTypeSafeKey), Model: ""},
		"short key": {APIKey: secrets.NewHidden("tsk_short"), Model: "jev-1.13.0"},
		"no key":    {APIKey: secrets.NewHidden(""), Model: "jev-1.13.0"},
	} {
		client, err := NewOrgTypeSafeClient(cfg)
		if err == nil || client != nil {
			t.Errorf("%s: built", name)
			continue
		}
		if strings.Contains(err.Error(), orgTypeSafeKey) || strings.Contains(err.Error(), "tsk_short") {
			t.Errorf("%s: the key is in %q", name, err)
		}
	}
	if client, err := NewOrgTypeSafeClient(OrgTypeSafeClientConfig{APIKey: secrets.NewHidden(orgTypeSafeKey), Model: "jev-1.13.0"}); err != nil || client == nil {
		t.Fatalf("default client: %v", err)
	}
}
