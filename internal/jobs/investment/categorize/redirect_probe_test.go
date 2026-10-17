package categorize

import (
	"context"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// A caller-supplied client of the default redirect policy never carries the provider's API key to another origin: the
// base answers a redirect and the other origin sees no request at all (D4124).
func TestASuppliedClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	for _, kind := range sharedProviderKinds {
		t.Run(string(kind), func(t *testing.T) {
			probe := redirectprobe.New(t)
			sharedRedirectAttempt(t, kind, probe.Base.URL, probe.Client())
			probe.Assert(t)
		})
	}
	t.Run("ollama", func(t *testing.T) {
		probe := redirectprobe.New(t)
		provider := NewOllamaProvider(OllamaProviderConfig{APIKey: secrets.NewHidden("SECRET"), BaseURL: probe.Base.URL, HTTPClient: probe.Client()})
		_, _, _, _ = provider.executeChatRequest(context.Background(), ollamaChatRequest{})
		probe.Assert(t)
	})
}

// No client supplied (the nil branch: newHardenedHTTPClient) follows no redirect either.
func TestTheDefaultProviderClientsNeverFollowARedirectToAnotherOrigin(t *testing.T) {
	for _, kind := range sharedProviderKinds {
		t.Run(string(kind), func(t *testing.T) {
			probe := redirectprobe.New(t)
			sharedRedirectAttempt(t, kind, probe.Base.URL, nil)
			probe.Assert(t)
		})
	}
	t.Run("ollama", func(t *testing.T) {
		probe := redirectprobe.New(t)
		provider := NewOllamaProvider(OllamaProviderConfig{APIKey: secrets.NewHidden("SECRET"), BaseURL: probe.Base.URL})
		_, _, _, _ = provider.executeChatRequest(context.Background(), ollamaChatRequest{})
		probe.Assert(t)
	})
}

// The hardened default's own policy (llm/providers/_http.py: follow_redirects=False) is pinned on its own.
func TestTheHardenedClientRefusesRedirectsOnItsOwn(t *testing.T) {
	client := newHardenedHTTPClient()
	if client.CheckRedirect == nil || client.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
		t.Fatal("the hardened client follows redirects")
	}
}

// sharedProviderKinds are the kinds served by the shared llm client.
var sharedProviderKinds = []ProviderKind{ProviderKindOpenAI, ProviderKindAnthropic, ProviderKindGemini, ProviderKindQwen, ProviderKindLocal}

// sharedRedirectAttempt makes one categorization call of kind against base,
// with client (nil = the shared package's own client).
func sharedRedirectAttempt(t *testing.T, kind ProviderKind, base string, client *http.Client) {
	t.Helper()
	provider, err := NewSharedProvider(SharedProviderConfig{Kind: kind, APIKey: secrets.NewHidden("SECRET"), BaseURL: base, Model: "m", HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = provider.Complete(context.Background(), CategorizationRequest("p"))
}
