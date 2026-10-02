package categorize

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// A caller-supplied client of the default redirect policy never carries the provider's API key to another origin: the
// base answers a redirect and the other origin sees no request at all (D4124).
func TestASuppliedClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	t.Run("openai", func(t *testing.T) {
		probe := redirectprobe.New(t)
		provider := NewOpenAIProvider(OpenAIProviderConfig{APIKey: "SECRET", BaseURL: probe.Base.URL, HTTPClient: probe.Client()})
		_, _, _ = provider.executeResponsesRequest(context.Background(), openAIResponsesRequest{})
		probe.Assert(t)
	})
	t.Run("ollama", func(t *testing.T) {
		probe := redirectprobe.New(t)
		provider := NewOllamaProvider(OllamaProviderConfig{APIKey: "SECRET", BaseURL: probe.Base.URL, HTTPClient: probe.Client()})
		_, _, _, _ = provider.executeChatRequest(context.Background(), ollamaChatRequest{})
		probe.Assert(t)
	})
	t.Run("local", func(t *testing.T) {
		probe := redirectprobe.New(t)
		provider := NewLocalProvider(LocalProviderConfig{APIKey: "SECRET", BaseURL: probe.Base.URL, HTTPClient: probe.Client()})
		_, _, _ = provider.executeChatCompletionRequest(context.Background(), localChatRequest{})
		probe.Assert(t)
	})
}

// No client supplied (the nil branch: newHardenedHTTPClient) follows no redirect either.
func TestTheDefaultProviderClientsNeverFollowARedirectToAnotherOrigin(t *testing.T) {
	t.Run("openai", func(t *testing.T) {
		probe := redirectprobe.New(t)
		provider := NewOpenAIProvider(OpenAIProviderConfig{APIKey: "SECRET", BaseURL: probe.Base.URL})
		_, _, _ = provider.executeResponsesRequest(context.Background(), openAIResponsesRequest{})
		probe.Assert(t)
	})
	t.Run("ollama", func(t *testing.T) {
		probe := redirectprobe.New(t)
		provider := NewOllamaProvider(OllamaProviderConfig{APIKey: "SECRET", BaseURL: probe.Base.URL})
		_, _, _, _ = provider.executeChatRequest(context.Background(), ollamaChatRequest{})
		probe.Assert(t)
	})
	t.Run("local", func(t *testing.T) {
		probe := redirectprobe.New(t)
		provider := NewLocalProvider(LocalProviderConfig{APIKey: "SECRET", BaseURL: probe.Base.URL})
		_, _, _ = provider.executeChatCompletionRequest(context.Background(), localChatRequest{})
		probe.Assert(t)
	})
}
