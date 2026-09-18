package categorize

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestNewOllamaProviderNormalizesBaseURLForm proves NewOllamaProvider joins
// its native "/api/chat" route onto the SAME server address regardless of
// which of the two real-world BaseURL spellings a caller supplies --
// normalizeOllamaBaseURL's own doc comment explains why both spellings
// exist (the documented platform config and Python's OllamaProvider default
// both carry a trailing "/v1", which this native client's own route does
// not use). Each case starts an httptest server and asserts the EXACT
// request path it receives, not just the constructed cfg.BaseURL value --
// a normalization bug in only the join step (rather than construction)
// would pass a cfg-level assertion while still sending a broken request.
func TestNewOllamaProviderNormalizesBaseURLForm(t *testing.T) {
	cases := []struct {
		name       string
		baseURLFor func(serverURL string) string
	}{
		{
			name:       "documented /v1 form",
			baseURLFor: func(serverURL string) string { return serverURL + "/v1" },
		},
		{
			name:       "bare host",
			baseURLFor: func(serverURL string) string { return serverURL },
		},
		{
			name:       "trailing slash",
			baseURLFor: func(serverURL string) string { return serverURL + "/" },
		},
		{
			name:       "documented /v1 form with a trailing slash",
			baseURLFor: func(serverURL string) string { return serverURL + "/v1/" },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotPath string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				_, _ = w.Write([]byte(`{"message":{"role":"assistant","content":"ok"},"done":true}`))
			}))
			t.Cleanup(server.Close)

			provider := NewOllamaProvider(OllamaProviderConfig{
				BaseURL: tc.baseURLFor(server.URL),
				Model:   "gemma3",
			})
			t.Cleanup(func() { provider.Close() })

			if _, err := provider.Complete(context.Background(), CompletionRequest{Prompt: "x", SystemMessage: "y"}); err != nil {
				t.Fatalf("Complete: %v", err)
			}
			if gotPath != "/api/chat" {
				t.Fatalf("request path = %q, want /api/chat (BaseURL %q was not normalized before joining)", gotPath, provider.cfg.BaseURL)
			}
		})
	}
}

// TestNewOllamaProviderUnsetBaseURLDefaultsToNoSuffix is the fourth required
// case (unset/default): with no BaseURL supplied at all, the default itself
// carries no "/v1" to strip, so normalization is a no-op and the default
// stays exactly defaultOllamaBaseURL -- the SAME server Python's own
// OllamaProvider default ("http://localhost:11434/v1", local.py:42) points
// at, host:port for host:port, differing only in the path segment each
// plane's own wire protocol actually needs.
func TestNewOllamaProviderUnsetBaseURLDefaultsToNoSuffix(t *testing.T) {
	provider := NewOllamaProvider(OllamaProviderConfig{Model: "gemma3"})
	if provider.cfg.BaseURL != defaultOllamaBaseURL {
		t.Fatalf("BaseURL = %q, want unchanged default %q", provider.cfg.BaseURL, defaultOllamaBaseURL)
	}
	if provider.cfg.BaseURL != normalizeOllamaBaseURL(provider.cfg.BaseURL) {
		t.Fatalf("normalizeOllamaBaseURL is not idempotent on the default: %q", provider.cfg.BaseURL)
	}
}
