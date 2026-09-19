package categorize

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestProviderErrorsCarryNoResponseContent: a 2xx body whose value the
// decoder rejects, and a non-2xx body, both yield an error whose text holds
// no response content.
func TestProviderErrorsCarryNoResponseContent(t *testing.T) {
	t.Parallel()
	for name, response := range map[string]struct {
		status int
		body   string
	}{
		"overflowing number": {http.StatusOK, `{"output_text":"x","usage":{"input_tokens":98765432101234567890}}`},
		"syntax":             {http.StatusOK, `{"output_text":canary-syntax}`},
		"non-2xx body":       {http.StatusBadRequest, `{"error":{"message":"canary-message invalid_request_error"}}`},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(response.status)
			_, _ = w.Write([]byte(response.body))
		}))
		provider := NewOpenAIProvider(OpenAIProviderConfig{APIKey: "unused", BaseURL: server.URL, Model: "gpt-5-nano"})
		_, err := provider.Complete(context.Background(), CompletionRequest{})
		provider.Close()
		server.Close()
		if err == nil {
			t.Fatalf("%s: Complete() = nil", name)
		}
		// What a log line formats is Error(); errors.Unwrap still reaches
		// the original for errors.Is / errors.As.
		if text := err.Error(); strings.Contains(text, "canary") || strings.Contains(text, "98765") {
			t.Errorf("%s: %q carries response content", name, text)
		}
	}
}
