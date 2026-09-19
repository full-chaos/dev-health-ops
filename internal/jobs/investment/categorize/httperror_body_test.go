package categorize

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// TestHTTPStatusErrorCarriesTheBodyForTheClassifierOnly: Error() is the
// status alone; the classifier still reads the provider's error vocabulary
// from the body, and the classified error's text does not carry the body.
func TestHTTPStatusErrorCarriesTheBodyForTheClassifierOnly(t *testing.T) {
	t.Parallel()
	for body, want := range map[string]llmErrorKind{
		`{"error":{"code":"insufficient_quota","message":"quota canary-body"}}`: llmErrorAuth,
		`{"error":{"code":"model_not_found","message":"canary-body"}}`:          llmErrorModelNotFound,
		`{"error":"context_length_exceeded canary-body"}`:                       llmErrorContextLength,
		`{"error":"rate_limit canary-body"}`:                                    llmErrorRateLimit,
	} {
		statusErr := &httpStatusError{statusCode: http.StatusForbidden, body: body}
		if got := statusErr.Error(); got != "http 403" {
			t.Errorf("Error() = %q, want the status only", got)
		}
		classified := classifyProviderError(fmt.Errorf("call: %w", statusErr), statusErr.statusCode, nil, "openai", "gpt")
		if classified.kind != want {
			t.Errorf("body %q classified %v, want %v", body, classified.kind, want)
		}
		if strings.Contains(classified.Error(), "canary") {
			t.Errorf("classified error carries the body: %q", classified.Error())
		}
	}
}
