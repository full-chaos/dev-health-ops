package providersync

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type artifactRedirectDoer struct{ t *testing.T }

func (d artifactRedirectDoer) Do(request *http.Request) (*http.Response, error) {
	d.t.Helper()
	response := &http.Response{Request: request, Header: http.Header{}, Body: http.NoBody}
	if strings.HasSuffix(request.URL.Path, "/artifacts") {
		response.StatusCode = http.StatusFound
		response.Header.Set("Location", "https://storage.example/artifacts/blob")
		return response, nil
	}
	response.StatusCode = http.StatusTooManyRequests
	response.Header.Set("Retry-After", "60")
	return response, nil
}

// A rate limit on the redirected artifact download is classified by this route
// itself (the download is unauthenticated), so it must carry the same budget
// dimension as an authenticated request, or the durable observation is not
// written.
func TestGitLabArtifactRateLimitCarriesADimension(t *testing.T) {
	client := gitHubRepositoryClient(t, artifactRedirectDoer{t: t}, "https://gitlab.example/api/v4")
	_, err := downloadGitLabTestsArtifact(t.Context(), client, "/projects/1", "2")
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorRateLimited {
		t.Fatalf("err = %v, want a rate-limited ProviderError", err)
	}
	if providerErr.Dimension != "rest_core" || providerErr.RetryAfter != time.Minute {
		t.Fatalf("Dimension = %q RetryAfter = %s, want rest_core and 1m", providerErr.Dimension, providerErr.RetryAfter)
	}
}
