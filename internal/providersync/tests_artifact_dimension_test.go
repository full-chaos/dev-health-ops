package providersync

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type artifactRedirectDoer struct{ t *testing.T }

func (d artifactRedirectDoer) Do(request *http.Request) (*http.Response, error) {
	d.t.Helper()
	response := &http.Response{Request: request, Header: http.Header{}, Body: http.NoBody}
	if request.URL.Host != "storage.example" {
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

// The GitHub artifact route classifies its redirected download like the GitLab
// one: a rate limit defers (rate_limited with a dimension) instead of becoming a
// permanent failure.
func TestGitHubArtifactRateLimitIsRateLimitedWithADimension(t *testing.T) {
	client := gitHubRepositoryClient(t, artifactRedirectDoer{t: t}, "https://api.github.com")
	_, _, _, err := downloadGitHubTestsArtifact(t.Context(), client, "/repos/acme/api", "9", 1<<20)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorRateLimited {
		t.Fatalf("err = %v, want a rate-limited ProviderError", err)
	}
	if providerErr.Dimension != "rest_core" || providerErr.RetryAfter != time.Minute {
		t.Fatalf("Dimension = %q RetryAfter = %s, want rest_core and 1m", providerErr.Dimension, providerErr.RetryAfter)
	}
}

type graphQLLimitedDoer struct{}

func (graphQLLimitedDoer) Do(request *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusTooManyRequests, Request: request, Body: http.NoBody,
		Header: http.Header{"Retry-After": {"60"}},
	}, nil
}

// The blob reads of file and blame sync go through GraphQL but their dispatch
// budget is contents_blob (GitHub) or rest_core (GitLab): a rate limit on them
// carries that dimension, not graphql_cost, so the cooldown gate holds the
// units that budgeted it.
func TestBlobReadRateLimitsCarryTheDimensionTheirUnitsBudget(t *testing.T) {
	size := 10
	cases := []struct {
		name string
		call func(client *providerfoundation.HTTPClient) error
		want string
	}{
		{"github files", func(client *providerfoundation.HTTPClient) error {
			_, err := fetchGitHubFileContents(t.Context(), client, "acme", "api", "main", []string{"main.go"}, map[string]*int{"main.go": &size})
			return err
		}, "contents_blob"},
		{"github blame", func(client *providerfoundation.HTTPClient) error {
			_, err := fetchGitHubBlame(t.Context(), client, "acme", "api", "main", "main.go")
			return err
		}, "contents_blob"},
		{"gitlab files", func(client *providerfoundation.HTTPClient) error {
			_, err := fetchGitLabGraphQLBlobs(t.Context(), client, "acme/api", "main", []string{"main.go"}, "")
			return err
		}, "rest_core"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call(gitHubRepositoryClient(t, graphQLLimitedDoer{}, "https://api.github.com"))
			var providerErr *providerfoundation.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorRateLimited {
				t.Fatalf("err = %v, want a rate-limited ProviderError", err)
			}
			if providerErr.Dimension != tc.want {
				t.Fatalf("Dimension = %q, want %q", providerErr.Dimension, tc.want)
			}
		})
	}
}
