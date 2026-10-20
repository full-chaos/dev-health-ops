package providersync

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type sizeRepeatReader struct{ n int }

func (r *sizeRepeatReader) Read(p []byte) (int, error) {
	if r.n <= 0 {
		return 0, io.EOF
	}
	n := min(len(p), r.n)
	for i := 0; i < n; i++ {
		p[i] = ' '
	}
	r.n -= n
	return n, nil
}

type sizeDoer struct{ bytes int }

func (d sizeDoer) Do(request *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Request: request,
		Body: io.NopCloser(io.MultiReader(strings.NewReader("["), &sizeRepeatReader{n: d.bytes}))}, nil
}

func sizeClient(t *testing.T, provider string, bytes int) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(provider, "https://api.example.test", fakehttp.Client(sizeDoer{bytes: bytes}),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestSizeCapFailuresOnRoutedBodiesCarryRouteAndKeepTypedErrors(t *testing.T) {
	ctx := context.Background()
	t.Run("fetchObjectRoute keeps ObjectTooLargeError", func(t *testing.T) {
		var out map[string]any
		err := fetchObjectRoute(ctx, sizeClient(t, "github", nativeMaxObjectBytes+10), "/repos/{owner}/{repo}", "/repos/acme-secret/api", &out)
		requireRouteFailure(t, err, "/repos/{owner}/{repo}", providerfoundation.ReasonBodyTooLarge)
		var tooLarge *providerfoundation.ObjectTooLargeError
		if !errors.As(err, &tooLarge) || !errors.Is(err, providerfoundation.ErrNormalizationInvalid) {
			t.Fatalf("error=%v lost ObjectTooLargeError or its sentinel", err)
		}
	})
	t.Run("github artifact download keeps the oversized error", func(t *testing.T) {
		_, _, _, err := downloadGitHubTestsArtifact(ctx, sizeClient(t, "github", 4096), "/repos/acme-secret/api", "7", 1024)
		requireRouteFailure(t, err, githubArtifactZipRoute, providerfoundation.ReasonBodyTooLarge)
		if !errors.Is(err, ErrGitHubTestsArtifactOversized) {
			t.Fatalf("error=%v lost ErrGitHubTestsArtifactOversized", err)
		}
	})
	t.Run("gitlab artifact download keeps ErrGitLabTestsIncomplete", func(t *testing.T) {
		_, err := downloadGitLabTestsArtifact(ctx, sizeClient(t, "gitlab", gitLabTestsMaxDownload+10), "/projects/acme-secret", "9")
		requireRouteFailure(t, err, gitlabJobArtifactsRoute, providerfoundation.ReasonBodyTooLarge)
		if !errors.Is(err, ErrGitLabTestsIncomplete) {
			t.Fatalf("error=%v lost ErrGitLabTestsIncomplete", err)
		}
	})
}
