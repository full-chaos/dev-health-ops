package sync

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

type paddedJiraArrayDoer struct{}

type spaceReader struct{ n int }

func (r *spaceReader) Read(p []byte) (int, error) {
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

func (paddedJiraArrayDoer) Do(request *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Request: request,
		Body: io.NopCloser(io.MultiReader(strings.NewReader("[]"), &spaceReader{n: 32<<20 + 100}))}, nil
}

func TestLegacyJiraDiscoveryBodyOverTheCapFailsWithRoute(t *testing.T) {
	client, err := providerfoundation.NewHTTPClient("jira", "https://acme-secret.example.test", fakehttp.Client(paddedJiraArrayDoer{}),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	_, err = discoverJiraLegacyProjects(context.Background(), client)
	var providerErr *providerfoundation.ProviderError
	if err == nil {
		t.Fatal("a body over the 32 MiB cap was accepted")
	}
	if !errors.As(err, &providerErr) || providerErr.Route != "/rest/api/3/project" || providerErr.Reason != providerfoundation.ReasonBodyTooLarge {
		t.Fatalf("error=%v, want route=/rest/api/3/project reason=body_too_large", err)
	}
}
