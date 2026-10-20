package sync

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"testing/iotest"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type legacyJiraBodyDoer struct{ read bool }

func (d legacyJiraBodyDoer) Do(request *http.Request) (*http.Response, error) {
	var body io.Reader = strings.NewReader("not json{")
	if d.read {
		body = io.MultiReader(strings.NewReader(`[{"key":`), iotest.ErrReader(syscall.ECONNRESET))
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(body), Request: request}, nil
}

func TestLegacyJiraDiscoveryCarriesRouteOnBodyReadAndDecodeFailure(t *testing.T) {
	for name, read := range map[string]bool{"read": true, "decode": false} {
		t.Run(name, func(t *testing.T) {
			client, err := providerfoundation.NewHTTPClient("jira", "https://acme-secret.example.test", fakehttp.Client(legacyJiraBodyDoer{read: read}),
				func(*http.Request) error { return nil },
				providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
				providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
			if err != nil {
				t.Fatal(err)
			}
			_, err = discoverJiraLegacyProjects(context.Background(), client)
			var providerErr *providerfoundation.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Route != "/rest/api/3/project" {
				t.Fatalf("error=%v, want a ProviderError naming route=/rest/api/3/project", err)
			}
			if !strings.Contains(err.Error(), "route=/rest/api/3/project") || strings.Contains(err.Error(), "acme-secret") {
				t.Fatalf("error text %q", err.Error())
			}
			if read && !errors.Is(err, syscall.ECONNRESET) {
				t.Fatalf("read failure lost its cause: %v", err)
			}
			if !read && !errors.Is(err, providerfoundation.ErrNormalizationInvalid) {
				t.Fatalf("decode failure lost its sentinel: %v", err)
			}
		})
	}
}
