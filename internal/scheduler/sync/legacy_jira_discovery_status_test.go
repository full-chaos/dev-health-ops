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

type acceptedStatusDoer struct{}

func (acceptedStatusDoer) Do(request *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusAccepted, Header: http.Header{}, Request: request, Body: io.NopCloser(strings.NewReader("[]"))}, nil
}

func TestLegacyJiraDiscoveryNon200Success(t *testing.T) {
	client, err := providerfoundation.NewHTTPClient("jira", "https://acme-secret.example.test", fakehttp.Client(acceptedStatusDoer{}),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	_, err = discoverJiraLegacyProjects(context.Background(), client)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Route != "/rest/api/3/project" || providerErr.Reason != providerfoundation.ReasonStatus {
		t.Fatalf("error=%v, want route=/rest/api/3/project reason=status", err)
	}
}
