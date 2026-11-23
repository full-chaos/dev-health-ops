package providersync

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/egressguard"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type reproRefusedDoer func(*http.Request) (*http.Response, error)

func (f reproRefusedDoer) Do(r *http.Request) (*http.Response, error) { return f(r) }

// REPRO F2 (classifier): a refused outbound connection is a transport error with status 0; it must not be a recorded 4xx.
func TestReproRefusedConnectionIsNotServedAsOther4xx(t *testing.T) {
	err := &providerfoundation.ProviderError{Class: providerfoundation.ErrorPermanent, Reason: providerfoundation.TransportReasonEgressRefused}
	verdict, reason := classifyLookupFailure(err, nil)
	t.Logf("verdict=%v reason=%q status=%d", verdict, reason, err.StatusCode)
	if verdict != lookupFails {
		t.Fatalf("status-0 egress refusal is classed verdict=%v reason=%q, want lookupFails (a returned error)", verdict, reason)
	}
}

// REPRO F2 (route): github deployments list refused at the dial: the unit must return an error and hold the watermark.
func TestReproRefusedConnectionGitHubDeploymentsListHoldsWatermark(t *testing.T) {
	doer := reproRefusedDoer(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/repos/acme/api/deployments":
			return nil, &egressguard.RefusedError{Class: "private"}
		case "/repos/acme/api/releases":
			return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`[]`)), Request: r}, nil
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(gitHubRepositoryFixture)), Request: r}, nil
	})
	batch, err := (GitHubDeploymentsRouteHandler{}).Collect(context.Background(), nativeTestClaim("github", "deployments"), providerfoundation.Credential{}, gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com"), time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	t.Logf("err=%v watermark=%v result=%v", err, batch.Watermark, batch.Result)
	if err == nil && batch.Watermark != nil {
		t.Fatalf("refused connection: no error and the watermark advanced to %v", *batch.Watermark)
	}
}
