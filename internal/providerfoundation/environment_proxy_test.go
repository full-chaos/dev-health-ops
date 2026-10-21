package providerfoundation

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/httpguard"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
)

func sendCanaryThrough(ctx context.Context, doer HTTPDoer) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, proxycapture.Target, nil)
	if err != nil {
		panic(err)
	}
	request.Header.Set("Authorization", "Bearer "+proxycapture.Canary)
	if response, err := doer.Do(request); err == nil {
		_ = response.Body.Close()
	}
}

func TestTheDefaultPagerDutyValidationClientNeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, func(ctx context.Context) {
		sendCanaryThrough(ctx, pagerDutyValidationClient(nil, 5*time.Second))
	}).AssertNothingReached(t)
}

// A supplied client with no transport gets one without the environment proxy where the provider client is built.
func TestASuppliedClientWithoutATransportNeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, func(ctx context.Context) {
		sendCanaryThrough(ctx, originGuardedAtConstruction(&http.Client{Timeout: 5 * time.Second}))
	}).AssertNothingReached(t)
}

func TestASuppliedPagerDutyValidationClientWithoutATransportNeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, func(ctx context.Context) {
		sendCanaryThrough(ctx, pagerDutyValidationClient(&http.Client{Timeout: 5 * time.Second}, 5*time.Second))
	}).AssertNothingReached(t)
}

func TestThePerRequestOriginGuardGivesASuppliedClientATransport(t *testing.T) {
	target, err := url.Parse(proxycapture.Target)
	if err != nil {
		t.Fatal(err)
	}
	guarded, _ := originGuardedDoer(&http.Client{}, target, nil)
	if client, ok := guarded.(*http.Client); !ok || client.Transport != http.RoundTripper(httpguard.SharedTransport()) {
		t.Fatalf("originGuardedDoer over a client with no transport = %#v, want a copy over httpguard.SharedTransport()", guarded)
	}
}
