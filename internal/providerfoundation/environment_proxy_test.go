package providerfoundation

import (
	"context"
	"net/http"
	"testing"
	"time"

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
