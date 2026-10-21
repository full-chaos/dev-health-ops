package main

import (
	"context"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
)

func TestARecorderWithoutAnInnerTransportNeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, func(ctx context.Context) {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, proxycapture.Target, nil)
		if err != nil {
			panic(err)
		}
		request.Header.Set("Authorization", "Bearer "+proxycapture.Canary)
		if response, err := (&recorder{last: &capture{}}).RoundTrip(request); err == nil {
			_ = response.Body.Close()
		}
	}).AssertNothingReached(t)
}
