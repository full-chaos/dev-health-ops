package stripeclient

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
)

func TestTheDefaultClientNeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, func(ctx context.Context) {
		provider := New(Options{Key: "sk_test_" + proxycapture.Canary, BaseURL: "http://upstream.invalid"})
		_, _ = provider.RawGet(ctx, "/v1/balance")
	}).AssertNothingReached(t)
}
