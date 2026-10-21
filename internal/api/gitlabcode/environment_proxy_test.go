package gitlabcode

import (
	"context"
	"math/big"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
)

func TestTheDefaultListingClientNeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, func(ctx context.Context) {
		client := Client{BaseURL: proxycapture.Target, Token: proxycapture.Canary}
		_, _ = client.request(ctx, proxycapture.Target+"api/v4/projects", "environment_proxy", "", big.NewInt(1))
	}).AssertNothingReached(t)
}
