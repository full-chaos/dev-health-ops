package restcore

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
)

func TestTheDefaultCoreClientNeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, func(ctx context.Context) {
		core := Core{Provider: "github", Headers: map[string]string{"Authorization": "Bearer " + proxycapture.Canary}, MaxRetries: 1}
		_, _ = core.Get(ctx, proxycapture.Target, "environment_proxy")
	}).AssertNothingReached(t)
}
