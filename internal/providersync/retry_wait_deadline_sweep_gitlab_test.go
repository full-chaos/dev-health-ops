package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func init() {
	registerRetrySweepRoute(retrySweepRoute{
		name:    "gitlab/security",
		newDoer: func() HTTPDoerForSweep { return &gitLabSecurityRouteDoer{responses: gitLabSecurityRouteFixtures()} },
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			return (GitLabSecurityRouteHandler{}).Collect(
				ctx, nativeTestClaim("gitlab", "security"), providerfoundation.Credential{},
				sweepClient(t, "gitlab", "https://gitlab.example", doer, wait), time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
			)
		},
	})
}
