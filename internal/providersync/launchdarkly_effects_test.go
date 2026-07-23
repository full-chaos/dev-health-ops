package providersync

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestLaunchDarklyClickHouseEffectsRejectCrossTenantRowsBeforeIO(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("launchdarkly", "feature-flags")
	sink := LaunchDarklyClickHouseEffects{
		Lease: providerfoundation.LeaseGuardFunc(
			func(context.Context) error { return nil },
		),
	}
	for _, test := range []struct {
		destination string
		row         any
		readback    bool
	}{
		{
			destination: "feature_flag",
			row: launchDarklyFlagRow{
				OrgID: "other-org", Provider: "launchdarkly",
			},
		},
		{
			destination: "feature_flag_event",
			row:         launchDarklyEventRow{OrgID: "other-org"},
			readback:    true,
		},
		{
			destination: "feature_flag_link",
			row: launchDarklyLinkRow{
				OrgID: claim.OrgID, Provider: "other-provider",
			},
		},
		{
			destination: "work_graph_edges",
			row: launchDarklyEdgeRow{
				OrgID: "other-org", Provider: "launchdarkly",
			},
		},
	} {
		effect, err := effectBatchFromValues(
			test.destination, EffectReplaySafe, []any{test.row},
		)
		if err != nil {
			t.Fatal(err)
		}
		if test.readback {
			effect.Recovery = EffectReadbackRequired
			_, err = sink.InspectEffect(context.Background(), claim, effect)
		} else {
			err = sink.WriteEffect(context.Background(), claim, effect)
		}
		if !errors.Is(err, providerfoundation.ErrInvalidScope) {
			t.Fatalf("%s error=%v", test.destination, err)
		}
	}
}
