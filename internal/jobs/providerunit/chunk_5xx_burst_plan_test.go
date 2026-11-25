package providerunit

import (
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// PIN (CHAOS-14696): the gateway plan clamps AFTER jitter. Every granted step
// snoozes at most gatewayBurstMaxDelay, wakes at most gatewayBurstMaxTotalWait
// after the episode's first deferral, and the durable fence is the same wake
// time as the snooze.
func TestGatewayBurstPlanStaysInsideItsCapsWithJitter(t *testing.T) {
	t.Parallel()
	first := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	granted := 0
	for unit := 0; unit < 200; unit++ {
		unitID := fmt.Sprintf("00000000-0000-0000-0000-%012d", unit)
		for deferrals := 0; deferrals < gatewayBurstMaxDeferrals; deferrals++ {
			for _, elapsed := range []time.Duration{0, 16 * time.Minute, 50 * time.Minute, 57 * time.Minute, 59*time.Minute + 30*time.Second} {
				seen := first
				now := first.Add(elapsed)
				plan, ok := planGatewayBurstDeferral(providersync.RateLimitEpisode{Deferrals: deferrals, FirstSeenAt: &seen}, unitID, now)
				if !ok {
					continue
				}
				granted++
				if plan.countdown > gatewayBurstMaxDelay {
					t.Fatalf("unit %s deferrals=%d elapsed=%s: snooze %s > %s", unitID, deferrals, elapsed, plan.countdown, gatewayBurstMaxDelay)
				}
				if plan.notBefore.After(first.Add(gatewayBurstMaxTotalWait)) {
					t.Fatalf("unit %s deferrals=%d elapsed=%s: wake %s is past first_seen + %s", unitID, deferrals, elapsed, plan.notBefore, gatewayBurstMaxTotalWait)
				}
				if !plan.notBefore.Equal(now.Add(plan.countdown)) {
					t.Fatalf("unit %s deferrals=%d elapsed=%s: fence %s != snooze wake %s", unitID, deferrals, elapsed, plan.notBefore, now.Add(plan.countdown))
				}
			}
		}
	}
	if granted == 0 {
		t.Fatal("no step was granted; the pin examined nothing")
	}
}
