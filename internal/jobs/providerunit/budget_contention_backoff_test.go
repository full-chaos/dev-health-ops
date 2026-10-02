package providerunit

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

const backoffUnit = "11111111-1111-4111-8111-111111111111"

func TestBudgetContentionDelayGrowsWithTheDeferralCountAndIsCapped(t *testing.T) {
	t.Parallel()
	jitter := providerBudgetContentionDelay(backoffUnit, 0) - time.Second
	if jitter < 0 || jitter >= time.Second {
		t.Fatalf("jitter=%v; want [0, 1s)", jitter)
	}
	for prior, base := range map[int]time.Duration{
		0: time.Second, 1: 2 * time.Second, 2: 4 * time.Second, 3: 8 * time.Second,
		4: 16 * time.Second, 5: 30 * time.Second, 6: 30 * time.Second, 1000: 30 * time.Second,
	} {
		if got := providerBudgetContentionDelay(backoffUnit, prior); got != base+jitter {
			t.Fatalf("prior=%d delay=%v; want %v (base %v + the unit's jitter %v)", prior, got, base+jitter, base, jitter)
		}
	}
	if got := providerBudgetContentionDelay(backoffUnit, -3); got != time.Second+jitter {
		t.Fatalf("negative prior delay=%v; want the first-deferral delay", got)
	}
	if got := providerBudgetContentionDelay(backoffUnit, 1000); got >= 5*time.Minute {
		t.Fatalf("delay=%v exceeds DeferForBudgetContention's 5 minute bound", got)
	}
}

func TestBudgetContentionDeferralsReadsTheUnitsResultKey(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		result map[string]any
		want   int
	}{
		"missing":        {map[string]any{}, 0},
		"nil result":     {nil, 0},
		"float64 (json)": {map[string]any{"provider_budget_contention_deferrals": float64(7)}, 7},
		"int":            {map[string]any{"provider_budget_contention_deferrals": 3}, 3},
		"json number":    {map[string]any{"provider_budget_contention_deferrals": json.Number("12")}, 12},
		"negative":       {map[string]any{"provider_budget_contention_deferrals": float64(-4)}, 0},
		"huge":           {map[string]any{"provider_budget_contention_deferrals": float64(1e12)}, 0},
		"wrong type":     {map[string]any{"provider_budget_contention_deferrals": "9"}, 0},
	} {
		claim := providersync.Claim{Unit: providersync.Unit{Result: tc.result}}
		if got := budgetContentionDeferrals(claim); got != tc.want {
			t.Fatalf("%s: budgetContentionDeferrals=%d want %d", name, got, tc.want)
		}
	}
}

// TestWorkBacksOffByTheUnitsOwnContentionCount drives Handler.Work: a unit
// that already lost the request reservation three times is snoozed 8-9 s (1 s
// doubled three times plus the unit's jitter), not the first-deferral 1-2 s.
func TestWorkBacksOffByTheUnitsOwnContentionCount(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	unit := providerUnit()
	unit.Result = map[string]any{"provider_budget_contention_deferrals": float64(3)}
	repository := newMemoryUnitRepository(unit)
	handler := &Handler{
		Repository: repository, LeaseDuration: time.Minute, Heartbeat: 10 * time.Second,
		Now: func() time.Time { return now },
		BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			return providersync.CompleteRouteExecutor{}, providerfoundation.ErrBudgetContended
		},
	}
	execution := providerExecution(unit, now, 5)
	execution.Definition.MaxAttempts = 5
	err := handler.Work(context.Background(), execution)
	delay, snoozed := jobruntime.SnoozeDelay(err)
	if !snoozed || delay < 8*time.Second || delay >= 9*time.Second {
		t.Fatalf("Work() = %v, delay=%v; want a typed 8s <= snooze < 9s after 3 prior deferrals", err, delay)
	}
	if !repository.availableAt.Equal(now.Add(delay)) {
		t.Fatalf("durable available_at=%v want %v", repository.availableAt, now.Add(delay))
	}
}
