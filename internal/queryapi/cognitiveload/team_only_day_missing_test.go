package cognitiveload

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// Missing is not healthy: a day with team commit-timing rows but no per-developer row has no developer load to serve.
// The three developer fields stay Float! and stay 0 (the contract is additive), so a signal carries one state for them:
// MEASURED when developer rows exist that day, NO_ROWS when the day is team-only. A day whose developer rows sum to 0
// is a measured 0 and says MEASURED.
func TestCognitiveLoadSignalSaysWhetherDevelopersWereMeasuredThatDay(t *testing.T) {
	signals := mergeUserAndTeamRows(
		[]userLoadRow{
			{day: day("2026-08-20"), prInterruptionLoad: 4, contextSpreadCount: 2, reviewRequestLoad: 1},
			{day: day("2026-08-22"), prInterruptionLoad: 0, contextSpreadCount: 0, reviewRequestLoad: 0},
		},
		[]teamRatioRow{{day: day("2026-08-21"), afterHoursCommitRatio: 0.5, weekendCommitRatio: 0}},
	)
	if len(signals) != 3 {
		t.Fatalf("signals = %+v, want 3 days (the union)", signals)
	}

	// Control: the values of a day with a developer row are today's, byte for byte.
	control := signals[0]
	if control.PrInterruptionLoad != 4 || control.ContextSpreadCount != 2 || control.ReviewRequestLoad != 1 {
		t.Errorf("control, day with a developer row: values = %v %v %v, want 4 2 1", control.PrInterruptionLoad, control.ContextSpreadCount, control.ReviewRequestLoad)
	}
	if got := control.DeveloperState; got != model.NewestDayStateMeasured {
		t.Errorf("day with a developer row: developerState = %s, want MEASURED", got)
	}

	// A team-only day: no developer row, so the 0 is not a measurement. The values stay 0; the state says so.
	teamOnly := signals[1]
	if teamOnly.Day.String() != "2026-08-21" {
		t.Fatalf("second day = %s, want 2026-08-21", teamOnly.Day.String())
	}
	if teamOnly.PrInterruptionLoad != 0 || teamOnly.ContextSpreadCount != 0 || teamOnly.ReviewRequestLoad != 0 {
		t.Errorf("team-only day: values = %v %v %v, want 0 0 0 (Float! stays)", teamOnly.PrInterruptionLoad, teamOnly.ContextSpreadCount, teamOnly.ReviewRequestLoad)
	}
	if got := teamOnly.DeveloperState; got != model.NewestDayStateNoRows {
		t.Errorf("team-only day: developerState = %s, want NO_ROWS (no developer row that day: unknown, not 0)", got)
	}

	// A day whose developer rows sum to 0 is a measured 0.
	measuredZero := signals[2]
	if got := measuredZero.DeveloperState; got != model.NewestDayStateMeasured {
		t.Errorf("day whose developer rows sum to 0: developerState = %s, want MEASURED (a measured 0)", got)
	}
}
