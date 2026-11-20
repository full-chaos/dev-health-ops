package syncreconciler

import (
	"testing"
	"time"
)

// CHAOS-14386: a stage budget is anchored on its measured prod p99. When the
// p99 reaches 3/4 of the budget, the budget is 2 x p99, capped at 1200ms;
// below that the budget stays. Source of every p99 here: the max of 24 hourly
// p99s of dev_health_reconciler_stage_duration_seconds to 2026-11-20
// (PRODREAD-14238-R1-R3-1).
func TestStageBudgetsAreAnchoredOnMeasuredP99(t *testing.T) {
	const anchorCap = 1200 * time.Millisecond
	for _, anchor := range []struct {
		stage     StageName
		measured  time.Duration
		previous  time.Duration
		wantFinal time.Duration
	}{
		{StageTerminalDeliveryRepair, 719 * time.Millisecond, 750 * time.Millisecond, anchorCap},
		{StageOrphanedUnitRepair, 147 * time.Millisecond, 600 * time.Millisecond, 600 * time.Millisecond},
	} {
		want := anchor.previous
		if anchor.measured*4 >= anchor.previous*3 {
			want = min(2*anchor.measured, anchorCap)
		}
		if want != anchor.wantFinal {
			t.Fatalf("%s: rule gives %s, table says %s", anchor.stage, want, anchor.wantFinal)
		}
		if got := DefaultStageBudgets()[anchor.stage]; got != want {
			t.Errorf("%s budget = %s, want %s (measured p99 %s)", anchor.stage, got, want, anchor.measured)
		}
	}
}
