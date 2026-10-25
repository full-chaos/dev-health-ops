package synchandoff

import (
	"errors"
	"testing"
)

func TestCheckBackfillScaleIsOnlyForARunTheSchedulerRefuses(t *testing.T) {
	// A run needs at least one unit per source per 14-day window (the widest
	// planner chunk), so only a run over that floor is refused at submit.
	for _, tc := range []struct {
		name               string
		days, sources, cap int64
		refused            bool
	}{
		{"no cap set", 100000, 5000, 0, false},
		{"under the cap", 28, 10, 100, false},
		{"exactly at the cap", 14 * 10, 10, 100, false},
		{"one window over", 14*10 + 1, 10, 100, true},
		{"many sources", 14, 101, 100, true},
		{"no sources", 36500, 0, 100, false},
	} {
		err := CheckBackfillScale(tc.days, tc.sources, tc.cap)
		if (err != nil) != tc.refused {
			t.Errorf("%s: err=%v, refused want %v", tc.name, err, tc.refused)
		}
	}
}

func TestCheckBackfillScaleTextNamesOnlyTheCallersNumbers(t *testing.T) {
	err := CheckBackfillScale(28, 2, 3)
	var refusal *ScaleRefusal
	if !errors.As(err, &refusal) {
		t.Fatalf("%v is not a ScaleRefusal", err)
	}
	const want = "Backfill is too large: at least 4 work units for this window and scope, over the limit of 3. Narrow the window or the scope."
	if refusal.Error() != want {
		t.Fatalf("%q, want %q", refusal.Error(), want)
	}
}
