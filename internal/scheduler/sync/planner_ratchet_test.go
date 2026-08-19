package sync

import (
	"testing"
	"time"
)

// The ratchet's ten behavioral clauses (C1-C11) are pinned against the LIVE
// Python planner in planner_oracle_test.go, which is the only proof that
// actually forbids divergence. The tests here cover exactly the clauses that
// differential table structurally CANNOT reach:
//
//   - C5's open-start guard for a HEAVY dataset. No dataset is registered
//     HEAVY + WatermarkBehavior.NONE today, so a registry-driven case
//     short-circuits on the cost-class check and never reaches the nil-start
//     guard. On the Python side that blind spot let a deletion mutation
//     SURVIVE until a forced case (monkeypatched cost class) was added; the
//     Go mirror needs its own forced case for the same reason. The clause is
//     defensive on both sides and becomes live the moment such a dataset is
//     registered.
//   - C1's env-override parsing, which the oracle cannot vary per case (the
//     cap is process-wide on both sides).
//   - C8's overlap clamp, which needs an overlap wider than the cap -- a
//     configuration the pinned oracle inputs deliberately exclude.

// TestHeavyRatchetNeverCapsAnOpenStartWindow is the forced HEAVY + open-start
// case (clause C5). It calls applyHeavyWindowRatchet directly with a HEAVY
// cost class and a nil start, which is the combination the dataset registry
// cannot currently produce.
func TestHeavyRatchetNeverCapsAnOpenStartWindow(t *testing.T) {
	end := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)

	got := applyHeavyWindowRatchet(SyncModeIncremental, heavyCostClass, nil, end, 0)

	if !got.Equal(end) {
		t.Fatalf("applyHeavyWindowRatchet(heavy, open start) = %s, want the end unchanged (%s)", got, end)
	}
}

// TestHeavyRatchetOpenStartSurvivesTheWholePlanner is the same clause one
// level up: a HEAVY dataset with no window start must plan a unit whose start
// stays nil. The cap must not SYNTHESIZE a start in order to have something to
// add the cap to -- that would silently narrow an intentionally unbounded
// crawl. Forced via a spec built by hand, for the reason in the file comment.
func TestHeavyRatchetOpenStartSurvivesTheWholePlanner(t *testing.T) {
	now := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	spec := datasetSpec{CostClass: heavyCostClass, Incremental: false}
	input := PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeIncremental, Now: now,
	}
	source := PlanSource{ID: "source", ExternalID: "owner/repo", Provider: "github"}

	start, end, ok := resolveWindow(input, source, PlanDataset{Key: "forced-heavy-none"}, spec, now, now)

	if !ok {
		t.Fatal("resolveWindow planned no unit for a HEAVY open-start dataset")
	}
	if start != nil {
		t.Fatalf("window start = %s, want nil: the cap must not synthesize a start", start)
	}
	if !end.Equal(now) {
		t.Fatalf("window end = %s, want %s: an open-start window is never capped", end, now)
	}
}

// TestIncrementalHeavyMaxWindowDaysHonoursTheEnvOverride pins clause C1: a
// positive integer wins; zero, negative and non-integer values fall back to
// the 7-day default rather than erroring. Failing closed here would recreate
// the do-nothing failure mode the ratchet exists to kill.
func TestIncrementalHeavyMaxWindowDaysHonoursTheEnvOverride(t *testing.T) {
	for _, test := range []struct {
		name  string
		set   bool
		value string
		want  int
	}{
		{name: "unset", want: defaultIncrementalHeavyMaxWindowDays},
		{name: "positive", set: true, value: "3", want: 3},
		{name: "zero", set: true, value: "0", want: defaultIncrementalHeavyMaxWindowDays},
		{name: "negative", set: true, value: "-5", want: defaultIncrementalHeavyMaxWindowDays},
		{name: "non-integer", set: true, value: "seven", want: defaultIncrementalHeavyMaxWindowDays},
		{name: "empty", set: true, value: "", want: defaultIncrementalHeavyMaxWindowDays},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.set {
				t.Setenv(incrementalHeavyMaxWindowDaysEnv, test.value)
			}
			if got := incrementalHeavyMaxWindowDays(); got != test.want {
				t.Fatalf("incrementalHeavyMaxWindowDays() = %d, want %d", got, test.want)
			}
		})
	}
}

// TestEffectiveHeavyMaxWindowClampsToExceedTheOverlap pins clause C8. The
// incremental read starts at `watermark - overlap`, so a cap <= overlap ends
// at or before the watermark it started from and the monotonic watermark
// write DISCARDS it: every tick re-plans the identical slice, re-fetches it,
// and reports SUCCESS while the watermark never moves.
func TestEffectiveHeavyMaxWindowClampsToExceedTheOverlap(t *testing.T) {
	day := 24 * time.Hour
	for _, test := range []struct {
		name    string
		capDays string
		overlap time.Duration
		want    time.Duration
	}{
		{name: "no overlap", capDays: "7", overlap: 0, want: 7 * day},
		{name: "overlap well under cap", capDays: "7", overlap: 5 * time.Minute, want: 7 * day},
		{name: "overlap equal to cap clamps up", capDays: "1", overlap: day, want: 2 * day},
		{name: "overlap wider than cap clamps up", capDays: "2", overlap: 10 * day, want: 11 * day},
		{name: "fractional overlap floors then adds one", capDays: "1", overlap: 36 * time.Hour, want: 2 * day},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(incrementalHeavyMaxWindowDaysEnv, test.capDays)
			warnedCapClamps.Clear()
			got := effectiveHeavyMaxWindow(test.overlap)
			if got != test.want {
				t.Fatalf("effectiveHeavyMaxWindow(%s) = %s, want %s", test.overlap, got, test.want)
			}
			if got <= test.overlap {
				t.Fatalf("effective cap %s does not strictly exceed overlap %s: "+
					"every capped window would end at or before its own watermark",
					got, test.overlap)
			}
		})
	}
}

// TestClampedCapStillAdvancesTheWatermark is clause C8 stated as the property
// it exists to protect, at the planner level rather than on the helper: with a
// pathological overlap >= cap, a behind-watermark HEAVY unit must still plan a
// window whose END is strictly after the stored watermark, or the ratchet
// stalls while every run reports SUCCESS.
func TestClampedCapStillAdvancesTheWatermark(t *testing.T) {
	t.Setenv(incrementalHeavyMaxWindowDaysEnv, "1")
	warnedCapClamps.Clear()
	now := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	stored := now.Add(-90 * 24 * time.Hour)
	overlap := 3 * 24 * time.Hour

	units, err := BuildScheduledPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeIncremental, Now: now,
		WatermarkOverlap: overlap,
		Sources:          []PlanSource{{ID: "source", ExternalID: "owner/repo", Provider: "github"}},
		Datasets:         []PlanDataset{{Key: "commit-stats"}},
		Watermarks: map[WatermarkKey]time.Time{
			{SourceID: "owner/repo", Dataset: "commit-stats"}: stored,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 {
		t.Fatalf("len(units) = %d, want 1", len(units))
	}
	if !units[0].WindowEnd.After(stored) {
		t.Fatalf("window end %s does not advance past the stored watermark %s: "+
			"the monotonic write would discard it and the ratchet would stall "+
			"while every run reported SUCCESS", units[0].WindowEnd, stored)
	}
}

// TestHeavyRatchetCapsOneWindowAndNeverSplitsTheUnit pins clause C6:
// continuation is the next scheduled tick, not a bigger plan.
func TestHeavyRatchetCapsOneWindowAndNeverSplitsTheUnit(t *testing.T) {
	now := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	stored := now.Add(-365 * 24 * time.Hour)

	units, err := BuildScheduledPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeIncremental, Now: now,
		Sources:  []PlanSource{{ID: "source", ExternalID: "owner/repo", Provider: "github"}},
		Datasets: []PlanDataset{{Key: "commit-stats"}},
		Watermarks: map[WatermarkKey]time.Time{
			{SourceID: "owner/repo", Dataset: "commit-stats"}: stored,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 {
		t.Fatalf("len(units) = %d, want exactly 1: a year behind the watermark "+
			"must still plan ONE capped window, not a fan-out", len(units))
	}
	want := stored.Add(defaultIncrementalHeavyMaxWindowDays * 24 * time.Hour)
	if !units[0].WindowEnd.Equal(want) {
		t.Fatalf("window end = %s, want %s", units[0].WindowEnd, want)
	}
}
