package providersync

import (
	"testing"
	"time"
)

// The production feed for computed_at is time.Now() (complete_route.go:118),
// which carries NANOSECONDS. None of the three destinations can store them:
// estimate coverage and team attributions are DateTime64(3) and state
// durations is a plain DateTime. A stamp the builder emits at a precision the
// column cannot hold is written, quantized by the server, read back different,
// and the replay verdict is Absent forever.
//
// Every oracle case uses a whole-second ComputedAt, so the oracle pairs cannot
// see this: it needs a fixture that actually carries sub-precision digits.
func TestGitHubWorkItemDerivedSurfacesQuantizeStampsAtBuilderEntry(t *testing.T) {
	claim := githubWorkItemOracleClaim()
	day := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	// 123456789ns is chosen so the millisecond and the second truncation
	// disagree with each other AND with the raw value: a builder that
	// quantized everything to one precision fails this, not just a builder
	// that quantizes nothing.
	computedAt := time.Date(2026, 8, 5, 0, 30, 0, 123456789, time.UTC)
	wantMillis := time.Date(2026, 8, 5, 0, 30, 0, 123000000, time.UTC)
	wantSeconds := time.Date(2026, 8, 5, 0, 30, 0, 0, time.UTC)

	item := githubWorkItemRow{
		WorkItemID: "acme/api#1", Provider: "github", Title: "t", Type: "issue",
		Status: "todo", ProjectID: stringPointer("acme/api"),
		CreatedAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC),
		OrgID:     claim.OrgID,
	}
	rows := githubWorkItemRows{
		WorkItems: []githubWorkItemRow{item},
		StatusTransitions: []githubWorkItemTransitionRow{{
			WorkItemID: "acme/api#1", Provider: "github",
			OccurredAt: time.Date(2026, 8, 4, 6, 0, 0, 0, time.UTC),
			FromStatus: "todo", ToStatus: "in_progress", OrgID: claim.OrgID,
		}},
	}
	surfaces, err := buildGitHubWorkItemDerivedSurfaces(
		claim, rows, day, computedAt, newGitHubWorkItemDerivationContext(
			githubWorkItemDerivationFacts{},
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(surfaces.EstimateCoverage) == 0 || len(surfaces.TeamAttributions) == 0 ||
		len(surfaces.StateDurations) == 0 {
		t.Fatalf("fixture produced no rows on some surface: %d/%d/%d",
			len(surfaces.EstimateCoverage), len(surfaces.TeamAttributions),
			len(surfaces.StateDurations))
	}
	for _, row := range surfaces.EstimateCoverage {
		if !row.ComputedAt.Equal(wantMillis) {
			t.Errorf("estimate coverage computed_at = %v, want %v (DateTime64(3))",
				row.ComputedAt, wantMillis)
		}
	}
	for _, row := range surfaces.TeamAttributions {
		if !row.ComputedAt.Equal(wantMillis) {
			t.Errorf("team attribution computed_at = %v, want %v (DateTime64(3))",
				row.ComputedAt, wantMillis)
		}
	}
	for _, row := range surfaces.StateDurations {
		if !row.ComputedAt.Equal(wantSeconds) {
			t.Errorf("state duration computed_at = %v, want %v (plain DateTime)",
				row.ComputedAt, wantSeconds)
		}
	}
}

// Quantizing the STAMP must not quantize the value the compute uses. An open
// item's final segment ends at computed_at, so truncating the arithmetic input
// to seconds would shorten duration_hours by up to a second against Python,
// which passes computed_at through raw. This asserts the segment math still
// sees the FULL-precision instant while the stamp is quantized.
func TestGitHubWorkItemStateDurationsKeepFullPrecisionSegmentEnd(t *testing.T) {
	claim := githubWorkItemOracleClaim()
	day := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	// Sits INSIDE the window so the open segment's end is computed_at itself
	// rather than the window end, which is what exposes the truncation.
	computedAt := time.Date(2026, 8, 4, 12, 0, 0, 500000000, time.UTC)
	rows := githubWorkItemRows{
		WorkItems: []githubWorkItemRow{{
			WorkItemID: "acme/api#1", Provider: "github", Title: "t", Type: "issue",
			Status: "todo", ProjectID: stringPointer("acme/api"),
			CreatedAt: time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC),
			UpdatedAt: time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC),
			OrgID:     claim.OrgID,
		}},
		StatusTransitions: []githubWorkItemTransitionRow{{
			WorkItemID: "acme/api#1", Provider: "github",
			OccurredAt: time.Date(2026, 8, 4, 6, 0, 0, 0, time.UTC),
			FromStatus: "todo", ToStatus: "in_progress", OrgID: claim.OrgID,
		}},
	}
	surfaces, err := buildGitHubWorkItemDerivedSurfaces(
		claim, rows, day, computedAt, newGitHubWorkItemDerivationContext(
			githubWorkItemDerivationFacts{},
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	var total float64
	for _, row := range surfaces.StateDurations {
		total += row.DurationHours
	}
	// 00:00 -> 12:00:00.5 == 12.0001388...h. A seconds-truncated segment end
	// would give exactly 12.
	want := 12.0 + 0.5/3600.0
	if diff := total - want; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("total duration_hours = %.12f, want %.12f (full-precision segment end)",
			total, want)
	}
}
