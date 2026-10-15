package analytics

import (
	"context"
	"testing"
)

// TestResolveSankeyCoverage_NothingMeasurableIsNullOnlyWhenBothDenominatorsAreZero
// holds the clause of the CHAOS-6129 guard over the scanned row, without a
// server: the coverage is null only when BOTH denominators are zero. A window
// where either denominator is positive still has a coverage object, and the
// share whose own denominator is positive keeps its measured value. Each case
// kills one clause of `total == 0 && repoTotal == 0` (`&&` read as `||`, the
// total clause dropped, the repoTotal clause dropped). Columns, in the order
// resolveSankeyCoverage scans them: total, assigned_team, repo_total,
// assigned_repo, then the three nullable split columns and the two nullable unit counts.
func TestResolveSankeyCoverage_NothingMeasurableIsNullOnlyWhenBothDenominatorsAreZero(t *testing.T) {
	cases := []struct {
		name     string
		row      []any
		wantNil  bool
		wantTeam float64
		wantRepo float64
	}{
		{name: "both zero is null", row: []any{0.0, 0.0, 0.0, 0.0, nil, nil, nil, nil, nil}, wantNil: true},
		{name: "total zero, repo total positive keeps its coverage", row: []any{0.0, 0.0, 2.0, 1.0, nil, nil, nil, nil, nil}, wantTeam: 0, wantRepo: 0.5},
		{name: "repo total zero, total positive keeps its coverage", row: []any{4.0, 1.0, 0.0, 0.0, nil, nil, nil, nil, nil}, wantTeam: 0.25, wantRepo: 0},
		{name: "both positive", row: []any{4.0, 1.0, 2.0, 1.0, nil, nil, nil, nil, nil}, wantTeam: 0.25, wantRepo: 0.5},
		{name: "a measured zero share is not null", row: []any{4.0, 0.0, 2.0, 0.0, nil, nil, nil, nil, nil}, wantTeam: 0, wantRepo: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := &routingFakeClient{}
			client.on("AS assigned_team", &fakeRowScanner{rows: [][]any{tc.row}})
			req := SankeyRequest{
				Measure:   MeasureCount,
				StartDate: mustGraphQLDate("2026-01-01"),
				EndDate:   mustGraphQLDate("2026-01-08"),
			}
			got := resolveSankeyCoverage(context.Background(), client, "org-1", req, 30, false, nil)
			if tc.wantNil {
				if got != nil {
					t.Fatalf("coverage = %+v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("coverage is nil, want a coverage object")
			}
			if got.TeamCoverage != tc.wantTeam || got.RepoCoverage != tc.wantRepo {
				t.Fatalf("coverage = {team %v, repo %v}, want {team %v, repo %v}", got.TeamCoverage, got.RepoCoverage, tc.wantTeam, tc.wantRepo)
			}
		})
	}
}

// TestResolveSankeyCoverage_SizingCoverageIsTheSizedShareOfTheUnits pins the two CHAOS-9619 fields from the two
// scanned unit counts (sized units, units): the share and the unsized count when the window holds units, and null
// -- never 0 -- when the counts are not measured (NULL) or the window holds no unit.
func TestResolveSankeyCoverage_SizingCoverageIsTheSizedShareOfTheUnits(t *testing.T) {
	cases := []struct {
		name        string
		sized       any
		units       any
		wantNil     bool
		wantShare   float64
		wantUnsized int
	}{
		{name: "some units sized", sized: 3.0, units: 4.0, wantShare: 0.75, wantUnsized: 1},
		{name: "no unit sized is a measured zero", sized: 0.0, units: 5.0, wantShare: 0, wantUnsized: 5},
		{name: "every unit sized", sized: 2.0, units: 2.0, wantShare: 1, wantUnsized: 0},
		{name: "not measured is null", sized: nil, units: nil, wantNil: true},
		{name: "no unit is null", sized: 0.0, units: 0.0, wantNil: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := &routingFakeClient{}
			client.on("AS assigned_team", &fakeRowScanner{rows: [][]any{{4.0, 1.0, 2.0, 1.0, nil, nil, nil, tc.sized, tc.units}}})
			req := SankeyRequest{Measure: MeasureIssueSize, StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-08")}
			got := resolveSankeyCoverage(context.Background(), client, "org-1", req, 30, true, nil)
			if got == nil {
				t.Fatal("coverage is nil, want a coverage object")
			}
			if tc.wantNil {
				if got.SizingCoverage != nil || got.UnsizedUnitCount != nil {
					t.Fatalf("sizing = (%v, %v), want null", got.SizingCoverage, got.UnsizedUnitCount)
				}
				return
			}
			if got.SizingCoverage == nil || got.UnsizedUnitCount == nil {
				t.Fatalf("sizing = (%v, %v), want (%v, %d)", got.SizingCoverage, got.UnsizedUnitCount, tc.wantShare, tc.wantUnsized)
			}
			if *got.SizingCoverage != tc.wantShare || *got.UnsizedUnitCount != tc.wantUnsized {
				t.Fatalf("sizing = (%v, %d), want (%v, %d)", *got.SizingCoverage, *got.UnsizedUnitCount, tc.wantShare, tc.wantUnsized)
			}
		})
	}
}
