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
// assigned_repo, then the three nullable split columns.
func TestResolveSankeyCoverage_NothingMeasurableIsNullOnlyWhenBothDenominatorsAreZero(t *testing.T) {
	cases := []struct {
		name     string
		row      []any
		wantNil  bool
		wantTeam float64
		wantRepo float64
	}{
		{name: "both zero is null", row: []any{0.0, 0.0, 0.0, 0.0, nil, nil, nil}, wantNil: true},
		{name: "total zero, repo total positive keeps its coverage", row: []any{0.0, 0.0, 2.0, 1.0, nil, nil, nil}, wantTeam: 0, wantRepo: 0.5},
		{name: "repo total zero, total positive keeps its coverage", row: []any{4.0, 1.0, 0.0, 0.0, nil, nil, nil}, wantTeam: 0.25, wantRepo: 0},
		{name: "both positive", row: []any{4.0, 1.0, 2.0, 1.0, nil, nil, nil}, wantTeam: 0.25, wantRepo: 0.5},
		{name: "a measured zero share is not null", row: []any{4.0, 0.0, 2.0, 0.0, nil, nil, nil}, wantTeam: 0, wantRepo: 0},
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
