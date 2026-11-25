package cognitiveload

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-14628: every way Resolve builds a signal says whether developers were measured that day. Each path serves a
// team-only day (commit-timing rows, no developer row) as NO_ROWS and a day with developer rows as MEASURED, with the
// Float! loads served as before (0 on the team-only day).
func TestDeveloperStateOnEveryResolvePath(t *testing.T) {
	cases := []struct {
		name   string
		team   *string
		repo   *string
		client func() *fakeClient
	}{
		{
			name: "path 1: a named team, rollup rows",
			team: strPtr("team-a"),
			client: func() *fakeClient {
				return &fakeClient{
					responses: []*fakeRowScanner{
						{rows: [][]any{
							{day("2026-08-20"), 0.0, 0.0, 0.0, 0.5, 0.0},
							{day("2026-08-21"), 4.0, 2.0, 1.0, 0.25, 0.0},
						}},
						{rows: nil},
						{rows: [][]any{{day("2026-08-21")}}},
					},
					errs: []error{nil, nil, nil},
				}
			},
		},
		{
			name: "path 2: a team and a repository it owns",
			team: strPtr("team-a"),
			repo: strPtr("org/repo-a"),
			client: func() *fakeClient {
				return &fakeClient{
					responses: []*fakeRowScanner{
						{rows: [][]any{{"repo-uuid-1", "org/repo-a"}}},
						{rows: [][]any{{"repo-uuid-1", "team-a", uint8(1), uint16(150)}}},
						{rows: [][]any{{day("2026-08-21"), uint64(4), uint64(2), uint64(1)}}},
						{rows: nil},
						{rows: [][]any{{day("2026-08-20"), 0.5, 0.0}, {day("2026-08-21"), 0.25, 0.0}}},
					},
					errs: []error{nil, nil, nil, nil, nil},
				}
			},
		},
		{
			name: "path 3: the whole organization",
			client: func() *fakeClient {
				return &fakeClient{
					responses: []*fakeRowScanner{
						{rows: [][]any{{day("2026-08-21"), uint64(4), uint64(2), uint64(1)}}},
						{rows: nil},
						{rows: [][]any{{day("2026-08-20"), 0.5, 0.0}, {day("2026-08-21"), 0.25, 0.0}}},
						{rows: nil},
					},
					errs: []error{nil, nil, nil, nil},
				}
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result, err := Resolve(context.Background(), c.client(), "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), c.team, c.repo)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if len(result.Signals) != 2 {
				t.Fatalf("signals = %+v, want 2 days", result.Signals)
			}
			teamOnly, measured := result.Signals[0], result.Signals[1]
			if teamOnly.PrInterruptionLoad != 0 || teamOnly.ContextSpreadCount != 0 || teamOnly.ReviewRequestLoad != 0 {
				t.Errorf("team-only day: loads = %v %v %v, want 0 0 0 (Float! stays)", teamOnly.PrInterruptionLoad, teamOnly.ContextSpreadCount, teamOnly.ReviewRequestLoad)
			}
			if teamOnly.DeveloperState != model.NewestDayStateNoRows {
				t.Errorf("team-only day: developerState = %q, want NO_ROWS", teamOnly.DeveloperState)
			}
			if measured.PrInterruptionLoad != 4 || measured.ContextSpreadCount != 2 || measured.ReviewRequestLoad != 1 {
				t.Errorf("day with developer rows: loads = %v %v %v, want 4 2 1", measured.PrInterruptionLoad, measured.ContextSpreadCount, measured.ReviewRequestLoad)
			}
			if measured.DeveloperState != model.NewestDayStateMeasured {
				t.Errorf("day with developer rows: developerState = %q, want MEASURED", measured.DeveloperState)
			}
		})
	}
}
