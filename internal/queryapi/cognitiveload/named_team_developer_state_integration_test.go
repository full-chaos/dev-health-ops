//go:build integration

package cognitiveload

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// CHAOS-14628: a named team with no repository reads team_cognitive_load_daily (Path 1). The rollup counts the
// distinct authors whose developer rows fed a day (sample_author_count), so a day whose only inputs were commit-timing
// rows (no author) has no developer measurement: its 0 loads are not a measured 0 and the state says NO_ROWS. A day
// whose developer rows sum to 0 is a measured 0 and says MEASURED. The values and ratios are served as stored.
func TestNamedTeamSignalSaysWhetherDevelopersWereMeasuredThatDay(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()
	const teamID = "team-x-chaos-14628"
	first := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	days := []struct {
		offset     int
		load       [3]float64
		authors    uint32
		afterHours float64
		weekend    float64
		want       model.NewestDayState
	}{
		{0, [3]float64{4, 2, 1}, 3, 0.5, 0, model.NewestDayStateMeasured},
		{1, [3]float64{0, 0, 0}, 0, 0.25, 0.125, model.NewestDayStateNoRows},
		{2, [3]float64{0, 0, 0}, 2, 0.125, 0, model.NewestDayStateMeasured},
	}
	for _, d := range days {
		day := first.AddDate(0, 0, d.offset)
		crossorg.Exec(ctx, t, admin, `
        INSERT INTO team_cognitive_load_daily (org_id, team_id, day, pr_interruption_load, context_spread_count, review_request_load,
            after_hours_commit_ratio, weekend_commit_ratio, contributing_repo_count, sample_author_count, computed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, now())`,
			f.OrgA, teamID, day, d.load[0], d.load[1], d.load[2], d.afterHours, d.weekend, d.authors)
	}
	team := teamID
	result, err := Resolve(ctx, client, f.OrgA, graphqldate.New(first), graphqldate.New(first.AddDate(0, 0, 2)), &team, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Signals) != len(days) {
		t.Fatalf("signals = %+v, want %d days", result.Signals, len(days))
	}
	for index, d := range days {
		got := result.Signals[index]
		if got.PrInterruptionLoad != d.load[0] || got.ContextSpreadCount != d.load[1] || got.ReviewRequestLoad != d.load[2] {
			t.Errorf("day %d: loads = %v %v %v, want %v (as stored)", d.offset, got.PrInterruptionLoad, got.ContextSpreadCount, got.ReviewRequestLoad, d.load)
		}
		if got.AfterHoursCommitRatio == nil || *got.AfterHoursCommitRatio != d.afterHours || got.WeekendCommitRatio == nil || *got.WeekendCommitRatio != d.weekend {
			t.Errorf("day %d: ratios = %v %v, want %v %v (as stored)", d.offset, got.AfterHoursCommitRatio, got.WeekendCommitRatio, d.afterHours, d.weekend)
		}
		if got.DeveloperState != d.want {
			t.Errorf("day %d: developerState = %q, want %s", d.offset, got.DeveloperState, d.want)
		}
	}
}
