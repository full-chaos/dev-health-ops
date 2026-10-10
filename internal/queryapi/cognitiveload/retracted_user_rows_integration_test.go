//go:build integration

package cognitiveload

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// The per-person load of a day is the sum over the newest row of each key. A
// retraction row adds 0 to that sum, which is right; a day whose every key is
// retracted is no day with a load of 0 (CHAOS-9135).
func TestADayWhoseEveryUserKeyIsRetractedIsNoDay(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()
	crossorg.SeedRepos(ctx, t, admin, f)
	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	crossorg.SeedRetractedPeople(ctx, t, admin, f, day)
	// The next day holds one person who was measured and is retracted.
	next := day.AddDate(0, 0, 1)
	for _, row := range []struct {
		marker uint8
		at     time.Time
	}{{0, next.Add(10 * time.Hour)}, {1, next.Add(11 * time.Hour)}} {
		load := uint32(7)
		if row.marker == 1 {
			load = 0
		}
		crossorg.Exec(ctx, t, admin, `
        INSERT INTO user_metrics_daily (org_id, repo_id, day, author_email, identity_id, pr_interruption_load, team_id, team_name, computed_at, retracted)
        VALUES (?, ?, ?, 'dave@example.com', 'dave@example.com', ?, ?, 'Team A', ?, ?)`,
			f.OrgA, f.RepoID, next, load, crossorg.RetractedPeopleTeam, row.at, row.marker)
	}
	team := crossorg.RetractedPeopleTeam
	for name, teamID := range map[string]*string{"the whole organization": nil, "the team": &team} {
		rows, err := fetchUserMetrics(ctx, client, f.OrgA, day.Format("2006-01-02"), next.Format("2006-01-02"), teamID, nil)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(rows) != 1 || !rows[0].day.Equal(day) || rows[0].prInterruptionLoad != 20 {
			t.Errorf("%s: rows = %+v, want the first day alone with a load of 20 (alice and carol; bob and the retracted day add nothing and the day of dave is no day)", name, rows)
		}
	}
}
