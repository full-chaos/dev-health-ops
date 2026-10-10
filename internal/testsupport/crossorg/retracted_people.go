package crossorg

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

// RetractedPeopleTeam is the team of every row of SeedRetractedPeople.
const RetractedPeopleTeam = "team-a"

// SeedRetractedPeople writes, for org A of f and one day, three people into
// each of the two user-level daily tables (CHAOS-9135), so a reader shows what
// it does with a retraction row:
//
//   - alice: one measured row                         -> a person
//   - bob:   a measured row, then a NEWER retraction  -> no person
//   - carol: a retraction, then a NEWER measured row  -> a person
//
// A measured row holds 10 in loc_touched (user_metrics_daily, with the shared
// repository of f) and in items_completed (work_item_user_metrics_daily,
// provider jira, scope PLAT) and a retraction row holds 0 and the marker 1. Both
// carry the team RetractedPeopleTeam, as the writer's row of zeros does. The
// email of a person is <name>@example.com.
func SeedRetractedPeople(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, f Fixture, day time.Time) {
	t.Helper()
	older, newer := day.Add(10*time.Hour), day.Add(11*time.Hour)
	for _, row := range []struct {
		name   string
		marker uint8
		at     time.Time
	}{
		{"alice", 0, newer},
		{"bob", 0, older}, {"bob", 1, newer},
		{"carol", 1, older}, {"carol", 0, newer},
	} {
		loc, email := uint32(10), row.name+"@example.com"
		if row.marker == 1 {
			loc = 0
		}
		Exec(ctx, t, admin, `
        INSERT INTO user_metrics_daily (org_id, repo_id, day, author_email, identity_id, loc_touched, reviews_given, pr_interruption_load, team_id, team_name, computed_at, retracted)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'Team A', ?, ?)`,
			f.OrgA, f.RepoID, day, email, email, loc, loc, loc, RetractedPeopleTeam, row.at, row.marker)
		Exec(ctx, t, admin, `
        INSERT INTO work_item_user_metrics_daily (org_id, day, provider, work_scope_id, user_identity, team_id, team_name, items_completed, computed_at, retracted)
        VALUES (?, ?, 'jira', 'PLAT', ?, ?, 'Team A', ?, ?, ?)`,
			f.OrgA, day, email, RetractedPeopleTeam, loc, row.at, row.marker)
	}
}
