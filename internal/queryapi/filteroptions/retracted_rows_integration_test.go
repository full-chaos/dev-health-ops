//go:build integration

package filteroptions

import (
	"context"
	"reflect"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// The developer list and the team list leave out a key whose newest row is a
// retraction (CHAOS-9135). The row of zeros of a person carries the team of the
// row it retracts, so a team that only a retracted person held is no longer
// listed from the user-level tables.
func TestTheListsLeaveOutRetractedPeople(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()
	crossorg.SeedRepos(ctx, t, admin, f)
	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	crossorg.SeedRetractedPeople(ctx, t, admin, f, day)
	bindings := []dhclickhouse.Binding{{Name: "org_id", Value: f.OrgA}}
	developers, err := distinctValues(ctx, client, developersQuery, "developers", bindings)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"alice@example.com", "carol@example.com"}; !reflect.DeepEqual(developers, want) {
		t.Errorf("developers = %v, want %v", developers, want)
	}
}

// The team list takes a team of the user-level tables only when it is an
// active team or the unassigned fallback, so the one team id that shows a
// retraction there is the fallback: a person held under it by a measured row
// that a NEWER retraction row retracts does not list it (the row of zeros
// carries the team of the row it retracts).
func TestTheTeamListLeavesOutTheFallbackOnlyARetractedPersonHeld(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()
	crossorg.SeedRepos(ctx, t, admin, f)
	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	older, newer := day.Add(10*time.Hour), day.Add(11*time.Hour)
	for _, row := range []struct {
		at     time.Time
		marker uint8
	}{{older, 0}, {newer, 1}} {
		crossorg.Exec(ctx, t, admin, `
        INSERT INTO user_metrics_daily (org_id, repo_id, day, author_email, identity_id, loc_touched, team_id, team_name, computed_at, retracted)
        VALUES (?, ?, ?, 'dan@example.com', 'dan@example.com', 5, 'unassigned', 'Unassigned', ?, ?)`, f.OrgA, f.RepoID, day, row.at, row.marker)
		crossorg.Exec(ctx, t, admin, `
        INSERT INTO work_item_user_metrics_daily (org_id, day, provider, work_scope_id, user_identity, team_id, team_name, items_completed, computed_at, retracted)
        VALUES (?, ?, 'jira', 'PLAT', 'dan@example.com', 'unassigned', 'Unassigned', 5, ?, ?)`, f.OrgA, day, row.at, row.marker)
	}
	bindings := []dhclickhouse.Binding{{Name: "org_id", Value: f.OrgA}}
	teams, err := distinctValues(ctx, client, teamsQuery, "teams", bindings)
	if err != nil {
		t.Fatal(err)
	}
	if len(teams) != 0 {
		t.Errorf("teams = %v, want none: the only person under the fallback is retracted in both tables", teams)
	}
}
