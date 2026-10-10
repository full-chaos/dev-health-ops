//go:build integration

package atlassianteams

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
)

// TestThePersonsTeamOfTheLandscapeComesFromAnAtlassianTeamMembership is the
// Jira line of the provider matrix of the landscape's person-to-team rule
// (the GitHub, GitLab and Linear lines are in package providersync). The
// membership rows are the ones this package's Collect and Write produce from
// the Teams API answers of the test gateway; no row is written by hand.
//
// An Atlassian membership names a person by the account id (the API gives no
// email), so the person is found as the identity a Jira work item stores for
// that assignee: the same facet.
//
// On a day after the sync the person's point is under the Atlassian team. On a
// day BEFORE the sync the membership is not valid yet (its valid_from is the
// time of the sync) and the person has no team, as the work of that day has
// none from this membership.
func TestThePersonsTeamOfTheLandscapeComesFromAnAtlassianTeamMembership(t *testing.T) {
	conn := openClickHouse(t)
	ctx := context.Background()
	const org = "org-1"
	syncedAt := time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC)
	dayAfter := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	dayBefore := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)

	g := newGateway(t, standard)
	p := params(everything)
	p.Now = syncedAt
	rows, err := Collect(ctx, g.client(), p)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := Write(ctx, conn, org, rows, everything, soleScope()); err != nil || result.MembershipsWritten == 0 {
		t.Fatalf("write = %+v, %v; want membership rows", result, err)
	}
	const teamID = "jira:aaaaaaaa-0000-4000-8000-000000000001"
	people := lines(t, conn, `SELECT identity_facets[1] FROM team_memberships FINAL
WHERE org_id = 'org-1' AND team_id = '`+teamID+`' ORDER BY member_id`)
	if len(people) != 2 {
		t.Fatalf("the sync stored %d members of the team (%v), want 2", len(people), people)
	}
	// The work item rows of the two members and of one person who is no
	// member, on both days.
	people = append(people, "jira:nobody-9")
	for _, day := range []time.Time{dayBefore, dayAfter} {
		for _, person := range people {
			exec(t, conn, `INSERT INTO work_item_user_metrics_daily
    (day, provider, work_scope_id, user_identity, team_id, team_name, items_started, items_completed, wip_count_end_of_day, computed_at, org_id)
    VALUES (?, 'jira', 'PLAT', ?, 'unassigned', 'Unassigned', 1, 2, 1, ?, ?)`, day, person, day.Add(30*time.Hour), org)
		}
	}

	read := func(day time.Time) map[string][]string {
		t.Helper()
		if _, err := daily.NewICFinalizeExecutor(conn).ComputeFinalizeFamily(ctx, daily.Run{
			ID: uuid.NewString(), OrganizationID: org, TargetDay: day,
		}); err != nil {
			t.Fatalf("ic_finalize of %s: %v", day.Format("2006-01-02"), err)
		}
		result, err := conn.Query(ctx, `SELECT identity_id, groupUniqArray(toString(team_id)) FROM ic_landscape_rolling_30d FINAL
WHERE org_id = ? AND as_of_day = ? AND (x_norm != 0 OR y_norm != 0) GROUP BY identity_id`, org, day)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()
		points := map[string][]string{}
		for result.Next() {
			var person string
			var teams []string
			if err := result.Scan(&person, &teams); err != nil {
				t.Fatal(err)
			}
			sort.Strings(teams)
			points[person] = teams
		}
		if err := result.Err(); err != nil {
			t.Fatal(err)
		}
		return points
	}

	member := []string{teamID}
	unassigned := []string{"unassigned"}
	if got, want := read(dayBefore), (map[string][]string{people[0]: unassigned, people[1]: unassigned, "jira:nobody-9": unassigned}); !reflect.DeepEqual(got, want) {
		t.Errorf("the day before the sync: points = %v, want %v", got, want)
	}
	if got, want := read(dayAfter), (map[string][]string{people[0]: member, people[1]: member, "jira:nobody-9": unassigned}); !reflect.DeepEqual(got, want) {
		t.Errorf("a day after the sync: points = %v, want %v", got, want)
	}
}
