//go:build integration

package providersync

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/identityalias"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/teamid"
)

// TestThePersonsTeamOfTheLandscapeComesFromTheMembershipRowOfEachProvider is
// the provider matrix of the landscape's person-to-team rule for GitHub,
// GitLab and Linear (the Jira line is in package atlassianteams, whose
// collector produces the Jira membership rows). Each membership row is made
// by the provider's own normalizer from a member as the provider's API gives
// it, and stored by the provider's own writer: no row is written by hand.
//
// The person commits under the email the provider reports. On a day after the
// sync the person's point and row are under the team of the membership. On a
// day BEFORE the sync the membership is not valid yet (its valid_from is the
// time of the sync), so the person has no team: the same answer the work of
// that day gets from this membership.
//
// A member the provider reports with no email is stored with the provider
// name only, which no commit email equals: that person stays unassigned.
func TestThePersonsTeamOfTheLandscapeComesFromTheMembershipRowOfEachProvider(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	resolver := identityalias.Load("")
	syncedAt := time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC)
	dayAfter := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	dayBefore := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)
	const withEmail, noEmail, nobody = "dev@example.com", "quiet@example.com", "nobody@example.com"
	email := withEmail

	for _, provider := range []string{"github", "gitlab", "linear"} {
		t.Run(provider, func(t *testing.T) {
			org := "org-landscape-" + provider
			var teamID string
			switch provider {
			case "github":
				sink := GitHubTeamCatalogClickHouseEffects{Conn: conn}
				member, err := normalizeGitHubMembership(org, "platform", "dev-login", withEmail, resolver, syncedAt)
				if err != nil {
					t.Fatal(err)
				}
				quiet, err := normalizeGitHubMembership(org, "platform", "quiet-login", "", resolver, syncedAt)
				if err != nil {
					t.Fatal(err)
				}
				if err := sink.WriteMemberships(ctx, org, []githubMembershipRow{member, quiet}); err != nil {
					t.Fatal(err)
				}
				teamID = member.TeamID
			case "gitlab":
				sink := GitLabTeamCatalogClickHouseEffects{Conn: conn, Lease: lease}
				team := gitlabTeamID("acme/ops")
				member, _, ok := normalizeGitLabMembershipRow(org, team, gitlabTeamCatalogMemberPayload{Username: "dev-login", Name: "Dev", Email: &email}, resolver, syncedAt)
				quiet, _, quietOK := normalizeGitLabMembershipRow(org, team, gitlabTeamCatalogMemberPayload{Username: "quiet-login", Name: "Quiet"}, resolver, syncedAt)
				if !ok || !quietOK {
					t.Fatal("the GitLab normalizer refused a member")
				}
				if err := sink.writeMemberships(ctx, []gitlabTeamCatalogMembershipRow{member, quiet}); err != nil {
					t.Fatal(err)
				}
				teamID = member.TeamID
			case "linear":
				sink := LinearReferenceCatalogClickHouseEffects{Conn: conn, Lease: lease}
				claim := Claim{Unit: Unit{OrgID: org, Provider: "linear"}}
				team := teamid.Of("linear", "CORE")
				_, member, _, err := normalizeLinearReferenceMember(claim, team, linearReferenceCatalogMemberPayload{ID: "lin-dev", Name: "Dev", Email: withEmail}, resolver, syncedAt)
				if err != nil {
					t.Fatal(err)
				}
				_, quiet, _, err := normalizeLinearReferenceMember(claim, team, linearReferenceCatalogMemberPayload{ID: "lin-quiet", Name: "Quiet"}, resolver, syncedAt)
				if err != nil {
					t.Fatal(err)
				}
				if err := sink.writeMemberships(ctx, []linearReferenceMembershipRow{member, quiet}); err != nil {
					t.Fatal(err)
				}
				teamID = member.TeamID
			}
			if teamID == "" {
				t.Fatal("the producer gave the membership no team id")
			}

			// What the repository/user family writes for each person on each day.
			repo := uuid.NewSHA1(uuid.NameSpaceURL, []byte("repo:"+provider))
			for _, day := range []time.Time{dayBefore, dayAfter} {
				for _, person := range []string{withEmail, noEmail, nobody} {
					if err := conn.Exec(ctx, `INSERT INTO user_metrics_daily
    (repo_id, day, author_email, identity_id, team_id, team_name, commits_count, loc_added, loc_deleted,
     prs_authored, prs_merged, loc_touched, delivery_units, computed_at, org_id)
    VALUES (?, ?, ?, ?, 'unassigned', 'Unassigned', 3, 40, 10, 1, 1, 50, 1, ?, ?)`,
						repo, day, person, person, day.Add(30*time.Hour), org); err != nil {
						t.Fatal(err)
					}
				}
			}

			read := func(day time.Time) map[string][]string {
				t.Helper()
				if _, err := daily.NewICFinalizeExecutor(conn).ComputeFinalizeFamily(ctx, daily.Run{
					ID: uuid.NewString(), OrganizationID: org, TargetDay: day,
				}); err != nil {
					t.Fatalf("ic_finalize of %s: %v", day.Format("2006-01-02"), err)
				}
				answers := map[string][]string{}
				points, err := conn.Query(ctx, `SELECT identity_id, groupUniqArray(toString(team_id)) FROM ic_landscape_rolling_30d FINAL
WHERE org_id = ? AND as_of_day = ? AND (x_norm != 0 OR y_norm != 0) GROUP BY identity_id`, org, day)
				if err != nil {
					t.Fatal(err)
				}
				defer points.Close()
				for points.Next() {
					var person string
					var teams []string
					if err := points.Scan(&person, &teams); err != nil {
						t.Fatal(err)
					}
					sort.Strings(teams)
					answers["point of "+person] = teams
				}
				if err := points.Err(); err != nil {
					t.Fatal(err)
				}
				rows, err := conn.Query(ctx, `SELECT author_email, toString(argMax(team_id, computed_at)) FROM user_metrics_daily
WHERE org_id = ? AND day = ? GROUP BY author_email`, org, day)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				for rows.Next() {
					var person, team string
					if err := rows.Scan(&person, &team); err != nil {
						t.Fatal(err)
					}
					answers["row of "+person] = []string{team}
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				return answers
			}

			unassigned := []string{"unassigned"}
			want := map[string][]string{}
			for _, person := range []string{withEmail, noEmail, nobody} {
				want["point of "+person], want["row of "+person] = unassigned, unassigned
			}
			if got := read(dayBefore); !reflect.DeepEqual(got, want) {
				t.Errorf("the day before the sync:\n got  %v\n want %v", got, want)
			}
			want["point of "+withEmail], want["row of "+withEmail] = []string{teamID}, []string{teamID}
			if got := read(dayAfter); !reflect.DeepEqual(got, want) {
				t.Errorf("a day after the sync:\n got  %v\n want %v", got, want)
			}
		})
	}
}
