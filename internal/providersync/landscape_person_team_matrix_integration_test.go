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

// TestAMembershipGivesTheRowATeamAndTheLandscapeNone is the provider matrix of
// the membership rule for GitHub, GitLab and Linear (the Jira line is in
// package atlassianteams, whose collector produces the Jira membership rows).
// Each membership row is made by the provider's own normalizer from a member
// as the provider's API gives it, and stored by the provider's own writer: no
// row is written by hand.
//
// The person commits under the email the provider reports. On a day after the
// sync the person's user row is under the team of the membership; on a day
// BEFORE the sync the membership is not valid yet, so the row has no team. A
// member the provider reports with no email stays unassigned.
//
// The landscape point follows neither: its team is the owner of the
// repository (ruling D5985), and no team owns this repository, so every point
// is "unassigned" on both days, the member's included.
func TestAMembershipGivesTheRowATeamAndTheLandscapeNone(t *testing.T) {
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
			want["row of "+withEmail] = []string{teamID}
			if got := read(dayAfter); !reflect.DeepEqual(got, want) {
				t.Errorf("a day after the sync:\n got  %v\n want %v", got, want)
			}
		})
	}
}

// TestTheLandscapeTeamIsTheOwnerOfTheRepositoryOfEachProvider is the provider
// matrix of the landscape's team rule (ruling D5985): a person's point is in
// the team that OWNS the repository the person worked in, for an ownership
// row of each provider made by that provider's own producer:
//
//   - github: a team's access grant (normalizeGitHubTeamRepoOwnership, written
//     by the team catalog writer; repo_id NULL, resolved by the repos name join);
//   - gitlab: a group's project grant (normalizeGitLabOwnershipRow) and the
//     project's merge request, derived into repository ownership;
//   - jira and linear: a team's project, an issue of that project and the
//     issue's native link to a pull request of the repository
//     (work_graph_issue_pr), derived into repository ownership.
//
// In the same organization a second person works only in a repository no
// team owns and is "unassigned", whatever the stored team id of the row says,
// and a person of the owned repository who also worked in the unowned one has
// a point in each: the team holds only the person's work in its repository.
func TestTheLandscapeTeamIsTheOwnerOfTheRepositoryOfEachProvider(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	now := time.Now().UTC()
	day := now.Truncate(24 * time.Hour)
	grantedAt := now.Add(-time.Hour)

	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		t.Run(provider, func(t *testing.T) {
			org := "org-landscape-owner-" + provider
			owned := uuid.NewSHA1(uuid.NameSpaceURL, []byte("owned:"+provider))
			unowned := uuid.NewSHA1(uuid.NameSpaceURL, []byte("unowned:"+provider))
			repoProvider := provider
			if provider == "jira" || provider == "linear" {
				repoProvider = "github"
			}
			if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)`,
				owned, "acme/owned-"+provider, org, repoProvider, now, unowned, "acme/unowned-"+provider, org, repoProvider, now); err != nil {
				t.Fatal(err)
			}

			var teamID string
			switch provider {
			case "github":
				row, err := normalizeGitHubTeamRepoOwnership(org, "platform", "acme/owned-github", grantedAt)
				if err != nil {
					t.Fatal(err)
				}
				if err := (GitHubTeamCatalogClickHouseEffects{Conn: conn}).WriteTeamRepoOwnership(ctx, org, []githubTeamRepoOwnershipRow{row}); err != nil {
					t.Fatal(err)
				}
				teamID = row.TeamID
			case "gitlab":
				teamID = gitlabTeamID("acme/ops")
				row, ok := normalizeGitLabOwnershipRow(org, teamID, testGitLabNativeProjectID("acme/owned-gitlab"), "acme/owned-gitlab", 100, grantedAt)
				if !ok {
					t.Fatal("the GitLab normalizer refused the project grant")
				}
				if err := (GitLabTeamCatalogClickHouseEffects{Conn: conn, Lease: lease}).writeOwnership(ctx, []gitlabTeamCatalogOwnershipRow{row}); err != nil {
					t.Fatal(err)
				}
				// A GitLab work item names its project by the path (the grant's project_key).
				seedWorkItemOfType(t, ctx, conn, org, "gitlab:acme/owned-gitlab!1", "gitlab", "merge_request", owned, *row.ProjectKey, grantedAt)
			case "jira", "linear":
				teamID = provider + "-team-core"
				seedTeamProjectOwnership(t, ctx, conn, org, provider, "proj-core", teamID, true, grantedAt)
				issue := provider + ":CORE-1"
				seedWorkItem(t, ctx, conn, org, issue, provider, uuid.Nil, "proj-core", grantedAt)
				seedWorkGraphIssuePR(t, ctx, conn, org, owned, issue, 7, grantedAt)
			}
			if provider != "github" {
				if _, _, _, _, err := (TeamRepoOwnershipDerivationService{Conn: conn}).Derive(ctx, org); err != nil {
					t.Fatalf("derive the repository ownership: %v", err)
				}
			}

			// What the repository/user family writes: the stored team id is a
			// decoy the landscape must not read.
			for _, row := range []struct {
				repo   uuid.UUID
				person string
				loc    uint32
			}{
				{owned, "owner-dev@example.com", 50},
				{unowned, "owner-dev@example.com", 7},
				{unowned, "other@example.com", 9},
			} {
				if err := conn.Exec(ctx, `INSERT INTO user_metrics_daily
    (repo_id, day, author_email, identity_id, team_id, team_name, commits_count, loc_added, loc_deleted,
     prs_authored, prs_merged, loc_touched, delivery_units, computed_at, org_id)
    VALUES (?, ?, ?, ?, 'stored-decoy', 'Stored Decoy', 3, ?, 0, 1, 1, ?, 1, ?, ?)`,
					row.repo, day, row.person, row.person, row.loc, row.loc, now, org); err != nil {
					t.Fatal(err)
				}
			}

			if _, err := daily.NewICFinalizeExecutor(conn).ComputeFinalizeFamily(ctx, daily.Run{
				ID: uuid.NewString(), OrganizationID: org, TargetDay: day,
			}); err != nil {
				t.Fatalf("ic_finalize: %v", err)
			}
			points, err := conn.Query(ctx, `SELECT identity_id, toString(team_id), churn_loc_30d FROM ic_landscape_rolling_30d FINAL
WHERE org_id = ? AND as_of_day = ? AND map_name = 'churn_throughput' AND (x_norm != 0 OR y_norm != 0)`, org, day)
			if err != nil {
				t.Fatal(err)
			}
			defer points.Close()
			got := map[string]uint64{}
			for points.Next() {
				var person, team string
				var churn uint64
				if err := points.Scan(&person, &team, &churn); err != nil {
					t.Fatal(err)
				}
				got[person+" in "+team] = churn
			}
			if err := points.Err(); err != nil {
				t.Fatal(err)
			}
			want := map[string]uint64{
				"owner-dev@example.com in " + teamID:  50,
				"owner-dev@example.com in unassigned": 7,
				"other@example.com in unassigned":     9,
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("landscape points (person in team: churn):\n got  %v\n want %v", got, want)
			}
		})
	}
}
