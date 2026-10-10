//go:build integration

package teamsidentity

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
	"github.com/full-chaos/dev-health-ops/internal/teamid"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// After an admin delete the team takes no work item at the next compute, by
// any path the attribution has: its project key, its own id as a key, its
// native team key, the ownership of a repository, a membership. The rows that
// name the team are NOT removed by the delete (the membership, the ownership
// row, the project key on the team row itself); the inactive team row is what
// stops them.
//
// The team is written by the admin store and deleted through the real delete
// handler. The facts are read by the real loaders of the attribution and
// resolved by its real context, before and after. Each path must give the
// team BEFORE the delete, so a path that never resolved cannot pass as
// "takes nothing". One case for a team of each provider.
func TestADeletedTeamTakesNoWorkItem(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	chschema.Apply(ctx, t, instance)
	conn, err := clickhouse.Open(ctx, clickhouse.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	store := Store{Conn: conn}
	h := newTestHandlers(store)
	since := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	for index, provider := range []string{"github", "gitlab", "jira", "linear"} {
		t.Run(provider, func(t *testing.T) {
			org := fmt.Sprintf("00000000-0000-4000-8000-00000091192%d", index)
			// Three teams, one for each kind of path, so that a path is never
			// helped or hidden by another: one found by its key, one by a
			// repository it owns, one by a member.
			byKey, byRepo, byMember := teamid.Of(provider, "platform"), teamid.Of(provider, "owners"), teamid.Of(provider, "people")
			keys := []string{"PLAT"}
			if _, err := store.CreateOrUpdateTeam(ctx, org, TeamWrite{Origin: provider, TeamID: byKey, Name: "Platform", ProjectKeys: &keys}); err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{byRepo, byMember} {
				if _, err := store.CreateOrUpdateTeam(ctx, org, TeamWrite{Origin: provider, TeamID: id, Name: id}); err != nil {
					t.Fatal(err)
				}
			}
			repo, other := uuid.NewSHA1(uuid.NameSpaceURL, []byte("repo:"+provider)), uuid.NewSHA1(uuid.NameSpaceURL, []byte("other:"+provider))
			// The member team owns ANOTHER repository, so its membership passes
			// the ownership gate of the attribution.
			for _, claim := range []struct {
				team string
				repo uuid.UUID
			}{{byRepo, repo}, {byMember, other}} {
				if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership
    (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, valid_from, updated_at)
    VALUES (?, ?, ?, ?, ?, 'exact', 'native', 1, 10, ?, ?)`,
					org, provider, claim.team, claim.repo, "acme/"+claim.repo.String(), since, since); err != nil {
					t.Fatal(err)
				}
			}
			if err := conn.Exec(ctx, `INSERT INTO team_memberships
    (org_id, provider, team_id, member_id, identity_facets, source, is_primary, specificity, priority, valid_from, updated_at)
    VALUES (?, ?, ?, 'dev', ['dev'], 'native', 1, 100, 10, ?, ?)`, org, provider, byMember, since, since); err != nil {
				t.Fatal(err)
			}

			repoID, projectKey, dev := repo.String(), "PLAT", "dev"
			memberSubject := teamattribution.GithubWorkItemDerivationSubject{WorkItemID: provider + ":3", Provider: provider, Type: "issue", Assignees: []string{dev}}
			if provider == "github" || provider == "gitlab" {
				memberSubject.Type = map[string]string{"github": "pr", "gitlab": "merge_request"}[provider]
				memberSubject.Reporter = &dev
			}
			paths := []struct {
				name    string
				team    string
				subject teamattribution.GithubWorkItemDerivationSubject
			}{
				{"by the project key", byKey, teamattribution.GithubWorkItemDerivationSubject{WorkItemID: provider + ":1", Provider: provider, Type: "issue", ProjectKey: &projectKey}},
				{"by the ownership of a repository", byRepo, teamattribution.GithubWorkItemDerivationSubject{WorkItemID: provider + ":2", Provider: provider, Type: "issue", RepoID: &repoID}},
				{"by a membership", byMember, memberSubject},
			}
			resolve := func() map[string]string {
				t.Helper()
				source := teamattribution.ClickHouseFactSource{Conn: conn}
				asOf := since.AddDate(0, 0, 30)
				teams, err := source.LoadTeams(ctx, org)
				if err != nil {
					t.Fatal(err)
				}
				members, err := source.LoadProviderMembers(ctx, org, asOf)
				if err != nil {
					t.Fatal(err)
				}
				repos, err := source.LoadRepos(ctx, org, asOf)
				if err != nil {
					t.Fatal(err)
				}
				derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{Teams: teams, ProviderMembers: members, Repos: repos})
				got := map[string]string{}
				for _, path := range paths {
					team, _, _ := derived.Resolve(path.subject)
					if team != nil {
						got[path.name] = *team
					} else {
						got[path.name] = ""
					}
				}
				return got
			}

			before := resolve()
			for _, path := range paths {
				if before[path.name] != path.team {
					t.Fatalf("%s, while the team is there: resolved %q, want %q: the path must work before the delete", path.name, before[path.name], path.team)
				}
			}
			for _, id := range []string{byKey, byRepo, byMember} {
				recorder := callWithBody(t, h, func(w http.ResponseWriter, r *http.Request) {
					r.SetPathValue("team_id", id)
					h.deleteTeam(w, r)
				}, http.MethodDelete, "/api/v1/admin/teams/"+id, org, nil)
				if recorder.Code != http.StatusOK {
					t.Fatalf("delete %s: status %d body %s", id, recorder.Code, recorder.Body.String())
				}
			}
			// The rows that name the teams are still there.
			var memberships, ownerships uint64
			if err := conn.QueryRow(ctx, `SELECT (SELECT count() FROM team_memberships WHERE org_id = ? AND valid_to IS NULL), (SELECT count() FROM team_repo_ownership WHERE org_id = ? AND valid_to IS NULL)`,
				org, org).Scan(&memberships, &ownerships); err != nil {
				t.Fatal(err)
			}
			if memberships != 1 || ownerships != 2 {
				t.Fatalf("after the delete: %d open memberships and %d open ownership rows, want 1 and 2 (the delete closes none; the inactive team row is what stops them)", memberships, ownerships)
			}
			for name, team := range resolve() {
				if team != "" {
					t.Errorf("%s, after the delete: resolved %q, want no team", name, team)
				}
			}
		})
	}
}
