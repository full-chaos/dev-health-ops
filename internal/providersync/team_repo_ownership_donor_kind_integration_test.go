//go:build integration

package providersync

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// The donor of the derivation's dependency walk must be an issue, the same rule as the linked_issue donor of
// the attribution cascade: a pull or merge request donor, or a donor with an empty type, gives no team.
func TestDerivedRepoOwnershipDonorMustBeAnIssue(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	now := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)

	type item struct {
		id, provider, itemType string
		repo                   string
		project                string
	}
	type link struct {
		provider, project, team string
	}
	type edge struct{ source, target string }
	type issuePR struct {
		repo     string
		workItem string
		number   uint32
	}
	for _, tc := range []struct {
		name     string
		links    []link
		items    []item
		edges    []edge
		issuePRs []issuePR
		repo     string
		wantTeam string
	}{
		{
			name:  "github pr donor of a pr gives no team",
			links: []link{{"github", "proj-donor", "team-donor"}},
			items: []item{
				{"ghpr:acme/a#1", "github", "pr", "a", ""},
				{"ghpr:acme/b#2", "github", "pr", "b", "proj-donor"},
			},
			edges: []edge{{"ghpr:acme/a#1", "ghpr:acme/b#2"}},
			repo:  "a", wantTeam: "",
		},
		{
			name:  "gitlab merge request donor of a merge request gives no team",
			links: []link{{"gitlab", "proj-donor", "team-donor"}},
			items: []item{
				{"gl:acme/a!1", "gitlab", "merge_request", "a", "proj-unowned"},
				{"gl:acme/b!2", "gitlab", "merge_request", "b", "proj-donor"},
			},
			edges: []edge{{"gl:acme/a!1", "gl:acme/b!2"}},
			repo:  "a", wantTeam: "",
		},
		{
			name:  "gitlab merge request donor of a linked issue gives its pull request no team",
			links: []link{{"gitlab", "proj-donor", "team-donor"}},
			items: []item{
				{"gl:acme/c#7", "gitlab", "issue", "c", "proj-unowned"},
				{"gl:acme/b!2", "gitlab", "merge_request", "b", "proj-donor"},
			},
			edges:    []edge{{"gl:acme/c#7", "gl:acme/b!2"}},
			issuePRs: []issuePR{{"c", "gl:acme/c#7", 5}},
			repo:     "c", wantTeam: "",
		},
		{
			name:  "github pr donor of a linked issue gives its pull request no team",
			links: []link{{"github", "proj-donor", "team-donor"}},
			items: []item{
				{"gh:acme/c#7", "github", "issue", "c", ""},
				{"ghpr:acme/b#2", "github", "pr", "b", "proj-donor"},
			},
			edges:    []edge{{"gh:acme/c#7", "ghpr:acme/b#2"}},
			issuePRs: []issuePR{{"c", "gh:acme/c#7", 5}},
			repo:     "c", wantTeam: "",
		},
		{
			name:  "linear donor with an empty type gives no team",
			links: []link{{"linear", "proj-donor", "team-donor"}},
			items: []item{
				{"ghpr:acme/a#1", "github", "pr", "a", ""},
				{"linear:PLAT-1", "linear", "", "", "proj-donor"},
			},
			edges: []edge{{"ghpr:acme/a#1", "linear:PLAT-1"}},
			repo:  "a", wantTeam: "",
		},
		{
			name: "a pr donor with the smaller id does not shadow an issue donor",
			links: []link{
				{"github", "proj-pr", "team-pr"},
				{"linear", "proj-issue", "team-issue"},
			},
			items: []item{
				{"ghpr:acme/a#1", "github", "pr", "a", ""},
				{"ghpr:acme/b#2", "github", "pr", "b", "proj-pr"},
				{"linear:PLAT-1", "linear", "issue", "", "proj-issue"},
			},
			edges: []edge{{"ghpr:acme/a#1", "ghpr:acme/b#2"}, {"ghpr:acme/a#1", "linear:PLAT-1"}},
			repo:  "a", wantTeam: "team-issue",
		},
		{
			name:  "control: linear issue donor of a pr gives its team",
			links: []link{{"linear", "proj-donor", "team-donor"}},
			items: []item{
				{"ghpr:acme/a#1", "github", "pr", "a", ""},
				{"linear:PLAT-1", "linear", "issue", "", "proj-donor"},
			},
			edges: []edge{{"ghpr:acme/a#1", "linear:PLAT-1"}},
			repo:  "a", wantTeam: "team-donor",
		},
		{
			name:  "control: jira task donor of a pr gives its team",
			links: []link{{"jira", "proj-donor", "team-donor"}},
			items: []item{
				{"ghpr:acme/a#1", "github", "pr", "a", ""},
				{"jira:OPS-1", "jira", "task", "", "proj-donor"},
			},
			edges: []edge{{"ghpr:acme/a#1", "jira:OPS-1"}},
			repo:  "a", wantTeam: "team-donor",
		},
		{
			name:  "control: gitlab issue donor of a merge request gives its team",
			links: []link{{"gitlab", "proj-donor", "team-donor"}},
			items: []item{
				{"gl:acme/a!1", "gitlab", "merge_request", "a", "proj-unowned"},
				{"gl:acme/b#2", "gitlab", "issue", "b", "proj-donor"},
			},
			edges: []edge{{"gl:acme/a!1", "gl:acme/b#2"}},
			repo:  "a", wantTeam: "team-donor",
		},
		{
			name:  "control: github issue donor of a pr gives its team",
			links: []link{{"github", "proj-donor", "team-donor"}},
			items: []item{
				{"ghpr:acme/a#1", "github", "pr", "a", ""},
				{"gh:acme/b#2", "github", "issue", "b", "proj-donor"},
			},
			edges: []edge{{"ghpr:acme/a#1", "gh:acme/b#2"}},
			repo:  "a", wantTeam: "team-donor",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orgID := "donor-kind-" + uuid.NewString()
			repos := map[string]uuid.UUID{"a": uuid.New(), "b": uuid.New(), "c": uuid.New()}
			names := map[uuid.UUID]string{}
			for key, id := range repos {
				names[id] = "acme/" + key
			}
			seedTeamRepoOwnershipRepos(t, ctx, conn, orgID, names)
			for _, l := range tc.links {
				seedTeamProjectOwnership(t, ctx, conn, orgID, l.provider, l.project, l.team, true, now)
			}
			for _, it := range tc.items {
				repoID := uuid.Nil
				if it.repo != "" {
					repoID = repos[it.repo]
				}
				seedWorkItemOfType(t, ctx, conn, orgID, it.id, it.provider, it.itemType, repoID, it.project, now)
			}
			for _, e := range tc.edges {
				seedWorkItemDependency(t, ctx, conn, orgID, e.source, e.target, "relates_to", now)
			}
			for _, l := range tc.issuePRs {
				seedWorkGraphIssuePR(t, ctx, conn, orgID, repos[l.repo], l.workItem, l.number, now)
			}

			service := TeamRepoOwnershipDerivationService{Conn: conn}
			written, retracted, ready, _, err := service.Derive(ctx, orgID)
			if err != nil {
				t.Fatalf("Derive: %v", err)
			}
			if !ready {
				t.Fatalf("Derive: inputs not ready (written=%d retracted=%d)", written, retracted)
			}
			got := readTeamRepoOwnership(t, ctx, conn, orgID)
			row, ok := got["acme/"+tc.repo]
			gotTeam := ""
			if ok {
				gotTeam = row.teamID
			}
			t.Logf("ROW case=%q repo=acme/%s want_team=%q got_team=%q all=%d", tc.name, tc.repo, tc.wantTeam, gotTeam, len(got))
			if gotTeam != tc.wantTeam {
				t.Errorf("acme/%s: team %q, want %q", tc.repo, gotTeam, tc.wantTeam)
			}
		})
	}
}
