package remaining

import (
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// The trigger names the donor the cascade copies: for a pull request, the
// donor of the best link tier of record, never one with no link of record.
func TestDonorChangedDependentsFollowTheLinkTier(t *testing.T) {
	const pullRequest = "ghpr:acme/api#7"
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	before, after := base, base.Add(time.Hour)
	edge := func(target string) teamattribution.GithubWorkItemDerivationDependencyEdge {
		return teamattribution.GithubWorkItemDerivationDependencyEdge{SourceWorkItemID: pullRequest, TargetWorkItemID: target, RelationshipType: "relates_to", LastSynced: base}
	}
	linked := func(donor, team string) workItemAttributionNewestPrimary {
		return workItemAttributionNewestPrimary{TeamID: team, Source: "linked_issue", ComputedAt: before,
			Evidence: fmt.Sprintf("linked_issue=%s;dependency_type=relates_to;donor_provider=jira;donor_source=project_ownership", donor)}
	}
	own := func(team string, at time.Time) workItemAttributionNewestPrimary {
		return workItemAttributionNewestPrimary{TeamID: team, Source: "project_ownership", ComputedAt: at}
	}
	edges := []teamattribution.GithubWorkItemDerivationDependencyEdge{edge("jira:A-1"), edge("jira:Z-9")}
	for _, tc := range []struct {
		name  string
		tiers map[teamattribution.LinkPair]string
		rows  map[string]workItemAttributionNewestPrimary
		want  []string
	}{
		{"the native donor of a higher id is the donor", map[teamattribution.LinkPair]string{{PullRequest: pullRequest, Issue: "jira:A-1"}: "explicit_text", {PullRequest: pullRequest, Issue: "jira:Z-9"}: "native"},
			map[string]workItemAttributionNewestPrimary{pullRequest: linked("jira:Z-9", "team-z"), "jira:A-1": own("team-a", after), "jira:Z-9": own("team-z", before)}, nil},
		{"the item still copies the text donor and a native donor exists", map[teamattribution.LinkPair]string{{PullRequest: pullRequest, Issue: "jira:A-1"}: "explicit_text", {PullRequest: pullRequest, Issue: "jira:Z-9"}: "native"},
			map[string]workItemAttributionNewestPrimary{pullRequest: linked("jira:A-1", "team-a"), "jira:A-1": own("team-a", after), "jira:Z-9": own("team-z", before)}, []string{pullRequest}},
		{"an unusable native donor leaves the text donor out", map[teamattribution.LinkPair]string{{PullRequest: pullRequest, Issue: "jira:A-1"}: "explicit_text", {PullRequest: pullRequest, Issue: "jira:Z-9"}: "native"},
			map[string]workItemAttributionNewestPrimary{pullRequest: linked("jira:A-1", "team-a"), "jira:A-1": own("team-a", after), "jira:Z-9": {Source: "unassigned", ComputedAt: before}}, []string{pullRequest}},
		{"an edge with no link of record is no donor", map[teamattribution.LinkPair]string{},
			map[string]workItemAttributionNewestPrimary{pullRequest: linked("jira:A-1", "team-a"), "jira:A-1": own("team-b", after)}, []string{pullRequest}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := donorChangedDependents(edges, nil, tc.rows, issueKindsOf(tc.rows), nil, nil, tc.tiers)
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Errorf("owed %v, want %v", got, tc.want)
			}
		})
	}
}

// K1, K2, K6, K7 through the trigger: the want-donor is the cascade's, and a
// refused native donor is counted by reason but still blocks the lower tiers.
func TestDonorChangedDependentsRefusedNativeDonorStillBlocksTheLowerTiers(t *testing.T) {
	const pullRequest = "ghpr:acme/api#7"
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	before, after := base, base.Add(time.Hour)
	edge := func(target string) teamattribution.GithubWorkItemDerivationDependencyEdge {
		return teamattribution.GithubWorkItemDerivationDependencyEdge{SourceWorkItemID: pullRequest, TargetWorkItemID: target, RelationshipType: "relates_to", LastSynced: base}
	}
	own := func(team string) workItemAttributionNewestPrimary {
		return workItemAttributionNewestPrimary{TeamID: team, Source: "project_ownership", ComputedAt: after}
	}
	linked := func(donor, team string) workItemAttributionNewestPrimary {
		return workItemAttributionNewestPrimary{TeamID: team, Source: "linked_issue", ComputedAt: before,
			Evidence: fmt.Sprintf("linked_issue=%s;dependency_type=relates_to;donor_provider=jira;donor_source=project_ownership", donor)}
	}
	kinds := map[string]teamattribution.GithubWorkItemDerivationSubject{
		pullRequest:      {WorkItemID: pullRequest, Provider: "github", Type: "pr"},
		"gh:acme/api#99": {WorkItemID: "gh:acme/api#99", Provider: "github", Type: "pr"},
		"jira:EMP-1":     {WorkItemID: "jira:EMP-1", Provider: "jira"},
		"jira:AAA-1":     {WorkItemID: "jira:AAA-1", Provider: "jira", Type: "issue"},
		"jira:ZZZ-9":     {WorkItemID: "jira:ZZZ-9", Provider: "jira", Type: "issue"},
	}
	rows := func(recordedDonor, recordedTeam string) map[string]workItemAttributionNewestPrimary {
		return map[string]workItemAttributionNewestPrimary{
			pullRequest:      linked(recordedDonor, recordedTeam),
			"gh:acme/api#99": own("team-pr"), "jira:EMP-1": own("team-empty"), "jira:AAA-1": own("team-a"), "jira:ZZZ-9": own("team-b"),
		}
	}
	pair := func(issue, tier string) (teamattribution.LinkPair, string) {
		return teamattribution.LinkPair{PullRequest: pullRequest, Issue: issue}, tier
	}
	for _, tc := range []struct {
		name       string
		edges      []teamattribution.GithubWorkItemDerivationDependencyEdge
		tiers      map[teamattribution.LinkPair]string
		recorded   [2]string
		wantOwed   string
		wantRefuse map[string]int
	}{
		{"K1 refused native pull request donor and a text issue donor", []teamattribution.GithubWorkItemDerivationDependencyEdge{edge("gh:acme/api#99"), edge("jira:AAA-1")},
			map[teamattribution.LinkPair]string{}, [2]string{"jira:AAA-1", "team-a"}, "[ghpr:acme/api#7]", map[string]int{"pr_kind": 1}},
		{"K2 refused native empty-type donor and a text issue donor", []teamattribution.GithubWorkItemDerivationDependencyEdge{edge("jira:EMP-1"), edge("jira:AAA-1")},
			map[teamattribution.LinkPair]string{}, [2]string{"jira:AAA-1", "team-a"}, "[ghpr:acme/api#7]", map[string]int{"empty_type": 1}},
		{"K6 eligible native issue donor beside a refused native pull request donor", []teamattribution.GithubWorkItemDerivationDependencyEdge{edge("gh:acme/api#99"), edge("jira:ZZZ-9"), edge("jira:AAA-1")},
			map[teamattribution.LinkPair]string{}, [2]string{"jira:AAA-1", "team-a"}, "[ghpr:acme/api#7]", map[string]int{"pr_kind": 1}},
		{"K6 stable: the item already copies the eligible native donor", []teamattribution.GithubWorkItemDerivationDependencyEdge{edge("gh:acme/api#99"), edge("jira:ZZZ-9"), edge("jira:AAA-1")},
			map[teamattribution.LinkPair]string{}, [2]string{"jira:ZZZ-9", "team-b"}, "[]", map[string]int{"pr_kind": 1}},
		{"K7 native edge to an unknown item and a text issue donor", []teamattribution.GithubWorkItemDerivationDependencyEdge{edge("jira:NOPE-1"), edge("jira:AAA-1")},
			map[teamattribution.LinkPair]string{}, [2]string{"jira:AAA-1", "team-a"}, "[ghpr:acme/api#7]", map[string]int{"unknown": 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tiers := map[teamattribution.LinkPair]string{}
			for _, e := range tc.edges {
				tier := "explicit_text"
				switch e.TargetWorkItemID {
				case "gh:acme/api#99", "jira:EMP-1", "jira:ZZZ-9", "jira:NOPE-1":
					tier = "native"
				}
				p, v := pair(e.TargetWorkItemID, tier)
				tiers[p] = v
			}
			refused := map[string]int{}
			got := donorChangedDependents(tc.edges, nil, rows(tc.recorded[0], tc.recorded[1]), kinds, nil, refused, tiers)
			if fmt.Sprint(got) != tc.wantOwed {
				t.Errorf("owed %v, want %s", got, tc.wantOwed)
			}
			if fmt.Sprint(refused) != fmt.Sprint(tc.wantRefuse) {
				t.Errorf("refused %v, want %v", refused, tc.wantRefuse)
			}
		})
	}
}
