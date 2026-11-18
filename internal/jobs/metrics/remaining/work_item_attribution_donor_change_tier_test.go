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
			got := donorChangedDependents(edges, nil, tc.rows, nil, tc.tiers)
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Errorf("owed %v, want %v", got, tc.want)
			}
		})
	}
}
