package remaining

import (
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/teamactive"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// A linked item is owed a rederive when one of its donors has a newer primary
// row AND the donor the item would copy now (the cascade's winner over the
// stored rows) differs from the one its row records (CHAOS-9478).
func TestADonorRowChangeRederivesOnlyTheItemsItChanges(t *testing.T) {
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	before, after := base, base.Add(time.Hour)
	edge := func(source, target string) teamattribution.GithubWorkItemDerivationDependencyEdge {
		return teamattribution.GithubWorkItemDerivationDependencyEdge{SourceWorkItemID: source, TargetWorkItemID: target, RelationshipType: "relates_to", LastSynced: base}
	}
	linked := func(donor, team string) workItemAttributionNewestPrimary {
		return workItemAttributionNewestPrimary{TeamID: team, Source: "linked_issue", ComputedAt: before,
			Evidence: fmt.Sprintf("linked_issue=%s;dependency_type=relates_to;donor_provider=jira;donor_source=project_ownership", donor)}
	}
	own := func(team string, at time.Time) workItemAttributionNewestPrimary {
		return workItemAttributionNewestPrimary{TeamID: team, Source: "project_ownership", ComputedAt: at}
	}
	unassigned := func(at time.Time) workItemAttributionNewestPrimary {
		return workItemAttributionNewestPrimary{Source: "unassigned", ComputedAt: at}
	}
	for _, tc := range []struct {
		name     string
		edges    []teamattribution.GithubWorkItemDerivationDependencyEdge
		keys     map[string]string
		rows     map[string]workItemAttributionNewestPrimary
		inactive teamactive.Inactive
		want     []string
	}{
		{"the donor changed team", []teamattribution.GithubWorkItemDerivationDependencyEdge{edge("P", "D")}, nil,
			map[string]workItemAttributionNewestPrimary{"P": linked("D", "team-a"), "D": own("team-b", after)}, nil, []string{"P"}},
		{"the donor lost its own row", []teamattribution.GithubWorkItemDerivationDependencyEdge{edge("P", "D")}, nil,
			map[string]workItemAttributionNewestPrimary{"P": linked("D", "team-a"), "D": unassigned(after)}, nil, []string{"P"}},
		{"a donor gained an own row and the item copies none", []teamattribution.GithubWorkItemDerivationDependencyEdge{edge("P", "D")}, nil,
			map[string]workItemAttributionNewestPrimary{"P": unassigned(before), "D": own("team-b", after)}, nil, []string{"P"}},
		{"the donor was recomputed with the same team", []teamattribution.GithubWorkItemDerivationDependencyEdge{edge("P", "D")}, nil,
			map[string]workItemAttributionNewestPrimary{"P": linked("D", "team-a"), "D": own("team-a", after)}, nil, nil},
		{"the donor's row is not newer than the item's", []teamattribution.GithubWorkItemDerivationDependencyEdge{edge("P", "D")}, nil,
			map[string]workItemAttributionNewestPrimary{"P": linked("D", "team-a"), "D": own("team-b", before)}, nil, nil},
		{"a donor that does not win changed", []teamattribution.GithubWorkItemDerivationDependencyEdge{edge("P", "A"), edge("P", "B")}, nil,
			map[string]workItemAttributionNewestPrimary{"P": linked("A", "team-a"), "A": own("team-a", before), "B": own("team-b", after)}, nil, nil},
		{"the winning donor lost its row so the next one wins", []teamattribution.GithubWorkItemDerivationDependencyEdge{edge("P", "A"), edge("P", "B")}, nil,
			map[string]workItemAttributionNewestPrimary{"P": linked("A", "team-a"), "A": unassigned(after), "B": own("team-b", before)}, nil, []string{"P"}},
		{"an item of its own native team never inherits", []teamattribution.GithubWorkItemDerivationDependencyEdge{edge("P", "D")}, nil,
			map[string]workItemAttributionNewestPrimary{"P": {TeamID: "team-n", Source: "native_team", ComputedAt: before}, "D": own("team-b", after)}, nil, nil},
		{"an item of its own project ownership never inherits", []teamattribution.GithubWorkItemDerivationDependencyEdge{edge("P", "D")}, nil,
			map[string]workItemAttributionNewestPrimary{"P": own("team-p", before), "D": own("team-b", after)}, nil, nil},
		{"a donor of an inactive team is no donor", []teamattribution.GithubWorkItemDerivationDependencyEdge{edge("P", "D")}, nil,
			map[string]workItemAttributionNewestPrimary{"P": unassigned(before), "D": own("team-x", after)}, teamactive.Inactive{"team-x": {}}, nil},
		{"an item with no row is not this trigger's", []teamattribution.GithubWorkItemDerivationDependencyEdge{edge("P", "D")}, nil,
			map[string]workItemAttributionNewestPrimary{"D": own("team-b", after)}, nil, nil},
		{"an inherited donor row gives nothing", []teamattribution.GithubWorkItemDerivationDependencyEdge{edge("P", "D")}, nil,
			map[string]workItemAttributionNewestPrimary{"P": unassigned(before), "D": {TeamID: "team-b", Source: "linked_issue", ComputedAt: after}}, nil, nil},
		{"an extkey edge resolves to the one item of the key", []teamattribution.GithubWorkItemDerivationDependencyEdge{edge("P", "extkey:core-7")}, map[string]string{"CORE-7": "jira:CORE-7"},
			map[string]workItemAttributionNewestPrimary{"P": linked("jira:CORE-7", "team-a"), "jira:CORE-7": own("team-b", after)}, nil, []string{"P"}},
		{"an edge that is not inheritable", []teamattribution.GithubWorkItemDerivationDependencyEdge{{SourceWorkItemID: "P", TargetWorkItemID: "D", RelationshipType: "blocks", LastSynced: base}}, nil,
			map[string]workItemAttributionNewestPrimary{"P": unassigned(before), "D": own("team-b", after)}, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := donorChangedDependents(tc.edges, tc.keys, tc.rows, tc.inactive)
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Errorf("owed %v, want %v", got, tc.want)
			}
		})
	}
}

func TestTheKeyIndexDropsAKeyTwoItemsHold(t *testing.T) {
	index := workItemAttributionKeyIndex(map[string]teamattribution.GithubWorkItemDerivationSubject{
		"jira:CORE-1":   {WorkItemID: "jira:CORE-1", Provider: "jira"},
		"linear:CORE-1": {WorkItemID: "linear:CORE-1", Provider: "linear"},
		"jira:OPS-2":    {WorkItemID: "jira:OPS-2", Provider: "jira"},
		"gh:OPS-3":      {WorkItemID: "gh:OPS-3", Provider: "github"},
	})
	if fmt.Sprint(index) != "map[OPS-2:jira:OPS-2]" {
		t.Errorf("index %v, want only OPS-2", index)
	}
}
