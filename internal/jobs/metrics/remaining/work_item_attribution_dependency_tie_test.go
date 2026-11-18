package remaining

import (
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// The donor-change trigger and the donor-target load read the same latest
// edge as the cascade: on a last_synced tie of a native relates_to edge and a
// "Blocks #11" text edge to the same issue, #11 stays a donor.
func TestTheDonorChangeTriggerKeepsTheInheritableEdgeOnATie(t *testing.T) {
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	before, after := base, base.Add(time.Hour)
	edge := func(target, relationship string) teamattribution.GithubWorkItemDerivationDependencyEdge {
		return teamattribution.GithubWorkItemDerivationDependencyEdge{SourceWorkItemID: "P", TargetWorkItemID: target, RelationshipType: relationship, LastSynced: base}
	}
	tie := []teamattribution.GithubWorkItemDerivationDependencyEdge{edge("D11", "relates_to"), edge("D11", "blocks")}
	own := func(team string, at time.Time) workItemAttributionNewestPrimary {
		return workItemAttributionNewestPrimary{TeamID: team, Source: "project_ownership", ComputedAt: at}
	}
	for _, tc := range []struct {
		name  string
		edges []teamattribution.GithubWorkItemDerivationDependencyEdge
		rows  map[string]workItemAttributionNewestPrimary
	}{
		{"#11 is the only donor and gained a row", tie, map[string]workItemAttributionNewestPrimary{
			"P": {Source: "unassigned", ComputedAt: before}, "D11": own("team-11", after),
		}},
		{"the item copies #22 and #11 changed", append(append([]teamattribution.GithubWorkItemDerivationDependencyEdge(nil), tie...), edge("D22", "relates_to")), map[string]workItemAttributionNewestPrimary{
			"P": {TeamID: "team-22", Source: "linked_issue", ComputedAt: before,
				Evidence: "linked_issue=D22;dependency_type=relates_to;donor_provider=github;donor_source=project_ownership"},
			"D11": own("team-11", after), "D22": own("team-22", before),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := donorChangedDependents(tc.edges, nil, tc.rows, issueKindsOf(tc.rows), nil, nil); fmt.Sprint(got) != "[P]" {
				t.Errorf("owed %v, want [P]", got)
			}
		})
	}
	ids, _ := workItemAttributionDonorTargets(tie, map[string]teamattribution.GithubWorkItemDerivationSubject{})
	if fmt.Sprint(ids) != "[D11]" {
		t.Errorf("donor targets %v, want [D11]", ids)
	}
}
