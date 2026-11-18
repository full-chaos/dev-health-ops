package remaining

import (
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// An issue's linked_issue row is provenance only (is_primary = 0), so the
// donor the item records is the linked_issue row of its newest snapshot, not
// its primary row: its donor's change still rederives it, and a donor
// recomputed with the same team still triggers nothing.
func TestADonorChangeReadsTheLinkedIssueRowOfTheItemsNewestSnapshot(t *testing.T) {
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	before, after := base, base.Add(time.Hour)
	edges := []teamattribution.GithubWorkItemDerivationDependencyEdge{{SourceWorkItemID: "I", TargetWorkItemID: "D", RelationshipType: "relates_to", LastSynced: base}}
	issue := func(primarySource, donor, team string, linkedAt time.Time) workItemAttributionNewestPrimary {
		row := workItemAttributionNewestPrimary{Source: primarySource, ComputedAt: before}
		if donor != "" {
			row.LinkedTeamID, row.LinkedComputedAt = team, linkedAt
			row.LinkedEvidence = fmt.Sprintf("linked_issue=%s;dependency_type=relates_to;donor_provider=jira;donor_source=project_ownership", donor)
		}
		return row
	}
	own := func(team string) workItemAttributionNewestPrimary {
		return workItemAttributionNewestPrimary{TeamID: team, Source: "project_ownership", ComputedAt: after}
	}
	for _, tc := range []struct {
		name string
		rows map[string]workItemAttributionNewestPrimary
		want []string
	}{
		{"the last donor lost its team", map[string]workItemAttributionNewestPrimary{
			"I": issue("unassigned", "D", "team-a", before), "D": {Source: "unassigned", ComputedAt: after}}, []string{"I"}},
		{"the donor changed team", map[string]workItemAttributionNewestPrimary{
			"I": issue("unassigned", "D", "team-a", before), "D": own("team-b")}, []string{"I"}},
		{"the donor was recomputed with the same team", map[string]workItemAttributionNewestPrimary{
			"I": issue("unassigned", "D", "team-a", before), "D": own("team-a")}, nil},
		{"a project rule is the primary beside the linked row", map[string]workItemAttributionNewestPrimary{
			"I": issue("manual_fallback", "D", "team-a", before), "D": own("team-a")}, nil},
		{"a linked row of an older snapshot is not the item's", map[string]workItemAttributionNewestPrimary{
			"I": issue("unassigned", "D", "team-a", before.Add(-time.Hour)), "D": own("team-a")}, []string{"I"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := donorChangedDependents(edges, nil, tc.rows, nil)
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Errorf("owed %v, want %v", got, tc.want)
			}
		})
	}
}
