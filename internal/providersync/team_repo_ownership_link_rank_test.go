package providersync

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
)

// The ownership tally counts links at edges.ProvenanceRank, the one source of
// the link-tier ranks: one link at a higher rank outranks any number of links
// at every lower rank, and a provenance outside the three tiers ranks lowest.
func TestTeamRepoOwnershipLinkCountsRankByProvenanceRank(t *testing.T) {
	order := []string{edges.ProvenanceNative, edges.ProvenanceExplicitText, edges.ProvenanceHeuristic, "other"}
	for high := range order {
		for low := high + 1; low < len(order); low++ {
			one, many := teamRepoOwnershipLinkCounts{}, teamRepoOwnershipLinkCounts{}
			one[edges.ProvenanceRank(order[high])]++
			many[edges.ProvenanceRank(order[low])] += 50
			if one.compare(many) <= 0 || many.compare(one) >= 0 {
				t.Errorf("1 %s link vs 50 %s links: compare = %d, want the %s link to win",
					order[high], order[low], one.compare(many), order[high])
			}
		}
	}
}

// A text-key donor reached through a heuristic link stays at the heuristic
// tier: the text key caps a link at explicit_text, it never raises one.
func TestATextKeyDonorOnAHeuristicLinkStaysHeuristic(t *testing.T) {
	for _, tracker := range textKeyTrackers {
		for _, cr := range textKeyChangeRequests {
			t.Run(tracker+"/"+cr.provider, func(t *testing.T) {
				workItems := []TeamRepoOwnershipWorkItem{
					{WorkItemID: "linear:CHAOS-1", Provider: "linear", ProjectID: "owner-proj"},
					{WorkItemID: tracker + ":WEB-5", Provider: tracker, ProjectID: "web-proj"},
					{WorkItemID: changeRequestID(cr, 3), Provider: cr.provider, Type: cr.itemType},
				}
				dependencyEdges := []TeamRepoOwnershipDependencyEdge{
					{SourceWorkItemID: changeRequestID(cr, 3), TargetWorkItemID: "extkey:WEB-5", RelationshipType: "external_issue_key"},
				}
				links := []TeamRepoOwnershipIssuePRLink{
					{WorkItemID: "linear:CHAOS-1", RepoID: "repo-1", PRNumber: 1, Provenance: edges.ProvenanceExplicitText},
					{WorkItemID: changeRequestID(cr, 3), RepoID: "repo-1", PRNumber: 10, Provenance: edges.ProvenanceHeuristic},
					{WorkItemID: changeRequestID(cr, 3), RepoID: "repo-1", PRNumber: 11, Provenance: edges.ProvenanceHeuristic},
				}
				got := ownersOf(t, deriveTeamRepoOwnership("org-1", ownerAndWebLinks("linear", tracker), workItems, dependencyEdges, links, nil))
				if got["owner"].primary != teamRepoOwnershipMainOwner || got["web"].primary != teamRepoOwnershipCoOwner {
					t.Fatalf("owners %+v, want owner main (1 explicit_text) and web co-owner (2 heuristic)", got)
				}
			})
		}
	}
}

// A link whose provenance is none of the three tiers counts, below heuristic.
func TestAnOtherProvenanceLinkRanksBelowHeuristic(t *testing.T) {
	workItems := []TeamRepoOwnershipWorkItem{
		{WorkItemID: "jira:OPS-1", Provider: "jira", ProjectID: "owner-proj"},
		{WorkItemID: "jira:WEB-5", Provider: "jira", ProjectID: "web-proj"},
	}
	links := []TeamRepoOwnershipIssuePRLink{
		{WorkItemID: "jira:OPS-1", RepoID: "repo-1", PRNumber: 1, Provenance: edges.ProvenanceHeuristic},
		{WorkItemID: "jira:WEB-5", RepoID: "repo-1", PRNumber: 2, Provenance: "inferred_by_other_writer"},
		{WorkItemID: "jira:WEB-5", RepoID: "repo-1", PRNumber: 3, Provenance: "inferred_by_other_writer"},
	}
	got := ownersOf(t, deriveTeamRepoOwnership("org-1", ownerAndWebLinks("jira", "jira"), workItems, nil, links, nil))
	if got["owner"].primary != teamRepoOwnershipMainOwner || got["web"].primary != teamRepoOwnershipCoOwner {
		t.Fatalf("owners %+v, want owner main (1 heuristic) and web co-owner (2 other)", got)
	}
}
