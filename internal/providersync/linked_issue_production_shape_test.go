package providersync

import (
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// linkLikeProduction builds the linked-issue index the way the writers do
// (BuildLinkedIssueIndexFromRows, with every subject's row as this context
// gives it: the frozen cases hold no stored rows, and every subject is one
// the run writes) and sets the team and the evidence of each linked item.
func linkLikeProduction(
	derived *teamattribution.GithubWorkItemDerivationContext, provider string,
	subjects map[string]teamattribution.GithubWorkItemDerivationSubject,
	dependencies []teamattribution.GithubWorkItemDerivationDependencyEdge,
) {
	all := make(map[string]struct{}, len(subjects))
	for id := range subjects {
		all[id] = struct{}{}
	}
	teams, evidence, _, _ := derived.BuildLinkedIssueIndexFromRows(provider, subjects, derived.OwnDonorRows(subjects, all), dependencies, nil)
	derived.LinkedIssue, derived.LinkedIssueEvidence = teams, evidence
}

// pythonLinkedIssueEvidence is the declared divergence of the frozen Python
// oracles (CHAOS-9460): Python wrote a linked_issue row's evidence as
// "linked_issue=<item id>" (compute_work_items.py:537), the item's own id; Go
// writes the donor's provenance, "linked_issue=<donor id>;dependency_type=
// <relationship>;donor_provider=<provider>;donor_source=<donor row source>"
// (team-attribution.md section 0, row 5). The comparison reads Go's evidence
// back as the Python string; every other value is compared as is.
func pythonLinkedIssueEvidence(workItemID, evidence string) string {
	if strings.HasPrefix(evidence, "linked_issue=") && strings.Contains(evidence, ";dependency_type=") {
		return "linked_issue=" + workItemID
	}
	return evidence
}
