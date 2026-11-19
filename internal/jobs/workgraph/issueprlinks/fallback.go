package issueprlinks

import (
	"sort"
	"strconv"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks/witness"
)

// # Native outranks every fallback (chris, 2026-10-15)
//
// "Text inferred when there's a PR link should have never happened ...
// Inferred is 100% a fallback only." The explicit_text and heuristic tiers are
// written only for a pull request AND an issue that have no native link. A pull
// request with a native issue link gets no lower-tier link to any issue, and an
// issue with a native pull request link gets no lower-tier link to any pull
// request. The rule is provider-agnostic: a pull request is (repo_id, number)
// and an issue is its work_item_id, whatever system minted either.

// PullRequestKey is witness.PullRequestKey.
type PullRequestKey = witness.PullRequestKey

// NativeLinks is witness.NativeLinks: every pull request and every issue that
// carries at least one native link in an organization.
type NativeLinks = witness.NativeLinks

// NewNativeLinks indexes the native rows of links; other tiers are ignored.
func NewNativeLinks(links []Link) NativeLinks {
	native := witness.NewNativeLinks()
	for _, link := range links {
		if link.Provenance == ProvenanceNative {
			native.Add(link.RepoID, link.PRNumber, link.WorkItemID)
		}
	}
	return native
}

// IsPullRequestWorkItem is witness.IsPullRequestWorkItem.
func IsPullRequestWorkItem(workItemID string) bool { return witness.IsPullRequestWorkItem(workItemID) }

// PlanOutrankedFallbacks returns the stored lower-tier rows to retract: the
// rows a native link outranks, and the rows whose issue side is a pull
// request. stored is the current (deduplicated) content of
// work_graph_issue_pr for one organization, every tier.
func PlanOutrankedFallbacks(stored []Link) []Link {
	native := NewNativeLinks(stored)
	var outranked []Link
	for _, link := range stored {
		if link.Provenance != edges.ProvenanceExplicitText && link.Provenance != edges.ProvenanceHeuristic {
			continue
		}
		if native.Outranks(link.RepoID, link.PRNumber, link.WorkItemID) || IsPullRequestWorkItem(link.WorkItemID) {
			outranked = append(outranked, link)
		}
	}
	sort.Slice(outranked, func(i, j int) bool { return retractionKey(outranked[i]) < retractionKey(outranked[j]) })
	return outranked
}

// FallbacksBesideNative counts the stored lower-tier rows the native-first
// rule forbids, the same rows PlanOutrankedFallbacks retracts:
// ByPullRequest when the row's pull request has a native link, ByIssue when
// its issue has one, PullRequestIssueSide when its issue side is a pull
// request work item, and Total with each row once. Every count must be 0
// after a build's link writers ran.
type FallbacksBesideNative struct {
	ByPullRequest        int
	ByIssue              int
	PullRequestIssueSide int
	Total                int
}

// CountFallbacksBesideNative applies the native-first rule to stored, the
// current (deduplicated) content of work_graph_issue_pr for one organization.
func CountFallbacksBesideNative(stored []Link) FallbacksBesideNative {
	native := NewNativeLinks(stored)
	var counts FallbacksBesideNative
	for _, link := range stored {
		if link.Provenance != edges.ProvenanceExplicitText && link.Provenance != edges.ProvenanceHeuristic {
			continue
		}
		byPR := native.HasPullRequest(link.RepoID, link.PRNumber)
		byIssue := native.HasIssue(link.WorkItemID)
		prIssueSide := IsPullRequestWorkItem(link.WorkItemID)
		if byPR {
			counts.ByPullRequest++
		}
		if byIssue {
			counts.ByIssue++
		}
		if prIssueSide {
			counts.PullRequestIssueSide++
		}
		if byPR || byIssue || prIssueSide {
			counts.Total++
		}
	}
	return counts
}

// retractionKey is one stored row's identity as the retraction statement
// matches it: the table's sorting key minus org_id, plus the tier.
func retractionKey(link Link) string {
	return link.RepoID.String() + "|" + link.WorkItemID + "|" + strconv.FormatUint(uint64(link.PRNumber), 10) + "|" + link.Provenance
}
