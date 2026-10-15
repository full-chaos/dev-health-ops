package issueprlinks

import (
	"sort"
	"strconv"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
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

// PullRequestKey names one pull request the way work_graph_issue_pr does.
type PullRequestKey struct {
	RepoID   uuid.UUID
	PRNumber uint32
}

// NativeLinks is every pull request and every issue that carries at least one
// native link in an organization.
type NativeLinks struct {
	pullRequests map[PullRequestKey]struct{}
	issues       map[string]struct{}
}

// NewNativeLinks indexes the native rows of links; other tiers are ignored.
func NewNativeLinks(links []Link) NativeLinks {
	native := NativeLinks{
		pullRequests: make(map[PullRequestKey]struct{}),
		issues:       make(map[string]struct{}),
	}
	for _, link := range links {
		if link.Provenance != ProvenanceNative {
			continue
		}
		native.pullRequests[PullRequestKey{RepoID: link.RepoID, PRNumber: link.PRNumber}] = struct{}{}
		native.issues[link.WorkItemID] = struct{}{}
	}
	return native
}

// Outranks reports whether a native link already covers the pull request or
// the issue, so no lower-tier link between them may exist.
func (native NativeLinks) Outranks(repoID uuid.UUID, prNumber uint32, workItemID string) bool {
	if _, linked := native.pullRequests[PullRequestKey{RepoID: repoID, PRNumber: prNumber}]; linked {
		return true
	}
	_, linked := native.issues[workItemID]
	return linked
}

// HasPullRequest reports whether the pull request has a native link.
func (native NativeLinks) HasPullRequest(repoID uuid.UUID, prNumber uint32) bool {
	_, linked := native.pullRequests[PullRequestKey{RepoID: repoID, PRNumber: prNumber}]
	return linked
}

// HasIssue reports whether the issue has a native link.
func (native NativeLinks) HasIssue(workItemID string) bool {
	_, linked := native.issues[workItemID]
	return linked
}

// PullRequests lists the natively linked pull requests in a stable order.
func (native NativeLinks) PullRequests() []PullRequestKey {
	out := make([]PullRequestKey, 0, len(native.pullRequests))
	for key := range native.pullRequests {
		out = append(out, key)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RepoID != out[j].RepoID {
			return out[i].RepoID.String() < out[j].RepoID.String()
		}
		return out[i].PRNumber < out[j].PRNumber
	})
	return out
}

// Issues lists the natively linked issues in a stable order.
func (native NativeLinks) Issues() []string {
	out := make([]string, 0, len(native.issues))
	for id := range native.issues {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// Empty reports whether no native link exists.
func (native NativeLinks) Empty() bool {
	return len(native.pullRequests) == 0 && len(native.issues) == 0
}

// IsPullRequestWorkItem reports whether a work item id names a pull or merge
// request (ParsePRSource's grammar). Such an item is never the issue side of
// an issue <> pull request link.
func IsPullRequestWorkItem(workItemID string) bool {
	_, ok := ParsePRSource(workItemID)
	return ok
}

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

// retractionKey is one stored row's identity as the retraction statement
// matches it: the table's sorting key minus org_id, plus the tier.
func retractionKey(link Link) string {
	return link.RepoID.String() + "|" + link.WorkItemID + "|" + strconv.FormatUint(uint64(link.PRNumber), 10) + "|" + link.Provenance
}
