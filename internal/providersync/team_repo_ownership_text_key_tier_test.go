package providersync

import (
	"fmt"
	"testing"
)

// A key parsed from pull request text (an `extkey:KEY` dependency target,
// relationship external_issue_key or relates_to) is text-inferred, never a
// native link (chris, 2026-10-15: no tracker-to-pull-request link exists for
// a key in text; text is a fallback only). In the derivation it counts at the
// explicit_text tier, it is not used where the pull request has a native
// issue link, and a native donor of the same pull request is preferred.

// textKeyChangeRequest is one code-host shape of a pull or merge request.
type textKeyChangeRequest struct {
	provider, itemType, separator string
}

var textKeyChangeRequests = []textKeyChangeRequest{
	{"github", "pr", "#"},
	{"gitlab", "merge_request", "!"},
}

// textKeyTrackers are the trackers whose keys a pull request text can name.
var textKeyTrackers = []string{"jira", "linear"}

// ownerIssueProviders are the providers of the main owner's natively linked issue.
var ownerIssueProviders = map[string]string{
	"jira":   "jira:OPS-1",
	"linear": "linear:CHAOS-1",
	"github": "gh:full-chaos/deploy#5",
	"gitlab": "gitlab:full-chaos/deploy#5",
}

func changeRequestID(cr textKeyChangeRequest, number int) string {
	if cr.provider == "github" {
		return fmt.Sprintf("ghpr:full-chaos/deploy#%d", number)
	}
	return fmt.Sprintf("gitlab:full-chaos/deploy!%d", number)
}

func ownerAndWebLinks(ownerProvider, tracker string) []TeamRepoOwnershipProjectLink {
	return []TeamRepoOwnershipProjectLink{
		{Provider: ownerProvider, ProjectID: "owner-proj", TeamID: "owner", IsPrimary: true},
		{Provider: tracker, ProjectID: "web-proj", TeamID: "web", IsPrimary: true},
	}
}

func ownersOf(t *testing.T, got teamRepoOwnershipDerivation) map[string]repoOwnerWant {
	t.Helper()
	out := map[string]repoOwnerWant{}
	for _, row := range got.Rows {
		out[row.TeamID] = repoOwnerWant{team: row.TeamID, primary: row.IsPrimary, share: row.Share}
	}
	return out
}

// TestATextKeyDonorCountsAtTheExplicitTextTier: two deploy pull requests whose
// text names WEB-5 make Web a candidate, at the explicit_text tier, so one
// native link of the owner outranks them. Before, the two text keys counted as
// two native links and made Web the main owner.
func TestATextKeyDonorCountsAtTheExplicitTextTier(t *testing.T) {
	for ownerProvider, ownerIssue := range ownerIssueProviders {
		for _, tracker := range textKeyTrackers {
			for _, cr := range textKeyChangeRequests {
				t.Run(ownerProvider+"/"+tracker+"/"+cr.provider, func(t *testing.T) {
					webIssue := tracker + ":WEB-5"
					workItems := []TeamRepoOwnershipWorkItem{
						{WorkItemID: ownerIssue, Provider: ownerProvider, ProjectID: "owner-proj"},
						{WorkItemID: webIssue, Provider: tracker, ProjectID: "web-proj"},
						{WorkItemID: changeRequestID(cr, 427), Provider: cr.provider, Type: cr.itemType, RepoID: "repo-1"},
						{WorkItemID: changeRequestID(cr, 445), Provider: cr.provider, Type: cr.itemType, RepoID: "repo-1"},
					}
					edges := []TeamRepoOwnershipDependencyEdge{
						{SourceWorkItemID: changeRequestID(cr, 427), TargetWorkItemID: "extkey:WEB-5", RelationshipType: "external_issue_key"},
						{SourceWorkItemID: changeRequestID(cr, 445), TargetWorkItemID: "extkey:WEB-5", RelationshipType: "relates_to"},
					}
					links := []TeamRepoOwnershipIssuePRLink{{WorkItemID: ownerIssue, RepoID: "repo-1", PRNumber: 1, Provenance: "native"}}

					got := ownersOf(t, deriveTeamRepoOwnership("org-1", ownerAndWebLinks(ownerProvider, tracker), workItems, edges, links, nil))
					if got["owner"].primary != teamRepoOwnershipMainOwner || got["web"].primary != teamRepoOwnershipCoOwner {
						t.Fatalf("owners %+v, want owner main (1 native) and web co-owner (2 explicit_text)", got)
					}
				})
			}
		}
	}
}

// TestATextKeyDonorIsNotUsedForAPullRequestWithANativeLink: the pull request
// has a native link to the owner's issue, so the key its text names gives Web
// nothing.
func TestATextKeyDonorIsNotUsedForAPullRequestWithANativeLink(t *testing.T) {
	for ownerProvider, ownerIssue := range ownerIssueProviders {
		for _, tracker := range textKeyTrackers {
			for _, cr := range textKeyChangeRequests {
				t.Run(ownerProvider+"/"+tracker+"/"+cr.provider, func(t *testing.T) {
					workItems := []TeamRepoOwnershipWorkItem{
						{WorkItemID: ownerIssue, Provider: ownerProvider, ProjectID: "owner-proj"},
						{WorkItemID: tracker + ":WEB-5", Provider: tracker, ProjectID: "web-proj"},
						{WorkItemID: changeRequestID(cr, 704), Provider: cr.provider, Type: cr.itemType, RepoID: "repo-1"},
					}
					edges := []TeamRepoOwnershipDependencyEdge{
						{SourceWorkItemID: changeRequestID(cr, 704), TargetWorkItemID: "extkey:WEB-5", RelationshipType: "external_issue_key"},
					}
					links := []TeamRepoOwnershipIssuePRLink{{WorkItemID: ownerIssue, RepoID: "repo-1", PRNumber: 704, Provenance: "native"}}

					got := ownersOf(t, deriveTeamRepoOwnership("org-1", ownerAndWebLinks(ownerProvider, tracker), workItems, edges, links, nil))
					if _, web := got["web"]; web || len(got) != 1 {
						t.Fatalf("owners %+v, want only owner: the pull request has a native link", got)
					}
				})
			}
		}
	}
}

// TestANativeDonorOfThePullRequestIsPreferredOverATextKey: the pull request has
// a native donor edge to the owner's issue and a text key to Web's issue. The
// native donor decides the team, whatever the order of the two targets.
func TestANativeDonorOfThePullRequestIsPreferredOverATextKey(t *testing.T) {
	for _, tracker := range textKeyTrackers {
		for _, cr := range textKeyChangeRequests {
			t.Run(tracker+"/"+cr.provider, func(t *testing.T) {
				workItems := []TeamRepoOwnershipWorkItem{
					{WorkItemID: "linear:ZZZ-1", Provider: "linear", ProjectID: "owner-proj"},
					{WorkItemID: tracker + ":WEB-5", Provider: tracker, ProjectID: "web-proj"},
					{WorkItemID: changeRequestID(cr, 9), Provider: cr.provider, Type: cr.itemType, RepoID: "repo-1"},
				}
				edges := []TeamRepoOwnershipDependencyEdge{
					{SourceWorkItemID: changeRequestID(cr, 9), TargetWorkItemID: "linear:ZZZ-1", RelationshipType: "relates_to"},
					{SourceWorkItemID: changeRequestID(cr, 9), TargetWorkItemID: "extkey:WEB-5", RelationshipType: "external_issue_key"},
				}
				got := ownersOf(t, deriveTeamRepoOwnership("org-1", ownerAndWebLinks("linear", tracker), workItems, edges, nil, nil))
				if len(got) != 1 || got["owner"].primary != teamRepoOwnershipMainOwner {
					t.Fatalf("owners %+v, want only owner through the native donor", got)
				}
			})
		}
	}
}

// TestALinkWhoseItemReachesATeamOnlyThroughATextKeyCountsAsExplicitText: a
// link row (here a native one) whose work item reaches a team only through a
// text key counts at the explicit_text tier, not at the link's own tier.
func TestALinkWhoseItemReachesATeamOnlyThroughATextKeyCountsAsExplicitText(t *testing.T) {
	for _, tracker := range textKeyTrackers {
		for _, cr := range textKeyChangeRequests {
			t.Run(tracker+"/"+cr.provider, func(t *testing.T) {
				workItems := []TeamRepoOwnershipWorkItem{
					{WorkItemID: "linear:CHAOS-1", Provider: "linear", ProjectID: "owner-proj"},
					{WorkItemID: tracker + ":WEB-5", Provider: tracker, ProjectID: "web-proj"},
					{WorkItemID: changeRequestID(cr, 3), Provider: cr.provider, Type: cr.itemType},
				}
				edges := []TeamRepoOwnershipDependencyEdge{
					{SourceWorkItemID: changeRequestID(cr, 3), TargetWorkItemID: "extkey:WEB-5", RelationshipType: "external_issue_key"},
				}
				links := []TeamRepoOwnershipIssuePRLink{
					{WorkItemID: "linear:CHAOS-1", RepoID: "repo-1", PRNumber: 1, Provenance: "native"},
					{WorkItemID: changeRequestID(cr, 3), RepoID: "repo-1", PRNumber: 10, Provenance: "native"},
					{WorkItemID: changeRequestID(cr, 3), RepoID: "repo-1", PRNumber: 11, Provenance: "native"},
				}
				got := ownersOf(t, deriveTeamRepoOwnership("org-1", ownerAndWebLinks("linear", tracker), workItems, edges, links, nil))
				if got["owner"].primary != teamRepoOwnershipMainOwner || got["web"].primary != teamRepoOwnershipCoOwner {
					t.Fatalf("owners %+v, want owner main (1 native) and web co-owner (2 explicit_text)", got)
				}
			})
		}
	}
}
