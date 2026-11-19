package remaining

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

var (
	linkChangeRowAt = time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	linkChangeOld   = linkChangeRowAt.Add(-time.Hour)
	linkChangeIssue = teamattribution.GithubWorkItemDerivationSubject{WorkItemID: "jira:LC-1", Provider: "jira", Type: "story", OrgID: "org"}
)

const (
	linkChangeRepoA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	linkChangeRepoB = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
)

func linkChangePR(repo string, number uint32, tier string) teamattribution.GithubWorkItemDerivationLinkedPullRequest {
	return teamattribution.GithubWorkItemDerivationLinkedPullRequest{RepoID: repo, PRNumber: number, Tier: tier}
}

func linkChangeRow(repo string, number uint32, tier string, at time.Time) linkChangeLink {
	return linkChangeLink{GithubWorkItemDerivationLinkedPullRequest: linkChangePR(repo, number, tier), LastSynced: at}
}

func linkChangeStoredOn(named ...teamattribution.GithubWorkItemDerivationLinkedPullRequest) linkChangeStored {
	return linkChangeStored{PrimarySource: "repo_ownership", PrimaryTeam: "team-a", ComputedAt: linkChangeRowAt, Named: named}
}

// The link the stored row names is no longer stored (removed, or retracted).
func TestALinkChangeGoneLinkIsDirty(t *testing.T) {
	stored := linkChangeStoredOn(linkChangePR(linkChangeRepoA, 7, "explicit_text"))
	links := []linkChangeLink{linkChangeRow(linkChangeRepoA, 8, "explicit_text", linkChangeOld)}
	if got := linkChangeReason(linkChangeIssue, stored, links); got != linkChangeGone {
		t.Fatalf("reason %q, want %q", got, linkChangeGone)
	}
}

// The named link is stored at another tier now.
func TestALinkChangeTierChangedLinkIsDirty(t *testing.T) {
	stored := linkChangeStoredOn(linkChangePR(linkChangeRepoA, 7, "explicit_text"))
	links := []linkChangeLink{linkChangeRow(linkChangeRepoA, 7, "native", linkChangeOld)}
	if got := linkChangeReason(linkChangeIssue, stored, links); got != linkChangeTier {
		t.Fatalf("reason %q, want %q", got, linkChangeTier)
	}
}

// The named link is still stored at its tier, but the issue has a link of a
// higher tier now: the named one no longer counts.
func TestALinkChangeLinkBelowTheBestTierIsDirty(t *testing.T) {
	stored := linkChangeStoredOn(linkChangePR(linkChangeRepoA, 7, "heuristic"))
	links := []linkChangeLink{
		linkChangeRow(linkChangeRepoA, 7, "heuristic", linkChangeOld),
		linkChangeRow(linkChangeRepoB, 2, "explicit_text", linkChangeOld),
	}
	if got := linkChangeReason(linkChangeIssue, stored, links); got != linkChangeBelowBest {
		t.Fatalf("reason %q, want %q", got, linkChangeBelowBest)
	}
}

// A best-tier link written at the instant of the stored rows is not newer than
// them; one written after them is.
func TestALinkChangeLinkAsOldAsTheRowIsClean(t *testing.T) {
	stored := linkChangeStoredOn(linkChangePR(linkChangeRepoA, 7, "explicit_text"))
	same := []linkChangeLink{linkChangeRow(linkChangeRepoA, 7, "explicit_text", linkChangeRowAt)}
	if got := linkChangeReason(linkChangeIssue, stored, same); got != "" {
		t.Fatalf("link as old as the row: reason %q, want none", got)
	}
	newer := []linkChangeLink{linkChangeRow(linkChangeRepoA, 7, "explicit_text", linkChangeRowAt.Add(time.Millisecond))}
	if got := linkChangeReason(linkChangeIssue, stored, newer); got != linkChangeNewerLink {
		t.Fatalf("newer link: reason %q, want %q", got, linkChangeNewerLink)
	}
	lower := []linkChangeLink{
		linkChangeRow(linkChangeRepoA, 7, "explicit_text", linkChangeOld),
		linkChangeRow(linkChangeRepoB, 3, "heuristic", linkChangeRowAt.Add(time.Hour)),
	}
	if got := linkChangeReason(linkChangeIssue, stored, lower); got != "" {
		t.Fatalf("newer link below the best tier: reason %q, want none", got)
	}
}

// A pull or merge request takes no link-change rederive, whatever its stored
// rows and links say.
func TestALinkChangeIgnoresAPullRequest(t *testing.T) {
	for _, pr := range []teamattribution.GithubWorkItemDerivationSubject{
		{WorkItemID: "ghpr:acme/a#7", Provider: "github", Type: "pr", OrgID: "org"},
		{WorkItemID: "gitlab:acme/a!7", Provider: "gitlab", Type: "merge_request", OrgID: "org"},
	} {
		stored := linkChangeStoredOn(linkChangePR(linkChangeRepoA, 7, "explicit_text"))
		stored.PrimarySource = "unassigned"
		links := []linkChangeLink{linkChangeRow(linkChangeRepoB, 9, "native", linkChangeRowAt.Add(time.Hour))}
		if got := linkChangeReason(pr, stored, links); got != "" {
			t.Errorf("%s: reason %q, want none", pr.WorkItemID, got)
		}
		if linkChangeOwnedLinkCandidate(pr, stored, links) {
			t.Errorf("%s: an owned-link candidate", pr.WorkItemID)
		}
	}
}

// An unassigned issue with a best-tier link to an owned repository is dirty
// when the cascade gives it the owner; once its row carries the owner, or when
// the repository has no owner, it is not.
func TestAnUnassignedIssueWithAnOwnedLinkIsDirty(t *testing.T) {
	repo := linkChangeRepoA
	owned := teamattribution.GithubWorkItemDerivationFacts{Repos: []teamattribution.GithubWorkItemDerivationRepoFact{{
		Provider: "github", TeamID: "team-a", TeamName: "Team A", RepoID: &repo, RepoFullName: "acme/a", IsPrimary: 1, Specificity: 100,
	}}}
	links := []linkChangeLink{linkChangeRow(linkChangeRepoA, 5, "native", linkChangeOld)}
	derivedFrom := func(facts teamattribution.GithubWorkItemDerivationFacts) teamattribution.GithubWorkItemDerivationContext {
		derived := teamattribution.NewGitHubWorkItemDerivationContext(facts)
		derived.LinkedPullRequests = map[string][]teamattribution.GithubWorkItemDerivationLinkedPullRequest{
			linkChangeIssue.WorkItemID: {links[0].GithubWorkItemDerivationLinkedPullRequest},
		}
		return derived
	}
	unassigned := linkChangeStored{PrimarySource: "unassigned", ComputedAt: linkChangeRowAt}
	if got := linkChangeReason(linkChangeIssue, unassigned, links); got != "" {
		t.Fatalf("setup: the link is older than the row, reason %q", got)
	}
	if !linkChangeOwnedLinkCandidate(linkChangeIssue, unassigned, links) {
		t.Fatal("an unassigned issue with a best-tier link is not a candidate")
	}
	if !linkChangePrimaryDiffers(derivedFrom(owned), linkChangeIssue, unassigned) {
		t.Fatal("the cascade gives team-a, the stored row is unassigned: not dirty")
	}
	carried := linkChangeStored{PrimarySource: "repo_ownership", PrimaryTeam: "team-a", ComputedAt: linkChangeRowAt}
	if linkChangeOwnedLinkCandidate(linkChangeIssue, carried, links) {
		t.Fatal("an issue whose primary is repo_ownership is a candidate")
	}
	if linkChangePrimaryDiffers(derivedFrom(owned), linkChangeIssue, carried) {
		t.Fatal("the stored row already carries team-a: dirty")
	}
	if linkChangePrimaryDiffers(derivedFrom(teamattribution.GithubWorkItemDerivationFacts{}), linkChangeIssue, unassigned) {
		t.Fatal("no owner: the cascade still answers unassigned, yet dirty")
	}
}
