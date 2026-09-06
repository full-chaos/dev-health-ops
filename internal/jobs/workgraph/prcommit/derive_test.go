package prcommit_test

import (
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/prcommit"
)

func TestDeriveExplicitMergeCommitLinksToKnownPR(t *testing.T) {
	repo := uuid.New()
	inputs := prcommit.Inputs{
		OrgID:        "org-a",
		PullRequests: []prcommit.PullRequestRow{{OrgID: "org-a", RepoID: repo, Number: 42}},
		Commits: []prcommit.CommitRow{
			{OrgID: "org-a", RepoID: repo, Hash: "abc123", Message: "Merge pull request #42 from team/feature"},
		},
	}

	result := prcommit.Derive(inputs)

	if len(result.Links) != 1 {
		t.Fatalf("got %d links, want 1: %+v", len(result.Links), result.Links)
	}
	link := result.Links[0]
	if link.PRNumber != 42 || link.CommitHash != "abc123" {
		t.Fatalf("unexpected link: %+v", link)
	}
	if link.Provenance != edges.ProvenanceExplicitText || link.Confidence != 0.9 {
		t.Fatalf("explicit tier should tag explicit_text/0.9, got provenance=%s confidence=%v", link.Provenance, link.Confidence)
	}
	if link.Evidence != "commit_message_pr_ref" {
		t.Fatalf("unexpected evidence: %s", link.Evidence)
	}
}

func TestDeriveSquashCommitRequiresCorroborationAgainstKnownPR(t *testing.T) {
	repo := uuid.New()
	base := prcommit.Inputs{
		OrgID: "org-a",
		Commits: []prcommit.CommitRow{
			{OrgID: "org-a", RepoID: repo, Hash: "def456", Message: "Fix parser edge case (#42)"},
		},
	}

	// No known PR #42 in this (org, repo): the squash form must NOT be
	// promoted on its own (CHAOS-2435's whole reason for existing -- it is
	// lexically identical to a hand-authored issue reference).
	uncorroborated := prcommit.Derive(base)
	if len(uncorroborated.Links) != 0 {
		t.Fatalf("squash ref must not link without a known PR, got %+v", uncorroborated.Links)
	}

	// Known PR #42 in the SAME (org, repo): now it corroborates.
	withPR := base
	withPR.PullRequests = []prcommit.PullRequestRow{{OrgID: "org-a", RepoID: repo, Number: 42}}
	corroborated := prcommit.Derive(withPR)
	if len(corroborated.Links) != 1 {
		t.Fatalf("got %d links, want 1: %+v", len(corroborated.Links), corroborated.Links)
	}
	link := corroborated.Links[0]
	if link.Provenance != edges.ProvenanceHeuristic || link.Confidence != 0.6 {
		t.Fatalf("squash tier should tag heuristic/0.6, got provenance=%s confidence=%v", link.Provenance, link.Confidence)
	}
	if link.Evidence != "commit_message_squash_pr_ref" {
		t.Fatalf("unexpected evidence: %s", link.Evidence)
	}
}

func TestDeriveTenantIsolationSameRepoIDDifferentOrg(t *testing.T) {
	// repo_id can collide across tenants (documented in both the Python
	// docstring and the Go package doc) -- a commit in org B must never link
	// to org A's PR #42, even on an identical repo UUID.
	sharedRepoID := uuid.New()
	inputs := prcommit.Inputs{
		PullRequests: []prcommit.PullRequestRow{{OrgID: "org-a", RepoID: sharedRepoID, Number: 42}},
		Commits: []prcommit.CommitRow{
			{OrgID: "org-b", RepoID: sharedRepoID, Hash: "cross-tenant", Message: "Merge pull request #42 from team/x"},
		},
	}

	result := prcommit.Derive(inputs)
	if len(result.Links) != 0 {
		t.Fatalf("cross-tenant repo_id collision must not link, got %+v", result.Links)
	}
}

func TestDeriveExplicitTierWinsDedupOverSquashTierForSameCommit(t *testing.T) {
	// A message satisfying both shapes for the SAME pr_number on the SAME
	// commit: the explicit tier runs first and its higher-confidence link
	// wins the (org, repo, pr_number, commit_hash) dedup.
	repo := uuid.New()
	inputs := prcommit.Inputs{
		OrgID:        "org-a",
		PullRequests: []prcommit.PullRequestRow{{OrgID: "org-a", RepoID: repo, Number: 7}},
		Commits: []prcommit.CommitRow{
			{OrgID: "org-a", RepoID: repo, Hash: "dual-shape", Message: "Merge pull request #7 from team/x (#7)"},
		},
	}

	result := prcommit.Derive(inputs)
	if len(result.Links) != 1 {
		t.Fatalf("got %d links, want 1 (deduped): %+v", len(result.Links), result.Links)
	}
	if result.Links[0].Provenance != edges.ProvenanceExplicitText {
		t.Fatalf("explicit tier must win the dedup, got provenance=%s", result.Links[0].Provenance)
	}
}

func TestDeriveSkipsCommitWithEmptyHash(t *testing.T) {
	repo := uuid.New()
	inputs := prcommit.Inputs{
		OrgID:        "org-a",
		PullRequests: []prcommit.PullRequestRow{{OrgID: "org-a", RepoID: repo, Number: 42}},
		Commits: []prcommit.CommitRow{
			{OrgID: "org-a", RepoID: repo, Hash: "", Message: "Merge pull request #42 from team/x"},
		},
	}

	result := prcommit.Derive(inputs)
	if len(result.Links) != 0 {
		t.Fatalf("commit with empty hash must not link, got %+v", result.Links)
	}
}

func TestDeriveRejectsRevertOfMergeCommit(t *testing.T) {
	// Delegated to textrefs.ExtractPRRefs (already corpus-tested), but the
	// wiring itself is worth one end-to-end check: a revert quoting a merge
	// subject must not produce a link, matching CHAOS-2375 round-3.
	repo := uuid.New()
	inputs := prcommit.Inputs{
		OrgID:        "org-a",
		PullRequests: []prcommit.PullRequestRow{{OrgID: "org-a", RepoID: repo, Number: 42}},
		Commits: []prcommit.CommitRow{
			{
				OrgID: "org-a", RepoID: repo, Hash: "revert-1",
				Message: "Revert \"Merge pull request #42 from team/x\"\n\nThis reverts commit abc.",
			},
		},
	}

	result := prcommit.Derive(inputs)
	if len(result.Links) != 0 {
		t.Fatalf("revert of a merge commit must not link, got %+v", result.Links)
	}
}

func TestDeriveIgnoresCommitInRepoWithNoKnownPRs(t *testing.T) {
	repo := uuid.New()
	inputs := prcommit.Inputs{
		OrgID: "org-a",
		Commits: []prcommit.CommitRow{
			{OrgID: "org-a", RepoID: repo, Hash: "abc123", Message: "Merge pull request #42 from team/x"},
		},
	}

	result := prcommit.Derive(inputs)
	if len(result.Links) != 0 {
		t.Fatalf("repo with zero known PRs must not link, got %+v", result.Links)
	}
	if result.CommitsScanned != 1 {
		t.Fatalf("CommitsScanned should count every read commit regardless of outcome, got %d", result.CommitsScanned)
	}
}

func TestDeriveLastSyncedIsLeftZeroForCallerToStamp(t *testing.T) {
	repo := uuid.New()
	inputs := prcommit.Inputs{
		OrgID:        "org-a",
		PullRequests: []prcommit.PullRequestRow{{OrgID: "org-a", RepoID: repo, Number: 42}},
		Commits: []prcommit.CommitRow{
			{OrgID: "org-a", RepoID: repo, Hash: "abc123", Message: "Merge pull request #42 from team/x"},
		},
	}

	result := prcommit.Derive(inputs)
	if len(result.Links) != 1 {
		t.Fatalf("got %d links, want 1", len(result.Links))
	}
	if !result.Links[0].LastSynced.IsZero() {
		t.Fatalf("Derive must not stamp LastSynced itself, got %v", result.Links[0].LastSynced)
	}
}
