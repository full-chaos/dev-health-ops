package providersync

import (
	"sort"
	"testing"
)

func sortedDerivedRows(rows []DerivedTeamRepoOwnershipRow) []DerivedTeamRepoOwnershipRow {
	out := append([]DerivedTeamRepoOwnershipRow(nil), rows...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].RepoID != out[j].RepoID {
			return out[i].RepoID < out[j].RepoID
		}
		return out[i].TeamID < out[j].TeamID
	})
	return out
}

// TestDeriveTeamRepoOwnershipRedBeforeThisProducerExisted is the standing
// obligation's red-first proof: a fixture-shaped org with real
// team_project_ownership rows and work items linked to PRs produces ZERO
// team_repo_ownership rows today (prod: team_project_ownership=2789 rows,
// team_repo_ownership=0 -- confirmed by the Go Worker session's read on
// org c6a38355). Before this file existed, nothing in this repo derived
// team_repo_ownership from already-synced data; the only writer was the
// retired Python auto-import (chris 2026-08-28: "there is no autoimport
// anymore"). This test proves the NEW code path is non-vacuous: given
// realistic inputs, it produces real output, where the ABSENCE of this
// function (its git-history non-existence) produced none.
func TestDeriveTeamRepoOwnershipRedBeforeThisProducerExisted(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{ProjectID: "proj-1", TeamID: "team-platform", IsPrimary: true},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		{WorkItemID: "gh:acme/repo-a#42", RepoID: "repo-a", ProjectID: "proj-1"},
	}

	got := deriveTeamRepoOwnership(projectLinks, workItems, nil, nil)

	if len(got) != 1 {
		t.Fatalf("expected 1 derived row, got %d: %+v", len(got), got)
	}
	if got[0].TeamID != "team-platform" || got[0].RepoID != "repo-a" {
		t.Fatalf("unexpected row: %+v", got[0])
	}
}

// TestOwnProjectIDPath is team-lead's "design check (a)": a work item with
// BOTH repo_id and project_id set directly resolves without needing the
// dependency-donor walk.
func TestOwnProjectIDPath(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{ProjectID: "proj-1", TeamID: "team-platform", IsPrimary: true},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		{WorkItemID: "gh:acme/repo-a#1", RepoID: "repo-a", ProjectID: "proj-1"},
	}

	got := deriveTeamRepoOwnership(projectLinks, workItems, nil, nil)

	if len(got) != 1 || got[0].TeamID != "team-platform" || got[0].RepoID != "repo-a" {
		t.Fatalf("expected repo-a -> team-platform via own project_id, got %+v", got)
	}
}

// TestDependencyDonorWalkPath: a repo-bearing item with NO project_id of
// its own (the common GitHub-PR shape) inherits its team from a linked
// donor item that DOES carry a project_id, via a work_item_dependencies
// edge -- same directionality as the existing linked-issue resolver.
func TestDependencyDonorWalkPath(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{ProjectID: "proj-1", TeamID: "team-platform", IsPrimary: true},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		// The PR itself: repo_id set, no project_id (GitHub PRs have no
		// native project membership).
		{WorkItemID: "ghpr:acme/repo-a#7", RepoID: "repo-a", ProjectID: ""},
		// The donor: a Linear issue with the project_id, no repo_id.
		{WorkItemID: "linear:PLAT-9", RepoID: "", ProjectID: "proj-1"},
	}
	edges := []TeamRepoOwnershipDependencyEdge{
		{SourceWorkItemID: "ghpr:acme/repo-a#7", TargetWorkItemID: "linear:PLAT-9"},
	}

	got := deriveTeamRepoOwnership(projectLinks, workItems, edges, nil)

	if len(got) != 1 || got[0].TeamID != "team-platform" || got[0].RepoID != "repo-a" {
		t.Fatalf("expected repo-a -> team-platform via dependency-donor walk, got %+v", got)
	}
}

// TestPRInheritanceViaIssuePRLink is team-lead's "design check (b)": a PR
// that never became its own work_items row (or has no useful repo_id of
// its own) still gets a team, via work_graph_issue_pr linking it to a
// work item whose team IS resolvable. Uses work_graph_issue_pr's OWN
// repo_id, not the linked work item's repo_id, since those can genuinely
// differ (a cross-repo link).
func TestPRInheritanceViaIssuePRLink(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{ProjectID: "proj-1", TeamID: "team-platform", IsPrimary: true},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		// The linked work item: no repo_id of its own (a pure tracker
		// issue), but it DOES carry the project_id.
		{WorkItemID: "linear:PLAT-9", RepoID: "", ProjectID: "proj-1"},
	}
	issuePRLinks := []TeamRepoOwnershipIssuePRLink{
		// The PR lives in a DIFFERENT repo than anything else mentioned so
		// far -- proves this repo_id comes from the link table, not from
		// copying some other row's repo_id.
		{WorkItemID: "linear:PLAT-9", RepoID: "repo-b", PRNumber: 42},
	}

	got := deriveTeamRepoOwnership(projectLinks, workItems, nil, issuePRLinks)

	if len(got) != 1 || got[0].TeamID != "team-platform" || got[0].RepoID != "repo-b" {
		t.Fatalf("expected repo-b -> team-platform via work_graph_issue_pr, got %+v", got)
	}
}

// TestConflictingTeamsForTheSameRepoAreNeverGuessed: CHAOS-4321's "never
// guess" precedent -- if the SAME repo is reachable from two different
// teams' owned projects, neither wins; the repo is left unresolved.
func TestConflictingTeamsForTheSameRepoAreNeverGuessed(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{ProjectID: "proj-1", TeamID: "team-platform", IsPrimary: true},
		{ProjectID: "proj-2", TeamID: "team-growth", IsPrimary: true},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		{WorkItemID: "gh:acme/repo-a#1", RepoID: "repo-a", ProjectID: "proj-1"},
		{WorkItemID: "gh:acme/repo-a#2", RepoID: "repo-a", ProjectID: "proj-2"},
	}

	got := deriveTeamRepoOwnership(projectLinks, workItems, nil, nil)

	if len(got) != 0 {
		t.Fatalf("expected no rows for a repo claimed by two teams, got %+v", got)
	}
}

// TestNonPrimaryProjectOwnershipIsNeverGuessed: a project with zero (or
// more than one) PRIMARY team claim is never resolved, even if a
// non-primary candidate exists -- matches this schema's is_primary
// ownership-precedence convention rather than picking an arbitrary
// candidate.
func TestNonPrimaryProjectOwnershipIsNeverGuessed(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{ProjectID: "proj-1", TeamID: "team-platform", IsPrimary: false},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		{WorkItemID: "gh:acme/repo-a#1", RepoID: "repo-a", ProjectID: "proj-1"},
	}

	got := deriveTeamRepoOwnership(projectLinks, workItems, nil, nil)

	if len(got) != 0 {
		t.Fatalf("expected no rows for a non-primary-only project claim, got %+v", got)
	}
}

// TestRepoWithNoOwnershipSignalContributesNoRow: a repo-bearing item whose
// project (own or donor) has no team_project_ownership entry at all
// contributes nothing -- never guessed.
func TestRepoWithNoOwnershipSignalContributesNoRow(t *testing.T) {
	workItems := []TeamRepoOwnershipWorkItem{
		{WorkItemID: "gh:acme/repo-a#1", RepoID: "repo-a", ProjectID: "proj-unowned"},
	}

	got := deriveTeamRepoOwnership(nil, workItems, nil, nil)

	if len(got) != 0 {
		t.Fatalf("expected no rows, got %+v", got)
	}
}

// TestMultipleReposForTheSameTeamAllResolve: a team owning several repos
// (via several work items in the same project) gets a row per repo, not
// collapsed to one -- matches every other ownership table's contract in
// this schema (one row per (team, repo), never one row per team).
func TestMultipleReposForTheSameTeamAllResolve(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{ProjectID: "proj-1", TeamID: "team-platform", IsPrimary: true},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		{WorkItemID: "gh:acme/repo-a#1", RepoID: "repo-a", ProjectID: "proj-1"},
		{WorkItemID: "gh:acme/repo-b#1", RepoID: "repo-b", ProjectID: "proj-1"},
	}

	got := sortedDerivedRows(deriveTeamRepoOwnership(projectLinks, workItems, nil, nil))

	if len(got) != 2 {
		t.Fatalf("expected 2 rows (repo-a, repo-b), got %+v", got)
	}
	if got[0].RepoID != "repo-a" || got[1].RepoID != "repo-b" {
		t.Fatalf("unexpected repo set: %+v", got)
	}
	for _, row := range got {
		if row.TeamID != "team-platform" {
			t.Fatalf("expected team-platform for both repos, got %+v", row)
		}
	}
}

// TestDerivedRowsUseTheInferredSpecificityConstant: every row this
// producer emits carries the same, deliberately-low specificity so a
// future direct signal (PR B, not built here) can always outrank it.
func TestDerivedRowsUseTheInferredSpecificityConstant(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{ProjectID: "proj-1", TeamID: "team-platform", IsPrimary: true},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		{WorkItemID: "gh:acme/repo-a#1", RepoID: "repo-a", ProjectID: "proj-1"},
	}

	got := deriveTeamRepoOwnership(projectLinks, workItems, nil, nil)

	if len(got) != 1 || got[0].Specificity != teamRepoOwnershipInferredSpecificity {
		t.Fatalf("expected specificity=%d, got %+v", teamRepoOwnershipInferredSpecificity, got)
	}
}
