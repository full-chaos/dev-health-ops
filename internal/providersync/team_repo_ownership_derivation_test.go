package providersync

import (
	"sort"
	"testing"
	"time"
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
		{SourceWorkItemID: "ghpr:acme/repo-a#7", TargetWorkItemID: "linear:PLAT-9", RelationshipType: "relates_to"},
	}

	got := deriveTeamRepoOwnership(projectLinks, workItems, edges, nil)

	if len(got) != 1 || got[0].TeamID != "team-platform" || got[0].RepoID != "repo-a" {
		t.Fatalf("expected repo-a -> team-platform via dependency-donor walk, got %+v", got)
	}
}

// TestBlockingRelationshipTypeNeverInherits: CHAOS-4321's "never guess" applied
// to the donor walk -- a blocks/blocked_by edge routinely spans teams, so it
// must NOT transfer a team, exactly like compute_work_items.py's
// _INHERITABLE_RELATIONSHIP_TYPES gate.
func TestBlockingRelationshipTypeNeverInherits(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{ProjectID: "proj-1", TeamID: "team-platform", IsPrimary: true},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		{WorkItemID: "ghpr:acme/repo-a#7", RepoID: "repo-a", ProjectID: ""},
		{WorkItemID: "linear:PLAT-9", RepoID: "", ProjectID: "proj-1"},
	}
	edges := []TeamRepoOwnershipDependencyEdge{
		{SourceWorkItemID: "ghpr:acme/repo-a#7", TargetWorkItemID: "linear:PLAT-9", RelationshipType: "blocked_by"},
	}

	got := deriveTeamRepoOwnership(projectLinks, workItems, edges, nil)

	if len(got) != 0 {
		t.Fatalf("expected no inheritance through a blocking relationship, got %+v", got)
	}
}

// TestLatestEdgeByLastSyncedWinsPerPair: a relationship-type flip on the same
// (source, target) pair supersedes the stale row -- the newer edge (here,
// blocked_by superseding an earlier relates_to) decides whether inheritance
// happens, not whichever row happens to be scanned first.
func TestLatestEdgeByLastSyncedWinsPerPair(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{ProjectID: "proj-1", TeamID: "team-platform", IsPrimary: true},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		{WorkItemID: "ghpr:acme/repo-a#7", RepoID: "repo-a", ProjectID: ""},
		{WorkItemID: "linear:PLAT-9", RepoID: "", ProjectID: "proj-1"},
	}
	older := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	edges := []TeamRepoOwnershipDependencyEdge{
		{SourceWorkItemID: "ghpr:acme/repo-a#7", TargetWorkItemID: "linear:PLAT-9", RelationshipType: "relates_to", LastSynced: older},
		{SourceWorkItemID: "ghpr:acme/repo-a#7", TargetWorkItemID: "linear:PLAT-9", RelationshipType: "blocked_by", LastSynced: newer},
	}

	got := deriveTeamRepoOwnership(projectLinks, workItems, edges, nil)

	if len(got) != 0 {
		t.Fatalf("expected the newer blocked_by edge to supersede the older relates_to edge, got %+v", got)
	}
}

// TestExtkeyDependencyTargetResolvesCrossProvider: a PR's dependency edge can
// target the provider-neutral `extkey:KEY` form emitted by PR parsers; it
// must resolve against the Linear/Jira work-item-key index, same as
// compute_work_items.py's donor resolution.
func TestExtkeyDependencyTargetResolvesCrossProvider(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{ProjectID: "proj-1", TeamID: "team-platform", IsPrimary: true},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		{WorkItemID: "ghpr:acme/repo-a#7", RepoID: "repo-a", ProjectID: ""},
		{WorkItemID: "linear:PLAT-9", Provider: "linear", RepoID: "", ProjectID: "proj-1"},
	}
	edges := []TeamRepoOwnershipDependencyEdge{
		{SourceWorkItemID: "ghpr:acme/repo-a#7", TargetWorkItemID: "extkey:PLAT-9", RelationshipType: "relates_to"},
	}

	got := deriveTeamRepoOwnership(projectLinks, workItems, edges, nil)

	if len(got) != 1 || got[0].TeamID != "team-platform" || got[0].RepoID != "repo-a" {
		t.Fatalf("expected repo-a -> team-platform via extkey resolution, got %+v", got)
	}
}

// TestAmbiguousExtkeyDependencyTargetIsNeverGuessed: the same issue key
// claimed by two work items (e.g. a Linear and a Jira item both use "PLAT-9")
// is genuinely ambiguous -- CHAOS-4321 drops it rather than picking one.
func TestAmbiguousExtkeyDependencyTargetIsNeverGuessed(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{ProjectID: "proj-1", TeamID: "team-platform", IsPrimary: true},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		{WorkItemID: "ghpr:acme/repo-a#7", RepoID: "repo-a", ProjectID: ""},
		{WorkItemID: "linear:PLAT-9", Provider: "linear", RepoID: "", ProjectID: "proj-1"},
		{WorkItemID: "jira:PLAT-9", Provider: "jira", RepoID: "", ProjectID: "proj-1"},
	}
	edges := []TeamRepoOwnershipDependencyEdge{
		{SourceWorkItemID: "ghpr:acme/repo-a#7", TargetWorkItemID: "extkey:PLAT-9", RelationshipType: "relates_to"},
	}

	got := deriveTeamRepoOwnership(projectLinks, workItems, edges, nil)

	if len(got) != 0 {
		t.Fatalf("expected no inheritance through an ambiguous cross-provider key, got %+v", got)
	}
}

// TestMultipleDonorCandidatesPickLexicographicallySmallestTarget: when a
// source has more than one valid donor, the choice must be deterministic and
// run-independent (ClickHouse rows are unordered) -- the smallest canonical
// target wins, same as compute_work_items.py.
func TestMultipleDonorCandidatesPickLexicographicallySmallestTarget(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{ProjectID: "proj-1", TeamID: "team-platform", IsPrimary: true},
		{ProjectID: "proj-2", TeamID: "team-growth", IsPrimary: true},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		{WorkItemID: "ghpr:acme/repo-a#7", RepoID: "repo-a", ProjectID: ""},
		{WorkItemID: "linear:PLAT-9", RepoID: "", ProjectID: "proj-1"},
		{WorkItemID: "linear:ZETA-1", RepoID: "", ProjectID: "proj-2"},
	}
	edges := []TeamRepoOwnershipDependencyEdge{
		{SourceWorkItemID: "ghpr:acme/repo-a#7", TargetWorkItemID: "linear:ZETA-1", RelationshipType: "relates_to"},
		{SourceWorkItemID: "ghpr:acme/repo-a#7", TargetWorkItemID: "linear:PLAT-9", RelationshipType: "relates_to"},
	}

	got := deriveTeamRepoOwnership(projectLinks, workItems, edges, nil)

	// "linear:PLAT-9" < "linear:ZETA-1" lexicographically -> team-platform wins.
	if len(got) != 1 || got[0].TeamID != "team-platform" || got[0].RepoID != "repo-a" {
		t.Fatalf("expected the lexicographically smallest donor target to win, got %+v", got)
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

// TestNonPrimaryProjectOwnershipStillResolvesGitLabShaped is the codex
// adversarial-review fix (2026-08-28, confirmed high-severity): GitLab's
// provider_access writer (team_autoimport_gitlab.py's
// _project_ownership_rows) sets is_primary=0 UNCONDITIONALLY on every row it
// writes. A single non-primary candidate, with no competing claim, must
// still resolve -- IsPrimary is a tie-break among candidates, never a hard
// requirement, or this producer silently derives nothing for every
// GitLab-sourced org.
func TestNonPrimaryProjectOwnershipStillResolvesGitLabShaped(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{ProjectID: "proj-1", TeamID: "team-platform", IsPrimary: false, Specificity: 100},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		{WorkItemID: "gl:acme/repo-a#1", RepoID: "repo-a", ProjectID: "proj-1"},
	}

	got := deriveTeamRepoOwnership(projectLinks, workItems, nil, nil)

	if len(got) != 1 || got[0].TeamID != "team-platform" || got[0].RepoID != "repo-a" {
		t.Fatalf("expected repo-a -> team-platform via a lone non-primary candidate, got %+v", got)
	}
}

// TestPrimaryClaimBeatsHigherSpecificityNonPrimaryClaim: IsPrimary is the
// FIRST-ranked field, Specificity only breaks a tie within it -- a
// non-primary candidate never outranks a primary one no matter how much
// higher its specificity is.
func TestPrimaryClaimBeatsHigherSpecificityNonPrimaryClaim(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{ProjectID: "proj-1", TeamID: "team-platform", IsPrimary: true, Specificity: 10},
		{ProjectID: "proj-1", TeamID: "team-growth", IsPrimary: false, Specificity: 65535},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		{WorkItemID: "gh:acme/repo-a#1", RepoID: "repo-a", ProjectID: "proj-1"},
	}

	got := deriveTeamRepoOwnership(projectLinks, workItems, nil, nil)

	if len(got) != 1 || got[0].TeamID != "team-platform" {
		t.Fatalf("expected the primary claim to win regardless of specificity, got %+v", got)
	}
}

// TestTiedTopRankBetweenDifferentTeamsIsNeverGuessed: two DIFFERENT teams at
// the SAME (is_primary, specificity) rank for the same project is a genuine
// ambiguity -- CHAOS-4321's "never guess" precedent, unchanged by the
// ranking fix above.
func TestTiedTopRankBetweenDifferentTeamsIsNeverGuessed(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{ProjectID: "proj-1", TeamID: "team-platform", IsPrimary: false, Specificity: 50},
		{ProjectID: "proj-1", TeamID: "team-growth", IsPrimary: false, Specificity: 50},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		{WorkItemID: "gl:acme/repo-a#1", RepoID: "repo-a", ProjectID: "proj-1"},
	}

	got := deriveTeamRepoOwnership(projectLinks, workItems, nil, nil)

	if len(got) != 0 {
		t.Fatalf("expected no rows for a genuine tie between two teams, got %+v", got)
	}
}

// TestDuplicateGenerationOfTheSameTeamsClaimIsNotAFalseTie: a re-imported
// claim from the SAME team (e.g. a repeated sync run re-asserting an
// unchanged ownership grant) must never manufacture a false tie against
// itself, even at a lower score than its own later generation.
func TestDuplicateGenerationOfTheSameTeamsClaimIsNotAFalseTie(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{ProjectID: "proj-1", TeamID: "team-platform", IsPrimary: false, Specificity: 40},
		{ProjectID: "proj-1", TeamID: "team-platform", IsPrimary: false, Specificity: 50},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		{WorkItemID: "gl:acme/repo-a#1", RepoID: "repo-a", ProjectID: "proj-1"},
	}

	got := deriveTeamRepoOwnership(projectLinks, workItems, nil, nil)

	if len(got) != 1 || got[0].TeamID != "team-platform" {
		t.Fatalf("expected repeated claims from the SAME team to resolve, not tie against themselves, got %+v", got)
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
