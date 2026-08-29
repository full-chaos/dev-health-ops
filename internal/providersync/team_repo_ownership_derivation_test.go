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

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, nil, nil)

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

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, nil, nil)

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

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, edges, nil)

	if len(got) != 1 || got[0].TeamID != "team-platform" || got[0].RepoID != "repo-a" {
		t.Fatalf("expected repo-a -> team-platform via dependency-donor walk, got %+v", got)
	}
}

// TestNonResolvingOwnProjectIDFallsBackToDonorWalk: the real GitHub shape
// (codex adversarial review, 2026-08-28, confirmed high-severity finding).
// github_work_items_rows.go unconditionally sets a GitHub PR/issue's own
// ProjectID to the repo's full name -- a string that never appears in
// team_project_ownership, since GitHub never writes that table. Before this
// fix, buildDonorProjectIDResolver returned this non-resolving "own"
// ProjectID unconditionally, permanently shadowing a valid dependency-donor
// edge to a Linear issue that DOES resolve -- silently defeating the
// donor-walk fallback (design check a2) for the primary real-world use
// case chris's ruling described. The own ProjectID here ("acme/repo-a", the
// GitHub shape) must be ignored in favor of the donor's "proj-1".
func TestNonResolvingOwnProjectIDFallsBackToDonorWalk(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{ProjectID: "proj-1", TeamID: "team-platform", IsPrimary: true},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		// The PR: repo_id set, own ProjectID set to the GitHub repo-full-name
		// shape -- present, non-empty, but never resolves to any team.
		{WorkItemID: "ghpr:acme/repo-a#7", RepoID: "repo-a", ProjectID: "acme/repo-a"},
		// The donor: a Linear issue with the REAL project_id.
		{WorkItemID: "linear:PLAT-9", RepoID: "", ProjectID: "proj-1"},
	}
	edges := []TeamRepoOwnershipDependencyEdge{
		{SourceWorkItemID: "ghpr:acme/repo-a#7", TargetWorkItemID: "linear:PLAT-9", RelationshipType: "relates_to"},
	}

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, edges, nil)

	if len(got) != 1 || got[0].TeamID != "team-platform" || got[0].RepoID != "repo-a" {
		t.Fatalf("expected repo-a -> team-platform via donor walk despite non-resolving own project_id, got %+v", got)
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

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, edges, nil)

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

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, edges, nil)

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
		{Provider: "linear", ProjectID: "proj-1", TeamID: "team-platform", IsPrimary: true},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		{WorkItemID: "ghpr:acme/repo-a#7", RepoID: "repo-a", ProjectID: ""},
		{WorkItemID: "linear:PLAT-9", Provider: "linear", RepoID: "", ProjectID: "proj-1"},
	}
	edges := []TeamRepoOwnershipDependencyEdge{
		{SourceWorkItemID: "ghpr:acme/repo-a#7", TargetWorkItemID: "extkey:PLAT-9", RelationshipType: "relates_to"},
	}

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, edges, nil)

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

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, edges, nil)

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

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, edges, nil)

	// "linear:PLAT-9" < "linear:ZETA-1" lexicographically -> team-platform wins.
	if len(got) != 1 || got[0].TeamID != "team-platform" || got[0].RepoID != "repo-a" {
		t.Fatalf("expected the lexicographically smallest donor target to win, got %+v", got)
	}
}

// TestUnownedDonorNeverSuppressesAnOwnedDonor is the codex adversarial
// review fix (2026-08-28, confirmed finding): the lexicographic tie-break
// only applies AMONG donors that themselves already resolve to a team,
// mirroring compute_work_items.py::build_linked_issue_team_resolver exactly
// (its `donor_team` map -- and therefore `candidates` -- is populated ONLY
// for items with a resolved team_id). "linear:AAA-1" sorts before
// "linear:ZETA-1" but carries no ownership at all; before this fix, the
// donor walk would pick it anyway (lexicographically smallest), find it
// resolves to no team, and silently drop the row -- even though the OTHER
// donor DOES resolve.
func TestUnownedDonorNeverSuppressesAnOwnedDonor(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		// proj-unowned has NO entry here at all -- "linear:AAA-1" (below)
		// carries a project_id that never resolves to any team.
		{ProjectID: "proj-owned", TeamID: "team-platform", IsPrimary: true},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		{WorkItemID: "ghpr:acme/repo-a#7", RepoID: "repo-a", ProjectID: ""},
		{WorkItemID: "linear:AAA-1", RepoID: "", ProjectID: "proj-unowned"},
		{WorkItemID: "linear:ZETA-1", RepoID: "", ProjectID: "proj-owned"},
	}
	edges := []TeamRepoOwnershipDependencyEdge{
		{SourceWorkItemID: "ghpr:acme/repo-a#7", TargetWorkItemID: "linear:AAA-1", RelationshipType: "relates_to"},
		{SourceWorkItemID: "ghpr:acme/repo-a#7", TargetWorkItemID: "linear:ZETA-1", RelationshipType: "relates_to"},
	}

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, edges, nil)

	if len(got) != 1 || got[0].TeamID != "team-platform" || got[0].RepoID != "repo-a" {
		t.Fatalf("expected repo-a -> team-platform via the owned donor, unowned donor must never suppress it, got %+v", got)
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

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, nil, issuePRLinks)

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

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, nil, nil)

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

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, nil, nil)

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

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, nil, nil)

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

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, nil, nil)

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

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, nil, nil)

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

	got := deriveTeamRepoOwnership("org-1", nil, workItems, nil, nil)

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

	got := sortedDerivedRows(deriveTeamRepoOwnership("org-1", projectLinks, workItems, nil, nil))

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

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, nil, nil)

	if len(got) != 1 || got[0].Specificity != teamRepoOwnershipInferredSpecificity {
		t.Fatalf("expected specificity=%d, got %+v", teamRepoOwnershipInferredSpecificity, got)
	}
}

// CHAOS-4458 part (b): Linear's team_project_ownership writer
// (team_autoimport_linear.py) stamps project_id = "{org_id}:linear:{team_key}"
// (falling back to the team's own key when the team has no explicit Linear
// Project associations -- team_autoimport_linear.py:454-456,472,487), while
// the Linear work-item normalizer stamps a Linear item's OWN project_id with
// the raw Linear Project UUID -- a DELIBERATELY separate id space the same
// file's docstring calls out explicitly (team_autoimport_linear.py:309-314:
// "a SEPARATE id space and are unaffected"). Before this fix, the derivation
// joined ONLY on project_id, so these two values never intersected for a
// Linear-only org: confirmed locally (org 70d529e0, real data) at 0 of 3168
// project-id-bearing Linear work items matching their org's ownership row.
// These three tests are the red-first proof: on the parent commit (before
// NativeTeamKey/resolveWorkItemProjectRef existed), the fixtures below
// derive ZERO rows; after this fix, they derive the expected row via the
// linear_team_key resolution arm.

// TestLinearTeamKeyOwnResolutionMatchesTeamKeyShapedOwnership: the Linear
// issue itself carries a repo_id (a repo-bearing Linear item is unusual but
// not impossible) and resolves via native_team_key when its own project_id
// (the raw Linear Project UUID) does not match anything in
// team_project_ownership.
//
// CHAOS-4537 rewrite: no team_project_ownership row for the team-key-shaped
// identity is present here at all -- the point of CHAOS-4537 is that this
// arm no longer needs one. A DECOY row for that identity, pointing at a
// DIFFERENT (wrong) team, proves the arm genuinely ignores
// team_project_ownership for this resolution rather than merely tolerating
// its absence: the collector still writes that row today
// (linear_reference_catalog_route.go, out of this ticket's scope; removing
// the write is a deliberate fast-follow), so a real org's ClickHouse state
// still has it, and this test must pass with it present and wrong. The
// resolved team_id is now the raw NativeTeamKey value itself ("CHAOS"), not
// an arbitrary "team-chaos" label -- see resolveWorkItemTeamID's doc
// comment for why that is the byte-identical value the old indirection
// produced.
func TestLinearTeamKeyOwnResolutionMatchesTeamKeyShapedOwnership(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{Provider: "linear", ProjectID: "org-1:linear:CHAOS", TeamID: "team-WRONG-decoy", IsPrimary: true},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		{
			WorkItemID:    "linear:CHAOS-1",
			Provider:      "linear",
			RepoID:        "repo-a",
			ProjectID:     "11111111-1111-4111-8111-111111111111", // raw Linear Project UUID: disjoint id space
			NativeTeamKey: "CHAOS",
		},
	}

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, nil, nil)

	if len(got) != 1 || got[0].TeamID != "CHAOS" || got[0].RepoID != "repo-a" {
		t.Fatalf("expected repo-a -> CHAOS via native_team_key own resolution, got %+v", got)
	}
	if got[0].ResolutionArm != TeamRepoOwnershipResolutionArmLinearTeamKey {
		t.Fatalf("expected ResolutionArm=%q, got %q", TeamRepoOwnershipResolutionArmLinearTeamKey, got[0].ResolutionArm)
	}
}

// TestLinearTeamKeyDonorWalkMatchesTeamKeyShapedOwnership is the realistic
// shape from the trace lane: a bare GitHub PR (no project_id of its own)
// with a relates_to edge to a Linear issue. The donor's OWN project_id (the
// raw Linear Project UUID) never resolves, so before CHAOS-4458b the donor
// was never a valid candidate at all -- the PR derived nothing. The donor
// resolves via its native_team_key and the PR inherits "CHAOS" directly.
//
// CHAOS-4537 rewrite: same decoy-row pattern as
// TestLinearTeamKeyOwnResolutionMatchesTeamKeyShapedOwnership above -- the
// team_project_ownership row for this identity, still written today, points
// at a WRONG team, proving the donor walk's native_team_key resolution
// never consults it.
func TestLinearTeamKeyDonorWalkMatchesTeamKeyShapedOwnership(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{Provider: "linear", ProjectID: "org-1:linear:CHAOS", TeamID: "team-WRONG-decoy", IsPrimary: true},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		{WorkItemID: "ghpr:acme/repo-a#7", Provider: "github", RepoID: "repo-a", ProjectID: ""},
		{
			WorkItemID:    "linear:CHAOS-1",
			Provider:      "linear",
			ProjectID:     "11111111-1111-4111-8111-111111111111",
			NativeTeamKey: "CHAOS",
		},
	}
	edges := []TeamRepoOwnershipDependencyEdge{
		{SourceWorkItemID: "ghpr:acme/repo-a#7", TargetWorkItemID: "linear:CHAOS-1", RelationshipType: "relates_to"},
	}

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, edges, nil)

	if len(got) != 1 || got[0].TeamID != "CHAOS" || got[0].RepoID != "repo-a" {
		t.Fatalf("expected repo-a -> CHAOS via the donor walk's native_team_key resolution, got %+v", got)
	}
	if got[0].ResolutionArm != TeamRepoOwnershipResolutionArmLinearTeamKey {
		t.Fatalf("expected ResolutionArm=%q, got %q", TeamRepoOwnershipResolutionArmLinearTeamKey, got[0].ResolutionArm)
	}
}

// TestDirectProjectIDArmPreferredOverLinearTeamKeyArm: FIX SHAPE case (2) --
// if/when Linear's ownership writer ever emits a project-UUID-keyed row
// (matching a work item's own project_id directly), that direct match must
// win over the native_team_key fallback, exactly like every other provider.
// Never guesses between the two: the moment the direct arm resolves, the
// team-key arm is not consulted.
//
// CHAOS-4537: the linear_team_key arm no longer looks anything up in
// team_project_ownership at all (see resolveWorkItemTeamID) -- were the gate
// below to break, it would resolve directly to the raw NativeTeamKey value
// ("CHAOS"), not to this second row's team_id. The decoy row stays (proving
// team_project_ownership entries for this identity, still written today,
// remain harmless either way), but the thing actually under test is that
// resolveWorkItemTeamID's own-project_id branch never falls through to the
// native_team_key branch once the direct match resolves.
func TestDirectProjectIDArmPreferredOverLinearTeamKeyArm(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		// A hypothetical future project-UUID-keyed ownership row.
		{Provider: "linear", ProjectID: "22222222-2222-4222-8222-222222222222", TeamID: "team-direct", IsPrimary: true},
		// The team-key-shaped row this collector still writes today (out of
		// CHAOS-4537's scope) -- must never be consulted once the direct arm
		// already resolved.
		{Provider: "linear", ProjectID: "org-1:linear:CHAOS", TeamID: "team-team-key-decoy", IsPrimary: true},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		{
			WorkItemID:    "linear:CHAOS-1",
			Provider:      "linear",
			RepoID:        "repo-a",
			ProjectID:     "22222222-2222-4222-8222-222222222222",
			NativeTeamKey: "CHAOS",
		},
	}

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, nil, nil)

	if len(got) != 1 || got[0].TeamID != "team-direct" {
		t.Fatalf("expected repo-a -> team-direct via the direct project_id arm, got %+v", got)
	}
	if got[0].ResolutionArm != TeamRepoOwnershipResolutionArmProjectID {
		t.Fatalf("expected ResolutionArm=%q, got %q", TeamRepoOwnershipResolutionArmProjectID, got[0].ResolutionArm)
	}
}

// TestLinearTeamKeyArmNeverAppliesToNonLinearProviders: a non-Linear work
// item carrying a NativeTeamKey-shaped value in some other field would never
// happen in practice (only the Linear route ever sets NativeTeamKey), but
// the resolver's own provider=="linear" gate is the thing under test here,
// not the loader -- a GitHub/GitLab/Jira item's non-resolving own project_id
// must fall through to the (unchanged) donor walk or nothing, never resolve
// straight to NativeTeamKey (CHAOS-4537: this arm now returns that value
// directly, with no project_id/ref indirection at all -- irrelevant here
// since the gate never lets a non-Linear item reach it).
func TestLinearTeamKeyArmNeverAppliesToNonLinearProviders(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{Provider: "linear", ProjectID: "org-1:linear:CHAOS", TeamID: "team-chaos-unused", IsPrimary: true},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		{
			WorkItemID:    "gh:acme/repo-a#1",
			Provider:      "github",
			RepoID:        "repo-a",
			ProjectID:     "acme/repo-a", // never resolves (GitHub never writes team_project_ownership)
			NativeTeamKey: "CHAOS",       // never set by any real GitHub route; here only to prove the gate
		},
	}

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, nil, nil)

	if len(got) != 0 {
		t.Fatalf("expected 0 rows -- a non-linear provider must never resolve via the linear_team_key arm, got %+v", got)
	}
}

// TestResolutionArmIsDeterministicWhenBothArmsAgreeOnTheSameRepoAndTeam is
// the codex adversarial-review fix (2026-08-29, confirmed finding):
// loadTeamRepoOwnershipWorkItems has no ORDER BY, so ClickHouse may return
// work_items in a different order across otherwise-identical runs. Before
// this fix, the FIRST work item assign() happened to visit "won" the
// recorded ResolutionArm even when a second item resolving the SAME repo to
// the SAME team via a DIFFERENT arm was also present -- an unordered-scan
// artifact that could make the telemetry flicker between project_id and
// linear_team_key for byte-identical ClickHouse snapshots. The arm must be
// a deterministic function of the candidate SET, not of scan order: here
// two work items resolve "repo-a" to "CHAOS" via different arms, and the
// higher-priority project_id arm must always win regardless of which
// literal slice order they're passed in.
//
// CHAOS-4537 rewrite: item A's project_id arm now resolves to "CHAOS"
// directly (not an arbitrary "team-chaos" label), matching the literal value
// item B's linear_team_key arm produces from NativeTeamKey -- otherwise the
// two items would disagree on the team and this fixture would exercise the
// UNRELATED conflict path (assign()'s `existing != teamID`) instead of the
// tie-break this test targets. A decoy team_project_ownership row for the
// team-key-shaped identity, pointed at a WRONG team, proves item B's arm
// never consults it either.
func TestResolutionArmIsDeterministicWhenBothArmsAgreeOnTheSameRepoAndTeam(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{Provider: "linear", ProjectID: "proj-1", TeamID: "CHAOS", IsPrimary: true},
		{Provider: "linear", ProjectID: "org-1:linear:CHAOS", TeamID: "team-WRONG-decoy", IsPrimary: true},
	}
	// Item A resolves repo-a -> CHAOS via the direct project_id arm.
	itemA := TeamRepoOwnershipWorkItem{WorkItemID: "linear:CHAOS-1", Provider: "linear", RepoID: "repo-a", ProjectID: "proj-1"}
	// Item B is a SEPARATE PR (also repo-a) that only resolves via the
	// linear_team_key arm.
	itemB := TeamRepoOwnershipWorkItem{
		WorkItemID: "ghpr:acme/repo-a#2", Provider: "linear", RepoID: "repo-a",
		ProjectID: "22222222-2222-4222-8222-222222222222", NativeTeamKey: "CHAOS",
	}

	forward := deriveTeamRepoOwnership("org-1", projectLinks, []TeamRepoOwnershipWorkItem{itemA, itemB}, nil, nil)
	backward := deriveTeamRepoOwnership("org-1", projectLinks, []TeamRepoOwnershipWorkItem{itemB, itemA}, nil, nil)

	for _, got := range [][]DerivedTeamRepoOwnershipRow{forward, backward} {
		if len(got) != 1 || got[0].TeamID != "CHAOS" || got[0].RepoID != "repo-a" {
			t.Fatalf("expected exactly repo-a -> CHAOS, got %+v", got)
		}
		if got[0].ResolutionArm != TeamRepoOwnershipResolutionArmProjectID {
			t.Fatalf("expected the higher-priority project_id arm regardless of scan order, got %q", got[0].ResolutionArm)
		}
	}
}

// TestLinearTeamKeyOwnResolutionWithEmptyProjectID closes a fixture gap found
// during the compose live-proof (lane-4458b-live, 2026-08-29, org
// 70d529e0): every prior fixture for the own-resolution arm gives the Linear
// item a NON-EMPTY but non-matching project_id (a raw Linear Project UUID
// that just isn't in the catalog, e.g. TestLinearTeamKeyOwnResolutionMatchesTeamKeyShapedOwnership
// above). Real synced data on org 70d529e0 shows 264 Linear work items with
// `native_team_key` set and `project_id` = the EMPTY STRING (never a project
// association at all, not merely an unmatched one) -- ClickHouse's
// column-default shape for a Linear issue that was never assigned to a
// Project. resolveWorkItemProjectRef's `if item.ProjectID != ""` guard means
// this should be structurally equivalent to the non-matching-UUID case (both
// skip the direct-match branch and fall to the native_team_key fallback),
// but the live proof found ZERO linear_team_key arm hits despite this
// exact input shape being present and donor-linked -- so this pins the
// EXACT empty-string shape explicitly rather than relying on that equivalence
// as an unverified assumption.
//
// CHAOS-4537 rewrite: resolveWorkItemTeamID replaces resolveWorkItemProjectRef
// -- the guard (`if item.ProjectID != ""`) and its structural-equivalence
// point are unchanged, only the resolved value is (the raw NativeTeamKey,
// not a team_project_ownership lookup). Decoy row, same pattern as the other
// rewritten tests above.
func TestLinearTeamKeyOwnResolutionWithEmptyProjectID(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{Provider: "linear", ProjectID: "org-1:linear:CHAOS", TeamID: "team-WRONG-decoy", IsPrimary: true},
	}
	workItems := []TeamRepoOwnershipWorkItem{
		{
			WorkItemID:    "linear:CHAOS-3392",
			Provider:      "linear",
			RepoID:        "repo-a",
			ProjectID:     "", // exact live-data shape: never assigned to a Project, not just unmatched
			NativeTeamKey: "CHAOS",
		},
	}

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, nil, nil)

	if len(got) != 1 || got[0].TeamID != "CHAOS" || got[0].RepoID != "repo-a" {
		t.Fatalf("expected repo-a -> CHAOS via native_team_key own resolution with an EMPTY project_id, got %+v", got)
	}
	if got[0].ResolutionArm != TeamRepoOwnershipResolutionArmLinearTeamKey {
		t.Fatalf("expected ResolutionArm=%q, got %q", TeamRepoOwnershipResolutionArmLinearTeamKey, got[0].ResolutionArm)
	}
}

// TestLinearTeamKeyResolvesViaPRInheritanceIssueLink closes a second fixture
// gap found during the same live-proof: every prior donor-walk fixture
// (TestLinearTeamKeyDonorWalkMatchesTeamKeyShapedOwnership et al.) wires the
// donor through `dependencyEdges` (work_item_dependencies), but the
// ACTUAL live data path for these 264 items is exclusively via
// `issuePRLinks` (work_graph_issue_pr) -- the PR-inheritance loop
// (deriveTeamRepoOwnership's second loop), which no existing test exercised
// with a Linear donor at all (every existing issuePRLinks-bearing fixture,
// where any exist, predates this fix). This is the identical closure
// (donorProjectID) used by both loops, so it is expected to behave the same
// as the dependency-edge donor walk -- this test exists to STOP relying on
// that expectation and instead pin it directly, since it is the one
// real-world code path this fix's tier-1 suite never touched.
//
// CHAOS-4537 rewrite: the closure is now donorTeamID (renamed from
// donorProjectID, returns the team_id directly -- see
// buildDonorTeamIDResolver's doc comment); decoy row, same pattern as the
// other rewritten tests above.
func TestLinearTeamKeyResolvesViaPRInheritanceIssueLink(t *testing.T) {
	projectLinks := []TeamRepoOwnershipProjectLink{
		{Provider: "linear", ProjectID: "org-1:linear:CHAOS", TeamID: "team-WRONG-decoy", IsPrimary: true},
	}
	// The Linear issue itself: no repo of its own (RepoID empty, matching
	// Linear items' real repo_id=00000000-... zero UUID), never assigned to
	// a Project (ProjectID empty), native_team_key set -- exactly org
	// 70d529e0's 264 fallback-eligible items' shape.
	workItems := []TeamRepoOwnershipWorkItem{
		{
			WorkItemID:    "linear:CHAOS-3392",
			Provider:      "linear",
			RepoID:        "",
			ProjectID:     "",
			NativeTeamKey: "CHAOS",
		},
	}
	// work_graph_issue_pr: this Linear issue is mentioned by a PR in repo-a.
	// No work_item_dependencies edge exists at all -- the PR-inheritance
	// loop must resolve this on its own via donorTeamID(link.WorkItemID).
	issuePRLinks := []TeamRepoOwnershipIssuePRLink{
		{WorkItemID: "linear:CHAOS-3392", RepoID: "repo-a", PRNumber: 42},
	}

	got := deriveTeamRepoOwnership("org-1", projectLinks, workItems, nil, issuePRLinks)

	if len(got) != 1 || got[0].TeamID != "CHAOS" || got[0].RepoID != "repo-a" {
		t.Fatalf("expected repo-a -> CHAOS via PR-inheritance native_team_key resolution, got %+v", got)
	}
	if got[0].ResolutionArm != TeamRepoOwnershipResolutionArmLinearTeamKey {
		t.Fatalf("expected ResolutionArm=%q, got %q", TeamRepoOwnershipResolutionArmLinearTeamKey, got[0].ResolutionArm)
	}
}
