package investment

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// TestTeamOwnershipCascadeResolvesAStandaloneIssue is the core CHAOS-5459
// case, and it is the shape neither hierarchy tier can reach: a Linear issue
// with NO parent, NO children, and NO PR/commit edge of its own. Measured on
// org 70d529e0 (run c8065d4a8c1648568d369519045747f2, 2026-09-07T12:01Z),
// 625 of the 825 member issues behind the 767 unresolved units had no
// parent_id at all, so no depth limit on the ancestor walk could ever have
// resolved them -- the team's own repo ownership is the only link the data
// holds for them.
func TestTeamOwnershipCascadeResolvesAStandaloneIssue(t *testing.T) {
	repoA := "11111111-1111-4111-8111-111111111111"
	components := []units.Component{issueComponentOf("S1")}
	ownRepoByComponent := map[int]*uuid.UUID{0: nil}
	workItems := map[string]chquery.WorkItem{
		// No ParentID: the ancestor tier has nothing to walk. No other item
		// names S1 as a parent: the children tier has nothing to collect.
		"S1": {WorkItemID: "S1"},
	}
	teamRepos := map[string]chquery.TeamOwnedRepo{
		"S1": {TeamID: "CHAOS", RepoID: repoA},
	}

	got := computeRepoHierarchyCascade(
		components, ownRepoByComponent, workItems, buildIssueComponentIndex(components), teamRepos,
	)

	cascade, ok := got[0]
	if !ok {
		t.Fatal("component 0 (S1) got no cascade result; expected the team-ownership tier to resolve it")
	}
	if cascade.RepoID == nil || cascade.RepoID.String() != mustRepoID(t, repoA).String() {
		t.Errorf("repo = %v, want %s", cascade.RepoID, repoA)
	}
	if cascade.Source != TeamOwnershipSource("CHAOS") {
		t.Errorf("repo_source = %q, want %q", cascade.Source, TeamOwnershipSource("CHAOS"))
	}
	if cascade.AllocationSource != units.AllocationSourceTeamOwnership {
		t.Errorf("allocation_source = %q, want %q", cascade.AllocationSource, units.AllocationSourceTeamOwnership)
	}
}

// TestTeamOwnershipCascadeNeverDisplacesAnAncestor pins the TIER ORDER, which
// is the part of this change most likely to be broken by a later edit: a
// team-owned repo says who owns the code, an ancestor's repo says which code a
// related unit actually changed. The stronger signal must win even though both
// are available.
func TestTeamOwnershipCascadeNeverDisplacesAnAncestor(t *testing.T) {
	ancestorRepo := "11111111-1111-4111-8111-111111111111"
	teamRepo := "22222222-2222-4222-8222-222222222222"
	components := []units.Component{
		issueComponentOf("P"),  // idx 0: resolved on its own
		issueComponentOf("C1"), // idx 1: pure-issue, child of P, ALSO team-owned
	}
	ownRepoByComponent := map[int]*uuid.UUID{0: mustRepoID(t, ancestorRepo), 1: nil}
	workItems := map[string]chquery.WorkItem{
		"P":  {WorkItemID: "P"},
		"C1": {WorkItemID: "C1", ParentID: "P"},
	}
	teamRepos := map[string]chquery.TeamOwnedRepo{
		"C1": {TeamID: "CHAOS", RepoID: teamRepo},
	}

	got := computeRepoHierarchyCascade(
		components, ownRepoByComponent, workItems, buildIssueComponentIndex(components), teamRepos,
	)

	cascade, ok := got[1]
	if !ok {
		t.Fatal("component 1 (C1) got no cascade result at all")
	}
	if cascade.RepoID == nil || cascade.RepoID.String() != mustRepoID(t, ancestorRepo).String() {
		t.Fatalf("repo = %v, want the ANCESTOR's %s -- the team tier displaced a stronger signal",
			cascade.RepoID, ancestorRepo)
	}
	if cascade.AllocationSource != units.AllocationSourceHierarchyCascade {
		t.Errorf("allocation_source = %q, want %q", cascade.AllocationSource, units.AllocationSourceHierarchyCascade)
	}
	if cascade.Source != AncestorSource("P") {
		t.Errorf("repo_source = %q, want %q", cascade.Source, AncestorSource("P"))
	}
}

// TestTeamOwnershipCascadeRefusesDisagreeingTeams: a component fusing issues
// from two teams that own DIFFERENT repositories has no honest single answer,
// so it stays unassigned rather than picking whichever the map iterated first.
// Same unanimity discipline as the ancestor and children tiers.
func TestTeamOwnershipCascadeRefusesDisagreeingTeams(t *testing.T) {
	repoA := "11111111-1111-4111-8111-111111111111"
	repoB := "22222222-2222-4222-8222-222222222222"
	components := []units.Component{issueComponentOf("A1", "B1")}
	ownRepoByComponent := map[int]*uuid.UUID{0: nil}
	workItems := map[string]chquery.WorkItem{
		"A1": {WorkItemID: "A1"},
		"B1": {WorkItemID: "B1"},
	}
	teamRepos := map[string]chquery.TeamOwnedRepo{
		"A1": {TeamID: "ALPHA", RepoID: repoA},
		"B1": {TeamID: "BETA", RepoID: repoB},
	}

	got := computeRepoHierarchyCascade(
		components, ownRepoByComponent, workItems, buildIssueComponentIndex(components), teamRepos,
	)

	if cascade, ok := got[0]; ok {
		t.Errorf("component 0 resolved to %v (source %q); two teams owning different repos must yield NO signal",
			cascade.RepoID, cascade.Source)
	}
}

// TestTeamOwnershipCascadeSkipsIssuesWithoutATeam is the asymmetry that makes
// the tier usable in real data: an issue with no team-owned repo is SKIPPED,
// not counted as disagreement. Treating it as disagreement would mean one
// untriaged issue joining a component silently deletes the attribution of
// every other issue in it -- a strictly worse outcome than before this tier
// existed.
func TestTeamOwnershipCascadeSkipsIssuesWithoutATeam(t *testing.T) {
	repoA := "11111111-1111-4111-8111-111111111111"
	components := []units.Component{issueComponentOf("A1", "Z9")}
	ownRepoByComponent := map[int]*uuid.UUID{0: nil}
	workItems := map[string]chquery.WorkItem{
		"A1": {WorkItemID: "A1"},
		"Z9": {WorkItemID: "Z9"},
	}
	teamRepos := map[string]chquery.TeamOwnedRepo{
		"A1": {TeamID: "CHAOS", RepoID: repoA},
		// Z9 deliberately absent: no native team, or a team owning nothing.
	}

	got := computeRepoHierarchyCascade(
		components, ownRepoByComponent, workItems, buildIssueComponentIndex(components), teamRepos,
	)

	cascade, ok := got[0]
	if !ok {
		t.Fatal("component 0 got no cascade result; one team-less member must not veto the others")
	}
	if cascade.RepoID == nil || cascade.RepoID.String() != mustRepoID(t, repoA).String() {
		t.Errorf("repo = %v, want %s", cascade.RepoID, repoA)
	}
	if cascade.Source != TeamOwnershipSource("CHAOS") {
		t.Errorf("repo_source = %q, want %q", cascade.Source, TeamOwnershipSource("CHAOS"))
	}
}

// TestTeamOwnershipCascadeIsAbsentWithoutOwnershipData is the NEGATIVE
// control for every test above: with the same components and the same
// unresolved own-repo state, but an EMPTY teamRepos map, no cascade is
// produced at all. Without it, a bug that resolved every component
// unconditionally would still pass the positive tests.
func TestTeamOwnershipCascadeIsAbsentWithoutOwnershipData(t *testing.T) {
	components := []units.Component{issueComponentOf("S1")}
	ownRepoByComponent := map[int]*uuid.UUID{0: nil}
	workItems := map[string]chquery.WorkItem{"S1": {WorkItemID: "S1"}}

	got := computeRepoHierarchyCascade(
		components, ownRepoByComponent, workItems, buildIssueComponentIndex(components),
		map[string]chquery.TeamOwnedRepo{},
	)

	if cascade, ok := got[0]; ok {
		t.Errorf("component 0 resolved to %v with no ownership data at all", cascade.RepoID)
	}
}

// TestTeamOwnershipCascadeIgnoresAMalformedRepoID: the loader's SELECT already
// excludes NULL repo_ids, so a non-UUID value reaching here is a data defect.
// It must leave the unit unassigned, never panic and never attribute the unit
// to a zero UUID (which would count as "assigned" in sankeycoverage.go's
// `IS NOT NULL` test while pointing at no real repository).
func TestTeamOwnershipCascadeIgnoresAMalformedRepoID(t *testing.T) {
	components := []units.Component{issueComponentOf("S1")}
	ownRepoByComponent := map[int]*uuid.UUID{0: nil}
	workItems := map[string]chquery.WorkItem{"S1": {WorkItemID: "S1"}}
	teamRepos := map[string]chquery.TeamOwnedRepo{
		"S1": {TeamID: "CHAOS", RepoID: "not-a-uuid"},
	}

	got := computeRepoHierarchyCascade(
		components, ownRepoByComponent, workItems, buildIssueComponentIndex(components), teamRepos,
	)

	if cascade, ok := got[0]; ok {
		t.Errorf("component 0 resolved to %v from a malformed repo id", cascade.RepoID)
	}
}

// TestMaterializeComponentWritesTheTeamOwnershipAllocationSource closes the
// gap between the cascade computing an answer and that answer reaching the
// column the Sankey reads. materializecomponent.go's override used to hard-code
// units.AllocationSourceHierarchyCascade; if it still did, this unit would be
// resolved but MISLABELLED, and every dashboard partitioning coverage by
// allocation_source would attribute team-derived rows to the hierarchy cascade.
func TestMaterializeComponentWritesTheTeamOwnershipAllocationSource(t *testing.T) {
	repoA := mustRepoID(t, "11111111-1111-4111-8111-111111111111")
	created := time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC)
	windowStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)

	result, err := MaterializeComponent(MaterializeComponentInput{
		Component: issueComponentOf("S1"),
		WorkItems: map[string]chquery.WorkItem{
			"S1": {WorkItemID: "S1", CreatedAt: created, UpdatedAt: created},
		},
		PRs: map[string]chquery.PullRequest{}, Commits: map[string]chquery.Commit{},
		EdgeRepoIDs: map[string]string{},
		PRChurn:     map[string]float64{}, CommitChurn: map[string]float64{}, ActiveHours: map[string]float64{},
		FromTS: windowStart, ToTS: windowEnd,

		CascadeRepoID:           repoA,
		CascadeRepoSource:       TeamOwnershipSource("CHAOS"),
		CascadeAllocationSource: units.AllocationSourceTeamOwnership,
	})
	if err != nil {
		t.Fatalf("MaterializeComponent: %v", err)
	}
	if result.Skipped != "" {
		t.Fatalf("component skipped as %q; the fixture window should retain it", result.Skipped)
	}
	if len(result.RepoEffort) != 1 {
		t.Fatalf("got %d repo-effort rows, want exactly 1 (the overridden empty tier)", len(result.RepoEffort))
	}
	row := result.RepoEffort[0]
	if row.AllocationSource != units.AllocationSourceTeamOwnership {
		t.Errorf("allocation_source = %q, want %q", row.AllocationSource, units.AllocationSourceTeamOwnership)
	}
	if row.RepoID == nil || row.RepoID.String() != repoA.String() {
		t.Errorf("repo_id = %v, want %s", row.RepoID, repoA)
	}
	if row.RepoSource == nil || *row.RepoSource != TeamOwnershipSource("CHAOS") {
		t.Errorf("repo_source = %v, want %q", row.RepoSource, TeamOwnershipSource("CHAOS"))
	}
	if row.AllocationWeight != 1.0 {
		t.Errorf("allocation_weight = %v, want 1.0", row.AllocationWeight)
	}
	if result.Investment.RepoID == nil || result.Investment.RepoID.String() != repoA.String() {
		t.Errorf("work_unit_investments.repo_id = %v, want %s", result.Investment.RepoID, repoA)
	}
}

// TestMaterializeComponentDefaultsAnUnlabelledCascadeToHierarchy pins the
// back-compat arm explicitly rather than leaving it to be inferred: a caller
// that supplies a cascade repo but no allocation source (every call site
// predating CHAOS-5459, and any hand-built test input) still writes
// hierarchy_cascade, never the empty string -- an empty allocation_source
// would be neither a tier name nor "empty" and would land in a bucket no
// consumer knows about.
func TestMaterializeComponentDefaultsAnUnlabelledCascadeToHierarchy(t *testing.T) {
	repoA := mustRepoID(t, "11111111-1111-4111-8111-111111111111")
	created := time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC)
	windowStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)

	result, err := MaterializeComponent(MaterializeComponentInput{
		Component: issueComponentOf("S1"),
		WorkItems: map[string]chquery.WorkItem{
			"S1": {WorkItemID: "S1", CreatedAt: created, UpdatedAt: created},
		},
		PRs: map[string]chquery.PullRequest{}, Commits: map[string]chquery.Commit{},
		EdgeRepoIDs: map[string]string{},
		PRChurn:     map[string]float64{}, CommitChurn: map[string]float64{}, ActiveHours: map[string]float64{},
		FromTS: windowStart, ToTS: windowEnd,

		CascadeRepoID:     repoA,
		CascadeRepoSource: AncestorSource("P"),
		// CascadeAllocationSource deliberately unset.
	})
	if err != nil {
		t.Fatalf("MaterializeComponent: %v", err)
	}
	if len(result.RepoEffort) != 1 {
		t.Fatalf("got %d repo-effort rows, want exactly 1", len(result.RepoEffort))
	}
	if got := result.RepoEffort[0].AllocationSource; got != units.AllocationSourceHierarchyCascade {
		t.Errorf("allocation_source = %q, want %q", got, units.AllocationSourceHierarchyCascade)
	}
}
