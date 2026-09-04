package investment

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

const testRepoID = "11111111-1111-4111-8111-111111111111"
const otherRepoID = "22222222-2222-4222-8222-222222222222"

func windowedInput() MaterializeComponentInput {
	return MaterializeComponentInput{
		FromTS: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ToTS:   time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC),
	}
}

// TestMaterializeComponentGoldenPath exercises the full happy path: an
// issue, a linked PR and commit, commit churn present (so ComputeEffort
// picks the commit tier), a single consistent repo_id across edges, and a
// window that contains the computed time bounds.
func TestMaterializeComponentGoldenPath(t *testing.T) {
	issueID := "issue-1"
	prID := testRepoID + "#pr7"
	commitID := testRepoID + "@abc123"
	created := time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC)
	updated := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)

	input := windowedInput()
	input.Component = units.Component{
		Nodes: []units.NodeKey{
			{Type: "issue", ID: issueID},
			{Type: "pr", ID: prID},
			{Type: "commit", ID: commitID},
		},
		Edges: []units.Edge{
			{EdgeID: "e1", SourceType: "issue", SourceID: issueID, TargetType: "pr", TargetID: prID, Confidence: 1.0},
			{EdgeID: "e2", SourceType: "issue", SourceID: issueID, TargetType: "commit", TargetID: commitID, Confidence: 0.9},
		},
	}
	input.WorkItems = map[string]chquery.WorkItem{
		issueID: {WorkItemID: issueID, Provider: "github", Title: "Fix the login bug", Type: "bug", CreatedAt: created, UpdatedAt: updated},
	}
	input.PRs = map[string]chquery.PullRequest{
		prID: {RepoID: testRepoID, Number: 7, Title: "Fix login", CreatedAt: created},
	}
	input.Commits = map[string]chquery.Commit{
		commitID: {RepoID: testRepoID, Hash: "abc123", Message: "Fix login flow", AuthorWhen: created},
	}
	input.EdgeRepoIDs = map[string]string{"e1": testRepoID, "e2": testRepoID}
	input.CommitChurn = map[string]float64{commitID: 42.0}
	input.PRChurn = map[string]float64{prID: 10.0}

	result, err := MaterializeComponent(input)
	if err != nil {
		t.Fatalf("MaterializeComponent: %v", err)
	}
	if result.Skipped != "" {
		t.Fatalf("Skipped = %q, want \"\"", result.Skipped)
	}

	wantUnitID := units.WorkUnitID(input.Component.Nodes)
	if result.Investment.WorkUnitID != wantUnitID {
		t.Errorf("WorkUnitID = %q, want %q", result.Investment.WorkUnitID, wantUnitID)
	}
	// Commit churn (42.0) is present and positive, so ComputeEffort's first
	// tier wins over PR churn (10.0) -- proves the real units.ComputeEffort
	// is being called, not a stand-in.
	if result.Investment.EffortMetric != units.EffortMetricChurnLOC || result.Investment.EffortValue != 42.0 {
		t.Errorf("effort = (%s, %v), want (%s, 42)", result.Investment.EffortMetric, result.Investment.EffortValue, units.EffortMetricChurnLOC)
	}
	if result.Investment.WorkUnitType == nil || *result.Investment.WorkUnitType != "bug" {
		t.Errorf("WorkUnitType = %v, want \"bug\"", result.Investment.WorkUnitType)
	}
	if result.Investment.WorkUnitName == nil || *result.Investment.WorkUnitName != "Fix the login bug" {
		t.Errorf("WorkUnitName = %v, want \"Fix the login bug\" (the issue title, highest priority tier)", result.Investment.WorkUnitName)
	}
	if result.Investment.RepoID == nil || result.Investment.RepoID.String() != testRepoID {
		t.Errorf("RepoID = %v, want %s (single consistent repo across edges)", result.Investment.RepoID, testRepoID)
	}
	if result.Investment.Provider == nil || *result.Investment.Provider != "github" {
		t.Errorf("Provider = %v, want \"github\"", result.Investment.Provider)
	}
	if result.Investment.CategorizationStatus != CategorizationStatusNotYetCategorized {
		t.Errorf("CategorizationStatus = %q, want the placeholder %q", result.Investment.CategorizationStatus, CategorizationStatusNotYetCategorized)
	}
	if result.Investment.CategorizationInputHash == "" {
		t.Error("CategorizationInputHash is empty -- BuildTextBundle should have produced a real hash")
	}
	if result.Investment.EvidenceQuality <= 0 || result.Investment.EvidenceQuality > 1 {
		t.Errorf("EvidenceQuality = %v, want in (0, 1]", result.Investment.EvidenceQuality)
	}

	var structural map[string][]string
	if err := json.Unmarshal([]byte(result.Investment.StructuralEvidenceJSON), &structural); err != nil {
		t.Fatalf("StructuralEvidenceJSON did not unmarshal: %v (%s)", err, result.Investment.StructuralEvidenceJSON)
	}
	if got := structural["issues"]; len(got) != 1 || got[0] != issueID {
		t.Errorf("structural issues = %v, want [%s]", got, issueID)
	}
	if got := structural["edges"]; len(got) != 2 {
		t.Errorf("structural edges = %v, want 2 entries", got)
	}

	if len(result.RepoEffort) == 0 {
		t.Fatal("expected at least one repo-effort row")
	}
	for _, row := range result.RepoEffort {
		if row.WorkUnitID != wantUnitID {
			t.Errorf("repo-effort row WorkUnitID = %q, want %q", row.WorkUnitID, wantUnitID)
		}
	}
}

// TestMaterializeComponentNoTimeBounds proves a component whose nodes are
// entirely absent from the fetched maps (Present=false in every NodeTimes)
// is skipped rather than producing a record with zero-value timestamps.
func TestMaterializeComponentNoTimeBounds(t *testing.T) {
	input := windowedInput()
	input.Component = units.Component{
		Nodes: []units.NodeKey{{Type: "issue", ID: "missing-issue"}},
	}
	// WorkItems deliberately left nil -- the node is in the component but
	// absent from every fetch map, matching a dangling edge reference.

	result, err := MaterializeComponent(input)
	if err != nil {
		t.Fatalf("MaterializeComponent: %v", err)
	}
	if result.Skipped != skippedNoTimeBounds {
		t.Fatalf("Skipped = %q, want %q", result.Skipped, skippedNoTimeBounds)
	}
}

// TestMaterializeComponentOutOfWindow proves a component whose computed time
// bounds fall entirely before the run's window is skipped, not written with
// a stale window.
func TestMaterializeComponentOutOfWindow(t *testing.T) {
	issueID := "old-issue"
	old := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	input := windowedInput() // window is January 2026
	input.Component = units.Component{Nodes: []units.NodeKey{{Type: "issue", ID: issueID}}}
	input.WorkItems = map[string]chquery.WorkItem{
		issueID: {WorkItemID: issueID, CreatedAt: old, UpdatedAt: old},
	}

	result, err := MaterializeComponent(input)
	if err != nil {
		t.Fatalf("MaterializeComponent: %v", err)
	}
	if result.Skipped != skippedOutOfWindow {
		t.Fatalf("Skipped = %q, want %q", result.Skipped, skippedOutOfWindow)
	}
}

// TestMaterializeComponentMultipleReposLeavesRepoIDUnset proves
// collectSingleRepoID's ALL-OR-NOTHING rule: edges naming two different
// repos leave RepoID nil, same as naming zero.
func TestMaterializeComponentMultipleReposLeavesRepoIDUnset(t *testing.T) {
	issueID := "issue-1"
	prID := testRepoID + "#pr1"
	created := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)

	input := windowedInput()
	input.Component = units.Component{
		Nodes: []units.NodeKey{{Type: "issue", ID: issueID}, {Type: "pr", ID: prID}},
		Edges: []units.Edge{
			{EdgeID: "e1", SourceType: "issue", SourceID: issueID, TargetType: "pr", TargetID: prID, Confidence: 1.0},
		},
	}
	input.WorkItems = map[string]chquery.WorkItem{
		issueID: {WorkItemID: issueID, Title: "Cross-repo work", CreatedAt: created, UpdatedAt: created},
	}
	// Two DIFFERENT edges (well, here one edge but the map claims a second
	// distinct repo for it would be contradictory) -- exercise the multi-repo
	// case with two edges naming two different repos instead.
	input.Component.Edges = append(input.Component.Edges, units.Edge{
		EdgeID: "e2", SourceType: "issue", SourceID: issueID, TargetType: "pr", TargetID: prID, Confidence: 1.0,
	})
	input.EdgeRepoIDs = map[string]string{"e1": testRepoID, "e2": otherRepoID}

	result, err := MaterializeComponent(input)
	if err != nil {
		t.Fatalf("MaterializeComponent: %v", err)
	}
	if result.Skipped != "" {
		t.Fatalf("Skipped = %q, want \"\"", result.Skipped)
	}
	if result.Investment.RepoID != nil {
		t.Errorf("RepoID = %v, want nil (edges name two different repos)", result.Investment.RepoID)
	}
}

// TestMaterializeComponentEmptyWindowWritesNothing is the confirmation pass's
// P1, and it is WRITER-FACING by way of the retention chain rather than by
// mocking a writer: Materializer.Run skips a component whose Skipped is
// non-empty (materialize.go:219 `continue`), so nothing is appended at :355 and
// nothing reaches WriteInvestments at :392. A component that survives this
// predicate IS a written row.
//
// Both windows here are ACCEPTED by the executor on purpose -- removing the
// ordering refusal is what matches _parse_materialize_window, which has no
// ordering check. The bug was that acceptance silently became a WRITE: the two
// bounds checks admit any component that straddles an empty interval, so a
// zero-width window produced investment rows where Python produces none.
//
// The component spans Jan 1-30 deliberately: it straddles both windows, which
// is the only shape that reaches the defect. A component wholly before or after
// is skipped by the ordinary bounds checks and proves nothing.
func TestMaterializeComponentEmptyWindowWritesNothing(t *testing.T) {
	jan10 := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	jan20 := time.Date(2026, 1, 20, 0, 0, 0, 0, time.UTC)

	for _, testCase := range []struct {
		name   string
		fromTS time.Time
		toTS   time.Time
	}{
		// The reachable one: `window_days: 0` is an accepted scope key, so this
		// needs no hand-written scope to occur in production.
		{name: "zero width", fromTS: jan10, toTS: jan10},
		// Requires a hand-written from > to scope, but the executor accepts it.
		{name: "inverted", fromTS: jan20, toTS: jan10},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			issueID := "issue-straddling-the-window"
			start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			end := time.Date(2026, 1, 30, 0, 0, 0, 0, time.UTC)

			input := MaterializeComponentInput{FromTS: testCase.fromTS, ToTS: testCase.toTS}
			input.Component = units.Component{Nodes: []units.NodeKey{{Type: "issue", ID: issueID}}}
			input.WorkItems = map[string]chquery.WorkItem{
				issueID: {WorkItemID: issueID, CreatedAt: start, UpdatedAt: end},
			}

			result, err := MaterializeComponent(input)
			if err != nil {
				t.Fatalf("MaterializeComponent: %v", err)
			}

			// POSITIVE CONTROL: without it this test would also pass if the
			// component were skipped for having NO TIME BOUNDS, which is a
			// different code path and would prove nothing about the window.
			if result.Skipped == skippedNoTimeBounds {
				t.Fatalf("fixture is wrong: component has no time bounds, so the window predicate was never reached")
			}

			if result.Skipped != skippedOutOfWindow {
				t.Fatalf("Skipped = %q, want %q -- an empty window admitted a component, "+
					"and Materializer.Run writes every component it does not skip",
					result.Skipped, skippedOutOfWindow)
			}
			if result.Investment.WorkUnitID != "" {
				t.Fatalf("empty window produced work unit %q, want none -- this row would be written",
					result.Investment.WorkUnitID)
			}
			if len(result.RepoEffort) != 0 {
				t.Fatalf("empty window produced %d repo-effort rows, want 0", len(result.RepoEffort))
			}
		})
	}
}
