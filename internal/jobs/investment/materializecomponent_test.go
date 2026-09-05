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

// TestMaterializeComponentEmptyWindowRetainsStraddlingComponent pins PARITY
// with materialize.py:1335 for a window that contains no time at all.
//
// This test previously asserted the OPPOSITE -- that an empty window writes
// nothing -- and it passed, because a fix had been written to satisfy it. Both
// were wrong together. The justification was that "Python filters in SQL and
// returns nothing", a claim never checked against the Python source. It is
// false: materialize.py:1335 is the ONLY date filter in materialize_investments
//
//	if bounds.end < config.from_ts or bounds.start >= config.to_ts: continue
//
// which is structurally identical to MaterializeComponent's own two checks, and
// every fetch feeding it (edges, work items, PRs, commits, churn, active hours,
// parent titles) is id/org-scoped, never date-scoped. Python retains the same
// straddling component and writes the same row.
//
// So the porting-correct behaviour is to RETAIN, and this test now pins that.
// The rows themselves are questionable in both planes -- a component attributed
// to a window of zero duration -- but that is a defect in the plane being
// ported, filed separately. Changing it here would alter behaviour under cover
// of a cutover.
//
// The component spans Jan 1-30 deliberately: only a STRADDLING component
// reaches this predicate at all. One wholly before or after is skipped by the
// ordinary bounds checks and would prove nothing about empty windows.
func TestMaterializeComponentEmptyWindowRetainsStraddlingComponent(t *testing.T) {
	jan10 := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	jan20 := time.Date(2026, 1, 20, 0, 0, 0, 0, time.UTC)

	for _, testCase := range []struct {
		name   string
		fromTS time.Time
		toTS   time.Time
	}{
		// Reachable in production: `window_days: 0` is an accepted scope key
		// (worker_workgraph.py:82-95), so this needs no hand-written scope.
		{name: "zero width", fromTS: jan10, toTS: jan10},
		// Requires a hand-written from > to scope; the executor accepts it,
		// because _parse_materialize_window has no ordering check either.
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
			// component were skipped for having NO TIME BOUNDS -- a different
			// code path that proves nothing about the window predicate.
			if result.Skipped == skippedNoTimeBounds {
				t.Fatalf("fixture is wrong: component has no time bounds, so the window predicate was never reached")
			}

			if result.Skipped != "" {
				t.Fatalf("Skipped = %q, want \"\" -- materialize.py:1335 retains a straddling "+
					"component under an empty window, and this port must match it",
					result.Skipped)
			}
			if result.Investment.WorkUnitID == "" {
				t.Fatalf("no work unit produced; Python produces one here (materialize.py:1335 does not skip it)")
			}
		})
	}
}
