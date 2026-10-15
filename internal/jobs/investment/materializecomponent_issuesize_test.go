package investment

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// TestMaterializeComponentSizesTheUnitFromItsIssuesForEveryProvider pins that the unit's size row is the sum of
// its issues' story points, whatever provider the issue came from, never its lines of code, and that a unit with
// no sized issue carries size 0 (read as weight 1).
func TestMaterializeComponentSizesTheUnitFromItsIssuesForEveryProvider(t *testing.T) {
	created := time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC)
	updated := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		provider  string
		issues    []string
		points    map[string]float64
		wantSize  float64
		wantSized uint32
	}{
		{provider: "jira", issues: []string{"jira:PAY-1", "jira:PAY-2"}, points: map[string]float64{"jira:PAY-1": 3, "jira:PAY-2": 5}, wantSize: 8, wantSized: 2},
		{provider: "gitlab", issues: []string{"gitlab:g/billing#1"}, points: map[string]float64{"gitlab:g/billing#1": 2}, wantSize: 2, wantSized: 1},
		{provider: "linear", issues: []string{"linear:ENG-1", "linear:ENG-2"}, points: map[string]float64{"linear:ENG-1": 1}, wantSize: 1, wantSized: 1},
		{provider: "github", issues: []string{"gh:acme/api#3"}, points: map[string]float64{}, wantSize: 0, wantSized: 0},
	} {
		t.Run(tc.provider, func(t *testing.T) {
			prID := testRepoID + "#pr7"
			input := windowedInput()
			input.WorkItems = map[string]chquery.WorkItem{}
			nodes := []units.NodeKey{{Type: "pr", ID: prID}}
			var edges []units.Edge
			for i, id := range tc.issues {
				nodes = append(nodes, units.NodeKey{Type: "issue", ID: id})
				edges = append(edges, units.Edge{EdgeID: "e" + id, SourceType: "pr", SourceID: prID, TargetType: "issue", TargetID: id, Confidence: 1.0})
				input.WorkItems[id] = chquery.WorkItem{WorkItemID: id, Provider: tc.provider, Title: "issue", Type: "story", CreatedAt: created.Add(time.Duration(i) * time.Hour), UpdatedAt: updated}
			}
			input.Component = units.Component{Nodes: nodes, Edges: edges}
			input.PRs = map[string]chquery.PullRequest{prID: {RepoID: testRepoID, Number: 7, Title: "Change", CreatedAt: created}}
			input.PRChurn = map[string]float64{prID: 500}
			input.StoryPoints = tc.points

			result, err := MaterializeComponent(input)
			if err != nil {
				t.Fatalf("MaterializeComponent: %v", err)
			}
			got := result.IssueSize
			if got.WorkUnitID != result.Investment.WorkUnitID {
				t.Fatalf("size row unit %q, investment unit %q", got.WorkUnitID, result.Investment.WorkUnitID)
			}
			if got.IssueSize != tc.wantSize || got.SizedIssueCount != tc.wantSized || got.IssueCount != uint32(len(tc.issues)) {
				t.Fatalf("size row = %+v, want size %v sized %d issues %d (the 500 lines changed are never a size)", got, tc.wantSize, tc.wantSized, len(tc.issues))
			}
		})
	}
}
