package investment

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/google/uuid"
)

func TestTeamOwnershipConservesEffortAndKeepsStrongerSignals(t *testing.T) {
	a, b, c := uuid.MustParse(testRepoID), uuid.MustParse(otherRepoID), uuid.MustParse("33333333-3333-4333-8333-333333333333")
	donors := []chquery.TeamRepoDonor{
		{WorkItemID: "issue-a", TeamID: "alpha", RepoID: a},
		{WorkItemID: "issue-a", TeamID: "alpha", RepoID: b},
		{WorkItemID: "issue-b", TeamID: "beta", RepoID: b},
		{WorkItemID: "issue-b", TeamID: "beta", RepoID: c},
		{WorkItemID: "issue-a", TeamID: "alpha", RepoID: a},
		{WorkItemID: "other-unit", TeamID: "decoy", RepoID: uuid.New()},
	}
	for _, effort := range []float64{0, 12} {
		input := windowedInput()
		input.Component = issueComponentOf("issue-a", "issue-b")
		input.WorkItems = map[string]chquery.WorkItem{
			"issue-a": {WorkItemID: "issue-a", CreatedAt: time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC), UpdatedAt: time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)},
			"issue-b": {WorkItemID: "issue-b", CreatedAt: time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC), UpdatedAt: time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)},
		}
		input.ActiveHours = map[string]float64{"issue-a": effort}
		result, err := MaterializeComponent(input)
		if err != nil || result.Skipped != "" {
			t.Fatalf("nonempty component: %+v %v", result, err)
		}
		rows := allocateTeamOwnership(result, input, donors)
		if len(rows) != 3 {
			t.Fatalf("effort %g: got %d rows", effort, len(rows))
		}
		var total, weights float64
		for i, row := range rows {
			if row.RepoID.String() != []string{a.String(), b.String(), c.String()}[i] || math.Abs(row.AllocationWeight-1.0/3) > 1e-12 || math.Abs(row.EffortValue-effort/3) > 1e-12 {
				t.Fatalf("effort %g row %+v", effort, row)
			}
			total += row.EffortValue
			weights += row.AllocationWeight
		}
		if math.Abs(total-effort) > 1e-12 || math.Abs(weights-1) > 1e-12 {
			t.Fatalf("effort=%g total=%g weights=%g", effort, total, weights)
		}
		if *rows[1].RepoSource != "team:alpha,beta" {
			t.Fatalf("overlap provenance=%s", *rows[1].RepoSource)
		}
		reversed := append([]chquery.TeamRepoDonor(nil), donors...)
		for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
			reversed[i], reversed[j] = reversed[j], reversed[i]
		}
		if !reflect.DeepEqual(rows, allocateTeamOwnership(result, input, reversed)) {
			t.Fatal("donor order changed allocation")
		}
		if !reflect.DeepEqual(result.RepoEffort, allocateTeamOwnership(result, input, nil)) {
			t.Fatal("missing ownership changed allocation")
		}
		for _, signal := range []string{"own-edge", "ambiguous-edges", "ancestor", "children", "pr-churn", "commit-churn", "pr-zero-churn"} {
			t.Run(signal+"/"+result.Investment.EffortMetric, func(t *testing.T) {
				stronger := input
				stronger.Component = issueComponentOf("issue-a", "issue-b")
				switch signal {
				case "own-edge", "ambiguous-edges":
					stronger.Component.Edges = []units.Edge{{EdgeID: "a"}}
					stronger.EdgeRepoIDs = map[string]string{"a": a.String()}
					if signal == "ambiguous-edges" {
						stronger.Component.Edges = append(stronger.Component.Edges, units.Edge{EdgeID: "b"})
						stronger.EdgeRepoIDs["b"] = b.String()
					}
				case "ancestor", "children":
					stronger.CascadeRepoID = &a
					stronger.CascadeRepoSource = AncestorSource("parent")
					if signal == "children" {
						stronger.CascadeRepoSource = RepoSourceChildren
					}
				case "pr-churn", "pr-zero-churn":
					id := a.String() + "#pr7"
					stronger.Component.Nodes = append(stronger.Component.Nodes, units.NodeKey{Type: "pr", ID: id})
					stronger.PRs = map[string]chquery.PullRequest{id: {RepoID: a.String(), Number: 7, CreatedAt: time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)}}
					stronger.PRChurn = map[string]float64{id: 9}
					if signal == "pr-zero-churn" {
						stronger.PRChurn[id] = 0
					}
				case "commit-churn":
					id := a.String() + "@abc"
					stronger.Component.Nodes = append(stronger.Component.Nodes, units.NodeKey{Type: "commit", ID: id})
					stronger.Commits = map[string]chquery.Commit{id: {RepoID: a.String(), Hash: "abc", AuthorWhen: time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)}}
					stronger.CommitChurn = map[string]float64{id: 9}
				}
				before, err := MaterializeComponent(stronger)
				if err != nil || before.Skipped != "" {
					t.Fatalf("stronger signal setup: %s %v", before.Skipped, err)
				}
				if !reflect.DeepEqual(before.RepoEffort, allocateTeamOwnership(before, stronger, donors)) {
					t.Fatalf("fallback changed %s", signal)
				}
			})
		}
	}
}
