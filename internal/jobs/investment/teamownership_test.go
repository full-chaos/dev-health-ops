package investment

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
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
		rows, outcome := allocateTeamOwnership(result, input, donors)
		if outcome != ownershipOutcomeAllocated {
			t.Fatalf("effort %g: outcome=%s want %s", effort, outcome, ownershipOutcomeAllocated)
		}
		if len(rows) != 3 {
			t.Fatalf("effort %g: got %d rows", effort, len(rows))
		}
		var total, weights float64
		for i, row := range rows {
			if row.RepoID.String() != []string{a.String(), b.String(), c.String()}[i] || math.Abs(row.AllocationWeight-1.0/3) > 1e-12 || math.Abs(row.EffortValue-effort/3) > 1e-12 {
				t.Fatalf("effort %g row %+v", effort, row)
			}
			if row.AllocationSource != allocationSourceTeamOwnership {
				t.Fatalf("effort %g row %d allocation_source=%s", effort, i, row.AllocationSource)
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
		if *rows[0].RepoSource != "team:alpha" || *rows[2].RepoSource != "team:beta" {
			t.Fatalf("single-team provenance=%s/%s", *rows[0].RepoSource, *rows[2].RepoSource)
		}
		reversed := append([]chquery.TeamRepoDonor(nil), donors...)
		for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
			reversed[i], reversed[j] = reversed[j], reversed[i]
		}
		reorderedRows, reorderedOutcome := allocateTeamOwnership(result, input, reversed)
		if !reflect.DeepEqual(rows, reorderedRows) || reorderedOutcome != outcome {
			t.Fatal("donor order changed allocation")
		}
		emptyRows, emptyOutcome := allocateTeamOwnership(result, input, nil)
		if !reflect.DeepEqual(result.RepoEffort, emptyRows) || emptyOutcome != ownershipOutcomeNoEligibleOwner {
			t.Fatalf("missing ownership changed allocation: outcome=%s", emptyOutcome)
		}
		for signal, wantOutcome := range map[string]string{
			// The label records WHICH guard stopped the fallback, so a mutation
			// that deletes one guard but leaves another covering the same
			// fixture cannot report a false KILLED.
			"own-edge": ownershipOutcomeOwnRepo, "ambiguous-edges": ownershipOutcomeDirectRepo,
			"ancestor": ownershipOutcomeOwnRepo, "children": ownershipOutcomeOwnRepo,
			"pr-churn": ownershipOutcomeStrongerEffort, "commit-churn": ownershipOutcomeStrongerEffort,
			"pr-zero-churn": ownershipOutcomeDirectRepo, "commit-zero-churn": ownershipOutcomeDirectRepo,
		} {
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
				case "commit-churn", "commit-zero-churn":
					id := a.String() + "@abc"
					stronger.Component.Nodes = append(stronger.Component.Nodes, units.NodeKey{Type: "commit", ID: id})
					stronger.Commits = map[string]chquery.Commit{id: {RepoID: a.String(), Hash: "abc", AuthorWhen: time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)}}
					stronger.CommitChurn = map[string]float64{id: 9}
					if signal == "commit-zero-churn" {
						stronger.CommitChurn[id] = 0
					}
				}
				before, err := MaterializeComponent(stronger)
				if err != nil || before.Skipped != "" {
					t.Fatalf("stronger signal setup: %s %v", before.Skipped, err)
				}
				after, outcome := allocateTeamOwnership(before, stronger, donors)
				if !reflect.DeepEqual(before.RepoEffort, after) {
					t.Fatalf("fallback changed %s", signal)
				}
				if outcome != wantOutcome {
					t.Fatalf("%s outcome=%s want %s", signal, outcome, wantOutcome)
				}
			})
		}
	}
}

// TestTeamOwnershipDonorEligibilityGuards pins the three per-donor clauses
// separately. A wholesale mutation of the compound `if` reports KILLED while
// any one clause inside it is wrong and unasserted, so each clause gets a
// fixture that ONLY that clause rejects.
func TestTeamOwnershipDonorEligibilityGuards(t *testing.T) {
	a := uuid.MustParse(testRepoID)
	base := func() (MaterializeComponentResult, MaterializeComponentInput) {
		t.Helper()
		input := windowedInput()
		input.Component = issueComponentOf("issue-a")
		input.WorkItems = map[string]chquery.WorkItem{
			"issue-a": {WorkItemID: "issue-a", CreatedAt: time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC), UpdatedAt: time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)},
		}
		input.ActiveHours = map[string]float64{"issue-a": 6}
		result, err := MaterializeComponent(input)
		if err != nil || result.Skipped != "" {
			t.Fatalf("component setup: %+v %v", result, err)
		}
		return result, input
	}
	for name, donor := range map[string]chquery.TeamRepoDonor{
		"foreign work item": {WorkItemID: "issue-elsewhere", TeamID: "alpha", RepoID: a},
		"zero repo":         {WorkItemID: "issue-a", TeamID: "alpha", RepoID: uuid.Nil},
		"empty team":        {WorkItemID: "issue-a", TeamID: "", RepoID: a},
	} {
		t.Run(name, func(t *testing.T) {
			result, input := base()
			rows, outcome := allocateTeamOwnership(result, input, []chquery.TeamRepoDonor{donor})
			if outcome != ownershipOutcomeNoEligibleOwner {
				t.Fatalf("ineligible donor %+v allocated: outcome=%s rows=%+v", donor, outcome, rows)
			}
			if !reflect.DeepEqual(result.RepoEffort, rows) {
				t.Fatalf("ineligible donor %+v changed allocation to %+v", donor, rows)
			}
		})
	}
	t.Run("one eligible donor among ineligible ones still allocates", func(t *testing.T) {
		result, input := base()
		rows, outcome := allocateTeamOwnership(result, input, []chquery.TeamRepoDonor{
			{WorkItemID: "issue-elsewhere", TeamID: "alpha", RepoID: uuid.New()},
			{WorkItemID: "issue-a", TeamID: "", RepoID: uuid.New()},
			{WorkItemID: "issue-a", TeamID: "alpha", RepoID: uuid.Nil},
			{WorkItemID: "issue-a", TeamID: "alpha", RepoID: a},
		})
		if outcome != ownershipOutcomeAllocated || len(rows) != 1 || rows[0].RepoID.String() != a.String() {
			t.Fatalf("outcome=%s rows=%+v", outcome, rows)
		}
		if math.Abs(rows[0].AllocationWeight-1) > 1e-12 || math.Abs(rows[0].EffortValue-6) > 1e-12 {
			t.Fatalf("single eligible repo must take the whole share: %+v", rows[0])
		}
	})
}

// TestTeamOwnershipStrongerAllocationSourcesAreNotOverwritten pins the
// allocation-source clause independently of the repo-evidence clauses: an
// existing effort row whose source is neither empty nor the unassigned marker
// keeps precedence even with eligible donors present.
func TestTeamOwnershipStrongerAllocationSourcesAreNotOverwritten(t *testing.T) {
	a := uuid.MustParse(testRepoID)
	donors := []chquery.TeamRepoDonor{{WorkItemID: "issue-a", TeamID: "alpha", RepoID: a}}
	input := windowedInput()
	input.Component = issueComponentOf("issue-a")
	input.WorkItems = map[string]chquery.WorkItem{
		"issue-a": {WorkItemID: "issue-a", CreatedAt: time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC), UpdatedAt: time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)},
	}
	input.ActiveHours = map[string]float64{"issue-a": 6}
	result, err := MaterializeComponent(input)
	if err != nil || result.Skipped != "" {
		t.Fatalf("component setup: %+v %v", result, err)
	}
	cloneEffort := func() []chwrite.RepoEffortRecord {
		return append([]chwrite.RepoEffortRecord(nil), result.RepoEffort...)
	}
	if len(result.RepoEffort) == 0 {
		t.Fatal("fixture must produce at least one existing effort row")
	}
	// Both permeable sources must still let the fallback through.
	for _, permeable := range []string{units.AllocationSourceEmpty, units.AllocationSourceActiveHoursUnassign} {
		open := result
		open.RepoEffort = cloneEffort()
		for i := range open.RepoEffort {
			open.RepoEffort[i].AllocationSource = permeable
		}
		if _, outcome := allocateTeamOwnership(open, input, donors); outcome != ownershipOutcomeAllocated {
			t.Fatalf("permeable allocation source %q blocked the fallback: %s", permeable, outcome)
		}
	}
	blocked := result
	blocked.RepoEffort = cloneEffort()
	blocked.RepoEffort[0].AllocationSource = "pr_churn"
	rows, outcome := allocateTeamOwnership(blocked, input, donors)
	if outcome != ownershipOutcomeStrongerEffort || !reflect.DeepEqual(blocked.RepoEffort, rows) {
		t.Fatalf("stronger allocation overwritten: outcome=%s rows=%+v", outcome, rows)
	}
}

// TestTeamOwnershipZeroRepoEvidenceIsNotEvidence pins the `*repo != uuid.Nil`
// clause of each of the three direct-evidence guards separately. A zero UUID
// parses successfully, so dropping that clause from any one of them turns a
// repo-less edge, PR or commit into "direct evidence" and silently suppresses
// the fallback for every unit carrying one -- a survivor no `repo != nil`
// assertion can catch.
func TestTeamOwnershipZeroRepoEvidenceIsNotEvidence(t *testing.T) {
	a := uuid.MustParse(testRepoID)
	zero := uuid.Nil.String()
	donors := []chquery.TeamRepoDonor{{WorkItemID: "issue-a", TeamID: "alpha", RepoID: a}}
	at := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	// The EDGE guard's zero-UUID clause is deliberately absent from this
	// table. collectSingleRepoID (materializecomponent.go) filters only the
	// empty string, so a component whose only edge repo is the zero UUID gets
	// that zero as its scalar Investment.RepoID and is stopped one guard
	// earlier, by the own-repo check; with an ambiguous edge set the real repo
	// on the other edge returns direct evidence regardless. The clause is
	// therefore defense in depth with no reachable fixture -- recorded as an
	// equivalent mutant in the kill ledger rather than covered by a test that
	// cannot fail.
	for _, carrier := range []string{"pr", "commit"} {
		t.Run(carrier, func(t *testing.T) {
			input := windowedInput()
			input.Component = issueComponentOf("issue-a")
			input.WorkItems = map[string]chquery.WorkItem{
				"issue-a": {WorkItemID: "issue-a", CreatedAt: at, UpdatedAt: at},
			}
			input.ActiveHours = map[string]float64{"issue-a": 6}
			switch carrier {
			case "pr":
				id := zero + "#pr7"
				input.Component.Nodes = append(input.Component.Nodes, units.NodeKey{Type: "pr", ID: id})
				input.PRs = map[string]chquery.PullRequest{id: {RepoID: zero, Number: 7, CreatedAt: at}}
				input.PRChurn = map[string]float64{id: 0}
			case "commit":
				id := zero + "@abc"
				input.Component.Nodes = append(input.Component.Nodes, units.NodeKey{Type: "commit", ID: id})
				input.Commits = map[string]chquery.Commit{id: {RepoID: zero, Hash: "abc", AuthorWhen: at}}
				input.CommitChurn = map[string]float64{id: 0}
			}
			result, err := MaterializeComponent(input)
			if err != nil || result.Skipped != "" {
				t.Fatalf("%s setup: %+v %v", carrier, result, err)
			}
			if result.Investment.RepoID != nil {
				t.Fatalf("%s: a zero repo UUID became the unit's scalar repo %s", carrier, result.Investment.RepoID)
			}
			rows, outcome := allocateTeamOwnership(result, input, donors)
			if outcome != ownershipOutcomeAllocated {
				t.Fatalf("%s: zero repo UUID suppressed the fallback: outcome=%s rows=%+v", carrier, outcome, rows)
			}
			if len(rows) != 1 || rows[0].RepoID.String() != a.String() {
				t.Fatalf("%s: rows=%+v", carrier, rows)
			}
		})
	}
}
