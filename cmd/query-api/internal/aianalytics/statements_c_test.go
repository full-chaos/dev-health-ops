package aianalytics

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

func loadCases(t *testing.T) map[string]oracleCCase {
	t.Helper()
	raw, err := os.ReadFile("testdata/oracle_c_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Cases []oracleCCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	out := map[string]oracleCCase{}
	for _, c := range doc.Cases {
		out[c.Name] = c
	}
	return out
}

// Every table each statement reads is filtered on the caller's org, and the
// org is the one bound.
func TestGroupCStatements_CarryTheOrgPredicateOnEveryTable(t *testing.T) {
	cases := loadCases(t)
	dr := model.AIDateRangeInput{StartDate: day("2026-08-01"), EndDate: day("2026-08-03")}
	gov := &fixtureClientC{c: cases["gov/plain"]}
	if _, err := GovernanceSummary(context.Background(), gov, "org-1", dr, nil, 50); err != nil {
		t.Fatal(err)
	}
	wf := &fixtureClientC{c: cases["wf/issueChain"]}
	if _, err := WorkflowDrilldown(context.Background(), wf, "org-1", model.AIWorkflowRootTypeInputIssue, "ABC-1", 3, 100); err != nil {
		t.Fatal(err)
	}
	statements := append(append([]string(nil), gov.statements...), wf.statements...)
	bindings := append(append(gov.bindings[:0:0], gov.bindings...), wf.bindings...)
	want := map[string]int{
		"FROM ai_governance_coverage_daily":  1,
		"FROM ai_policy_events FINAL":        1,
		"FROM ai_workflow_issue_edges FINAL": 1,
	}
	for marker, n := range want {
		found := false
		for _, st := range statements {
			if strings.Contains(st, marker) {
				found = true
				if got := strings.Count(st, "org_id = {org_id:String}"); got < n {
					t.Errorf("%q: %d org predicates, want %d", marker, got, n)
				}
			}
		}
		if !found {
			t.Errorf("no statement matched %q", marker)
		}
	}
	for _, st := range statements {
		if strings.Contains(st, "FROM ai_workflow_issue_edges FINAL") {
			if got := strings.Count(st, "org_id = {org_id:String}"); got != 5 {
				t.Errorf("the edge union has %d org predicates, want one per branch (5)", got)
			}
			for _, table := range []string{"ai_workflow_artifact_edges", "work_graph_pr_review_outcome_edges", "work_graph_pr_deployment_edges", "work_graph_deployment_incident_edges"} {
				if !strings.Contains(st, "FROM "+table+" FINAL") {
					t.Errorf("the edge union lacks %s", table)
				}
			}
		}
	}
	for _, b := range bindings {
		found := false
		for _, x := range b {
			if x.Name == "org_id" {
				found = true
				if x.Value != "org-1" {
					t.Errorf("org_id = %v", x.Value)
				}
			}
		}
		if !found {
			t.Errorf("a statement does not bind org_id: %v", b)
		}
	}
}

func TestGovernanceStatements_AreReadOnlySelects(t *testing.T) {
	for name, st := range map[string]string{"coverage": coverageStatement, "violations": violationsStatement, "workflow": workflowEdgeStatement} {
		if !strings.HasPrefix(st, "SELECT") || strings.Contains(st, ";") {
			t.Errorf("%s is not a single SELECT", name)
		}
	}
}

// The scope predicates and the newest-first order reach the governance reads.
func TestGovernanceStatements_ScopeAndOrder(t *testing.T) {
	for name, st := range map[string]string{"coverage": coverageStatement, "violations": violationsStatement} {
		for _, p := range []string{"({team_id:String} = '' OR team_id = {team_id:String})", "({repo_id:String} = '' OR toString(repo_id) = {repo_id:String})"} {
			if !strings.Contains(st, p) {
				t.Errorf("%s lacks %q", name, p)
			}
		}
	}
	if !strings.Contains(violationsStatement, "ORDER BY observed_at DESC") || !strings.Contains(coverageStatement, "ORDER BY day, team_id, repo_id") {
		t.Errorf("read order changed")
	}
}
