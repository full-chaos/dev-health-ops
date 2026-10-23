package aianalytics

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-9240: a pull request root with no AI edge still walks to its issue
// through work_graph_issue_pr.

const ipRepo = "bbbbbbbb-2222-4222-8222-222222222222"

type ipLink struct {
	workItem string
	prNumber int
}

// issuePRClient answers the edge union only from the work_graph_issue_pr rows,
// and only when the statement reads that table; it honours the frontier ids and
// the {limit} like ClickHouse does. Name reads return no rows.
type issuePRClient struct {
	links      []ipLink
	statements []string
	bindings   [][]clickhouse.Binding
}

func bindingValue(b []clickhouse.Binding, name string) any {
	for _, x := range b {
		if x.Name == name {
			return x.Value
		}
	}
	return nil
}

func (c *issuePRClient) Query(_ context.Context, st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	if workflowNameRead(st) != "" || !strings.Contains(st, "ai_workflow_issue_edges") {
		return &scriptedRows{}, nil
	}
	c.statements = append(c.statements, st)
	c.bindings = append(c.bindings, b)
	if !strings.Contains(st, "FROM work_graph_issue_pr FINAL") {
		return &scriptedRows{}, nil
	}
	prIDs := map[string]bool{}
	for _, id := range bindingValue(b, "pr_ids").([]string) {
		prIDs[id] = true
	}
	issueIDs := map[string]bool{}
	for _, id := range bindingValue(b, "issue_ids").([]string) {
		issueIDs[id] = true
	}
	limit := int(bindingValue(b, "limit_n").(uint32))
	var rows [][]any
	for _, l := range c.links {
		pr := fmt.Sprintf("%s:%d", ipRepo, l.prNumber)
		if !prIDs[pr] && !issueIDs[l.workItem] {
			continue
		}
		if len(rows) == limit {
			break
		}
		rows = append(rows, []any{"issue_pr:" + pr + ":" + l.workItem, "pr", pr, "issue", l.workItem, "implements", float32(1), "native", "x", "work_graph", ipRepo})
	}
	return &scriptedRows{rows: rows}, nil
}

func TestWorkflowDrilldown_PRWithOnlyAnIssueLinkWalksToTheIssue(t *testing.T) {
	client := &issuePRClient{links: []ipLink{{"jira:ABC-1", 7}, {"jira:ABC-1", 9}}}
	got, err := WorkflowDrilldown(context.Background(), client, "org-1", model.AIWorkflowRootTypeInputPr, ipRepo+":7", 1, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !got.DataAvailable || len(got.Edges) != 1 {
		t.Fatalf("edges = %d, dataAvailable = %v, want 1 edge", len(got.Edges), got.DataAvailable)
	}
	want := map[string]bool{"pr " + ipRepo + ":7": true, "issue jira:ABC-1": true}
	for _, n := range got.Nodes {
		delete(want, n.NodeType+" "+n.NodeID)
	}
	if len(want) != 0 {
		t.Fatalf("missing nodes %v in %+v", want, got.Nodes)
	}

	deeper, err := WorkflowDrilldown(context.Background(), client, "org-1", model.AIWorkflowRootTypeInputPr, ipRepo+":7", 2, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(deeper.Edges) != 2 {
		t.Fatalf("hop 2 edges = %d, want 2 (the issue continues to its other pull request)", len(deeper.Edges))
	}
	found := false
	for _, n := range deeper.Nodes {
		found = found || (n.NodeType == "pr" && n.NodeID == ipRepo+":9")
	}
	if !found {
		t.Fatalf("hop 2 did not reach pr %s:9: %+v", ipRepo, deeper.Nodes)
	}
}

func TestWorkflowEdgeStatement_IssuePRBranchKeepsOnlyTheBestTier(t *testing.T) {
	for _, want := range []string{
		"FROM work_graph_issue_pr FINAL",
		"multiIf(provenance = 'native', 3, provenance = 'explicit_text', 2, provenance = 'heuristic', 1, 0) AS tier_rank",
		"max(tier_rank) OVER (PARTITION BY repo_id, pr_number) AS best_rank",
		"WHERE tier_rank = best_rank",
		"toString(repo_id) AS repo_id",
	} {
		if !strings.Contains(workflowEdgeStatement, want) {
			t.Errorf("the edge union lacks %q", want)
		}
	}
}

func TestWorkflowDrilldown_IssuePRBranchCarriesTheOrgPredicate(t *testing.T) {
	client := &issuePRClient{links: []ipLink{{"jira:ABC-1", 7}}}
	if _, err := WorkflowDrilldown(context.Background(), client, "org-1", model.AIWorkflowRootTypeInputPr, ipRepo+":7", 1, 100); err != nil {
		t.Fatal(err)
	}
	if len(client.statements) != 1 {
		t.Fatalf("statements = %d", len(client.statements))
	}
	st := client.statements[0]
	i := strings.Index(st, "FROM work_graph_issue_pr FINAL")
	if i < 0 {
		t.Fatal("the edge union does not read work_graph_issue_pr")
	}
	branch := st[i:]
	if got := strings.Count(branch, "org_id = {org_id:String}"); got != 2 {
		t.Errorf("the issue-pr branch has %d org predicates, want 2 (outer read and link subquery)", got)
	}
	if v := bindingValue(client.bindings[0], "org_id"); v != "org-1" {
		t.Errorf("org_id = %v", v)
	}
}

func TestWorkflowDrilldown_IssuePRBeyondTheRowCapIsPartial(t *testing.T) {
	var links []ipLink
	for i := 0; i < 1500; i++ {
		links = append(links, ipLink{fmt.Sprintf("jira:ABC-%d", i), 7})
	}
	got, err := WorkflowDrilldown(context.Background(), &issuePRClient{links: links}, "org-1", model.AIWorkflowRootTypeInputPr, ipRepo+":7", 1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Edges) != 1000 || !got.Partial {
		t.Fatalf("edges = %d, partial = %v, want 1000 edges and partial", len(got.Edges), got.Partial)
	}
}
