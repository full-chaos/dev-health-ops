//go:build integration

package aianalytics

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-9240: a pull request with no AI edge walks to its issue through
// work_graph_issue_pr, over the best provenance tier only, inside the org.
func TestRealClickHouse_WorkflowDrilldownWalksTheIssueLink(t *testing.T) {
	ctx, conn, client := startStore(t)
	link := func(org, workItem string, number int, provenance string) {
		exec(t, ctx, conn, `INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)
            SELECT '%s', '%s', %d, 0.9, '%s', 'x', now64(3), '%s'`, rA, workItem, number, provenance, org)
	}
	link(org1, "jira:ABC-1", 7, "native")
	link(org1, "jira:ABC-2", 7, "heuristic")
	link(org1, "jira:ABC-1", 9, "explicit_text")
	link(org2, "jira:LEAK-1", 7, "native")

	got, err := WorkflowDrilldown(ctx, client, org1, model.AIWorkflowRootTypeInputPr, rA+":7", 1, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !got.DataAvailable || len(got.Edges) != 1 || got.Edges[0].TargetID != "jira:ABC-1" || got.Edges[0].Source != "native" {
		t.Fatalf("hop 1: %+v", got.Edges)
	}

	deeper, err := WorkflowDrilldown(ctx, client, org1, model.AIWorkflowRootTypeInputPr, rA+":7", 2, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(deeper.Edges) != 2 {
		t.Fatalf("hop 2: %+v", deeper.Edges)
	}
	for _, e := range deeper.Edges {
		if e.TargetID == "jira:LEAK-1" || e.TargetID == "jira:ABC-2" {
			t.Fatalf("a lower tier or another org's link was served: %+v", e)
		}
	}
}

func TestRealClickHouse_UnlinkedPullRequestChipsAreHidden(t *testing.T) {
	ctx, conn, client := startStore(t)
	link := func(org, workItem string, number int, provenance string) {
		exec(t, ctx, conn, `INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)
            SELECT '%s', '%s', %d, 0.9, '%s', 'x', now64(3), '%s'`, rA, workItem, number, provenance, org)
	}
	link(org1, "jira:ABC-1", 7, "heuristic")
	link(org2, "jira:LEAK-1", 8, "native")

	chip := func(number string) model.AIWorkGraphDrilldownRef {
		return model.AIWorkGraphDrilldownRef{RootType: "pr", RootID: rA + ":" + number, Label: "PR " + number}
	}
	recs := []model.AIOpportunity{{WorkGraphDrilldowns: []model.AIWorkGraphDrilldownRef{chip("7"), chip("8"), chip("9")}}}
	if err := hideUnlinkedPullRequestChips(ctx, client, org1, recs); err != nil {
		t.Fatal(err)
	}
	if got := recs[0].WorkGraphDrilldowns; len(got) != 1 || got[0].RootID != rA+":7" {
		t.Fatalf("chips = %+v, want only PR 7 (linked in this organization at any tier); 8 is linked in another organization, 9 has no link", got)
	}
}
