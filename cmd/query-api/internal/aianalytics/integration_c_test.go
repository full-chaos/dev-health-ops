//go:build integration

package aianalytics

import (
	"context"
	"testing"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// seedGroupC adds governance and workflow rows. Every org-1 row has an org-2
// twin sharing its keys with different content.
func seedGroupC(t *testing.T, ctx context.Context, conn chdriver.Conn) {
	cov := func(org, team, repo, day string, ai, declared int, computed string) {
		teamSQL, repoSQL := "NULL", "NULL"
		if team != "" {
			teamSQL = "'" + team + "'"
		}
		if repo != "" {
			repoSQL = "'" + repo + "'"
		}
		exec(t, ctx, conn, `INSERT INTO ai_governance_coverage_daily (org_id, team_id, repo_id, day, ai_artifacts, declared_artifacts,
            human_reviewed_prs, security_scanned_prs, in_policy_artifacts, computed_at)
            SELECT '%s', %s, %s, '%s', %d, %d, 1, 1, %d, toDateTime64('%s', 3, 'UTC')`, org, teamSQL, repoSQL, day, ai, declared, ai, computed)
	}
	cov(org1, "team-a", rA, "2026-08-01", 99, 99, "2026-08-01 00:00:00") // stale version
	cov(org1, "team-a", rA, "2026-08-01", 4, 3, "2026-08-02 00:00:00")
	cov(org1, "", "", "2026-08-02", 0, 0, "2026-08-02 00:00:00")
	cov(org1, "team-a", rA, "2026-07-01", 7, 7, "2026-08-02 00:00:00") // outside the window
	cov(org2, "team-a", rA, "2026-08-01", 1000, 1000, "2026-08-02 00:00:00")

	ev := func(org, team, repo, rule, observed string) {
		teamSQL, repoSQL := "NULL", "NULL"
		if team != "" {
			teamSQL = "'" + team + "'"
		}
		if repo != "" {
			repoSQL = "'" + repo + "'"
		}
		exec(t, ctx, conn, `INSERT INTO ai_policy_events (event_id, org_id, team_id, repo_id, rule_id, severity, subject_type, subject_id, observed_at, evidence, computed_at)
            SELECT generateUUIDv4(), '%s', %s, %s, '%s', 'high', 'pull_request', '7', toDateTime64('%s', 3, 'UTC'), '', now64(3)`, org, teamSQL, repoSQL, rule, observed)
	}
	ev(org1, "team-a", rA, "r-old", "2026-08-01 10:00:00")
	ev(org1, "", "", "r-new", "2026-08-02 10:00:00")
	ev(org2, "team-a", rA, "r-other-org", "2026-08-02 11:00:00")

	// workflow: issue ABC-1 -> run-1 -> pull_request r:7 -> deployment d1 -> incident i1 (org-1),
	// with org-2 twins sharing edge ids and node ids.
	for _, o := range []struct{ org, run, art string }{{org1, "run-1", "r:7"}, {org2, "run-1", "leak-pr"}, {org2, "run-2", "leak-2"}} {
		exec(t, ctx, conn, `INSERT INTO ai_workflow_artifact_edges (edge_id, org_id, run_id, artifact_type, artifact_id, provider, repo_id, confidence, source, evidence, observed_at, computed_at)
            SELECT 'art-%s', '%s', '%s', 'pull_request', '%s', 'github', NULL, 0.5, 's', '', now64(3), now64(3)`, o.art, o.org, o.run, o.art)
	}
	for _, o := range []struct{ org, issue, run string }{{org1, "ABC-1", "run-1"}, {org2, "ABC-1", "run-2"}} {
		exec(t, ctx, conn, `INSERT INTO ai_workflow_issue_edges (edge_id, org_id, issue_id, run_id, provider, repo_id, confidence, source, evidence, observed_at, computed_at)
            SELECT 'iss-1', '%s', '%s', '%s', 'linear', '%s', 0.9, 's', '', now64(3), now64(3)`, o.org, o.issue, o.run, rA)
	}
	exec(t, ctx, conn, `INSERT INTO work_graph_pr_deployment_edges (edge_id, org_id, pr_id, deployment_id, provider, repo_id, confidence, source, evidence, observed_at, computed_at)
        SELECT 'dep-1', '%s', 'r:7', 'd1', 'github', NULL, 0.7, 's', '', now64(3), now64(3)`, org1)
	exec(t, ctx, conn, `INSERT INTO work_graph_deployment_incident_edges (edge_id, org_id, deployment_id, incident_id, provider, repo_id, confidence, source, evidence, observed_at, computed_at)
        SELECT 'inc-1', '%s', 'd1', 'i1', 'github', NULL, 0.7, 's', '', now64(3), now64(3)`, org1)
	exec(t, ctx, conn, `INSERT INTO work_graph_pr_deployment_edges (edge_id, org_id, pr_id, deployment_id, provider, repo_id, confidence, source, evidence, observed_at, computed_at)
        SELECT 'dep-x', '%s', 'r:7', 'dx', 'github', NULL, 0.7, 's', '', now64(3), now64(3)`, org2)
}

func TestRealClickHouse_AIGroupC(t *testing.T) {
	ctx, conn, client := startStore(t)
	seedStore(t, ctx, conn)
	seedGroupC(t, ctx, conn)
	dr := dateRange()

	t.Run("governance reads only the org's newest rows", func(t *testing.T) {
		got, err := GovernanceSummary(ctx, client, org1, dr, nil, 50)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Coverage) != 2 || got.Coverage[0].AiArtifacts != 4 || got.Coverage[0].DeclaredArtifacts != 3 {
			t.Fatalf("coverage %+v", got.Coverage)
		}
		if got.Coverage[1].DeclarationCoverage != 1.0 || got.Coverage[1].TeamID != nil || got.Coverage[1].RepoID != nil {
			t.Fatalf("empty-day coverage %+v", got.Coverage[1])
		}
		if got.Coverage[0].RepoID == nil || *got.Coverage[0].RepoID != rA {
			t.Fatalf("repo id %+v", got.Coverage[0])
		}
		if len(got.RecentViolations) != 2 || got.RecentViolations[0].RuleID != "r-new" || got.RecentViolations[0].Evidence != "{}" {
			t.Fatalf("violations %+v", got.RecentViolations)
		}
		other, err := GovernanceSummary(ctx, client, org2, dr, nil, 50)
		if err != nil {
			t.Fatal(err)
		}
		if len(other.Coverage) != 1 || other.Coverage[0].AiArtifacts != 1000 || len(other.RecentViolations) != 1 {
			t.Fatalf("org2 %+v", other)
		}
	})

	t.Run("governance scope and limit branches", func(t *testing.T) {
		team, none, repo := "team-a", "team-none", rA
		for name, c := range map[string]struct {
			scope       *model.AIScopeInput
			limit       int
			cov, violat int
		}{
			"team":         {&model.AIScopeInput{TeamID: &team}, 50, 1, 1},
			"unknown team": {&model.AIScopeInput{TeamID: &none}, 50, 0, 0},
			"repo":         {&model.AIScopeInput{RepoID: &repo}, 50, 1, 1},
			"limit one":    {nil, 1, 2, 1},
			"limit zero":   {nil, 0, 2, 0},
			"limit below":  {nil, -3, 2, 0},
		} {
			got, err := GovernanceSummary(ctx, client, org1, dr, c.scope, c.limit)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if len(got.Coverage) != c.cov || len(got.RecentViolations) != c.violat {
				t.Errorf("%s: coverage %d violations %d, want %d %d", name, len(got.Coverage), len(got.RecentViolations), c.cov, c.violat)
			}
		}
	})

	t.Run("workflow walk stays inside the org", func(t *testing.T) {
		got, err := WorkflowDrilldown(ctx, client, org1, model.AIWorkflowRootTypeInputIssue, "ABC-1", 4, 100)
		if err != nil {
			t.Fatal(err)
		}
		ids := map[string]bool{}
		for _, e := range got.Edges {
			ids[e.EdgeID] = true
		}
		for _, want := range []string{"iss-1", "art-r:7", "dep-1", "inc-1"} {
			if !ids[want] {
				t.Errorf("missing edge %s: %+v", want, got.Edges)
			}
		}
		if len(got.Edges) != 4 || got.Partial || !got.DataAvailable || got.RootType != "issue" {
			t.Fatalf("walk %+v", got)
		}
		for _, n := range got.Nodes {
			if n.NodeID == "leak-pr" || n.NodeID == "leak-2" || n.NodeID == "dx" || n.NodeID == "run-2" {
				t.Errorf("another org's node reached: %+v", n)
			}
		}
		shallow, err := WorkflowDrilldown(ctx, client, org1, model.AIWorkflowRootTypeInputIssue, "ABC-1", 1, 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(shallow.Edges) != 1 {
			t.Fatalf("depth 1 = %+v", shallow.Edges)
		}
		limited, err := WorkflowDrilldown(ctx, client, org1, model.AIWorkflowRootTypeInputIssue, "ABC-1", 4, 2)
		if err != nil {
			t.Fatal(err)
		}
		// a re-read edge consumes the row budget, so the walk can stop with one edge
		if len(limited.Edges) < 1 || len(limited.Edges) > 2 || !limited.Partial {
			t.Fatalf("limit 2 = %+v", limited)
		}
		unit, err := WorkflowDrilldown(ctx, client, org1, model.AIWorkflowRootTypeInputWorkUnit, "ABC-1", 1, 100)
		if err != nil {
			t.Fatal(err)
		}
		if unit.RootType != "work_unit" || len(unit.Edges) != 1 {
			t.Fatalf("work unit %+v", unit)
		}
		pr, err := WorkflowDrilldown(ctx, client, org1, model.AIWorkflowRootTypeInputPr, "r:7", 2, 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(pr.Edges) < 3 {
			t.Fatalf("pr root %+v", pr.Edges)
		}
	})
}
