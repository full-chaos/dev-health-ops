//go:build integration

package remaining

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

// A scoped backstop run of a repository whose ownership changed re-derives the
// issues linked to that repository in work_graph_issue_pr, not only the items
// whose own repo_id is the repository: the issue's team is the repository's
// owner. Fillers keep the run scoped.
func TestABackstopRunOfAChangedRepoRederivesTheIssuesLinkedToIt(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	executor, err := NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}

	orgID := "org-issue-linked-pr-" + uuid.NewString()
	repoX := uuid.New()
	now := time.Now().UTC()

	seedWorkItemAttributionPullRequest(t, ctx, conn, orgID, "ghpr:acme/x#1", repoX, now)
	if err := conn.Exec(ctx, `INSERT INTO work_items (repo_id, work_item_id, provider, type, project_id, project_key, org_id, last_synced)
		VALUES (?, 'jira:LI-1', 'jira', 'story', 'LI', 'LI', ?, ?)`, uuid.Nil, orgID, now); err != nil {
		t.Fatalf("seed issue: %v", err)
	}
	for i := 0; i < 18; i++ {
		seedWorkItemAttributionItem(t, ctx, conn, orgID, fmt.Sprintf("filler-%d", i), uuid.Nil, now)
	}
	seedWorkItemAttributionRepoOwnership(t, ctx, conn, orgID, repoX, "team-infra", now)
	if err := conn.Exec(ctx, `INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)
		VALUES (?, 'jira:LI-1', 1, 1.0, 'native', 'cell', ?, ?)`, repoX, now, orgID); err != nil {
		t.Fatalf("seed work_graph_issue_pr: %v", err)
	}

	waitForWorkItemAttributionRepoFactVisible(t, ctx, conn, orgID)
	outcome, err := executor.ComputeOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("ComputeOrg: %v", err)
	}
	if outcome.OrgWide {
		t.Fatalf("outcome.OrgWide = true, want a scoped run (outcome=%+v)", outcome)
	}

	rows, err := conn.Query(ctx, `
SELECT work_item_id, ifNull(team_id, ''), toString(source), is_primary
FROM work_item_team_attributions FINAL WHERE org_id = ? ORDER BY work_item_id, source`, orgID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var got []string
	for rows.Next() {
		var id, team, source string
		var primary uint8
		if err := rows.Scan(&id, &team, &source, &primary); err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("%s|%s|%s|%d", id, source, team, primary))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	t.Logf("outcome %+v rows %v", outcome, got)
	held := map[string]bool{}
	for _, line := range got {
		held[line] = true
	}
	for _, want := range []string{"ghpr:acme/x#1|repo_ownership|team-infra|1", "jira:LI-1|repo_ownership|team-infra|1"} {
		if !held[want] {
			t.Errorf("no row %s in %v", want, got)
		}
	}
	for _, line := range got {
		if len(line) > 7 && line[:7] == "filler-" {
			t.Errorf("a filler was written in a scoped run: %s", line)
		}
	}
}
