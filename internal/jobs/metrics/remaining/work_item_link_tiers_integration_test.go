//go:build integration

package remaining

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// T1: the tier of a pair is its best recorded tier. A pair with a native row
// and an explicit_text row (two physical rows, never merged) gives native; a
// pair with only a lower tier gives that tier; a link of another organization
// gives nothing; an edge whose pair has no row is absent from the map.
func TestLoadWorkItemLinkTiersGivesTheBestTierOfAPair(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	org, otherOrg := "org-link-tiers-"+uuid.NewString(), "org-link-tiers-"+uuid.NewString()
	repo := uuid.New()
	at := time.Now().UTC().Add(-time.Hour)
	for _, id := range []string{"ghpr:acme/web#7", "gitlab:acme/app!8"} {
		provider := "github"
		if id[:6] == "gitlab" {
			provider = "gitlab"
		}
		if err := conn.Exec(ctx, `INSERT INTO work_items (repo_id, work_item_id, provider, type, project_id, created_at, org_id, last_synced) VALUES (?, ?, ?, 'pr', '', ?, ?, ?)`,
			repo, id, provider, at, org, at); err != nil {
			t.Fatal(err)
		}
	}
	insert := func(orgID, issue string, number uint32, provenance string, syncedAt time.Time) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id) VALUES (?, ?, ?, 1, ?, 'e', ?, ?)`,
			repo, issue, number, provenance, syncedAt, orgID); err != nil {
			t.Fatal(err)
		}
	}
	insert(org, "jira:BOTH-1", 7, "native", at.Add(-time.Hour))
	insert(org, "jira:BOTH-1", 7, "explicit_text", at)
	insert(org, "jira:TEXT-1", 7, "explicit_text", at)
	insert(org, "jira:HEUR-1", 7, "heuristic", at)
	insert(org, "jira:MR-1", 8, "native", at)
	insert(otherOrg, "jira:OTHER-1", 7, "native", at)
	edge := func(source, target string) teamattribution.GithubWorkItemDerivationDependencyEdge {
		return teamattribution.GithubWorkItemDerivationDependencyEdge{SourceWorkItemID: source, TargetWorkItemID: target, RelationshipType: "relates_to", LastSynced: at}
	}
	edges := []teamattribution.GithubWorkItemDerivationDependencyEdge{
		edge("ghpr:acme/web#7", "jira:BOTH-1"), edge("ghpr:acme/web#7", "jira:TEXT-1"), edge("ghpr:acme/web#7", "jira:HEUR-1"),
		edge("ghpr:acme/web#7", "jira:NONE-1"), edge("ghpr:acme/web#7", "jira:OTHER-1"), edge("gitlab:acme/app!8", "jira:MR-1"),
	}
	tiers, err := LoadWorkItemLinkTiers(ctx, conn, org, edges)
	if err != nil {
		t.Fatal(err)
	}
	want := map[teamattribution.LinkPair]string{
		{PullRequest: "ghpr:acme/web#7", Issue: "jira:BOTH-1"}: "native",
		{PullRequest: "ghpr:acme/web#7", Issue: "jira:TEXT-1"}: "explicit_text",
		{PullRequest: "ghpr:acme/web#7", Issue: "jira:HEUR-1"}: "heuristic",
		{PullRequest: "gitlab:acme/app!8", Issue: "jira:MR-1"}: "native",
	}
	if len(tiers) != len(want) {
		t.Fatalf("tiers %v, want %v", tiers, want)
	}
	for pair, tier := range want {
		if tiers[pair] != tier {
			t.Errorf("pair %+v tier %q, want %q", pair, tiers[pair], tier)
		}
	}
}
