//go:build integration

package daily

import (
	"context"
	"reflect"
	"testing"
	"time"
)

// The daily rollups keep one team per item: their attribution readers read
// the primary row (is_primary = 1) only. A co-owner row (is_primary = 2) of
// a second team does not change what they read: the same store with and
// without it gives the same attributions, through both readers.
func TestTheDailyAttributionReadersIgnoreACoOwnerRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)

	at := scopeReadDay.Add(12 * time.Hour)
	for _, org := range []string{scopeReadOrgA, scopeReadOrgB} {
		seedScopeReadItems(t, ctx, conn,
			scopeReadItem{org: org, repo: scopeReadRepoA, id: "it-1", projectID: "shared", storyPoints: 1},
		)
		seedScopeReadAttribution(t, ctx, conn, org, scopeReadRepoA, "it-1", "team-a")
	}
	if err := conn.Exec(ctx, `
INSERT INTO work_item_team_attributions
    (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at)
VALUES (?, ?, 'it-1', 'github', 'team-b', 'team-b', 'issue_project', 2, 'high', 'issue_project_key=KEY', ?)`,
		scopeReadOrgA, scopeReadRepoA, at); err != nil {
		t.Fatal(err)
	}

	with := readWorkItemScope(t, ctx, conn, scopeReadOrgA, scopeReadRepoA).Attributions
	without := readWorkItemScope(t, ctx, conn, scopeReadOrgB, scopeReadRepoA).Attributions
	if len(with) != 1 || with["it-1"].TeamID != "team-a" {
		t.Errorf("scope read with a co-owner row = %+v, want it-1 = team-a", with)
	}
	if !reflect.DeepEqual(with, without) {
		t.Errorf("scope read with a co-owner row = %+v, without = %+v", with, without)
	}

	primaryWith, err := LoadWorkItemPrimaryTeamAttributions(ctx, conn, scopeReadOrgA, scopeReadRepoA)
	if err != nil {
		t.Fatal(err)
	}
	primaryWithout, err := LoadWorkItemPrimaryTeamAttributions(ctx, conn, scopeReadOrgB, scopeReadRepoA)
	if err != nil {
		t.Fatal(err)
	}
	if len(primaryWith) != 1 || primaryWith["it-1"].TeamID != "team-a" {
		t.Errorf("primary attributions with a co-owner row = %+v, want it-1 = team-a", primaryWith)
	}
	if !reflect.DeepEqual(primaryWith, primaryWithout) {
		t.Errorf("primary attributions with a co-owner row = %+v, without = %+v", primaryWith, primaryWithout)
	}
}
