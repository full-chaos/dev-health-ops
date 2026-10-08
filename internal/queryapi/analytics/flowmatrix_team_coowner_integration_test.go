//go:build integration

package analytics

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

var coOwnerProviders = []string{"jira", "gitlab", "github", "linear"}

// seedFlowMatrixCoOwnedItems writes, for every provider and through the real
// fact loaders, cascade and writer: one item of a project owned by teams A and
// B (active) and C (inactive), in repository shared; and one item of a project
// owned by team B only, in repository other, on the same day.
func seedFlowMatrixCoOwnedItems(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string, day time.Time, shared, other uuid.UUID) {
	t.Helper()
	opened := day.AddDate(0, -1, 0)
	subjects := map[string]teamattribution.GithubWorkItemDerivationSubject{}
	affected := map[string]struct{}{}
	item := func(provider, key string, repo uuid.UUID) {
		id := provider + ":" + key + "-1"
		projectID, projectKey, repoText := "proj-"+key, key, repo.String()
		subjects[id] = teamattribution.GithubWorkItemDerivationSubject{
			WorkItemID: id, Provider: provider, RepoID: &repoText, ProjectKey: &projectKey, ProjectID: &projectID, OrgID: org,
		}
		affected[id] = struct{}{}
		crossorg.Exec(ctx, t, admin, `
INSERT INTO work_item_cycle_times
    (work_item_id, provider, day, work_scope_id, type, status, created_at, started_at, completed_at, cycle_time_hours, lead_time_hours, computed_at, org_id)
VALUES (?, ?, ?, ?, 'story', 'done', ?, ?, ?, 5, 6, ?, ?)`,
			id, provider, day, key, day, day, day.Add(time.Hour), day.Add(2*time.Hour), org)
		crossorg.Exec(ctx, t, admin, `
INSERT INTO work_items (repo_id, work_item_id, provider, title, type, status, status_raw, project_key, project_id, created_at, updated_at, last_synced, org_id)
VALUES (?, ?, ?, 't', 'story', 'done', 'done', ?, ?, ?, ?, ?, ?)`,
			repo, id, provider, key, projectID, day, day, day, org)
	}
	own := func(provider, teamID, key string) {
		crossorg.Exec(ctx, t, admin, `INSERT INTO team_project_ownership
			(org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES (?, ?, ?, ?, ?, 'native', 1, 110, 10, ?, NULL, ?)`,
			org, provider, teamID, "proj-"+key, key, opened, opened)
	}
	for _, provider := range coOwnerProviders {
		key, solo := "F"+strings.ToUpper(provider), "FB"+strings.ToUpper(provider)
		for _, team := range []struct {
			id     string
			active uint8
		}{{"team-a-" + provider, 1}, {"team-b-" + provider, 1}, {"team-c-" + provider, 0}} {
			crossorg.Exec(ctx, t, admin, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES (?, ?, ?, [], [], ?, [], ?, ?, ?, ?, ?, ?)`,
				team.id, uuid.New(), "Team "+team.id, []string{key}, team.active, opened, opened, org, provider, team.id)
			own(provider, team.id, key)
		}
		own(provider, "team-b-"+provider, solo)
		item(provider, key, shared)
		item(provider, solo, other)
	}
	computedAt := day.Add(3 * time.Hour)
	facts, err := remaining.LoadWorkItemDerivationFacts(ctx, admin, org, computedAt)
	if err != nil {
		t.Fatalf("load facts: %v", err)
	}
	rows := remaining.BuildWorkItemAttributionRows(org, computedAt, affected, subjects, teamattribution.NewGitHubWorkItemDerivationContext(facts))
	writer, err := remaining.NewWorkItemAttributionClickHouseWriter(admin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.WriteAttributions(ctx, remaining.WorkItemAttributionProducer{
		Writer: remaining.WorkItemAttributionWriterDaily, RunID: uuid.NewString(),
	}, rows); err != nil {
		t.Fatalf("write attributions: %v", err)
	}
}

func flowMatrixFor(ctx context.Context, t *testing.T, client QueryClient, org string, dimension Dimension, day time.Time) (map[string]float64, map[string]float64) {
	t.Helper()
	nodesQuery, edgesQuery, err := CompileFlowMatrix(FlowMatrixRequest{
		Dimension: dimension, Measure: MeasureCount,
		StartDate: graphqldate.New(day.AddDate(0, 0, -7)), EndDate: graphqldate.New(day.AddDate(0, 0, 7)),
		MaxNodes: 50, MaxEdges: 50,
	}, org, 30, nil)
	if err != nil {
		t.Fatal(err)
	}
	nodes, edges, err := ExecuteFlowMatrix(ctx, client, nodesQuery, edgesQuery)
	if err != nil {
		t.Fatal(err)
	}
	return nodeValues(nodes), edgeValues(edges)
}

func nodeValues(nodes []model.SankeyNode) map[string]float64 {
	result := map[string]float64{}
	for _, node := range nodes {
		if node.Value != nil {
			result[node.ID] = *node.Value
		}
	}
	return result
}

func edgeValues(edges []model.SankeyEdge) map[string]float64 {
	result := map[string]float64{}
	for _, edge := range edges {
		if edge.Value != nil {
			result[edge.Source+">"+edge.Target] = *edge.Value
		}
	}
	return result
}

func sortedKeys(values map[string]float64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, fmt.Sprintf("%s=%v", key, values[key]))
	}
	sort.Strings(keys)
	return keys
}

// The TEAM and REPO flow-matrix dimensions read the team-scoped source. Teams A
// and B co-own one item (repository shared); team B alone owns a second item
// (repository other) on the same day; team C is inactive. Every provider, rows
// written by the real producer.
//   - TEAM nodes: A counts 1 item, B counts 2, C has no node.
//   - TEAM edges: A and B meet on the co-owned item's scope: one edge each way.
//   - REPO edges: team B links the two repositories through the co-owned item
//     (team A works in one repository only): one edge each way, one item per
//     provider.
func TestTheTeamFlowMatrixCountsAnItemOfAProjectOfTwoTeamsInEachTeamNode(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	org := crossorg.Default().OrgA
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	shared, other := uuid.New(), uuid.New()
	seedFlowMatrixCoOwnedItems(ctx, t, admin, org, day, shared, other)

	nodes, edges := flowMatrixFor(ctx, t, client, org, DimensionTeam, day)
	wantNodes, wantEdges := map[string]float64{}, map[string]float64{}
	for _, provider := range coOwnerProviders {
		a, b := "TEAM:team-a-"+provider, "TEAM:team-b-"+provider
		wantNodes[a], wantNodes[b] = 1, 2
		wantEdges[a+">"+b], wantEdges[b+">"+a] = 1, 1
	}
	if !reflect.DeepEqual(nodes, wantNodes) {
		t.Errorf("team nodes = %v, want %v", sortedKeys(nodes), sortedKeys(wantNodes))
	}
	if !reflect.DeepEqual(edges, wantEdges) {
		t.Errorf("team edges = %v, want %v", sortedKeys(edges), sortedKeys(wantEdges))
	}

	_, repoEdges := flowMatrixFor(ctx, t, client, org, DimensionRepo, day)
	s, o := "REPO:"+shared.String(), "REPO:"+other.String()
	wantRepoEdges := map[string]float64{s + ">" + o: float64(len(coOwnerProviders)), o + ">" + s: float64(len(coOwnerProviders))}
	if !reflect.DeepEqual(repoEdges, wantRepoEdges) {
		t.Errorf("repo edges = %v, want %v", sortedKeys(repoEdges), sortedKeys(wantRepoEdges))
	}
}
