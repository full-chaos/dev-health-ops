//go:build integration

package quadrant

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

var coOwnerProviders = []string{"jira", "gitlab", "github", "linear"}

// seedCoOwnedItemsThroughTheProducer writes, for every provider, one completed
// item of a project owned by teams A and B (active) and C (inactive), and its
// attribution rows through the real fact loaders, cascade and writer.
func seedCoOwnedItemsThroughTheProducer(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string, day time.Time) {
	t.Helper()
	opened := day.AddDate(0, -1, 0)
	repoID := uuid.NewString()
	subjects := map[string]teamattribution.GithubWorkItemDerivationSubject{}
	affected := map[string]struct{}{}
	for _, provider := range coOwnerProviders {
		key := "Q" + strings.ToUpper(provider)
		for _, team := range []struct {
			id     string
			active uint8
		}{{"team-a-" + provider, 1}, {"team-b-" + provider, 1}, {"team-c-" + provider, 0}} {
			crossorg.Exec(ctx, t, admin, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES (?, ?, ?, [], [], ?, [], ?, ?, ?, ?, ?, ?)`,
				team.id, uuid.New(), "Team "+team.id, []string{key}, team.active, opened, opened, org, provider, team.id)
			crossorg.Exec(ctx, t, admin, `INSERT INTO team_project_ownership
				(org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
				VALUES (?, ?, ?, ?, ?, 'native', 1, 110, 10, ?, NULL, ?)`,
				org, provider, team.id, "proj-"+provider, key, opened, opened)
		}
		id := provider + ":" + key + "-1"
		projectID, projectKey, repo := "proj-"+provider, key, repoID
		subjects[id] = teamattribution.GithubWorkItemDerivationSubject{
			WorkItemID: id, Provider: provider, RepoID: &repo, ProjectKey: &projectKey, ProjectID: &projectID, OrgID: org,
		}
		affected[id] = struct{}{}
		crossorg.Exec(ctx, t, admin, `
INSERT INTO work_item_cycle_times
    (work_item_id, provider, day, work_scope_id, type, status, created_at, started_at, completed_at, cycle_time_hours, lead_time_hours, computed_at, org_id)
VALUES (?, ?, ?, ?, 'story', 'done', ?, ?, ?, 5, 6, ?, ?)`,
			id, provider, day, key, day, day, day.Add(time.Hour), day.Add(2*time.Hour), org)
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

// The team cycle/throughput quadrant plots one point per team. An item of a
// project of teams A and B counts for both teams; the inactive team C has no
// point. Every provider, rows written by the real producer.
func TestTheTeamQuadrantCountsAnItemOfAProjectOfTwoTeamsForEachTeam(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	org := crossorg.Default().OrgA
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	seedCoOwnedItemsThroughTheProducer(ctx, t, admin, org, day)
	for _, metric := range []string{"throughput", "cycle_time"} {
		t.Run(metric, func(t *testing.T) {
			rows, err := fetchWorkItemTeamQuadrantMetric(ctx, client, metric, day.AddDate(0, 0, -7), day.AddDate(0, 0, 7), "week", org)
			if err != nil {
				t.Fatal(err)
			}
			got := map[string]float64{}
			for _, row := range rows {
				got[row.EntityID] += row.Value
			}
			var want []string
			for _, provider := range coOwnerProviders {
				want = append(want, "team-a-"+provider, "team-b-"+provider)
			}
			sort.Strings(want)
			var teams []string
			for team := range got {
				teams = append(teams, team)
			}
			sort.Strings(teams)
			if strings.Join(teams, ",") != strings.Join(want, ",") {
				t.Fatalf("%s points by team = %v, want one point for each of %v", metric, got, want)
			}
			wantValue := 1.0
			if metric == "cycle_time" {
				wantValue = 5
			}
			for team, value := range got {
				if value != wantValue {
					t.Errorf("%s of %s = %v, want %v", metric, team, value, wantValue)
				}
			}
		})
	}
}
