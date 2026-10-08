//go:build integration

package aggflame

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

var coOwnerItems = map[string]string{
	"github:acme/api#1": "github",
	"gitlab:acme/api#2": "gitlab",
	"jira:OPS-3":        "jira",
	"linear:ENG-4":      "linear",
}

// seedCoOwnedItems writes one completed item per provider and the attribution
// rows the cascade writes for a project of teams A and B: A primary (1), B
// co-owner (2) when withCoOwner, and both ownership rows (0). An inactive
// team C has no row.
func seedCoOwnedItems(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string, withCoOwner bool) {
	t.Helper()
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	computed := day.Add(2 * time.Hour)
	repoID := uuid.New()
	attribution := func(item, provider, team, source string, isPrimary uint8) {
		crossorg.Exec(ctx, t, admin, `
INSERT INTO work_item_team_attributions
    (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'high', ?, ?)`,
			org, repoID, item, provider, team, "Team "+team, source, isPrimary, source+"=KEY", computed)
	}
	for item, provider := range coOwnerItems {
		crossorg.Exec(ctx, t, admin, `
INSERT INTO work_item_cycle_times
    (work_item_id, provider, day, work_scope_id, type, status, created_at, started_at, completed_at, cycle_time_hours, lead_time_hours, computed_at, org_id)
VALUES (?, ?, ?, 'KEY', 'story', 'done', ?, ?, ?, 5, 6, ?, ?)`,
			item, provider, day, day, day, day.Add(time.Hour), computed, org)
		attribution(item, provider, "team-a", "issue_project", 1)
		attribution(item, provider, "team-a", "project_ownership", 0)
		attribution(item, provider, "team-b", "project_ownership", 0)
		if withCoOwner {
			attribution(item, provider, "team-b", "issue_project", 2)
		}
	}
}

func throughputByTeam(t *testing.T, rows []throughputRow) []string {
	t.Helper()
	result := []string{}
	for _, row := range rows {
		result = append(result, fmt.Sprintf("%s/%s=%v", row.WorkType, row.TeamName, row.ItemsCompleted))
	}
	sort.Strings(result)
	return result
}

// Throughput of a project of teams A and B: each team's flame counts the
// items; the inactive team C counts none; the organization flame counts each
// item once and equals the flame of the same store without co-owner rows.
// Both readers (all work, by work type).
func TestThroughputOfAProjectOfTwoTeamsCountsInEachTeamAndOnceInTheOrg(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()
	seedCoOwnedItems(ctx, t, admin, f.OrgA, true)
	seedCoOwnedItems(ctx, t, admin, f.OrgB, false)
	start, end := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	readers := map[string]func(org, team string) ([]throughputRow, error){
		"throughput": func(org, team string) ([]throughputRow, error) {
			return fetchThroughput(ctx, client, org, start, end, team, 50)
		},
		"throughput_by_type": func(org, team string) ([]throughputRow, error) {
			return fetchThroughputByType(ctx, client, org, start, end, team, 50)
		},
	}
	items := float64(len(coOwnerItems))
	for name, read := range readers {
		t.Run(name, func(t *testing.T) {
			get := func(org, team string) []string {
				t.Helper()
				rows, err := read(org, team)
				if err != nil {
					t.Fatal(err)
				}
				return throughputByTeam(t, rows)
			}
			workType := "All"
			if name == "throughput_by_type" {
				workType = "story"
			}
			for _, team := range []string{"team-a", "team-b"} {
				want := []string{fmt.Sprintf("%s/Team %s=%v", workType, team, items)}
				if got := get(f.OrgA, team); !reflect.DeepEqual(got, want) {
					t.Errorf("%s flame = %v, want %v", team, got, want)
				}
			}
			if got := get(f.OrgA, "team-c"); len(got) != 0 {
				t.Errorf("inactive team-c flame = %v, want empty", got)
			}
			orgWith := get(f.OrgA, "")
			want := []string{fmt.Sprintf("%s/Team team-a=%v", workType, items)}
			if !reflect.DeepEqual(orgWith, want) {
				t.Errorf("org flame = %v, want %v (each item once)", orgWith, want)
			}
			if orgWithout := get(f.OrgB, ""); !reflect.DeepEqual(orgWith, orgWithout) {
				t.Errorf("org flame with co-owner rows = %v, without = %v", orgWith, orgWithout)
			}
		})
	}
}
