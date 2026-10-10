//go:build integration

package home

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/repouser"
)

// The Home summary sentence names the drivers of the biggest move by their
// display names, never by the id a metrics row carries (CHAOS-9046): an id with
// no name is left out, an inactive team is not a driver. Rows are written the
// way the producers write them (a team id of every provider shape in the
// work-item metrics, repository ids in the repository metrics); the response
// is home.BuildResponse on a real ClickHouse.

// idShape finds what an id looks like in a served sentence: a provider-keyed id
// (`github:`, `gitlab:`, `jira:`, `linear:`, `custom:`) or a bare uuid.
var idShape = regexp.MustCompile(`(?i)\b(github|gitlab|jira|linear|custom):[^\s,.]+|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)

type driverTeam struct {
	id     string
	name   string // "" = no teams row
	active bool
}

func seedTeamDrivers(ctx context.Context, t *testing.T, admin interface {
	Exec(context.Context, string, ...any) error
}, org string, teams []driverTeam, prior, current time.Time) {
	t.Helper()
	computedAt := current.Add(24 * time.Hour)
	for _, team := range teams {
		for _, row := range []struct {
			day   time.Time
			cycle float64
		}{{prior, 24}, {current, 48}} {
			if err := admin.Exec(ctx, `INSERT INTO work_item_metrics_daily
(org_id, day, provider, work_scope_id, team_id, team_name, items_started, items_completed,
 items_started_unassigned, items_completed_unassigned, wip_count_end_of_day, wip_unassigned_end_of_day,
 cycle_time_p50_hours, cycle_time_p90_hours, lead_time_p50_hours, lead_time_p90_hours,
 wip_age_p50_hours, wip_age_p90_hours, bug_completed_ratio, story_points_completed,
 new_bugs_count, new_items_count, defect_intro_rate, wip_congestion_ratio, predictability_score, computed_at)
VALUES (?, ?, 'github', 'scope', ?, '', 1, 1, 0, 0, 1, 0, ?, ?, ?, ?, 1, 1, 0, 0, 0, 0, 0, 0.5, 0.5, ?)`,
				org, row.day, team.id, row.cycle, row.cycle, row.cycle, row.cycle, computedAt); err != nil {
				t.Fatal(err)
			}
		}
		if team.name != "" {
			active := uint8(0)
			if team.active {
				active = 1
			}
			if err := admin.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, repo_patterns, updated_at, org_id, provider, is_active)
VALUES (?, generateUUIDv4(), ?, [], [], ?, ?, 'github', ?)`, team.id, team.name, computedAt, org, active); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestHomeSummaryNamesTheTeamDriversAndNeverPrintsAnID(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	f := DefaultFilters()
	startDay, _, compareStart, _, err := TimeWindow(f, now)
	if err != nil {
		t.Fatal(err)
	}
	jiraID := "jira:" + uuid.NewString()
	cases := []struct {
		name         string
		org          string
		teams        []driverTeam
		wantNames    []string
		wantNotNames []string
	}{
		{"github gitlab jira, all named", "summary-names-a", []driverTeam{
			{"github:acme/ops", "Ops", true}, {"gitlab:grp/web", "Web", true}, {jiraID, "Payments", true}},
			[]string{"Ops", "Web", "Payments"}, nil},
		{"linear named, custom with no name, an inactive team", "summary-names-b", []driverTeam{
			{"linear:ENG", "Engineering", true}, {"custom:platform", "", false}, {"jira:" + uuid.NewString(), "Retired", false}},
			[]string{"Engineering"}, []string{"Retired"}},
		{"no driver has a name: the sentence names none", "summary-names-d", []driverTeam{
			{"custom:a", "", false}, {"linear:B", "", false}, {"jira:" + uuid.NewString(), "", false}},
			nil, nil},
		{"two teams with one name are named once", "summary-names-c", []driverTeam{
			{"linear:ENG", "Engineering", true}, {"gitlab:eng/mirror", "Engineering", true}, {"github:acme/ops", "Ops", true}},
			[]string{"Engineering", "Ops"}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seedTeamDrivers(ctx, t, admin, tc.org, tc.teams, compareStart, startDay)
			resp, err := BuildResponse(ctx, client, nil, tc.org, f, now)
			if err != nil {
				t.Fatal(err)
			}
			if len(resp.Summary) == 0 {
				t.Fatalf("no summary sentence")
			}
			text := resp.Summary[0].Text
			t.Logf("summary: %s", text)
			if loc := idShape.FindString(text); loc != "" {
				t.Errorf("the sentence prints an id %q: %q", loc, text)
			}
			for _, team := range tc.teams {
				if strings.Contains(text, team.id) {
					t.Errorf("the sentence prints the id %q: %q", team.id, text)
				}
			}
			if len(tc.wantNames) == 0 && strings.Contains(text, "driven by") {
				t.Errorf("no driver has a name, so the sentence names none: %q", text)
			}
			for _, name := range tc.wantNames {
				if !strings.Contains(text, name) {
					t.Errorf("the sentence lacks the name %q: %q", name, text)
				}
			}
			for _, name := range tc.wantNames {
				if strings.Count(text, name) != 1 {
					t.Errorf("the name %q is not named exactly once: %q", name, text)
				}
			}
			for _, name := range tc.wantNotNames {
				if strings.Contains(text, name) {
					t.Errorf("the sentence names %q, which is not an active driver: %q", name, text)
				}
			}
		})
	}
}

func TestHomeSummaryNamesTheRepositoryDriversAndNeverPrintsAnID(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)
	writer, err := repouser.NewWriter(admin)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	f := DefaultFilters()
	startDay, _, compareStart, _, err := TimeWindow(f, now)
	if err != nil {
		t.Fatal(err)
	}
	const org = "summary-names-repos"
	named, unnamed := uuid.New(), uuid.New()
	computedAt := now.Add(-time.Hour)
	for repo, churn := range map[uuid.UUID]int{named: 100, unnamed: 100} {
		if _, _, _, err := writer.WriteResult(ctx, repouser.Result{RepoMetrics: []repouser.RepoMetric{
			{RepoID: repo, Day: compareStart, CommitsCount: 3, TotalLOCTouched: churn, ComputedAt: computedAt},
			{RepoID: repo, Day: startDay, CommitsCount: 3, TotalLOCTouched: churn * 3, ComputedAt: computedAt},
		}}, org); err != nil {
			t.Fatal(err)
		}
	}
	if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider)
VALUES (?, 'acme/checkout', ?, ?, ?, 'github')`, named, computedAt, computedAt, org); err != nil {
		t.Fatal(err)
	}
	resp, err := BuildResponse(ctx, client, nil, org, f, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Summary) == 0 {
		t.Fatalf("no summary sentence")
	}
	text := resp.Summary[0].Text
	t.Logf("summary: %s", text)
	if loc := idShape.FindString(text); loc != "" {
		t.Errorf("the sentence prints an id %q: %q", loc, text)
	}
	if !strings.Contains(text, "acme/checkout") {
		t.Errorf("the sentence lacks the repository name: %q", text)
	}
}
