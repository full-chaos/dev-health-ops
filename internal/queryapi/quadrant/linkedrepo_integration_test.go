//go:build integration

// CHAOS-9211: a named repository narrows the team grain through its own work. The
// repository of a point is the items linked to its pull requests (work_graph_issue_pr),
// computed by the work_item family's own compute; the daily rollup is written by
// that same compute, so the filtered read is checked against the unfiltered read of a
// world that holds ONLY the repository's items (a differential oracle), and the
// filters AND, never widen.
package quadrant

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

const (
	lnkRepoA = "00000000-0000-4000-8000-0000000000c1" // held by team-alpha
	lnkRepoB = "00000000-0000-4000-8000-0000000000c2" // held by team-alpha
	lnkRepoC = "00000000-0000-4000-8000-0000000000c3" // held by team-beta
	lnkRepoD = "00000000-0000-4000-8000-0000000000c4" // held by team-alpha, no linked item
)

type lnkItem struct {
	id, team, repo string // repo: the one the item is linked to
	provenance     string
	started, done  string // "" = none
}

func lnkItems() []lnkItem {
	return []lnkItem{
		{id: "gh:a/r#1", team: "team-alpha", repo: lnkRepoA, provenance: "native", started: "2026-09-02 09:00:00", done: "2026-09-10 12:00:00"},
		{id: "gh:a/r#2", team: "team-alpha", repo: lnkRepoB, provenance: "heuristic", started: "2026-09-03 09:00:00", done: "2026-09-12 12:00:00"},
		{id: "gh:a/r#3", team: "team-beta", repo: lnkRepoC, provenance: "native", started: "2026-09-03 09:00:00", done: "2026-09-12 15:00:00"},
		{id: "gh:a/r#4", team: "team-alpha", repo: lnkRepoA, provenance: "explicit_text", started: "2026-09-09 09:00:00"},
		{id: "gh:a/r#5", team: "team-alpha", repo: lnkRepoB, provenance: "explicit_text", started: "2026-09-05 09:00:00", done: "2026-09-14 10:00:00"},
		// no link at all: in no repository's view
		{id: "gh:a/r#6", team: "team-alpha", started: "2026-09-06 09:00:00", done: "2026-09-16 10:00:00"},
	}
}

func lnkTime(value string) *time.Time {
	if value == "" {
		return nil
	}
	parsed, err := time.ParseInLocation("2006-01-02 15:04:05", value, time.UTC)
	if err != nil {
		panic(err)
	}
	return &parsed
}

// seedLinkedWorld writes one org: the repositories, the team holds, the teams, the
// items kept by `keep` with their links and primary teams, the rollup the daily job's
// own compute writes for them, and one review row per repository kept by keepRepo.
func seedLinkedWorld(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string, keep func(lnkItem) bool, keepRepo func(string) bool) {
	t.Helper()
	synced := time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC)
	holds := map[string]string{lnkRepoA: "team-alpha", lnkRepoB: "team-alpha", lnkRepoC: "team-beta", lnkRepoD: "team-alpha"}
	for repo, team := range holds {
		name := "acme/" + repo[len(repo)-2:]
		crossorg.Exec(ctx, t, admin, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`, repo, name, org, synced, synced)
		crossorg.Exec(ctx, t, admin, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 0, 0, 0, ?, NULL, ?)`,
			org, team, repo, name, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), synced)
		if keepRepo(repo) {
			crossorg.Exec(ctx, t, admin, `INSERT INTO user_metrics_daily (org_id, repo_id, day, author_email, identity_id, team_id, reviews_given, pr_first_review_p50_hours, loc_touched, computed_at) VALUES (?, ?, ?, ?, ?, ?, 7, 5, 10, now())`,
				org, repo, time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC), team+"@x", team+"@x", team)
		}
	}
	for _, team := range []string{"team-alpha", "team-beta"} {
		crossorg.Exec(ctx, t, admin, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider) VALUES (?, generateUUIDv4(), ?, [], [], [], 1, ?, ?, ?, 'github')`,
			team, team, synced, synced, org)
	}

	var stored []workitemmetrics.StoredItem
	attributions := map[string]workitemmetrics.PrimaryAttribution{}
	for _, item := range lnkItems() {
		if !keep(item) {
			continue
		}
		status, created := "in_progress", time.Date(2026, 8, 30, 9, 0, 0, 0, time.UTC)
		if item.done != "" {
			status = "done"
		}
		row := workitemmetrics.StoredItem{
			WorkItemID: item.id, Provider: "github", Status: status, ProjectID: "a/r", CreatedAt: created,
			CompletedAt: lnkTime(item.done), Type: "task", Assignees: []string{"ann"}, StartedAt: lnkTime(item.started),
		}
		stored = append(stored, row)
		attributions[item.id] = workitemmetrics.PrimaryAttribution{TeamID: item.team, TeamName: item.team}
		storedRepo := item.repo
		if storedRepo == "" {
			storedRepo = lnkRepoA
		}
		crossorg.Exec(ctx, t, admin, `INSERT INTO work_items (repo_id, work_item_id, provider, title, type, status, status_raw, project_key, project_id, native_team_key, project_name, assignees, reporter, created_at, updated_at, started_at, completed_at, closed_at, labels, sprint_id, sprint_name, parent_id, epic_id, url, last_synced, org_id) VALUES (?, ?, 'github', 't', 'task', ?, ?, '', 'a/r', '', '', ['ann'], '', ?, ?, ?, ?, NULL, [], '', '', '', '', '', ?, ?)`,
			storedRepo, item.id, status, status, created, synced, lnkTime(item.started), lnkTime(item.done), synced, org)
		crossorg.Exec(ctx, t, admin, `INSERT INTO work_item_team_attributions (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at) VALUES (?, ?, ?, 'github', ?, ?, 'repo_ownership', 1, 'high', '', ?)`,
			org, storedRepo, item.id, item.team, item.team, synced)
		if item.repo != "" {
			crossorg.Exec(ctx, t, admin, `INSERT INTO work_graph_issue_pr (org_id, repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced) VALUES (?, ?, ?, 1, 1, ?, '', ?)`,
				org, item.repo, item.id, item.provenance, synced)
		}
	}
	for day := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC); day.Before(time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC)); day = day.AddDate(0, 0, 1) {
		triplet := workitemmetrics.ComputeStoredItemsDay(day, stored, nil, attributions)
		for _, row := range triplet.MetricsDaily {
			crossorg.Exec(ctx, t, admin, `INSERT INTO work_item_metrics_daily (org_id, day, provider, work_scope_id, team_id, team_name, items_started, items_completed, items_started_unassigned, items_completed_unassigned, wip_count_end_of_day, wip_unassigned_end_of_day, bug_completed_ratio, story_points_completed, computed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, 0, 0, 0, now())`,
				org, row.Day, row.Provider, row.WorkScopeID, row.TeamID, row.TeamName, row.ItemsStarted, row.ItemsCompleted, row.WIPCountEndOfDay)
		}
		for _, row := range triplet.CycleTimes {
			crossorg.Exec(ctx, t, admin, `INSERT INTO work_item_cycle_times (org_id, work_item_id, provider, day, work_scope_id, team_id, team_name, type, status, created_at, started_at, completed_at, cycle_time_hours, lead_time_hours, computed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())`,
				org, row.WorkItemID, row.Provider, row.Day, row.WorkScopeID, row.TeamID, row.TeamName, row.Type, row.Status, row.CreatedAt, row.StartedAt, row.CompletedAt, row.CycleTimeHours, row.LeadTimeHours)
		}
	}
}

func linkedWorldRunner(ctx context.Context, t *testing.T) func(org string, p Params) *Response {
	t.Helper()
	admin, client := crossorg.Start(ctx, t)
	seedLinkedWorld(ctx, t, admin, "lnk-full", func(lnkItem) bool { return true }, func(string) bool { return true })
	// One oracle world per repository: ONLY the items linked to it (and its reviews).
	for _, repo := range []string{lnkRepoA, lnkRepoB, lnkRepoC, lnkRepoD} {
		repo := repo
		seedLinkedWorld(ctx, t, admin, "lnk-only-"+repo[len(repo)-2:], func(item lnkItem) bool { return item.repo == repo }, func(r string) bool { return r == repo })
	}
	end := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	return func(org string, p Params) *Response {
		t.Helper()
		p.Bucket, p.RangeDays, p.EndDate = "week", 30, &end
		resp, err := BuildResponse(ctx, client, org, p)
		if err != nil {
			t.Fatalf("BuildResponse(%s, %+v): %v", org, p, err)
		}
		return resp
	}
}

// The filtered read of a repository equals the unfiltered read of the world that holds
// only the repository's items, for every team-grain cell (both axes): the one
// definition of the metric, never a second one.
func TestQuadrantTeamGrainRepositoryEqualsTheWorldOfItsOwnItems(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	run := linkedWorldRunner(ctx, t)
	for _, quadrant := range []string{"cycle_throughput", "wip_throughput", "review_load_latency"} {
		for _, repo := range []string{lnkRepoA, lnkRepoB, lnkRepoC} {
			name := quadrant + "/" + repo[len(repo)-2:]
			t.Run(name, func(t *testing.T) {
				filtered := run("lnk-full", Params{Type: quadrant, ScopeType: "team", RepoRefs: []string{repo}})
				oracle := run("lnk-only-"+repo[len(repo)-2:], Params{Type: quadrant, ScopeType: "team"})
				if filtered.RepoFilterApplied == nil || !*filtered.RepoFilterApplied {
					t.Fatalf("repo_filter_applied = %s, want true", boolText(filtered.RepoFilterApplied))
				}
				if !reflect.DeepEqual(filtered.Points, oracle.Points) {
					t.Errorf("filtered points differ from the world of the repository's own items:\n got  %s\n want %s", describePoints(filtered), describePoints(oracle))
				}
			})
		}
	}
}

// A repository with a linked PR and issue plots its team; an unlinked repository
// plots nothing, and both axes are read.
func TestQuadrantTeamGrainLinkedVersusUnlinkedRepository(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	run := linkedWorldRunner(ctx, t)
	for _, quadrant := range []string{"cycle_throughput", "wip_throughput"} {
		linked := run("lnk-full", Params{Type: quadrant, ScopeType: "team", RepoRefs: []string{lnkRepoA}})
		alpha := pointOf(linked, "team-alpha")
		if alpha == nil || alpha.NoData || alpha.X == nil || alpha.Y == nil {
			t.Fatalf("%s: a repository with a linked item plots its team: %s", quadrant, describePoints(linked))
		}
		if beta := pointOf(linked, "team-beta"); beta == nil || !beta.NoData {
			t.Errorf("%s: a team with no item of the repository has no data: %s", quadrant, describePoints(linked))
		}
		unlinked := run("lnk-full", Params{Type: quadrant, ScopeType: "team", RepoRefs: []string{lnkRepoD}})
		for _, point := range unlinked.Points {
			if !point.NoData {
				t.Errorf("%s: an unlinked repository plots nothing: %s", quadrant, describePoints(unlinked))
			}
		}
		if unlinked.RepoFilterApplied == nil || !*unlinked.RepoFilterApplied {
			t.Errorf("%s: repo_filter_applied = %s, want true", quadrant, boolText(unlinked.RepoFilterApplied))
		}
	}
}

// With a team and a repository the filters AND and never widen: a repository the team
// does not hold leaves no point and says why, whatever item is linked to it and
// attributed to the team; an unknown repository says it was not found.
func TestQuadrantTeamGrainFiltersAndNeverWiden(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	run := linkedWorldRunner(ctx, t)
	for _, quadrant := range []string{"cycle_throughput", "wip_throughput", "review_load_latency"} {
		for _, tc := range []struct {
			name       string
			teams      []string
			repo       string
			wantReason string
			wantPoints []string
		}{
			{"team and a repository it holds", []string{"team-alpha"}, lnkRepoA, "null", []string{"team-alpha"}},
			{"team and a repository it does not hold", []string{"team-alpha"}, lnkRepoC, "repository_not_in_team", []string{"team-alpha"}},
			{"an unknown repository", []string{"team-alpha"}, "00000000-0000-4000-8000-0000000000cf", "repository_not_found", []string{"team-alpha"}},
		} {
			t.Run(quadrant+"/"+tc.name, func(t *testing.T) {
				resp := run("lnk-full", Params{Type: quadrant, ScopeType: "team", TeamIDs: tc.teams, RepoRefs: []string{tc.repo}})
				if got := entityIDs(resp); fmt.Sprint(got) != fmt.Sprint(tc.wantPoints) {
					t.Errorf("points = %v, want %v", got, tc.wantPoints)
				}
				if textOf(resp.FilterEmptyReason) != tc.wantReason || boolText(resp.RepoFilterApplied) != "true" {
					t.Errorf("repo_filter_applied = %s, filter_empty_reason = %s; want true, %s", boolText(resp.RepoFilterApplied), textOf(resp.FilterEmptyReason), tc.wantReason)
				}
				if tc.wantReason != "null" {
					for _, point := range resp.Points {
						if !point.NoData {
							t.Errorf("an empty answer plots a point: %s", describePoints(resp))
						}
					}
				}
			})
		}
	}
}

// A team whose work all sits in one repository reads the same through the filtered
// (linked items) and the unfiltered (daily table) path: the one definition, both ways.
func TestQuadrantTeamGrainRepositoryHoldingAllOfATeamsWorkEqualsTheTeam(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	run := linkedWorldRunner(ctx, t)
	all := run("lnk-full", Params{Type: "wip_throughput", ScopeType: "team", TeamIDs: []string{"team-beta"}})
	byRepo := run("lnk-full", Params{Type: "wip_throughput", ScopeType: "team", TeamIDs: []string{"team-beta"}, RepoRefs: []string{lnkRepoC}})
	if !reflect.DeepEqual(all.Points, byRepo.Points) {
		t.Errorf("team-beta's work is all in repository C:\n got  %s\n want %s", describePoints(byRepo), describePoints(all))
	}
	if all.RepoFilterApplied != nil {
		t.Errorf("no repository named: repo_filter_applied = %s, want null", boolText(all.RepoFilterApplied))
	}
}

func pointOf(resp *Response, id string) *Point {
	for index := range resp.Points {
		if resp.Points[index].EntityID == id {
			return &resp.Points[index]
		}
	}
	return nil
}

func describePoints(resp *Response) string {
	out := ""
	for _, point := range resp.Points {
		x, y := "nil", "nil"
		if point.X != nil {
			x = fmt.Sprint(*point.X)
		}
		if point.Y != nil {
			y = fmt.Sprint(*point.Y)
		}
		out += fmt.Sprintf("[%s x=%s y=%s nodata=%v traj=%d] ", point.EntityID, x, y, point.NoData, len(point.Trajectory))
	}
	return out
}
