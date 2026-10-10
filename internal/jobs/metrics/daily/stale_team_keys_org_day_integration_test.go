//go:build integration

package daily

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// emptyScopeReadConn answers the organization-wide read of the work scopes
// with no row, as a read that is cut off does. Every other statement goes to
// the server as it is.
type emptyScopeReadConn struct{ driver.Conn }

func (conn *emptyScopeReadConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if strings.Contains(query, "SELECT DISTINCT provider, project_key") && !strings.Contains(query, "repo_id IN") {
		return conn.Conn.Query(ctx, strings.Replace(query, "WHERE org_id = ?", "WHERE 1 = 0 AND org_id = ?", 1), args...)
	}
	return conn.Conn.Query(ctx, query, args...)
}

// A run of the whole organization owns the whole day. A work scope that the
// organization has no item in any more is computed by no run, so the rows an
// earlier compute stored under it (under a team id, and under `unassigned`)
// stay counted unless the run supersedes every live key of the day that it
// did not write. A run of some repositories must not do that: it stays inside
// the scopes its repositories reach.
//
// The shape, for each provider of the matrix (jira, gitlab, github, linear):
// the day holds stored rows of an earlier compute in a work scope `gone-<p>`
// of that provider, one under a team id and one under `unassigned`, in the
// three work-item tables. No work item of the organization is in that scope.
// The organization also has current items in the scope `board-1` (two
// repositories of the run) and one item in `board-x`, stored under a
// repository that the run does NOT list.
//
// The repository-scoped tables (team_metrics_daily, ai_impact_metrics_daily)
// hold a stored row of a repository that is in no partition of the run, and
// team_metrics_daily one row with no repository (the form from before the
// table held one); and a stored row of a repository of the run, which is its
// partition's to decide, not the end of the run's.
//
// The file uses one symbol of the rule: endStaleKeyRun.

var orgDayProviders = []string{"jira", "gitlab", "github", "linear"}

var (
	orgDayRepoGone     = uuid.MustParse("00000000-0000-4000-8000-0000000006e1")
	orgDayRepoUnlisted = uuid.MustParse("00000000-0000-4000-8000-0000000006e2")
)

func seedOrgDay(t *testing.T, ctx context.Context, exec func(what, query string, args ...any), org string, stored time.Time) {
	t.Helper()
	day := sharedScopeDay
	for _, provider := range orgDayProviders {
		scope := "gone-" + provider
		for _, team := range []string{"ENG", "unassigned"} {
			exec("stored work_item_metrics row", `INSERT INTO work_item_metrics_daily
    (day, provider, work_scope_id, team_id, team_name, items_started, items_completed, wip_count_end_of_day, computed_at, org_id)
    VALUES (?, ?, ?, ?, ?, 10, 9, 2, ?, ?)`, day, provider, scope, team, team, stored, org)
			exec("stored estimate row", `INSERT INTO estimate_coverage_metrics_daily
    (day, provider, work_scope_id, team_id, team_name, estimated_count, unestimated_count, backlog_size, computed_at, org_id)
    VALUES (?, ?, ?, ?, ?, 3, 1, 4, ?, ?)`, day, provider, scope, team, team, stored, org)
			exec("stored state row", `INSERT INTO work_item_state_durations_daily
    (day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, avg_wip, computed_at, org_id)
    VALUES (?, ?, ?, ?, ?, 'in_progress', 12, 2, 2, ?, ?)`, day, provider, scope, team, team, stored, org)
		}
	}
	// An item of a work scope that only a repository outside the run holds.
	exec("item of the unlisted repository", `INSERT INTO work_items (
    repo_id, work_item_id, provider, type, status, project_id, native_team_key, created_at, started_at, story_points, org_id, last_synced)
    VALUES (?, 'X-1', 'linear', 'story', 'in_progress', 'board-x', 'ENG', ?, ?, 2, ?, ?)`,
		orgDayRepoUnlisted, day.Add(-48*time.Hour), day.Add(-24*time.Hour), org, day.Add(12*time.Hour))
	exec("transition of that item", `INSERT INTO work_item_transitions
    (repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw, actor, org_id, last_synced)
    VALUES (?, 'X-1', ?, 'linear', 'todo', 'in_progress', 'todo', 'in_progress', '', ?, ?)`,
		orgDayRepoUnlisted, day.Add(-24*time.Hour), org, day.Add(12*time.Hour))
	// Repository-scoped tables: a repository in no partition, no repository,
	// and a repository of the run.
	for _, repo := range []string{orgDayRepoGone.String(), "", sharedScopeRepoAPI.String()} {
		exec("stored team_metrics row", `INSERT INTO team_metrics_daily
    (org_id, day, team_id, team_name, repo_id, commits_count, after_hours_commits_count, weekend_commits_count, computed_at)
    VALUES (?, ?, 'ENG', 'ENG', ?, 7, 1, 0, ?)`, org, day, repo, stored)
	}
	for _, repo := range []uuid.UUID{orgDayRepoGone, sharedScopeRepoAPI} {
		exec("stored ai_impact row", `INSERT INTO ai_impact_metrics_daily
    (org_id, team_id, repo_id, work_type, day, attribution_bucket, prs_total, prs_merged, human_prs, computed_at)
    VALUES (?, 'ENG', ?, 'earlier', ?, 'human', 5, 5, 5, ?)`, org, repo, day, stored)
	}
}

func TestARunOfTheWholeOrganizationSupersedesEveryKeyOfTheDayItDidNotWrite(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := sharedScopeDay
	stored := day.Add(30 * time.Hour)
	clock := stored.Add(10 * time.Hour)
	exec := func(what, query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	scan := func(what, query string, args []any, target *float64) {
		t.Helper()
		if err := conn.QueryRow(ctx, query, args...).Scan(target); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	// held is what the keys of one (provider, work scope) hold in the three
	// work-item tables, as "table=value".
	held := func(org, provider, scope, team string) [3]float64 {
		t.Helper()
		var out [3]float64
		for index, read := range []struct{ table, sum, teamColumn string }{
			{"work_item_metrics_daily", "items_started + items_completed + wip_count_end_of_day", "team_id"},
			{"estimate_coverage_metrics_daily", "estimated_count + unestimated_count + backlog_size", "ifNull(team_id, '')"},
			{"work_item_state_durations_daily", "duration_hours + items_touched + avg_wip", "team_id"},
		} {
			scan("read "+read.table, "SELECT toFloat64(sum("+read.sum+")) FROM "+read.table+
				" FINAL WHERE org_id = ? AND day = ? AND provider = ? AND work_scope_id = ? AND "+read.teamColumn+" = ?",
				[]any{org, day, provider, scope, team}, &out[index])
		}
		return out
	}
	// scopeHeld is what every key of one (provider, work scope) holds in
	// work_item_metrics_daily, under any team.
	scopeHeld := func(org, provider, scope string) float64 {
		t.Helper()
		var value float64
		scan("read the scope", `SELECT toFloat64(sum(items_started + items_completed + wip_count_end_of_day)) FROM work_item_metrics_daily
FINAL WHERE org_id = ? AND day = ? AND provider = ? AND work_scope_id = ?`, []any{org, day, provider, scope}, &value)
		return value
	}
	repoHeld := func(org, table, sum, repo string) float64 {
		t.Helper()
		var value float64
		scan("read "+table, "SELECT toFloat64(sum("+sum+")) FROM "+table+" FINAL WHERE org_id = ? AND day = ? AND toString(repo_id) = ?",
			[]any{org, day, repo}, &value)
		return value
	}
	setUp := func(org string) {
		t.Helper()
		seedSharedScopeTeams(t, ctx, conn, org, day.Add(-72*time.Hour),
			staleKeyAcceptanceTeam{id: "ENG", nativeKey: "ENG", active: true},
			staleKeyAcceptanceTeam{id: "OPS", nativeKey: "OPS", active: true},
		)
		seedSharedScopeItems(t, ctx, conn, org)
		seedOrgDay(t, ctx, exec, org, stored)
	}
	compute := func(run Run, repos ...uuid.UUID) {
		t.Helper()
		for _, repo := range repos {
			for _, family := range sharedScopeFamilies {
				runSharedScopeFamily(t, ctx, conn, family, run, repo, clock)
			}
		}
		endStaleKeyRun(t, ctx, conn, run, clock)
	}
	zero := [3]float64{}
	storedGone := [3]float64{21, 8, 16}
	listed := []RepositoryID{RepositoryID(sharedScopeRepoAPI.String()), RepositoryID(sharedScopeRepoWeb.String())}

	t.Run("a run of the whole organization", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000006e0001"
		setUp(org)
		for _, provider := range orgDayProviders {
			if got := held(org, provider, "gone-"+provider, "ENG"); got != storedGone {
				t.Fatalf("%s: the stored rows hold %v before the run, want %v: the case is not set", provider, got, storedGone)
			}
		}
		// The one line the end of a run logs: the class of the retraction and
		// the rows of zeros per table.
		var logged bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logged, nil)))
		compute(Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true, DiscoveredRepoIDs: listed},
			sharedScopeRepoAPI, sharedScopeRepoWeb)
		slog.SetDefault(previous)
		var line map[string]any
		for _, raw := range strings.Split(logged.String(), "\n") {
			var entry map[string]any
			if json.Unmarshal([]byte(raw), &entry) == nil && entry["msg"] == StaleKeysRetractedLogMessage {
				line = entry
			}
		}
		// In each work-item table: 4 providers x 2 teams of the scopes with no
		// item, and one key of board-1 that the first partition wrote before
		// the second partition attributed its items (the end of a run settles
		// that key too). One key of a repository in no partition in ai_impact,
		// and two in team_metrics (that repository, and the row with no
		// repository).
		for field, want := range map[string]any{
			"retraction_scope":                          StaleKeyRetractionOrganizationDay,
			"work_item_metrics_daily_zero_rows":         float64(9),
			"estimate_coverage_metrics_daily_zero_rows": float64(9),
			"work_item_state_durations_daily_zero_rows": float64(9),
			"ai_governance_coverage_daily_zero_rows":    float64(0),
			"team_metrics_daily_zero_rows":              float64(2),
			"ai_impact_metrics_daily_zero_rows":         float64(1),
		} {
			if line == nil || line[field] != want {
				t.Errorf("the log line of the run holds %s = %v, want %v (line %v)", field, line[field], want, line)
			}
		}

		for _, provider := range orgDayProviders {
			for _, team := range []string{"ENG", "unassigned"} {
				if got := held(org, provider, "gone-"+provider, team); got != zero {
					t.Errorf("%s: the key of team %q in a work scope that the organization has no item in still holds %v (metrics, estimate, state)",
						provider, team, got)
				}
			}
		}
		// The ninth superseded key of each table: board-1 under `unassigned`,
		// written by the first partition before the second one attributed its
		// items, and superseded at the end of the run.
		var transient uint64
		if err := conn.QueryRow(ctx, `SELECT count() FROM work_item_metrics_daily
WHERE org_id = ? AND day = ? AND provider = 'linear' AND work_scope_id = 'board-1' AND team_id = 'unassigned'`, org, day).Scan(&transient); err != nil {
			t.Fatal(err)
		}
		if transient == 0 || held(org, "linear", "board-1", "unassigned") != zero {
			t.Errorf("board-1 under unassigned: %d stored row(s), holding %v; want a key that a partition wrote and the end of the run superseded",
				transient, held(org, "linear", "board-1", "unassigned"))
		}
		// The scopes the organization has items in hold their rows.
		if got := held(org, "linear", "board-1", "ENG"); got[0] == 0 || got[1] == 0 || got[2] == 0 {
			t.Errorf("board-1, team ENG holds %v after the run, want its rows in all three tables", got)
		}
		// A scope that only an unlisted repository holds is the run's too: it
		// is computed at the end of the run (its item is in progress: one item
		// of work in progress at the end of the day), not left out and not
		// superseded. No partition attributed the item, so the key is not
		// under a team id; the read is over every team of the scope.
		if got := scopeHeld(org, "linear", "board-x"); got != 1 {
			t.Errorf("board-x (an item of a repository the run does not list) holds %v after the run, want its 1 item in progress", got)
		}
		// The repository-scoped tables.
		for _, table := range []struct{ name, sum string }{
			{"team_metrics_daily", "commits_count + after_hours_commits_count"},
			{"ai_impact_metrics_daily", "prs_total + prs_merged"},
		} {
			if got := repoHeld(org, table.name, table.sum, orgDayRepoGone.String()); got != 0 {
				t.Errorf("%s: the key of a repository in no partition of the run still holds %v", table.name, got)
			}
			if got := repoHeld(org, table.name, table.sum, sharedScopeRepoAPI.String()); got == 0 {
				t.Errorf("%s: the key of a repository OF the run holds 0: it is its partition's to decide, not the end of the run's", table.name)
			}
		}
		if got := repoHeld(org, "team_metrics_daily", "commits_count + after_hours_commits_count", ""); got != 0 {
			t.Errorf("team_metrics_daily: the row with no repository still holds %v", got)
		}
		// A reader of team_metrics_daily does not count a superseded row. The
		// daily job's own reader (the input of team_cognitive_load) takes the
		// newest generation of each repository: the repository in no
		// partition gives 0 commits, the row with no repository gives none,
		// and the repository of the run gives its 7.
		inputs, err := loadTeamMetricsCognitiveLoadInputsForDay(ctx, conn, org, day)
		if err != nil {
			t.Fatal(err)
		}
		commits := map[string]int{}
		for _, input := range inputs {
			commits[input.RepoID.String()] += input.CommitsCount
		}
		total := 0
		for _, count := range commits {
			total += count
		}
		if total != 7 || commits[sharedScopeRepoAPI.String()] != 7 || commits[orgDayRepoGone.String()] != 0 {
			t.Errorf("the reader of team_metrics_daily counts %d commit(s) for the day (by repository %v), want only the 7 of the run's repository",
				total, commits)
		}
		// A read with no newest-generation step and no filter: the newest row
		// of each key, summed (FINAL), is the 7 too.
		var final float64
		scan("read team_metrics_daily", "SELECT toFloat64(sum(commits_count)) FROM team_metrics_daily FINAL WHERE org_id = ? AND day = ?",
			[]any{org, day}, &final)
		if final != 7 {
			t.Errorf("a FINAL read of team_metrics_daily with no filter counts %v commit(s) for the day, want 7", final)
		}
		// A second end of the same run writes nothing: a superseded key is
		// not live.
		run := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true, DiscoveredRepoIDs: listed}
		var before, after uint64
		count := func(target *uint64) {
			t.Helper()
			total := uint64(0)
			for _, table := range []string{"team_metrics_daily", "ai_impact_metrics_daily"} {
				var rows uint64
				if err := conn.QueryRow(ctx, "SELECT count() FROM "+table+" WHERE org_id = ? AND day = ?", org, day).Scan(&rows); err != nil {
					t.Fatal(err)
				}
				total += rows
			}
			*target = total
		}
		count(&before)
		endStaleKeyRun(t, ctx, conn, run, clock.Add(time.Hour))
		count(&after)
		if after != before {
			t.Errorf("a second end of the run wrote %d more row(s) in the repository-scoped tables, want none", after-before)
		}
	})

	// A day with NO work item in the organization: every stored key of the
	// day is stale. The run supersedes them only because a second read states
	// that the day has no item.
	t.Run("a run of the whole organization on a day with no work item", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000006e0003"
		for _, team := range []string{"ENG", "unassigned"} {
			exec("stored work_item_metrics row", `INSERT INTO work_item_metrics_daily
    (day, provider, work_scope_id, team_id, team_name, items_started, items_completed, wip_count_end_of_day, computed_at, org_id)
    VALUES (?, 'linear', 'gone-linear', ?, ?, 10, 9, 2, ?, ?)`, day, team, team, stored, org)
		}
		run := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true, DiscoveredRepoIDs: listed}
		var logged bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logged, nil)))
		endStaleKeyRun(t, ctx, conn, run, clock)
		slog.SetDefault(previous)
		for _, team := range []string{"ENG", "unassigned"} {
			if got := held(org, "linear", "gone-linear", team); got != zero {
				t.Errorf("an empty day: the stored key of team %q still holds %v", team, got)
			}
		}
		if !strings.Contains(logged.String(), StaleKeysEmptyOrganizationDayLogMessage) {
			t.Errorf("the run superseded every key of a day with no work item and did not log %q", StaleKeysEmptyOrganizationDayLogMessage)
		}
	})

	// The same run, and the scope read comes back EMPTY while the
	// organization has items for the day (here: the read is cut off by the
	// connection). The step must fail and supersede nothing: an empty read is
	// not a proof of an empty day.
	t.Run("a run of the whole organization whose scope read is empty and the day is not", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000006e0004"
		setUp(org)
		retractor, err := NewRunStaleKeyRetractor(&emptyScopeReadConn{Conn: conn})
		if err != nil {
			t.Fatal(err)
		}
		retractor.nowUTC = func() time.Time { return clock }
		run := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true, DiscoveredRepoIDs: listed}
		written, err := retractor.RetractStaleKeys(ctx, run)
		if !errors.Is(err, ErrOrganizationDayNotProvenEmpty) || written != 0 {
			t.Fatalf("the step wrote %d row(s) with error %v, want no row and ErrOrganizationDayNotProvenEmpty", written, err)
		}
		for _, provider := range orgDayProviders {
			for _, team := range []string{"ENG", "unassigned"} {
				if got := held(org, provider, "gone-"+provider, team); got != storedGone {
					t.Errorf("%s: after the failed step the key of team %q holds %v, want the stored %v: nothing may be superseded",
						provider, team, got, storedGone)
				}
			}
		}
	})

	t.Run("a run of one repository", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000006e0002"
		setUp(org)
		compute(Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: false,
			DiscoveredRepoIDs: []RepositoryID{RepositoryID(sharedScopeRepoAPI.String())}}, sharedScopeRepoAPI)

		for _, provider := range orgDayProviders {
			for _, team := range []string{"ENG", "unassigned"} {
				if got := held(org, provider, "gone-"+provider, team); got != storedGone {
					t.Errorf("%s: a run of one repository changed the key of team %q in another work scope: %v, want the stored %v",
						provider, team, got, storedGone)
				}
			}
		}
		for _, table := range []struct{ name, sum string }{
			{"team_metrics_daily", "commits_count + after_hours_commits_count"},
			{"ai_impact_metrics_daily", "prs_total + prs_merged"},
		} {
			for _, repo := range []string{orgDayRepoGone.String(), sharedScopeRepoAPI.String()} {
				if got := repoHeld(org, table.name, table.sum, repo); got == 0 {
					t.Errorf("%s: a run of one repository superseded the key of repository %s at its end", table.name, repo)
				}
			}
		}
		if got := repoHeld(org, "team_metrics_daily", "commits_count + after_hours_commits_count", ""); got == 0 {
			t.Errorf("team_metrics_daily: a run of one repository superseded the row with no repository")
		}
		if got := scopeHeld(org, "linear", "board-x"); got != 0 {
			t.Errorf("a run of one repository computed board-x, a scope its repository does not reach: %v", got)
		}
	})
}
