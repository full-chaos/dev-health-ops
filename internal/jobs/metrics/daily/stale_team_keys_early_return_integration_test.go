//go:build integration

package daily

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// The stale-key rule on the returns of a family that writes no row: a day
// whose input is gone, and a day whose input gives no team row. A table still
// holds the row of an earlier compute under a team id then, and that key is
// stale too. Each case stores such a row, gives the family the input of the
// case, runs the real executor on the schema of the migration chain, and reads
// what the key holds.
//
// The file uses one symbol of the rule, endStaleKeyRun, for the table that the
// end of a run decides (work_item_state_durations_daily).
//
// Two returns of the families are not cases here because no input reaches
// them: ai_impact with pull requests and no record (its compute emits the
// 'unknown' bucket for every group of pull requests), and
// compounding_risk_team with a repository-to-team map and no record (the map
// holds only repositories that have a metrics row, and each gives its team an
// input).

var earlyReturnDay = time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)

func TestAFamilyThatWritesNoRowStillSupersedesTheKeysOfTheDay(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := earlyReturnDay
	earlier := day.Add(30 * time.Hour)
	clock := earlier.Add(10 * time.Hour)
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000007a1")
	exec := func(t *testing.T, what, query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	// held is the sum of the named columns of the newest rows of a team key.
	held := func(t *testing.T, table, teamColumn, sum, org string, extra string) float64 {
		t.Helper()
		var value float64
		if err := conn.QueryRow(ctx, "SELECT toFloat64(sum("+sum+")) FROM "+table+" FINAL WHERE org_id = ? AND day = ? AND "+
			teamColumn+" = 'platform'"+extra, org, day).Scan(&value); err != nil {
			t.Fatalf("read %s: %v", table, err)
		}
		return value
	}
	// An unowned repository: no ownership row and no team pattern names it.
	unownedRepo := func(t *testing.T, org string) {
		t.Helper()
		exec(t, "insert repo", "INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, ?, ?)",
			repo, "acme/unowned", org, "github", earlier)
	}
	finalize := func(t *testing.T, org string, executor NativeFinalizeFamilyExecutor) {
		t.Helper()
		if _, err := executor.ComputeFinalizeFamily(ctx, Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day}); err != nil {
			t.Fatalf("the family: %v", err)
		}
	}

	cognitiveLoad := func(t *testing.T) NativeFinalizeFamilyExecutor {
		executor, err := NewTeamCognitiveLoadExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		executor.nowUTC = func() time.Time { return clock }
		return executor
	}
	seedCognitiveLoad := func(t *testing.T, org string) {
		exec(t, "insert the earlier cognitive-load row", `INSERT INTO team_cognitive_load_daily
    (org_id, team_id, day, pr_interruption_load, context_spread_count, review_request_load, contributing_repo_count, sample_author_count, computed_at)
    VALUES (?, 'platform', ?, 3, 1, 2, 1, 1, ?)`, org, day, earlier)
	}
	t.Run("team_cognitive_load, no input for the day", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000007e0001"
		seedCognitiveLoad(t, org)
		finalize(t, org, cognitiveLoad(t))
		if value := held(t, "team_cognitive_load_daily", "team_id", "pr_interruption_load + contributing_repo_count", org, ""); value != 0 {
			t.Errorf("the key of the earlier compute still holds %v", value)
		}
	})
	t.Run("team_cognitive_load, input that gives no team row", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000007e0002"
		seedCognitiveLoad(t, org)
		unownedRepo(t, org)
		exec(t, "insert user metrics", `INSERT INTO user_metrics_daily
    (repo_id, day, author_email, pr_interruption_load, review_request_load, computed_at, org_id)
    VALUES (?, ?, 'dev@example.com', 3, 2, ?, ?)`, repo, day, earlier, org)
		finalize(t, org, cognitiveLoad(t))
		if value := held(t, "team_cognitive_load_daily", "team_id", "pr_interruption_load + contributing_repo_count", org, ""); value != 0 {
			t.Errorf("the key of the earlier compute still holds %v", value)
		}
	})

	complexity := func(t *testing.T) NativeFinalizeFamilyExecutor {
		executor, err := NewTeamComplexityExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		executor.nowUTC = func() time.Time { return clock }
		return executor
	}
	seedComplexity := func(t *testing.T, org string) {
		exec(t, "insert the earlier complexity row", `INSERT INTO team_complexity_daily
    (org_id, team_id, day, loc_total, cyclomatic_total, cyclomatic_per_kloc, high_complexity_functions, very_high_complexity_functions, contributing_repo_count, computed_at)
    VALUES (?, 'platform', ?, 1000, 200, 200, 4, 1, 1, ?)`, org, day, earlier)
	}
	t.Run("team_complexity, no input for the day", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000007e0003"
		seedComplexity(t, org)
		finalize(t, org, complexity(t))
		if value := held(t, "team_complexity_daily", "team_id", "loc_total + contributing_repo_count", org, ""); value != 0 {
			t.Errorf("the key of the earlier compute still holds %v", value)
		}
	})
	t.Run("team_complexity, input that gives no team row", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000007e0004"
		seedComplexity(t, org)
		unownedRepo(t, org)
		exec(t, "insert repository complexity", `INSERT INTO repo_complexity_daily
    (repo_id, day, loc_total, cyclomatic_total, cyclomatic_per_kloc, high_complexity_functions, very_high_complexity_functions, computed_at, org_id)
    VALUES (?, ?, 1000, 200, 200, 4, 1, ?, ?)`, repo, day, earlier, org)
		finalize(t, org, complexity(t))
		if value := held(t, "team_complexity_daily", "team_id", "loc_total + contributing_repo_count", org, ""); value != 0 {
			t.Errorf("the key of the earlier compute still holds %v", value)
		}
	})

	risk := func(t *testing.T) NativeFinalizeFamilyExecutor {
		executor, err := NewCompoundingRiskTeamExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		executor.nowUTC = func() time.Time { return clock }
		return executor
	}
	seedRisk := func(t *testing.T, org string) {
		exec(t, "insert the earlier team risk row", `INSERT INTO compounding_risk_daily
    (org_id, day, scope, scope_id, compounding_risk, severity, w_churn, w_complexity, w_ownership, w_review, threshold_elevated, threshold_high, computed_at)
    VALUES (?, ?, 'team', 'platform', 0.8, 'high', 0.3, 0.3, 0.2, 0.2, 0.4, 0.65, ?)`, org, day, earlier)
	}
	const riskHeld = "ifNull(compounding_risk, 0) + w_churn"
	t.Run("compounding_risk_team, no repository metrics for the day", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000007e0005"
		seedRisk(t, org)
		finalize(t, org, risk(t))
		if value := held(t, "compounding_risk_daily", "scope_id", riskHeld, org, " AND scope = 'team'"); value != 0 {
			t.Errorf("the key of the earlier compute still holds %v", value)
		}
	})
	t.Run("compounding_risk_team, repository metrics of a repository with no team", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000007e0006"
		seedRisk(t, org)
		unownedRepo(t, org)
		exec(t, "insert repository metrics", `INSERT INTO repo_metrics_daily
    (repo_id, day, rework_churn_ratio_30d, single_owner_file_ratio_30d, code_ownership_gini, bus_factor, pr_first_review_p90_hours, computed_at, org_id)
    VALUES (?, ?, 0.3, 0.4, 0.5, 2, 30, ?, ?)`, repo, day, earlier, org)
		finalize(t, org, risk(t))
		if value := held(t, "compounding_risk_daily", "scope_id", riskHeld, org, " AND scope = 'team'"); value != 0 {
			t.Errorf("the key of the earlier compute still holds %v", value)
		}
	})

	t.Run("ai_impact, no pull request in the window", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000007e0007"
		other := uuid.MustParse("00000000-0000-4000-8000-0000000007a2")
		for _, stored := range []uuid.UUID{repo, other} {
			exec(t, "insert the earlier ai_impact row", `INSERT INTO ai_impact_metrics_daily
    (org_id, team_id, repo_id, work_type, day, attribution_bucket, prs_total, prs_merged, human_prs, computed_at)
    VALUES (?, 'platform', ?, 'earlier', ?, 'human', 5, 5, 5, ?)`, org, stored, day, earlier)
		}
		executor, err := NewAIImpactExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		executor.nowUTC = func() time.Time { return clock }
		run := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day}
		partition := Partition{ID: uuid.NewString(), RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repo.String())}}
		if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
			t.Fatalf("ai_impact: %v", err)
		}
		if value := held(t, "ai_impact_metrics_daily", "team_id", "prs_total", org, fmt.Sprintf(" AND repo_id = '%s'", repo)); value != 0 {
			t.Errorf("the key of the partition's repository still holds %v", value)
		}
		if value := held(t, "ai_impact_metrics_daily", "team_id", "prs_total", org, fmt.Sprintf(" AND repo_id = '%s'", other)); value != 5 {
			t.Errorf("the key of a repository outside the partition holds %v, want its 5", value)
		}
	})

	// work_item_state_durations_daily is decided by the end of a run. The key
	// of the earlier compute is in a work scope that the run's repository
	// still has an item in.
	stateCase := func(t *testing.T, org string, withTransition bool) {
		t.Helper()
		exec(t, "insert the earlier state row", `INSERT INTO work_item_state_durations_daily
    (day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, avg_wip, computed_at, org_id)
    VALUES (?, 'linear', 'board-1', 'platform', 'Platform', 'in_progress', 10, 2, 2, ?, ?)`, day, earlier, org)
		exec(t, "insert work item", `INSERT INTO work_items (
    repo_id, work_item_id, provider, type, status, project_id, created_at, started_at, org_id, last_synced)
    VALUES (?, 'ITEM-1', 'linear', 'story', 'in_progress', 'board-1', ?, ?, ?, ?)`,
			repo, day.Add(-48*time.Hour), day.Add(-24*time.Hour), org, earlier)
		if withTransition {
			// A transition after the day: the item has a history and no span
			// in the day.
			exec(t, "insert transition", `INSERT INTO work_item_transitions
    (repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw, actor, org_id, last_synced)
    VALUES (?, 'ITEM-1', ?, 'linear', 'todo', 'in_progress', 'todo', 'in_progress', '', ?, ?)`,
				repo, day.Add(72*time.Hour), org, earlier)
		}
		run := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, DiscoveredRepoIDs: []RepositoryID{RepositoryID(repo.String())}}
		runSharedScopeFamily(t, ctx, conn, "work_item_state", run, repo, clock)
		endStaleKeyRun(t, ctx, driver.Conn(conn), run, clock)
		if value := held(t, "work_item_state_durations_daily", "team_id", "duration_hours + items_touched", org, ""); value != 0 {
			t.Errorf("the key of the earlier compute still holds %v", value)
		}
	}
	t.Run("work_item_state, an item with no transition", func(t *testing.T) {
		stateCase(t, "00000000-0000-4000-8000-0000007e0008", false)
	})
	t.Run("work_item_state, transitions that give no row for the day", func(t *testing.T) {
		stateCase(t, "00000000-0000-4000-8000-0000007e0009", true)
	})
}
