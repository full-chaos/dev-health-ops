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

// A redrive of an OLD run of a day after a NEWER run of the same day stored its
// rows (the stale-finalize stage of the reconciler, CHAOS-14090) must write the
// answer of the CURRENT inputs: its rows carry a newer computed_at, so they
// replace the newer run's rows for every reader. To a finalize family the two
// runs differ only in Run.ID; this cell proves by execution, family by family,
// that the redriven old run writes what a fresh compute on current data writes
// (FAMILIES-14090-1 §6).
//
// For each family, in its own organization:
//  1. inputs v1; the OLD run computes the day at t1 (R1);
//  2. the inputs change to v2 (newer rows);
//  3. a NEWER run computes the day at t2: R2, and R2 differs from R1;
//  4. the OLD run is computed again (the redrive) at t3 > t2, with the SAME
//     executor (a long-lived worker);
//
// WANT: the reader form after step 4 equals R2, and the newest stored row of
// the day was computed at t3.
func TestARedriveOfAnOldRunAfterANewerRunWritesTheCurrentInputs(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	t1, t2, t3 := day.Add(30*time.Hour), day.Add(40*time.Hour), day.Add(50*time.Hour)

	for _, family := range []struct {
		name string
		// table and dayColumn name where the family's rows of the day are.
		table, dayColumn, extra string
		setup                   func(t *testing.T, org string, repo uuid.UUID)
		changeInputs            func(t *testing.T, org string, repo uuid.UUID)
		executor                func(t *testing.T) (NativeFinalizeFamilyExecutor, func(time.Time))
		// partition is run before the finalize of steps 1 and 3 when the
		// family reads a partition family's output.
		partition bool
		read      func(t *testing.T, org string) string
	}{
		{
			name: "team_cognitive_load", table: "team_cognitive_load_daily", dayColumn: "day",
			setup: func(t *testing.T, org string, repo uuid.UUID) {
				seedRedriveOwner(t, ctx, conn, org, repo, day)
				for hash, when := range map[string]time.Time{"c1": day.Add(12 * time.Hour), "c2": day.Add(23 * time.Hour)} {
					seedRedriveCommit(t, ctx, conn, org, repo, hash, when, day.Add(26*time.Hour))
				}
				execRedrive(t, ctx, conn, "user metrics v1", `INSERT INTO user_metrics_daily
    (repo_id, day, author_email, pr_interruption_load, review_request_load, computed_at, org_id)
    VALUES (?, ?, ?, ?, ?, ?, ?)`, repo, day, "dev@example.com", uint32(3), uint32(2), day.Add(26*time.Hour), org)
			},
			changeInputs: func(t *testing.T, org string, repo uuid.UUID) {
				seedRedriveCommit(t, ctx, conn, org, repo, "c3", day.Add(10*time.Hour), day.Add(35*time.Hour))
				execRedrive(t, ctx, conn, "user metrics v2", `INSERT INTO user_metrics_daily
    (repo_id, day, author_email, pr_interruption_load, review_request_load, computed_at, org_id)
    VALUES (?, ?, ?, ?, ?, ?, ?)`, repo, day, "dev@example.com", uint32(7), uint32(5), day.Add(35*time.Hour), org)
			},
			executor: func(t *testing.T) (NativeFinalizeFamilyExecutor, func(time.Time)) {
				executor, err := NewTeamCognitiveLoadExecutor(conn)
				if err != nil {
					t.Fatal(err)
				}
				return executor, func(at time.Time) { executor.nowUTC = func() time.Time { return at } }
			},
			partition: true,
			read: func(t *testing.T, org string) string {
				var load float64
				var ratio *float64
				scanRedrive(t, ctx, conn, `SELECT sum(pr_interruption_load), max(after_hours_commit_ratio)
FROM team_cognitive_load_daily FINAL WHERE org_id = ? AND day = ?`, []any{org, day}, &load, &ratio)
				return fmt.Sprintf("pr_interruption_load=%v after_hours_commit_ratio=%v", load, derefRedrive(ratio))
			},
		},
		{
			name: "team_complexity", table: "team_complexity_daily", dayColumn: "day",
			setup: func(t *testing.T, org string, repo uuid.UUID) {
				seedRedriveOwner(t, ctx, conn, org, repo, day)
				seedRedriveComplexity(t, ctx, conn, org, repo, day, 1000, day.Add(26*time.Hour))
			},
			changeInputs: func(t *testing.T, org string, repo uuid.UUID) {
				seedRedriveComplexity(t, ctx, conn, org, repo, day, 1500, day.Add(35*time.Hour))
			},
			executor: func(t *testing.T) (NativeFinalizeFamilyExecutor, func(time.Time)) {
				executor, err := NewTeamComplexityExecutor(conn)
				if err != nil {
					t.Fatal(err)
				}
				return executor, func(at time.Time) { executor.nowUTC = func() time.Time { return at } }
			},
			read: func(t *testing.T, org string) string {
				var loc, cyclomatic float64
				scanRedrive(t, ctx, conn, `SELECT toFloat64(sum(loc_total)), toFloat64(sum(cyclomatic_total))
FROM team_complexity_daily FINAL WHERE org_id = ? AND day = ?`, []any{org, day}, &loc, &cyclomatic)
				return fmt.Sprintf("loc_total=%v cyclomatic_total=%v", loc, cyclomatic)
			},
		},
		{
			name: "compounding_risk_team", table: "compounding_risk_daily", dayColumn: "day", extra: " AND scope = 'team'",
			setup: func(t *testing.T, org string, repo uuid.UUID) {
				seedRedriveOwner(t, ctx, conn, org, repo, day)
				seedRedriveComplexity(t, ctx, conn, org, repo, day, 1000, day.Add(26*time.Hour))
				seedRedriveComplexity(t, ctx, conn, org, repo, day.AddDate(0, 0, -20), 800, day.Add(26*time.Hour))
				seedRedriveRepoMetrics(t, ctx, conn, org, repo, day, 0.3, day.Add(26*time.Hour))
			},
			changeInputs: func(t *testing.T, org string, repo uuid.UUID) {
				seedRedriveRepoMetrics(t, ctx, conn, org, repo, day, 0.6, day.Add(35*time.Hour))
				execRedrive(t, ctx, conn, "merged pull request", `INSERT INTO git_pull_requests
    (repo_id, number, created_at, merged_at, first_review_at, last_synced, org_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
					repo, uint32(1), day.AddDate(0, 0, -6), day.AddDate(0, 0, -5), day.AddDate(0, 0, -6).Add(3*time.Hour), day.Add(35*time.Hour), org)
			},
			executor: func(t *testing.T) (NativeFinalizeFamilyExecutor, func(time.Time)) {
				executor, err := NewCompoundingRiskTeamExecutor(conn)
				if err != nil {
					t.Fatal(err)
				}
				return executor, func(at time.Time) { executor.nowUTC = func() time.Time { return at } }
			},
			read: func(t *testing.T, org string) string {
				var scored uint64
				var risk float64
				scanRedrive(t, ctx, conn, `SELECT toUInt64(countIf(compounding_risk IS NOT NULL)), toFloat64(sum(ifNull(compounding_risk, 0)))
FROM compounding_risk_daily FINAL WHERE org_id = ? AND day = ? AND scope = 'team'`, []any{org, day}, &scored, &risk)
				return fmt.Sprintf("scored_teams=%d risk=%.9f", scored, risk)
			},
		},
		{
			name: "work_item_measure_capability", table: "work_item_measure_capability", dayColumn: "window_end",
			setup: func(t *testing.T, org string, _ uuid.UUID) {
				seedCapabilityWorkItem(t, ctx, conn, org, "github", "gh-1", day.Add(-48*time.Hour))
			},
			changeInputs: func(t *testing.T, org string, _ uuid.UUID) {
				// A second provider's item, created in the last day of the window.
				seedCapabilityWorkItem(t, ctx, conn, org, "jira", "ji-1", day.Add(-12*time.Hour))
			},
			executor: func(t *testing.T) (NativeFinalizeFamilyExecutor, func(time.Time)) {
				executor, err := NewWorkItemMeasureCapabilityExecutor(conn)
				if err != nil {
					t.Fatal(err)
				}
				return executor, func(at time.Time) { executor.nowUTC = func() time.Time { return at } }
			},
			read: func(t *testing.T, org string) string {
				return fmt.Sprint(capabilityAnswers(t, ctx, conn, org, day))
			},
		},
	} {
		t.Run(family.name, func(t *testing.T) {
			org := uuid.NewString()
			repo := uuid.New()
			old := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day}
			newer := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day}
			executor, setClock := family.executor(t)
			compute := func(step string, run Run, at time.Time) {
				t.Helper()
				if family.partition {
					runRedrivePartition(t, ctx, conn, run, repo, at)
				}
				setClock(at)
				if _, err := executor.ComputeFinalizeFamily(ctx, run); err != nil {
					t.Fatalf("%s: %v", step, err)
				}
			}

			family.setup(t, org, repo)
			compute("step 1, the old run", old, t1)
			r1 := family.read(t, org)
			family.changeInputs(t, org, repo)
			compute("step 3, the newer run", newer, t2)
			r2 := family.read(t, org)
			if r2 == r1 {
				t.Fatalf("fixture: the newer run's answer equals the old run's (%s): the inputs did not change the answer", r1)
			}

			// Step 4: the redrive runs the finalize only; the partitions of
			// the old run succeeded long ago.
			setClock(t3)
			if _, err := executor.ComputeFinalizeFamily(ctx, old); err != nil {
				t.Fatalf("step 4, the redrive of the old run: %v", err)
			}
			if r4 := family.read(t, org); r4 != r2 {
				t.Fatalf("after the redrive of the old run the reader sees %s, want the current answer %s (the old run's was %s)", r4, r2, r1)
			}
			var newest time.Time
			scanRedrive(t, ctx, conn, "SELECT max(computed_at) FROM "+family.table+" WHERE org_id = ? AND "+family.dayColumn+" = ?"+family.extra,
				[]any{org, day}, &newest)
			if !newest.Equal(t3) {
				t.Fatalf("the newest stored row was computed at %s, want the redrive's %s", newest, t3)
			}
		})
	}
}

func execRedrive(t *testing.T, ctx context.Context, conn driver.Conn, what, query string, args ...any) {
	t.Helper()
	if err := conn.Exec(ctx, query, args...); err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

func scanRedrive(t *testing.T, ctx context.Context, conn driver.Conn, query string, args []any, targets ...any) {
	t.Helper()
	if err := conn.QueryRow(ctx, query, args...).Scan(targets...); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

func derefRedrive(value *float64) string {
	if value == nil {
		return "NULL"
	}
	return fmt.Sprint(*value)
}

// seedRedriveOwner stores the repository, one active team and its ownership
// of the repository, valid from before the day.
func seedRedriveOwner(t *testing.T, ctx context.Context, conn driver.Conn, org string, repo uuid.UUID, day time.Time) {
	t.Helper()
	at := day.Add(-72 * time.Hour)
	execRedrive(t, ctx, conn, "insert repo",
		"INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, ?, ?)", repo, "acme/api", org, "github", at)
	execRedrive(t, ctx, conn, "insert team", `INSERT INTO teams
    (id, team_uuid, name, repo_patterns, updated_at, last_synced, org_id, provider, is_active)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"platform", uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:platform:"+org)), "Team platform", []string{"acme/*"},
		at, at, org, "github", uint8(1))
	execRedrive(t, ctx, conn, "insert ownership", `INSERT INTO team_repo_ownership
    (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, valid_from, updated_at)
    VALUES (?, ?, ?, ?, ?, 'exact', 'native', ?, ?, ?, ?)`,
		org, "github", "platform", repo, "acme/api", uint8(1), uint16(10), at, at)
}

func seedRedriveCommit(t *testing.T, ctx context.Context, conn driver.Conn, org string, repo uuid.UUID, hash string, when, synced time.Time) {
	t.Helper()
	execRedrive(t, ctx, conn, "insert commit "+hash, `INSERT INTO git_commits
    (repo_id, hash, author_name, author_email, author_when, committer_when, parents, last_synced, org_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, repo, hash, "Dev", "dev@example.com", when, when, uint32(1), synced, org)
}

func seedRedriveComplexity(t *testing.T, ctx context.Context, conn driver.Conn, org string, repo uuid.UUID, day time.Time, loc uint64, computed time.Time) {
	t.Helper()
	execRedrive(t, ctx, conn, "insert repository complexity", `INSERT INTO repo_complexity_daily
    (repo_id, day, loc_total, cyclomatic_total, cyclomatic_per_kloc, high_complexity_functions, very_high_complexity_functions, computed_at, org_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, repo, day, loc, loc/5, float64(loc)/5, uint64(4), uint64(1), computed, org)
}

func seedRedriveRepoMetrics(t *testing.T, ctx context.Context, conn driver.Conn, org string, repo uuid.UUID, day time.Time, rework float64, computed time.Time) {
	t.Helper()
	execRedrive(t, ctx, conn, "insert repository metrics", `INSERT INTO repo_metrics_daily
    (repo_id, day, rework_churn_ratio_30d, single_owner_file_ratio_30d, code_ownership_gini, bus_factor,
     pr_first_review_p90_hours, computed_at, org_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, repo, day, rework, 0.4, 0.5, uint32(2), 30.0, computed, org)
}

// runRedrivePartition runs the team_wellbeing partition family of a run, which
// writes the team_metrics_daily rows team_cognitive_load reads.
func runRedrivePartition(t *testing.T, ctx context.Context, conn driver.Conn, run Run, repo uuid.UUID, at time.Time) {
	t.Helper()
	wellbeing, err := NewTeamWellbeingExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	wellbeing.nowUTC = func() time.Time { return at.Add(-time.Minute) }
	partition := Partition{ID: uuid.NewString(), RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repo.String())}}
	if _, err := wellbeing.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatalf("team_wellbeing of run %s: %v", run.ID, err)
	}
}
