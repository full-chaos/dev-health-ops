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

// The acceptance case of the stale-key rule and of the active-team rule for
// the families that take the team of a repository: team_wellbeing
// (team_metrics_daily), team_cognitive_load, team_complexity and
// compounding_risk_team, through the real executors on the schema of the
// migration chain.
//
// A stored day is computed while the bare team id owns the repository. Then
// the team is replaced by a provider-keyed id (the bare id goes inactive, the
// keyed id owns the repository with the same patterns) and the same day is
// computed again. After that:
//
//   - a read with no team filter returns what it returned before;
//   - the keyed id holds what the bare id held;
//   - no measure stays under the bare id, and a third compute writes no row
//     under it;
//   - team_cognitive_load still reads the commits of the day: the row of
//     zeros of team_metrics_daily is in the generation of the rows of the
//     repository, so the read of the newest generation does not lose them.
//
// The file uses no symbol of either rule, so the same file runs on a tree
// without them.

const repoOwnerFamiliesOrg = "00000000-0000-4000-8000-00000000f0c2"

var (
	repoOwnerFamiliesDay  = time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	repoOwnerFamiliesRepo = uuid.MustParse("00000000-0000-4000-8000-0000000000b1")
)

func execRepoOwnerFamilies(t *testing.T, ctx context.Context, conn driver.Conn, what, query string, args ...any) {
	t.Helper()
	if err := conn.Exec(ctx, query, args...); err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

func seedRepoOwnerFamiliesTeam(
	t *testing.T, ctx context.Context, conn driver.Conn, id string, active uint8, at time.Time,
) {
	t.Helper()
	execRepoOwnerFamilies(t, ctx, conn, "insert team "+id, `INSERT INTO teams
    (id, team_uuid, name, members, repo_patterns, updated_at, last_synced, org_id, provider, is_active)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+id)), "Team "+id, []string{}, []string{"acme/*"},
		at, at, repoOwnerFamiliesOrg, "github", active)
}

func seedRepoOwnerFamiliesOwnership(
	t *testing.T, ctx context.Context, conn driver.Conn, teamID string, primary uint8, validFrom, updatedAt time.Time,
) {
	t.Helper()
	execRepoOwnerFamilies(t, ctx, conn, "insert ownership by "+teamID, `INSERT INTO team_repo_ownership
    (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, valid_from, updated_at)
    VALUES (?, ?, ?, ?, ?, 'exact', 'native', ?, ?, ?, ?)`,
		repoOwnerFamiliesOrg, "github", teamID, repoOwnerFamiliesRepo, "acme/api", primary, uint16(10), validFrom, updatedAt)
}

// seedRepoOwnerFamiliesInputs stores what the four families read for the day:
// two commits (one after hours), one author's review load, the complexity and
// the repository metrics of the repository.
func seedRepoOwnerFamiliesInputs(t *testing.T, ctx context.Context, conn driver.Conn) {
	t.Helper()
	org, day, repo := repoOwnerFamiliesOrg, repoOwnerFamiliesDay, repoOwnerFamiliesRepo
	seeded := day.Add(26 * time.Hour)
	execRepoOwnerFamilies(t, ctx, conn, "insert repo",
		"INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, ?, ?)", repo, "acme/api", org, "github", seeded)
	for hash, when := range map[string]time.Time{"c1": day.Add(12 * time.Hour), "c2": day.Add(23 * time.Hour)} {
		execRepoOwnerFamilies(t, ctx, conn, "insert commit "+hash, `INSERT INTO git_commits
    (repo_id, hash, author_name, author_email, author_when, committer_when, parents, last_synced, org_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, repo, hash, "Dev", "dev@example.com", when, when, uint32(1), seeded, org)
	}
	execRepoOwnerFamilies(t, ctx, conn, "insert user metrics", `INSERT INTO user_metrics_daily
    (repo_id, day, author_email, pr_interruption_load, review_request_load, computed_at, org_id)
    VALUES (?, ?, ?, ?, ?, ?, ?)`, repo, day, "dev@example.com", uint32(3), uint32(2), seeded, org)
	// Two days of complexity, one in each half of the 30-day window, so the
	// complexity delta of the compounding-risk score has both of its inputs.
	for offset, loc := range map[int]uint64{0: 1000, -20: 800} {
		execRepoOwnerFamilies(t, ctx, conn, "insert repository complexity", `INSERT INTO repo_complexity_daily
    (repo_id, day, loc_total, cyclomatic_total, cyclomatic_per_kloc, high_complexity_functions, very_high_complexity_functions, computed_at, org_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, repo, day.AddDate(0, 0, offset), loc, loc/5, float64(loc)/5, uint64(4), uint64(1), seeded, org)
	}
	execRepoOwnerFamilies(t, ctx, conn, "insert repository metrics", `INSERT INTO repo_metrics_daily
    (repo_id, day, rework_churn_ratio_30d, single_owner_file_ratio_30d, code_ownership_gini, bus_factor,
     pr_first_review_p90_hours, computed_at, org_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, repo, day, 0.3, 0.4, 0.5, uint32(2), 30.0, seeded, org)
}

// runRepoOwnerFamilies computes the day once: the partition family first, then
// the finalize families. The clock of team_wellbeing moves one second for each
// call, as a real clock does between the rows of a repository and a later
// write.
func runRepoOwnerFamilies(t *testing.T, ctx context.Context, conn driver.Conn, clock time.Time) {
	t.Helper()
	run := Run{ID: uuid.NewString(), OrganizationID: repoOwnerFamiliesOrg, TargetDay: repoOwnerFamiliesDay}
	partition := Partition{ID: uuid.NewString(), RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repoOwnerFamiliesRepo.String())}}

	wellbeing, err := NewTeamWellbeingExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	ticks := 0
	wellbeing.nowUTC = func() time.Time {
		ticks++
		return clock.Add(time.Duration(ticks) * time.Second)
	}
	if _, err := wellbeing.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatalf("team_wellbeing at %s: %v", clock.Format(time.RFC3339), err)
	}

	finalize := clock.Add(time.Minute)
	now := func() time.Time { return finalize }
	cognitive, err := NewTeamCognitiveLoadExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	cognitive.nowUTC = now
	complexity, err := NewTeamComplexityExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	complexity.nowUTC = now
	risk, err := NewCompoundingRiskTeamExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	risk.nowUTC = now
	for _, family := range []struct {
		name     string
		executor NativeFinalizeFamilyExecutor
	}{
		{"team_cognitive_load", cognitive}, {"team_complexity", complexity}, {"compounding_risk_team", risk},
	} {
		if _, err := family.executor.ComputeFinalizeFamily(ctx, run); err != nil {
			t.Fatalf("%s at %s: %v", family.name, clock.Format(time.RFC3339), err)
		}
	}
}

// repoOwnerFamiliesRead is what a reader of the newest rows sees for the day,
// for one team id or (team "") for every team.
type repoOwnerFamiliesRead struct {
	commits, afterHoursCommits uint64   // team_metrics_daily
	interruptionLoad           float64  // team_cognitive_load_daily
	afterHoursRatio            *float64 // team_cognitive_load_daily
	locTotal                   uint64   // team_complexity_daily
	scoredTeams                uint64   // compounding_risk_daily, scope team
	riskSum                    float64
}

func (read repoOwnerFamiliesRead) String() string {
	ratio := "NULL"
	if read.afterHoursRatio != nil {
		ratio = fmt.Sprint(*read.afterHoursRatio)
	}
	return fmt.Sprintf("commits=%d after_hours_commits=%d interruption_load=%v after_hours_ratio=%s loc_total=%d scored_teams=%d risk=%.6f",
		read.commits, read.afterHoursCommits, read.interruptionLoad, ratio, read.locTotal, read.scoredTeams, read.riskSum)
}

func readRepoOwnerFamilies(t *testing.T, ctx context.Context, conn driver.Conn, team string) repoOwnerFamiliesRead {
	t.Helper()
	org, day := repoOwnerFamiliesOrg, repoOwnerFamiliesDay
	filter := func(column string) (string, []any) {
		if team == "" {
			return "", []any{org, day}
		}
		return " AND " + column + " = ?", []any{org, day, team}
	}
	var read repoOwnerFamiliesRead
	scan := func(what, query, column string, targets ...any) {
		t.Helper()
		predicate, args := filter(column)
		if err := conn.QueryRow(ctx, fmt.Sprintf(query, predicate), args...).Scan(targets...); err != nil {
			t.Fatalf("%s of team %q: %v", what, team, err)
		}
	}
	scan("team_metrics_daily", `SELECT toUInt64(sum(commits_count)), toUInt64(sum(after_hours_commits_count))
FROM team_metrics_daily FINAL WHERE org_id = ? AND day = ?%s`, "team_id", &read.commits, &read.afterHoursCommits)
	scan("team_cognitive_load_daily", `SELECT sum(pr_interruption_load), max(after_hours_commit_ratio)
FROM team_cognitive_load_daily FINAL WHERE org_id = ? AND day = ?%s`, "team_id", &read.interruptionLoad, &read.afterHoursRatio)
	scan("team_complexity_daily", `SELECT toUInt64(sum(loc_total))
FROM team_complexity_daily FINAL WHERE org_id = ? AND day = ?%s`, "team_id", &read.locTotal)
	scan("compounding_risk_daily", `SELECT toUInt64(countIf(compounding_risk IS NOT NULL)), toFloat64(sum(ifNull(compounding_risk, 0)))
FROM compounding_risk_daily FINAL WHERE org_id = ? AND day = ? AND scope = 'team'%s`, "scope_id", &read.scoredTeams, &read.riskSum)
	return read
}

// repoOwnerFamiliesRowsSince is the number of stored row versions of a team
// in the four tables that were computed at or after a time.
func repoOwnerFamiliesRowsSince(t *testing.T, ctx context.Context, conn driver.Conn, team string, since time.Time) uint64 {
	t.Helper()
	var total uint64
	for table, column := range map[string]string{
		"team_metrics_daily": "team_id", "team_cognitive_load_daily": "team_id", "team_complexity_daily": "team_id",
		"compounding_risk_daily": "scope_id",
	} {
		var count uint64
		if err := conn.QueryRow(ctx,
			"SELECT count() FROM "+table+" WHERE org_id = ? AND day = ? AND "+column+" = ? AND computed_at >= ?",
			repoOwnerFamiliesOrg, repoOwnerFamiliesDay, team, since,
		).Scan(&count); err != nil {
			t.Fatalf("count the rows of team %s in %s: %v", team, table, err)
		}
		total += count
	}
	return total
}

func TestARecomputedDayOfTheRepositoryOwnerFamiliesIsCountedOnceAfterATeamIDChange(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := repoOwnerFamiliesDay
	t0 := day.Add(-72 * time.Hour)

	seedRepoOwnerFamiliesTeam(t, ctx, conn, "platform", 1, t0)
	seedRepoOwnerFamiliesOwnership(t, ctx, conn, "platform", 1, t0, t0)
	seedRepoOwnerFamiliesInputs(t, ctx, conn)

	first := day.Add(30 * time.Hour)
	runRepoOwnerFamilies(t, ctx, conn, first)
	before := readRepoOwnerFamilies(t, ctx, conn, "")
	bare := readRepoOwnerFamilies(t, ctx, conn, "platform")
	if before.commits != 2 || before.afterHoursCommits == 0 || before.interruptionLoad != 3 || before.afterHoursRatio == nil ||
		*before.afterHoursRatio == 0 || before.locTotal != 1000 || before.scoredTeams != 1 || before.riskSum == 0 ||
		bare.String() != before.String() {
		t.Fatalf("the first compute is not the case:\n every team %s\n platform   %s", before, bare)
	}

	// The team is replaced by its provider-keyed id.
	carried := first.Add(time.Hour)
	seedRepoOwnerFamiliesTeam(t, ctx, conn, "platform", 0, carried)
	seedRepoOwnerFamiliesTeam(t, ctx, conn, "github:platform", 1, carried)
	seedRepoOwnerFamiliesOwnership(t, ctx, conn, "github:platform", 0, t0, carried)

	second := first.Add(10 * time.Hour)
	runRepoOwnerFamilies(t, ctx, conn, second)
	after := readRepoOwnerFamilies(t, ctx, conn, "")
	if after.String() != before.String() {
		t.Errorf("a read with no team filter must return the recomputed day unchanged:\n before %s\n after  %s", before, after)
	}
	if keyed := readRepoOwnerFamilies(t, ctx, conn, "github:platform"); keyed.String() != bare.String() {
		t.Errorf("the keyed team must hold what the bare team held:\n bare (before) %s\n keyed (after) %s", bare, keyed)
	}
	zero := repoOwnerFamiliesRead{}
	if left := readRepoOwnerFamilies(t, ctx, conn, "platform"); left.String() != zero.String() {
		t.Errorf("the inactive team still holds measures of the recomputed day: %s", left)
	}

	// A third compute changes nothing a reader sees and writes no row under
	// the inactive id.
	third := second.Add(10 * time.Hour)
	runRepoOwnerFamilies(t, ctx, conn, third)
	if again := readRepoOwnerFamilies(t, ctx, conn, ""); again.String() != after.String() {
		t.Errorf("a further compute changed what a reader sees:\n second %s\n third  %s", after, again)
	}
	if rows := repoOwnerFamiliesRowsSince(t, ctx, conn, "platform", third); rows != 0 {
		t.Errorf("the third compute wrote %d row(s) under the inactive team, want 0", rows)
	}
}
