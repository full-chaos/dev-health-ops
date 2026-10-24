//go:build integration

package daily

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/icfinalize"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
)

// The stale-key rule of the two user-level tables (CHAOS-9135), through the
// real executors on the schema of the migration chain. Their sorting key holds
// no team (the team of a person is a value column), so a key that goes stale is
// a (scope, person, day) the compute no longer produces. The row of zeros of
// such a key carries the retraction marker, because a MEASURED row of these
// tables can hold 0 in every measure.

// livePeople reads the persons of one table the way the readers do: the newest
// row of each key, then the marker.
func livePeople(t *testing.T, ctx context.Context, conn driver.Conn, table, personColumn, org string, day time.Time) []string {
	t.Helper()
	// The real writers store each row of a work-item table in the org and the
	// scope view: a person counts once, in the org view.
	predicate := liverow.Predicate(table, "")
	if rowview.Has(table) {
		predicate += " AND " + rowview.OrgPredicate(table, "")
	}
	rows, err := conn.Query(ctx, fmt.Sprintf(
		"SELECT %[2]s FROM %[1]s FINAL WHERE org_id = ? AND day = ? AND %[3]s ORDER BY %[2]s",
		table, personColumn, predicate), org, day)
	if err != nil {
		t.Fatalf("read the live people of %s: %v", table, err)
	}
	defer rows.Close()
	var people []string
	for rows.Next() {
		var person string
		if err := rows.Scan(&person); err != nil {
			t.Fatal(err)
		}
		people = append(people, person)
	}
	sort.Strings(people)
	return people
}

// newestRow is the newest row of one key: the marker, the team and the version.
type newestRow struct {
	retracted uint8
	team      string
	teamName  string
	versions  uint64
}

func newestUserRow(t *testing.T, ctx context.Context, conn driver.Conn, query string, args ...any) newestRow {
	t.Helper()
	var row newestRow
	if err := conn.QueryRow(ctx, query, args...).Scan(&row.retracted, &row.team, &row.teamName, &row.versions); err != nil {
		t.Fatalf("read the newest row: %v", err)
	}
	return row
}

func TestARepositoryPartitionRetractsAPersonItNoLongerProduces(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	const org = "00000000-0000-4000-8000-0000000093b0"
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	repo, otherRepo := "00000000-0000-4000-8000-0000000093b1", "00000000-0000-4000-8000-0000000093b2"
	exec := func(what, query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	// What an earlier compute stored for the day: a person who has no commit on
	// the day now (the key is stale), a person of ANOTHER repository (outside the
	// partition), and the person who still has a commit.
	older := day.Add(10 * time.Hour)
	stored := func(repoID, person, team string) {
		exec("store the row of "+person, `INSERT INTO user_metrics_daily
 (repo_id, day, author_email, identity_id, team_id, team_name, commits_count, loc_added, computed_at, org_id)
 VALUES (?, ?, ?, ?, ?, ?, 5, 50, ?, ?)`, repoID, day, person, person, team, "Name of "+team, older, org)
	}
	stored(repo, "ghost@example.com", "linear:OLD")
	stored(otherRepo, "elsewhere@example.com", "linear:OTHER")
	exec("commit of alice", `INSERT INTO git_commits (repo_id, hash, author_name, author_email, committer_when, org_id, last_synced) VALUES
 (toUUID(?), 'a1', 'Alice', 'alice@example.com', toDateTime64('2026-08-24 12:00:00', 3, 'UTC'), ?, now64(3))`, repo, org)

	executor, err := NewRepoUserCommitExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	clock := day.Add(30 * time.Hour)
	executor.nowUTC = func() time.Time { return clock }
	run := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day}
	partition := Partition{ID: uuid.NewString(), RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repo)}}
	if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatalf("the partition: %v", err)
	}

	people := livePeople(t, ctx, conn, "user_metrics_daily", "author_email", org, day)
	if got, want := strings.Join(people, ","), "alice@example.com,elsewhere@example.com"; got != want {
		t.Errorf("live people = %s, want %s: the person with no commit is retracted, the person of another repository is left alone", got, want)
	}
	ghost := newestUserRow(t, ctx, conn, `SELECT argMax(retracted, computed_at), argMax(ifNull(team_id, ''), computed_at),
 argMax(ifNull(team_name, ''), computed_at), count() FROM user_metrics_daily WHERE org_id = ? AND day = ? AND author_email = 'ghost@example.com'`, org, day)
	if ghost.retracted != 1 || ghost.team != "linear:OLD" || ghost.teamName != "Name of linear:OLD" {
		t.Errorf("the newest row of the stale key = %+v, want the marker set and the team and its name carried from the row it retracts", ghost)
	}
	other := newestUserRow(t, ctx, conn, `SELECT argMax(retracted, computed_at), argMax(ifNull(team_id, ''), computed_at),
 argMax(ifNull(team_name, ''), computed_at), count() FROM user_metrics_daily WHERE org_id = ? AND day = ? AND author_email = 'elsewhere@example.com'`, org, day)
	if other.retracted != 0 || other.team != "linear:OTHER" {
		t.Errorf("a person of a repository outside the partition = %+v, want the stored row as it was", other)
	}
	// A repeat compute writes no further row of zeros.
	clock = clock.Add(time.Hour)
	if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatalf("the repeat: %v", err)
	}
	var written uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM user_metrics_daily WHERE org_id = ? AND day = ? AND author_email = 'ghost@example.com' AND computed_at >= ?`,
		org, day, clock).Scan(&written); err != nil {
		t.Fatal(err)
	}
	if written != 0 {
		t.Errorf("the repeat compute wrote %d row(s) for the stale key, want 0: a key that is retracted is not retracted again", written)
	}
	// The person who comes back: a commit again -> the key is live, on a newer row.
	exec("the ghost commits again", `INSERT INTO git_commits (repo_id, hash, author_name, author_email, committer_when, org_id, last_synced) VALUES
 (toUUID(?), 'g1', 'Ghost', 'ghost@example.com', toDateTime64('2026-08-24 13:00:00', 3, 'UTC'), ?, now64(3))`, repo, org)
	clock = clock.Add(time.Hour)
	if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatalf("the return: %v", err)
	}
	people = livePeople(t, ctx, conn, "user_metrics_daily", "author_email", org, day)
	if got, want := strings.Join(people, ","), "alice@example.com,elsewhere@example.com,ghost@example.com"; got != want {
		t.Errorf("live people after the return = %s, want %s: a retracted key that is produced again is live", got, want)
	}
}

func TestTheOrganizationRunLeavesTheSynthesizedKeysOfTheFinalizeFamilyAndRetractsAGoneRepository(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	const org = "00000000-0000-4000-8000-0000000093c0"
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	goneRepo := "00000000-0000-4000-8000-0000000093c1"
	synthesized := icfinalize.SynthesizedRepoID(org, "wi-only@example.com").String()
	older := day.Add(10 * time.Hour)
	exec := func(what, query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	for _, row := range []struct{ repo, person string }{{goneRepo, "gone@example.com"}, {synthesized, "wi-only@example.com"}} {
		exec("store "+row.person, `INSERT INTO user_metrics_daily
 (repo_id, day, author_email, identity_id, team_id, team_name, work_items_completed, computed_at, org_id)
 VALUES (?, ?, ?, ?, 'unassigned', 'Unassigned', 2, ?, ?)`, row.repo, day, row.person, row.person, older, org)
	}
	// An organization-scope run whose partitions hold no repository: the
	// repository of "gone" is in no partition and the organization no longer
	// holds it; the synthesized repository is no repository at all.
	run := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true}
	endStaleKeyRun(t, ctx, conn, run, day.Add(30*time.Hour))
	people := livePeople(t, ctx, conn, "user_metrics_daily", "author_email", org, day)
	if got, want := strings.Join(people, ","), "wi-only@example.com"; got != want {
		t.Errorf("live people after the run of the organization = %s, want %s: the key of a repository the organization no longer holds is retracted, a key under the synthesized repository is not (only the finalize family decides it)", got, want)
	}
}

func TestTheFinalizeFamilyRetractsASynthesizedPersonItNoLongerProduces(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	const org = "00000000-0000-4000-8000-0000000093d0"
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	older := day.Add(10 * time.Hour)
	exec := func(what, query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	// A person who has work-item metrics and no git row: the family writes a row
	// for her under the synthesized repository.
	exec("work-item metrics of wi-only", `INSERT INTO work_item_user_metrics_daily
 (day, provider, work_scope_id, user_identity, team_id, team_name, items_completed, computed_at, org_id)
 VALUES (?, 'jira', 'PLAT', 'wi-only@example.com', 'unassigned', 'Unassigned', 2, ?, ?)`, day, older, org)
	executor := NewICFinalizeExecutor(conn)
	run := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day}
	if _, err := executor.ComputeFinalizeFamily(ctx, run); err != nil {
		t.Fatalf("the first finalize: %v", err)
	}
	if got, want := strings.Join(livePeople(t, ctx, conn, "user_metrics_daily", "author_email", org, day), ","), "wi-only@example.com"; got != want {
		t.Fatalf("live people after the first finalize = %s, want %s: the case is not set", got, want)
	}
	// The recompute of the work-item day no longer produces her: the row of her
	// key in work_item_user_metrics_daily is retracted.
	exec("retract the work-item row of wi-only", `INSERT INTO work_item_user_metrics_daily
 (day, provider, work_scope_id, user_identity, team_id, team_name, computed_at, org_id, retracted)
 VALUES (?, 'jira', 'PLAT', 'wi-only@example.com', 'unassigned', 'Unassigned', ?, ?, 1)`, day, older.Add(time.Hour), org)
	if _, err := executor.ComputeFinalizeFamily(ctx, run); err != nil {
		t.Fatalf("the second finalize: %v", err)
	}
	if got := livePeople(t, ctx, conn, "user_metrics_daily", "author_email", org, day); len(got) != 0 {
		t.Errorf("live people after the second finalize = %v, want none: the person has no work-item row now and no git row, so her synthesized row is retracted", got)
	}
	if got := livePeople(t, ctx, conn, "work_item_user_metrics_daily", "user_identity", org, day); len(got) != 0 {
		t.Errorf("live people of the work-item table = %v, want none", got)
	}
}

// A person that the work-item compute no longer produces is retracted by the
// end of the run of the organization; a person with a MEASURED row of zeros
// (assigned only to an item created that day) stays live.
func TestAPersonTheWorkItemComputeNoLongerProducesIsRetractedAndAMeasuredZeroStaysLive(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := staleKeyAcceptanceDay
	seedStaleKeyAcceptanceTeams(t, ctx, conn, day.Add(-72*time.Hour),
		staleKeyAcceptanceTeam{id: "ENG", nativeKey: "ENG", active: true})
	done, started, created := day.Add(10*time.Hour), day.Add(-24*time.Hour), day.Add(3*time.Hour)
	storeItem := func(id, assignee, status string, startedAt, completedAt *time.Time, createdAt time.Time, synced time.Time) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO work_items (repo_id, work_item_id, provider, type, status, project_id, native_team_key,
 assignees, created_at, started_at, completed_at, org_id, last_synced) VALUES (?, ?, 'linear', 'story', ?, 'proj-eng', 'ENG', ?, ?, ?, ?, ?, ?)`,
			uuid.Nil, id, status, []string{assignee}, createdAt, startedAt, completedAt, staleKeyAcceptanceOrg, synced); err != nil {
			t.Fatalf("store item %s: %v", id, err)
		}
	}
	stored := day.Add(12 * time.Hour)
	storeItem("ENG-1", "alice", "done", &started, &done, day.Add(-48*time.Hour), stored)
	storeItem("ENG-2", "bob", "in_progress", &started, nil, day.Add(-48*time.Hour), stored)
	storeItem("ENG-3", "dave", "todo", nil, nil, created, stored)

	first := day.Add(30 * time.Hour)
	runStaleKeyAcceptanceFamilies(t, ctx, conn, first)
	people := func() string {
		return strings.Join(livePeople(t, ctx, conn, "work_item_user_metrics_daily", "user_identity", staleKeyAcceptanceOrg, day), ",")
	}
	if got, want := people(), "alice,bob,dave"; got != want {
		t.Fatalf("live people after the first compute = %s, want %s: the case is not set", got, want)
	}
	var daveMeasures uint64
	if err := conn.QueryRow(ctx, `SELECT toUInt64(items_started + items_completed + wip_count_end_of_day) FROM work_item_user_metrics_daily FINAL
 WHERE org_id = ? AND day = ? AND user_identity = 'dave' AND row_view IN ('', 'org')`, staleKeyAcceptanceOrg, day).Scan(&daveMeasures); err != nil {
		t.Fatal(err)
	}
	if daveMeasures != 0 {
		t.Fatalf("the row of dave holds measures %d, want a measured row of zeros: the case is not set", daveMeasures)
	}
	aliceBefore := newestUserRow(t, ctx, conn, `SELECT argMax(retracted, computed_at), argMax(team_id, computed_at), argMax(team_name, computed_at), count()
 FROM work_item_user_metrics_daily WHERE org_id = ? AND day = ? AND user_identity = 'alice' AND row_view IN ('', 'org')`, staleKeyAcceptanceOrg, day)

	// The item of alice goes to carol: alice has no row on the day any more.
	storeItem("ENG-1", "carol", "done", &started, &done, day.Add(-48*time.Hour), stored.Add(time.Hour))
	second := first.Add(10 * time.Hour)
	runStaleKeyAcceptanceFamilies(t, ctx, conn, second)
	if got, want := people(), "bob,carol,dave"; got != want {
		t.Errorf("live people after the recompute = %s, want %s: alice is retracted, carol takes the item, dave (a measured row of zeros) and bob stay", got, want)
	}
	alice := newestUserRow(t, ctx, conn, `SELECT argMax(retracted, computed_at), argMax(team_id, computed_at), argMax(team_name, computed_at), count()
 FROM work_item_user_metrics_daily WHERE org_id = ? AND day = ? AND user_identity = 'alice' AND row_view IN ('', 'org')`, staleKeyAcceptanceOrg, day)
	if alice.retracted != 1 || alice.team != aliceBefore.team || alice.teamName != aliceBefore.teamName || alice.team == "" {
		t.Errorf("the newest row of alice = %+v (before %+v), want the marker set and the team of the row it retracts carried", alice, aliceBefore)
	}
	if daveAfter := newestUserRow(t, ctx, conn, `SELECT argMax(retracted, computed_at), argMax(team_id, computed_at), argMax(team_name, computed_at), count()
 FROM work_item_user_metrics_daily WHERE org_id = ? AND day = ? AND user_identity = 'dave' AND row_view IN ('', 'org')`, staleKeyAcceptanceOrg, day); daveAfter.retracted != 0 {
		t.Errorf("the measured row of zeros of dave = %+v, want no marker", daveAfter)
	}
	// A further compute writes no further row for alice.
	third := second.Add(10 * time.Hour)
	runStaleKeyAcceptanceFamilies(t, ctx, conn, third)
	var written uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM work_item_user_metrics_daily WHERE org_id = ? AND day = ? AND user_identity = 'alice' AND computed_at >= ? AND row_view IN ('', 'org')`,
		staleKeyAcceptanceOrg, day, third).Scan(&written); err != nil {
		t.Fatal(err)
	}
	if written != 0 {
		t.Errorf("the third compute wrote %d row(s) for alice, want 0: a retracted key is not retracted again", written)
	}
}

// A person whose row under a real repository is retracted has no git record
// any more: the finalize family must neither read the retraction row as one
// (and write a live row over the key again) nor drop her work-item metrics.
func TestARetractedGitRowIsNoGitRecordOfTheFinalizeFamily(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	const org = "00000000-0000-4000-8000-0000000093d1"
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	older, newer := day.Add(10*time.Hour), day.Add(11*time.Hour)
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000093d2")
	exec := func(what, query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	for _, row := range []struct {
		at     time.Time
		marker uint8
		loc    uint32
	}{{older, 0, 40}, {newer, 1, 0}} {
		exec("git row of eve", `INSERT INTO user_metrics_daily
 (org_id, repo_id, day, author_email, identity_id, team_id, team_name, loc_added, computed_at, retracted)
 VALUES (?, ?, ?, 'eve@example.com', 'eve@example.com', 'unassigned', 'Unassigned', ?, ?, ?)`, org, repo, day, row.loc, row.at, row.marker)
	}
	exec("work-item metrics of eve", `INSERT INTO work_item_user_metrics_daily
 (day, provider, work_scope_id, user_identity, team_id, team_name, items_completed, computed_at, org_id)
 VALUES (?, 'jira', 'PLAT', 'eve@example.com', 'unassigned', 'Unassigned', 2, ?, ?)`, day, newer, org)
	if _, err := NewICFinalizeExecutor(conn).ComputeFinalizeFamily(ctx, Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day}); err != nil {
		t.Fatalf("the finalize: %v", err)
	}
	var realRepoMarker uint8
	if err := conn.QueryRow(ctx, `SELECT argMax(retracted, computed_at) FROM user_metrics_daily WHERE org_id = ? AND day = ? AND repo_id = ?`,
		org, day, repo).Scan(&realRepoMarker); err != nil {
		t.Fatal(err)
	}
	if realRepoMarker != 1 {
		t.Errorf("the key under the real repository has marker %d, want 1: the retraction row was read as a git record and written live again", realRepoMarker)
	}
	synthesized := icfinalize.SynthesizedRepoID(org, "eve@example.com")
	var live uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM (SELECT argMax(retracted, computed_at) AS r FROM user_metrics_daily
 WHERE org_id = ? AND day = ? AND repo_id = ? GROUP BY repo_id, author_email, day) WHERE r = 0`, org, day, synthesized).Scan(&live); err != nil {
		t.Fatal(err)
	}
	if live != 1 {
		t.Errorf("live rows of eve under the synthesized repository = %d, want 1: her work items still count", live)
	}
}
