//go:build integration

package daily

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The end of a run works on the repositories of EVERY partition of the run.
// The list has no bound, and the driver writes a list into the statement
// text, so the read of the work scopes must not grow with it. Each case holds
// the row of an earlier compute in a work scope that ONE repository of the run
// still has an item in; that repository is the first or the last of the list,
// so a read that takes a part of the list only leaves the row.
//
// The path to the read: RetractStaleKeys -> retractStaleTeamKeysOfRun, which
// returns before its compute for a table that holds NO live key of the day
// -> the compute of the table -> loadWorkItemPartitionScopes. So a run of any
// size reaches the read only when a work-item table holds a measure for the
// day; the stored row of each case is what makes that so. The limit is the
// server's max_query_size (the parser's limit on the statement text; the
// default is 262144 bytes) and one repository id is about 40 bytes of the
// text. The test reads the server's value and shows on the same connection
// that one statement with the whole list is refused where it must be, so a
// case is not green because its list was short for this server.
func TestTheEndOfARunReadsTheWorkScopesOfAnyNumberOfRepositories(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	var maxQuerySize uint64
	if err := conn.QueryRow(ctx, "SELECT toUInt64(value) FROM system.settings WHERE name = 'max_query_size'").Scan(&maxQuerySize); err != nil {
		t.Fatalf("read max_query_size of the server: %v", err)
	}
	refused := 0
	day := earlyReturnDay
	earlier := day.Add(30 * time.Hour)
	clock := earlier.Add(10 * time.Hour)
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000007d1")
	index := 0
	for _, others := range []int{1000, 7000, 30000} {
		for _, position := range []string{"first", "last"} {
			index++
			org := fmt.Sprintf("00000000-0000-4000-8000-0000007d%04d", index)
			t.Run(fmt.Sprintf("%d other repositories, the repository of the scope is the %s", others, position), func(t *testing.T) {
				if err := conn.Exec(ctx, `INSERT INTO work_item_state_durations_daily
    (day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, avg_wip, computed_at, org_id)
    VALUES (?, 'linear', 'board-1', 'platform', 'Platform', 'in_progress', 10, 2, 2, ?, ?)`, day, earlier, org); err != nil {
					t.Fatalf("insert the earlier state row: %v", err)
				}
				if err := conn.Exec(ctx, `INSERT INTO work_items (
    repo_id, work_item_id, provider, type, status, project_id, created_at, started_at, org_id, last_synced)
    VALUES (?, 'ITEM-1', 'linear', 'story', 'in_progress', 'board-1', ?, ?, ?, ?)`,
					repo, day.Add(-48*time.Hour), day.Add(-24*time.Hour), org, earlier); err != nil {
					t.Fatalf("insert work item: %v", err)
				}
				repos := make([]RepositoryID, 0, others+1)
				if position == "first" {
					repos = append(repos, RepositoryID(repo.String()))
				}
				for other := 0; other < others; other++ {
					id := uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("%s:other:%d", org, other)))
					repos = append(repos, RepositoryID(id.String()))
				}
				if position == "last" {
					repos = append(repos, RepositoryID(repo.String()))
				}
				// The mechanism on this server: the read of the scopes as ONE
				// statement over the whole list.
				ids := make([]uuid.UUID, len(repos))
				for at, id := range repos {
					ids[at] = uuid.MustParse(string(id))
				}
				listBytes := uint64(len(ids)) * 40
				rows, err := conn.Query(ctx, "SELECT DISTINCT provider FROM work_items FINAL WHERE org_id = ? AND repo_id IN ?", org, ids)
				if err == nil {
					err = rows.Close()
				}
				switch {
				case listBytes > maxQuerySize && (err == nil || !strings.Contains(err.Error(), "code: 62")):
					t.Fatalf("a list of about %d bytes in one statement gave %v on a server with max_query_size %d, want code 62: the case is not set",
						listBytes, err, maxQuerySize)
				case listBytes > maxQuerySize:
					refused++
				case err != nil:
					t.Fatalf("a list of about %d bytes in one statement (max_query_size %d): %v", listBytes, maxQuerySize, err)
				}
				run := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, DiscoveredRepoIDs: repos}
				// A failed step ends the test here: the finalize of the run
				// would fail and be tried again with the same list.
				endStaleKeyRun(t, ctx, conn, run, clock)
				var held float64
				if err := conn.QueryRow(ctx, `SELECT toFloat64(sum(duration_hours + items_touched)) FROM work_item_state_durations_daily FINAL
WHERE org_id = ? AND day = ? AND team_id = 'platform'`, org, day).Scan(&held); err != nil {
					t.Fatalf("read the key: %v", err)
				}
				if held != 0 {
					t.Errorf("the key of the earlier compute still holds %v: the work scope of the %s repository of %d was not read",
						held, position, len(repos))
				}
			})
		}
	}
	if refused < 2 {
		t.Errorf("%d case(s) hold a list that one statement cannot carry on this server (max_query_size %d): the test needs two or more",
			refused, maxQuerySize)
	}
}

// ai_governance_coverage_daily: the end of a run supersedes the keys that no
// artifact of the day gives, and ONLY those. The key of the day's artifact
// keeps its measures.
func TestTheEndOfARunKeepsTheGovernanceKeysOfTheDay(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	const org = "00000000-0000-4000-8000-0000007d0101"
	day := earlyReturnDay
	earlier := day.Add(30 * time.Hour)
	clock := earlier.Add(10 * time.Hour)
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000007d2")
	gone := uuid.MustParse("00000000-0000-4000-8000-0000000007d3")
	exec := func(what, query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	// An earlier compute left a coverage row whose artifacts are gone.
	exec("insert the earlier coverage row", `INSERT INTO ai_governance_coverage_daily
    (org_id, team_id, repo_id, day, ai_artifacts, declared_artifacts, human_reviewed_prs, security_scanned_prs, in_policy_artifacts, computed_at)
    VALUES (?, 'platform', ?, ?, 4, 3, 2, 1, 1, ?)`, org, gone, day, earlier)
	// One artifact of the day.
	exec("insert the artifact", `INSERT INTO ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id,
    kind, source, confidence, actor, evidence, observed_at, ingested_at, superseded_by, computed_at)
    VALUES (generateUUIDv4(), toUUID(?), 'github', 'pull_request', '1', ?, 'ai_assisted', 'pr_label', 0.95, NULL,
            ?, ?, ?, NULL, ?)`,
		org, repo, `{"tool_name":"copilot","model_name":"gpt-4o"}`, day.Add(12*time.Hour), earlier, earlier)

	executor, err := NewAIGovernanceExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	executor.nowUTC = func() time.Time { return clock }
	run := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, DiscoveredRepoIDs: []RepositoryID{RepositoryID(repo.String())}}
	if _, err := executor.ComputeFamily(ctx, run, Partition{ID: uuid.NewString(), RunID: run.ID}); err != nil {
		t.Fatalf("ai_governance: %v", err)
	}
	artifacts := func(of uuid.UUID) uint64 {
		t.Helper()
		var count uint64
		if err := conn.QueryRow(ctx, `SELECT toUInt64(sum(ai_artifacts)) FROM ai_governance_coverage_daily FINAL
WHERE org_id = ? AND day = ? AND repo_id = ?`, org, day, of).Scan(&count); err != nil {
			t.Fatalf("read the coverage: %v", err)
		}
		return count
	}
	before := artifacts(repo)
	if before == 0 || artifacts(gone) != 4 {
		t.Fatalf("after the partition the day's repository holds %d artifact(s) and the earlier key %d: the case is not set", before, artifacts(gone))
	}
	endStaleKeyRun(t, ctx, conn, run, clock.Add(time.Hour))
	if after := artifacts(repo); after != before {
		t.Errorf("the key of the day's artifact holds %d artifact(s) after the end of the run, want its %d", after, before)
	}
	if stale := artifacts(gone); stale != 0 {
		t.Errorf("the key with no artifact of the day still holds %d", stale)
	}
}
