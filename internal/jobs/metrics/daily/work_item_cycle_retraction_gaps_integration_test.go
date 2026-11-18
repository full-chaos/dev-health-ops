//go:build integration

package daily

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

func storeWorkItemCopy(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string, repoID uuid.UUID,
	workItemID, status, projectKey string, createdAt time.Time, completedAt *time.Time, syncedAt time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (
		repo_id, work_item_id, provider, title, type, status, project_key, created_at, started_at, completed_at,
		labels, story_points, org_id, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	started := createdAt.Add(time.Hour)
	if err := batch.Append(
		repoID, workItemID, "github", "Item "+workItemID, "issue", status, projectKey, createdAt, &started, completedAt,
		[]string{"quality"}, nil, orgID, syncedAt,
	); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

func computeWorkItemDay(t *testing.T, ctx context.Context, executor *WorkItemExecutor, orgID string, day time.Time, repos ...uuid.UUID) {
	t.Helper()
	ids := make([]RepositoryID, 0, len(repos))
	for _, repo := range repos {
		ids = append(ids, RepositoryID(repo.String()))
	}
	run := Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day}
	if _, err := executor.ComputeFamily(ctx, run, Partition{ID: "p", RunID: run.ID, RepoIDs: ids}); err != nil {
		t.Fatal(err)
	}
}

func executorAt(t *testing.T, conn driver.Conn, at time.Time) *WorkItemExecutor {
	t.Helper()
	executor, err := NewWorkItemExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	executor.nowUTC = func() time.Time { return at }
	return executor
}

// (a) The newest repository copy of the item is done with no completed_at: the item is not completed. An older
// copy that still holds a completion on the day must not make the compute of that day write it.
func TestCycleRowIsNotEmittedFromAnOlderCopyWhenTheNewestCopyHasNoCompletion(t *testing.T) {
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	created := day.Add(-48 * time.Hour)
	done := day.Add(10 * time.Hour)
	now := time.Now().UTC().Truncate(time.Second)
	for _, layout := range []struct{ name, newestProject string }{
		{"same_scope", "proj-a"},
		{"different_scopes", "proj-b"},
	} {
		t.Run(layout.name, func(t *testing.T) {
			ctx := context.Background()
			conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
			orgID := "org-current-copy-" + uuid.NewString()
			olderRepo, newestRepo := uuid.New(), uuid.New()
			storeWorkItemCopy(t, ctx, conn, orgID, olderRepo, "cc-1", "done", "proj-a", created, &done, now.Add(-4*time.Hour))
			computeWorkItemDay(t, ctx, executorAt(t, conn, now.Add(-3*time.Hour)), orgID, day, olderRepo, newestRepo)
			if got := completedWorkItemCycleRows(t, ctx, conn, orgID, day); got != 1 {
				t.Fatalf("after the first compute: %d completed cycle rows, want 1", got)
			}
			storeWorkItemCopy(t, ctx, conn, orgID, newestRepo, "cc-1", "done", layout.newestProject, created, nil, now.Add(-2*time.Hour))
			computeWorkItemDay(t, ctx, executorAt(t, conn, now.Add(-time.Hour)), orgID, day, olderRepo, newestRepo)
			if got := completedWorkItemCycleRows(t, ctx, conn, orgID, day); got != 0 {
				t.Fatalf("after the newest copy lost its completed_at and a recompute of the day: %d completed cycle rows, want 0", got)
			}
		})
	}
}

// (b) A run of listed repositories computes every item of the work scopes it reaches, also items stored in a
// repository it does not list. A reopened item of such a scope keeps no completion row.
func TestRepositoryScopedComputeRetractsAReopenedItemOfItsWorkScope(t *testing.T) {
	day := time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC)
	past := day.AddDate(0, 0, -2)
	created := day.AddDate(0, 0, -3)
	done := past.Add(10 * time.Hour)
	now := time.Now().UTC().Truncate(time.Second)
	for _, kept := range []struct {
		name      string
		completed *time.Time
	}{
		{"completed_at_cleared", nil},
		{"completed_at_kept", &done},
	} {
		t.Run(kept.name, func(t *testing.T) {
			ctx := context.Background()
			conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
			orgID := "org-shared-scope-" + uuid.NewString()
			anchorRepo, itemRepo := uuid.New(), uuid.New()
			storeWorkItemCopy(t, ctx, conn, orgID, anchorRepo, "anchor-1", "in_progress", "shared", created, nil, now.Add(-5*time.Hour))
			storeWorkItemCopy(t, ctx, conn, orgID, itemRepo, "wi-1", "done", "shared", created, &done, now.Add(-5*time.Hour))
			computeWorkItemDay(t, ctx, executorAt(t, conn, now.Add(-4*time.Hour)), orgID, past, itemRepo)
			if got := completedWorkItemCycleRows(t, ctx, conn, orgID, past); got != 1 {
				t.Fatalf("after the compute of the completion day: %d completed cycle rows, want 1", got)
			}
			storeWorkItemCopy(t, ctx, conn, orgID, itemRepo, "wi-1", "in_progress", "shared", created, kept.completed, now.Add(-2*time.Hour))
			executor := executorAt(t, conn, now.Add(-time.Hour))
			scope, err := newWorkItemPartitionScope(Run{OrganizationID: orgID, TargetDay: day},
				Partition{RepoIDs: []RepositoryID{RepositoryID(anchorRepo.String())}}, "work_item")
			if err != nil {
				t.Fatal(err)
			}
			read, _, err := computeWorkItemTriplet(ctx, conn, Run{OrganizationID: orgID, TargetDay: day},
				Partition{ID: "p", RepoIDs: []RepositoryID{RepositoryID(anchorRepo.String())}}, scope)
			if err != nil {
				t.Fatal(err)
			}
			if read.Stats.ItemsOutsidePartition != 1 {
				t.Fatalf("the anchor repository's compute holds %d items of unlisted repositories, want 1 (wi-1)", read.Stats.ItemsOutsidePartition)
			}
			computeWorkItemDay(t, ctx, executor, orgID, day, anchorRepo)
			if got := completedWorkItemCycleRows(t, ctx, conn, orgID, past); got != 0 {
				t.Fatalf("after wi-1 reopened and a compute of the anchor repository: %d completed cycle rows on %s, want 0",
					got, past.Format("2006-01-02"))
			}
		})
	}
}

// pauseBeforeCycleInsert runs pause once, when the wrapped executor is about to insert into work_item_cycle_times:
// after its reads, before its writes.
type pauseBeforeCycleInsert struct {
	driver.Conn
	pause func()
}

func (conn *pauseBeforeCycleInsert) PrepareBatch(
	ctx context.Context, query string, opts ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	if conn.pause != nil && strings.Contains(query, "INSERT INTO work_item_cycle_times") {
		pause := conn.pause
		conn.pause = nil
		pause()
	}
	return conn.Conn.PrepareBatch(ctx, query, opts...)
}

// stalledHistoricalCompute is the overlap of (c): a compute of the completion day (clock historicalAt) reads the
// closed item, then stalls before its cycle insert. Meanwhile the item is reopened and a compute of the next day
// (clock laterDayAt) retracts the completion. The stalled compute then resumes. It returns the winning cycle row of
// the completion day: its completed_at and its version.
func stalledHistoricalCompute(t *testing.T, laterDayAt, historicalAt time.Time) (*time.Time, time.Time) {
	t.Helper()
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	orgID := "org-overlap-" + uuid.NewString()
	repoID := uuid.New()
	day := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	created := day.Add(-48 * time.Hour)
	done := day.Add(10 * time.Hour)
	first := historicalAt.Add(-5 * time.Hour)
	storeWorkItemCopy(t, ctx, conn, orgID, repoID, "ov-1", "done", "proj", created, &done, first.Add(-time.Hour))
	computeWorkItemDay(t, ctx, executorAt(t, conn, first), orgID, day, repoID)
	if got := completedWorkItemCycleRows(t, ctx, conn, orgID, day); got != 1 {
		t.Fatalf("after the first compute: %d completed cycle rows, want 1", got)
	}

	paused := false
	stalled := &pauseBeforeCycleInsert{Conn: conn, pause: func() {
		paused = true
		storeWorkItemCopy(t, ctx, conn, orgID, repoID, "ov-1", "in_progress", "proj", created, nil, first.Add(time.Hour))
		computeWorkItemDay(t, ctx, executorAt(t, conn, laterDayAt), orgID, day.AddDate(0, 0, 1), repoID)
		if got := completedWorkItemCycleRows(t, ctx, conn, orgID, day); got != 0 {
			t.Errorf("the later day's compute left %d completed cycle rows, want 0", got)
		}
	}}
	computeWorkItemDay(t, ctx, executorAt(t, stalled, historicalAt), orgID, day, repoID)
	if !paused {
		t.Fatal("the historical compute never reached its cycle insert")
	}
	var completedAt *time.Time
	var version time.Time
	if err := conn.QueryRow(ctx, `
SELECT completed_at, computed_at FROM work_item_cycle_times FINAL
WHERE org_id = ? AND provider = 'github' AND work_item_id = 'ov-1' AND day = ?`, orgID, day).Scan(&completedAt, &version); err != nil {
		t.Fatal(err)
	}
	return completedAt, version
}

// (c) The stalled compute writes the completion with a newer version than the retraction. The item is open: the
// completion must not be the winning row.
func TestStalledHistoricalComputeDoesNotRestoreARetractedCompletion(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	completedAt, version := stalledHistoricalCompute(t, now.Add(-2*time.Hour), now.Add(-time.Hour))
	if completedAt != nil {
		t.Fatalf("after the stalled historical compute resumed: the winning row is completed at %s (version %s), want no completion (the item is open)",
			completedAt.Format(time.RFC3339), version.Format(time.RFC3339))
	}
}

// (c) in one wall second: the next day's retraction, the stalled compute's completion and its own retraction all get
// the same whole-second clock. The completion ties the retraction it overwrites; the stalled compute's own
// retraction is still strictly newer than its completion.
func TestStalledHistoricalComputeRetractsItsOwnCompletionInTheSameSecond(t *testing.T) {
	second := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
	completedAt, version := stalledHistoricalCompute(t, second.Add(100*time.Millisecond), second.Add(900*time.Millisecond))
	if completedAt != nil {
		t.Fatalf("the winning row is completed at %s (version %s), want the retraction", completedAt.Format(time.RFC3339), version.Format(time.RFC3339))
	}
	if want := second.Add(time.Second); !version.Equal(want) {
		t.Fatalf("the retraction's version is %s, want %s (one second after the completion it replaces)",
			version.Format(time.RFC3339), want.Format(time.RFC3339))
	}
}

// The current copy on a last_synced tie is the copy of the LOWER repository id (workitemmetrics.OncePerProviderAndID),
// the rule of the day read. It decides whether the completion stays.
func TestRetractionTieOfLastSyncedIsDecidedByTheLowerRepositoryID(t *testing.T) {
	lowRepo := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	highRepo := uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
	day := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	created := day.Add(-48 * time.Hour)
	done := day.Add(10 * time.Hour)
	synced := time.Now().UTC().Truncate(time.Second).Add(-4 * time.Hour)
	for _, tc := range []struct {
		name                    string
		completedRepo, openRepo uuid.UUID
		want                    uint64
	}{
		{"lower_copy_open_retracts", highRepo, lowRepo, 0},
		{"lower_copy_done_keeps", lowRepo, highRepo, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
			orgID := "org-tie-" + uuid.NewString()
			storeWorkItemCopy(t, ctx, conn, orgID, tc.completedRepo, "tie-1", "done", "proj", created, &done, synced)
			computeWorkItemDay(t, ctx, executorAt(t, conn, synced.Add(time.Hour)), orgID, day, lowRepo, highRepo)
			if got := completedWorkItemCycleRows(t, ctx, conn, orgID, day); got != 1 {
				t.Fatalf("after the completion compute: %d completed cycle rows, want 1", got)
			}
			storeWorkItemCopy(t, ctx, conn, orgID, tc.openRepo, "tie-1", "in_progress", "proj", created, nil, synced)
			computeWorkItemDay(t, ctx, executorAt(t, conn, synced.Add(2*time.Hour)), orgID, day.AddDate(0, 0, 1), lowRepo, highRepo)
			if got := completedWorkItemCycleRows(t, ctx, conn, orgID, day); got != tc.want {
				t.Fatalf("two copies with one last_synced: %d completed cycle rows, want %d", got, tc.want)
			}
		})
	}
}

func completedCycleRowsBetween(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, from, to time.Time) uint64 {
	t.Helper()
	return readCycleChart(t, ctx, conn, orgID, from, to).completedRows
}

// months guard: closed in August, reopened, closed in September, reopened again. Each month window reads 0.
func TestReopenedTwiceAcrossMonthsLeavesNoCompletionInEitherMonth(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	orgID := "org-months-" + uuid.NewString()
	repo := uuid.New()
	aug := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	sep := time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	created := aug.Add(-48 * time.Hour)
	doneAug, doneSep := aug.Add(10*time.Hour), sep.Add(10*time.Hour)
	now := time.Now().UTC().Truncate(time.Second).Add(-12 * time.Hour)
	storeWorkItemCopy(t, ctx, conn, orgID, repo, "m-1", "done", "proj", created, &doneAug, now)
	computeWorkItemDay(t, ctx, executorAt(t, conn, now.Add(time.Hour)), orgID, aug, repo)
	storeWorkItemCopy(t, ctx, conn, orgID, repo, "m-1", "done", "proj", created, &doneSep, now.Add(2*time.Hour))
	computeWorkItemDay(t, ctx, executorAt(t, conn, now.Add(3*time.Hour)), orgID, sep, repo)
	storeWorkItemCopy(t, ctx, conn, orgID, repo, "m-1", "in_progress", "proj", created, nil, now.Add(4*time.Hour))
	computeWorkItemDay(t, ctx, executorAt(t, conn, now.Add(5*time.Hour)), orgID, sep.AddDate(0, 0, 1), repo)
	august := completedCycleRowsBetween(t, ctx, conn, orgID, aug.AddDate(0, 0, -2), aug.AddDate(0, 1, 0))
	september := completedCycleRowsBetween(t, ctx, conn, orgID, sep.AddDate(0, 0, -2), sep.AddDate(0, 1, 0))
	if august != 0 || september != 0 {
		t.Fatalf("august=%d september=%d completed cycle rows, want 0 and 0 (the item is open)", august, september)
	}
}

// months guard: closed in August, September and October, reopened; the compute runs twice (a retry).
func TestReopenedAfterThreeMonthlyCompletionsLeavesNoneAfterARetry(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	orgID := "org-three-months-" + uuid.NewString()
	repo := uuid.New()
	days := []time.Time{
		time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC), time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC),
	}
	created := days[0].Add(-48 * time.Hour)
	now := time.Now().UTC().Truncate(time.Second).Add(-20 * time.Hour)
	tick := 0
	next := func() time.Time { tick++; return now.Add(time.Duration(tick) * time.Hour) }
	for _, day := range days {
		completed := day.Add(9 * time.Hour)
		storeWorkItemCopy(t, ctx, conn, orgID, repo, "t-1", "done", "proj", created, &completed, next())
		computeWorkItemDay(t, ctx, executorAt(t, conn, next()), orgID, day, repo)
	}
	storeWorkItemCopy(t, ctx, conn, orgID, repo, "t-1", "in_progress", "proj", created, nil, next())
	computeWorkItemDay(t, ctx, executorAt(t, conn, next()), orgID, days[2].AddDate(0, 0, 1), repo)
	computeWorkItemDay(t, ctx, executorAt(t, conn, next()), orgID, days[2].AddDate(0, 0, 1), repo)
	var counts []uint64
	for _, window := range [][2]time.Time{
		{days[0].AddDate(0, 0, -24), days[0].AddDate(0, 0, 7)}, {days[1].AddDate(0, 0, -4), days[1].AddDate(0, 0, 25)},
		{days[2].AddDate(0, 0, -4), days[2].AddDate(0, 0, 25)}, {days[0].AddDate(0, 0, -24), days[2].AddDate(0, 0, 25)},
	} {
		counts = append(counts, completedCycleRowsBetween(t, ctx, conn, orgID, window[0], window[1]))
	}
	for _, count := range counts {
		if count != 0 {
			t.Fatalf("aug/sep/oct/whole completed cycle rows = %v, want all 0 (the item is open)", counts)
		}
	}
}
