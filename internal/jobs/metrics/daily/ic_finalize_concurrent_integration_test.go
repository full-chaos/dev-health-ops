//go:build integration

package daily

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Overlapping ic_finalize computes of one organization and day (a repository
// scoped manual run beside a scheduled run: the first skips the day lock) must
// not write rows that tie on computed_at: a tie leaves the read to pick either
// run's row (CHAOS-9945).
func TestConcurrentICFinalizeRunsOfOneDayWriteDistinctVersions(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	const org = "00000000-0000-4000-8000-000000009945"
	const runs = 8
	day := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	seededAt := day.Add(30 * time.Hour)
	clock := day.Add(48 * time.Hour)
	if err := conn.Exec(ctx, `INSERT INTO user_metrics_daily
    (repo_id, day, author_email, identity_id, team_id, team_name, commits_count, loc_added, loc_deleted,
     prs_authored, prs_merged, loc_touched, delivery_units, computed_at, org_id)
    VALUES (?, ?, 'a@example.com', 'a@example.com', 'unassigned', 'Unassigned', 3, 40, 10, 1, 1, 50, 1, ?, ?)`,
		personRepo(org, "a"), day, seededAt, org); err != nil {
		t.Fatalf("seed: %v", err)
	}

	locker := dayLockerForTest(t, ctx)
	var wg sync.WaitGroup
	errs := make(chan error, runs)
	for range runs {
		executor := NewICFinalizeExecutor(conn)
		executor.SetClock(func() time.Time { return clock })
		setDayLockerForTest(executor, locker)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := executor.ComputeFinalizeFamily(ctx, Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("ic_finalize: %v", err)
		}
	}

	// Serialized runs under one fixed clock take clock, clock+1s, ... clock+(runs-1)s; overlapping runs that
	// tie reach a lower newest version. Rows of one key may be merged away, so the newest version is read.
	rows, err := conn.Query(ctx, `SELECT max(computed_at) FROM user_metrics_daily WHERE org_id = ? AND day = ?`, org, day)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var newest time.Time
	for rows.Next() {
		if err := rows.Scan(&newest); err != nil {
			t.Fatal(err)
		}
	}
	if want := clock.Add((runs - 1) * time.Second); !newest.UTC().Equal(want) {
		t.Fatalf("%d concurrent runs: newest computed_at = %s, want %s (each run one second after the one before)", runs, newest.UTC(), want)
	}
}
