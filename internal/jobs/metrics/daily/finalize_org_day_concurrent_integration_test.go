//go:build integration

package daily

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Runs of one organization and day that finalize at the same time write distinct versions to BOTH tables of
// ic_finalize (CHAOS-9945): each run claims its finalize through the store, as the worker does, and a refused claim
// is retried after its RetryAfter, as the queue's snooze does.
func TestConcurrentFinalizeRunsOfOneOrgDayWriteDistinctVersions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createDailyTables(t, ctx, pool)
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}

	const org = "00000000-0000-4000-8000-000000009945"
	const runs = 8
	day := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	longBefore := day.AddDate(0, 0, -60)
	seededAt := day.Add(30 * time.Hour)
	clock := day.Add(48 * time.Hour)
	exec := func(what, query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	exec("insert team", `INSERT INTO teams
    (id, team_uuid, name, repo_patterns, updated_at, last_synced, org_id, provider, is_active)
    VALUES ('github:alpha', ?, 'Team alpha', ?, ?, ?, ?, 'github', 1)`,
		uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:github:alpha")), []string{}, longBefore, longBefore, org)
	exec("insert membership", `INSERT INTO team_memberships
    (org_id, provider, team_id, member_id, raw_email, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, identity_facets)
    VALUES (?, 'github', 'github:alpha', 'a', 'a@example.com', 'native', 1, 100, 10, ?, NULL, ?, ?)`,
		org, longBefore, longBefore, []string{"a@example.com"})
	repoID := personRepo(org, "a")
	exec("insert repo", `INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, 'acme/a', ?, 'github', ?)`, repoID, org, longBefore)
	exec("insert ownership", `INSERT INTO team_repo_ownership
    (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, valid_from, valid_to, updated_at)
    VALUES (?, 'github', 'github:alpha', ?, 'acme/a', 'exact', 'native', 1, 10, ?, NULL, ?)`, org, repoID, longBefore, longBefore)
	exec("insert user metrics", `INSERT INTO user_metrics_daily
    (repo_id, day, author_email, identity_id, team_id, team_name, commits_count, loc_added, loc_deleted,
     prs_authored, prs_merged, loc_touched, delivery_units, computed_at, org_id)
    VALUES (?, ?, 'a@example.com', 'a@example.com', 'unassigned', 'Unassigned', 3, 40, 10, 1, 1, 50, 1, ?, ?)`,
		repoID, day, seededAt, org)

	runIDs := make([]string, runs)
	for index := range runIDs {
		runIDs[index] = uuid.NewString()
		if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_runs (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at)
VALUES ($1,$2,'2026-09-21',$3,'running','pending',now(),now())`, runIDs[index], org, fmt.Sprintf("g%d", index)); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, runs)
	for _, runID := range runIDs {
		executor := NewICFinalizeExecutor(conn)
		executor.SetClock(func() time.Time { return clock })
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				claim, err := store.ClaimFinalize(ctx, runID)
				var active *LeaseActiveError
				if errors.As(err, &active) {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				if err != nil || claim == nil {
					errs <- fmt.Errorf("claim of %s: claim=%v err=%w", runID, claim, err)
					return
				}
				if _, err := executor.ComputeFinalizeFamily(ctx, claim.Run); err != nil {
					errs <- fmt.Errorf("compute of %s: %w", runID, err)
					return
				}
				errs <- store.CompleteFinalize(ctx, *claim)
				return
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("finalize: %v", err)
		}
	}

	// Serialized runs under one fixed clock take clock, clock+1s, ... clock+(runs-1)s; overlapping runs that tie reach
	// a lower newest version. Rows of one key can be merged away, so the newest version is read.
	want := clock.Add((runs - 1) * time.Second)
	for _, table := range []struct{ name, dayColumn string }{
		{"user_metrics_daily", "day"}, {"ic_landscape_rolling_30d", "as_of_day"},
	} {
		rows, err := conn.Query(ctx, "SELECT count(), max(computed_at) FROM "+table.name+" WHERE org_id = ? AND "+table.dayColumn+" = ? AND computed_at > ?", org, day, seededAt)
		if err != nil {
			t.Fatal(err)
		}
		var count uint64
		var newest time.Time
		for rows.Next() {
			if err := rows.Scan(&count, &newest); err != nil {
				t.Fatal(err)
			}
		}
		rows.Close()
		if count == 0 {
			t.Fatalf("%s holds no row written by the runs: the seed does not reach this table", table.name)
		}
		if !newest.UTC().Equal(want) {
			t.Fatalf("%s: newest computed_at = %s, want %s (each run one second after the one before)", table.name, newest.UTC(), want)
		}
	}
}
