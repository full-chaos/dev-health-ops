//go:build integration

package daily

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func rerunStore(t *testing.T, ctx context.Context) (*PostgresStore, *PostgresPublisher, *pgxpool.Pool) {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createDailyTables(t, ctx, pool)
	registry, err := jobruntime.Load(filepath.Join("..", "..", "..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := NewPostgresPublisher(pool, dailyTestRegistry{production: registry})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	return store, publisher, pool
}

func rerunRunCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, org, day string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM daily_metrics_runs WHERE org_id = $1::uuid AND target_day = $2::date", org, day,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

// The route of the team-id recompute: a repository-scoped manual run for a day
// that already has the same run. The plain call starts nothing (the need); a
// call with a tag starts one run; the same tag again starts nothing; a new tag
// starts one more.
func TestStartManualDailyRunWithARerunTagStartsOneRunPerTagForADayThatHasARun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	store, publisher, pool := rerunStore(t, ctx)
	const org = "00000000-0000-4000-8000-0000000009a1"
	const day = "2026-08-26"
	repos := []RepositoryID{"00000000-0000-4000-8000-0000000009b1", "00000000-0000-4000-8000-000000000000"}

	start := func(tag string) ManualDailyRunOutcome {
		t.Helper()
		outcome, err := store.StartManualDailyRun(ctx, org, day, ManualDailyRerunGeneration(org, day, repos, tag), repos, publisher)
		if err != nil {
			t.Fatalf("StartManualDailyRun(tag=%q): %v", tag, err)
		}
		return outcome
	}

	first := start("")
	if first.AlreadyStarted || rerunRunCount(t, ctx, pool, org, day) != 1 {
		t.Fatalf("the first plain call must start the first run: %+v", first)
	}
	// The run of the day succeeded before the repair.
	if _, err := pool.Exec(ctx, `UPDATE daily_metrics_runs SET status='succeeded', finalization_status='succeeded' WHERE id = $1::uuid`, first.RunID); err != nil {
		t.Fatal(err)
	}

	if again := start(""); !again.AlreadyStarted || again.RunID != first.RunID || rerunRunCount(t, ctx, pool, org, day) != 1 {
		t.Fatalf("a plain call for a day that has the run must start nothing: %+v runs=%d", again, rerunRunCount(t, ctx, pool, org, day))
	}

	tagged := start("fix-1")
	if tagged.AlreadyStarted || tagged.RunID == first.RunID || rerunRunCount(t, ctx, pool, org, day) != 2 {
		t.Fatalf("a tagged call must start one new run: %+v runs=%d", tagged, rerunRunCount(t, ctx, pool, org, day))
	}
	var partitions int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_partitions WHERE run_id = $1::uuid`, tagged.RunID).Scan(&partitions); err != nil || partitions == 0 {
		t.Fatalf("the tagged run must hold its partitions: count=%d err=%v", partitions, err)
	}

	if same := start("fix-1"); !same.AlreadyStarted || same.RunID != tagged.RunID || rerunRunCount(t, ctx, pool, org, day) != 2 {
		t.Fatalf("the same tag again must start nothing: %+v runs=%d", same, rerunRunCount(t, ctx, pool, org, day))
	}

	if next := start("fix-2"); next.AlreadyStarted || next.RunID == tagged.RunID || next.RunID == first.RunID ||
		rerunRunCount(t, ctx, pool, org, day) != 3 {
		t.Fatalf("a new tag must start one more run: %+v runs=%d", next, rerunRunCount(t, ctx, pool, org, day))
	}
}

// The guard of the deferred-discovery path stays in force for a tagged call:
// the tag changes the generation of the request, not the answer for a day the
// schedule already computed.
func TestStartManualDailyRunWithARerunTagStillRefusesADayTheScheduleCovered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	store, publisher, pool := rerunStore(t, ctx)
	const org = "00000000-0000-4000-8000-0000000009a2"
	const day = "2026-08-26"
	now := time.Now().UTC()
	if _, err := pool.Exec(ctx, `
INSERT INTO daily_metrics_runs (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at)
VALUES ('00000000-0000-4000-8000-0000000009c1',$1::uuid,$2::date,'fixed-schedule:daily_metrics_fanout:2026-08-26T01:00:00Z','succeeded','succeeded',$3,$3)`,
		org, day, now); err != nil {
		t.Fatal(err)
	}
	_, err := store.StartManualDailyRun(ctx, org, day, ManualDailyRerunGeneration(org, day, nil, "fix-1"), nil, publisher)
	if !errors.Is(err, ErrDayAlreadyCovered) {
		t.Fatalf("a tagged deferred-discovery call must still be refused for a covered day: %v", err)
	}
	if rerunRunCount(t, ctx, pool, org, day) != 1 {
		t.Fatalf("the refused call must start nothing")
	}
}
