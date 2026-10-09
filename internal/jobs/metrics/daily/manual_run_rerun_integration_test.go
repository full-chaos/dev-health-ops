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

// A call without a tag keeps the refusal for a day that the schedule or a sync
// already covered; a call with a tag is the operator's statement "compute this
// day again" and is admitted (CHAOS-8941 recompute route). Both for the two
// covering generations, through the real admission path.
func TestStartManualDailyRunRefusesACoveredDayAndARerunTagAdmitsIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	store, publisher, pool := rerunStore(t, ctx)
	const org = "00000000-0000-4000-8000-0000000009a2"
	now := time.Now().UTC()
	covers := []struct{ day, id, generation string }{
		{"2026-08-26", "00000000-0000-4000-8000-0000000009c1", "fixed-schedule:daily_metrics_fanout:2026-08-26T01:00:00Z"},
		{"2026-08-27", "00000000-0000-4000-8000-0000000009c2", "post-sync:00000000-0000-4000-8000-0000000009d2"},
	}
	for _, cover := range covers {
		if _, err := pool.Exec(ctx, `
INSERT INTO daily_metrics_runs (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at)
VALUES ($1::uuid,$2::uuid,$3::date,$4,'succeeded','succeeded',$5,$5)`,
			cover.id, org, cover.day, cover.generation, now); err != nil {
			t.Fatal(err)
		}
	}
	for _, cover := range covers {
		day := cover.day
		// no tag: refused, starts nothing
		_, err := store.StartManualDailyRun(ctx, org, day, ManualDailyRunGeneration(org, day, nil), nil, publisher)
		if !errors.Is(err, ErrDayAlreadyCovered) || rerunRunCount(t, ctx, pool, org, day) != 1 {
			t.Fatalf("%s: a call without a tag must be refused for a covered day: %v", cover.generation, err)
		}
		// the tag admits it: one new run, named after the run it overrides
		generation := ManualDailyRerunGeneration(org, day, nil, "recompute-1")
		tagged, err := store.StartManualDailyRerun(ctx, org, day, generation, nil, publisher, "recompute-1")
		if err != nil || tagged.AlreadyStarted || tagged.CoveredDayOverriddenBy != cover.id || rerunRunCount(t, ctx, pool, org, day) != 2 {
			t.Fatalf("%s: a tagged call must start one run and name the covering run: %+v err %v runs %d",
				cover.generation, tagged, err, rerunRunCount(t, ctx, pool, org, day))
		}
		var generationStored string
		if err := pool.QueryRow(ctx, `SELECT generation FROM daily_metrics_runs WHERE id = $1::uuid`, tagged.RunID).Scan(&generationStored); err != nil || generationStored != generation {
			t.Fatalf("the tagged run must carry the tag in its generation: %q err=%v", generationStored, err)
		}
		// the same tag again: one run (identity holds), still named
		same, err := store.StartManualDailyRerun(ctx, org, day, generation, nil, publisher, "recompute-1")
		if err != nil || !same.AlreadyStarted || same.RunID != tagged.RunID || rerunRunCount(t, ctx, pool, org, day) != 2 {
			t.Fatalf("%s: the same tag again must start nothing: %+v err %v", cover.generation, same, err)
		}
		// a new tag is a new run; the plain call is still refused afterwards
		next, err := store.StartManualDailyRerun(ctx, org, day, ManualDailyRerunGeneration(org, day, nil, "recompute-2"), nil, publisher, "recompute-2")
		if err != nil || next.AlreadyStarted || next.RunID == tagged.RunID || rerunRunCount(t, ctx, pool, org, day) != 3 {
			t.Fatalf("%s: a new tag must start one more run: %+v err %v", cover.generation, next, err)
		}
		if _, err := store.StartManualDailyRun(ctx, org, day, ManualDailyRunGeneration(org, day, nil), nil, publisher); !errors.Is(err, ErrDayAlreadyCovered) {
			t.Fatalf("%s: a call without a tag must still be refused: %v", cover.generation, err)
		}
	}
}

// A tagged call on a day that nothing covers names no overridden run, and an
// ill-formed tag is refused before anything is written.
func TestStartManualDailyRerunOnAnUncoveredDayOverridesNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	store, publisher, pool := rerunStore(t, ctx)
	const org = "00000000-0000-4000-8000-0000000009a3"
	const day = "2026-08-28"
	out, err := store.StartManualDailyRerun(ctx, org, day, ManualDailyRerunGeneration(org, day, nil, "t1"), nil, publisher, "t1")
	if err != nil || out.AlreadyStarted || out.CoveredDayOverriddenBy != "" {
		t.Fatalf("an uncovered day overrides nothing: %+v err %v", out, err)
	}
	if _, err := store.StartManualDailyRerun(ctx, org, "2026-08-29", "x", nil, publisher, "bad tag|"); !errors.Is(err, ErrInvalidState) ||
		rerunRunCount(t, ctx, pool, org, "2026-08-29") != 0 {
		t.Fatalf("an ill-formed tag must be refused before any write: %v", err)
	}
}
