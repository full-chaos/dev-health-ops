//go:build integration

package daily

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// holdUntilRivalBlocked delays the first dispatch publish until another
// backend waits on a lock while inserting a daily run: both calls have then
// read the run as absent and neither has committed.
type holdUntilRivalBlocked struct {
	inner RunPublisher
	pool  *pgxpool.Pool
	once  sync.Once
	err   error
}

func (h *holdUntilRivalBlocked) PublishDispatchTx(ctx context.Context, tx pgx.Tx, run Run, key string) error {
	h.once.Do(func() {
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			var waiting int
			if err := h.pool.QueryRow(ctx, `
SELECT count(*) FROM pg_stat_activity
WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE '%INSERT INTO public.daily_metrics_runs%'`).Scan(&waiting); err != nil {
				h.err = err
				return
			}
			if waiting > 0 {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		h.err = fmt.Errorf("the rival call never blocked on the run insert")
	})
	if h.err != nil {
		return h.err
	}
	return h.inner.PublishDispatchTx(ctx, tx, run, key)
}

// Two concurrent calls with the same tag store one run, so exactly one of them
// may report that it started it.
func TestStartManualDailyRerunConcurrentSameTagReportsOneStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	store, publisher, pool := rerunStore(t, ctx)
	const org = "00000000-0000-4000-8000-0000000009a5"
	const day = "2026-08-26"
	repos := []RepositoryID{"00000000-0000-4000-8000-0000000009b5"}
	generation := ManualDailyRerunGeneration(org, day, repos, "race-1")
	held := &holdUntilRivalBlocked{inner: publisher, pool: pool}

	type result struct {
		outcome ManualDailyRunOutcome
		err     error
	}
	results := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			outcome, err := store.StartManualDailyRerun(ctx, org, day, generation, repos, held, "race-1")
			results <- result{outcome, err}
		}()
	}
	started := 0
	for i := 0; i < 2; i++ {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		if !r.outcome.AlreadyStarted {
			started++
		}
	}
	if started != 1 {
		t.Fatalf("calls that report they started the run = %d, want 1", started)
	}
	if got := rerunRunCount(t, ctx, pool, org, day); got != 1 {
		t.Fatalf("stored runs = %d, want 1", got)
	}
}
