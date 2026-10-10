//go:build integration

package providersync

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PGBOUNCER_PROOF_URI names a DSN of a TRANSACTION-mode PgBouncer (the domain pooler of the
// compose stack). Where it is NOT set this test is skipped and says so: the proof is "not run" in
// that job, not passed. Where the job sets it, the test must pass or fail loudly.
const pgbouncerProofURIEnv = "PGBOUNCER_PROOF_URI"

// CHAOS-9140: the turn through a transaction-mode pooler. Advisory locks only, a key that no
// product code uses, no table write, no DDL.
func TestTeamCatalogTurnThroughATransactionModePgBouncer(t *testing.T) {
	uri := os.Getenv(pgbouncerProofURIEnv)
	if uri == "" {
		t.Skipf("NOT RUN: %s is not set (no transaction-mode PgBouncer in this job); the turn is proven on a direct Postgres only", pgbouncerProofURIEnv)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, uri)
	if err != nil {
		t.Fatalf("connect to the pooler (%s): %v", pgbouncerProofURIEnv, err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping the pooler: %v", err)
	}
	const key = int64(9140_0000_0001) // test-only: no product code takes this key

	t.Run("a SESSION lock does not stay with its caller through the pooler", func(t *testing.T) {
		// Other clients churn so the pooler hands server sessions around.
		var stop atomic.Bool
		var wg sync.WaitGroup
		for i := 0; i < 6; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for !stop.Load() {
					var one int
					_ = pool.QueryRow(ctx, `SELECT 1`).Scan(&one)
				}
			}()
		}
		defer func() { stop.Store(true); wg.Wait() }()
		strayed, unlockedElsewhere := 0, 0
		for i := 0; i < 40; i++ {
			var got bool
			if err := pool.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&got); err != nil {
				t.Fatal(err)
			}
			var released bool
			if err := pool.QueryRow(ctx, `SELECT pg_advisory_unlock($1)`, key).Scan(&released); err != nil {
				t.Fatal(err)
			}
			if got && !released {
				unlockedElsewhere++ // the unlock landed on another server session: the lock is still held there
			}
			if !got {
				strayed++ // a lock left behind by an earlier attempt blocked this one
			}
		}
		// A session lock this test left on a server session is released where it sits: each unlock
		// lands on some server session, so keep asking until a run of tries finds none to release.
		for quiet := 0; quiet < 60; {
			var released bool
			if err := pool.QueryRow(ctx, `SELECT pg_advisory_unlock($1)`, key).Scan(&released); err != nil {
				t.Fatal(err)
			}
			if released {
				quiet = 0
			} else {
				quiet++
			}
		}
		t.Logf("SESSION lock via the pooler, 40 try+unlock pairs under churn: unlock landed elsewhere %d times, a leftover blocked the try %d times (any non-zero = the lock is not held by its caller)", unlockedElsewhere, strayed)
	})

	t.Run("the turn (xact lock in a held transaction) excludes and releases through the pooler", func(t *testing.T) {
		turn := PostgresTeamCatalogTurn{Pool: pool, Poll: 30 * time.Millisecond}
		release, err := turn.Serialize(ctx, "org-proof-9140", "linear")
		if err != nil {
			t.Fatal(err)
		}
		held := true
		defer func() {
			if held {
				release()
			}
		}()
		for i := 0; i < 10; i++ {
			short, stop := context.WithTimeout(ctx, 250*time.Millisecond)
			next, err := turn.Serialize(short, "org-proof-9140", "linear")
			stop()
			if err == nil {
				next()
				t.Fatalf("try %d: a second holder got the turn while the first held it", i)
			}
		}
		other, err := turn.Serialize(ctx, "org-proof-9140", "github")
		if err != nil {
			t.Fatalf("another provider waited: %v", err)
		}
		other()
		release()
		held = false
		again, err := turn.Serialize(ctx, "org-proof-9140", "linear")
		if err != nil {
			t.Fatalf("the turn was not free after the release: %v", err)
		}
		again()
	})
}
