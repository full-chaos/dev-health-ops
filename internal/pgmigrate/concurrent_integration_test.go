//go:build integration

package pgmigrate_test

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// migrationLockKey is pgmigrate's advisory-lock key (lockKey): the test holds it to
// line every run up behind it.
const migrationLockKey int64 = 0x64686f5f7067

// Runs of `dho migrate postgres upgrade` that plan from the same state, one after
// another behind the migration lock, apply each chain revision once between them: a
// run that finds another has advanced the database does not run the SQL again (the
// revisions are not idempotent: 0139 adds a column). Six runs wait on the lock over a
// database at the baseline, the lock is released, and every run must succeed.
func TestConcurrentUpgradesApplyEachChainRevisionOnce(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	admin := connect(t, instance.URI)
	uri := databaseURI(t, instance.URI, scratchDatabase(t, admin))
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) < 2 {
		t.Fatalf("the chain has %d revisions: a race needs at least two", len(chain))
	}
	// The database at the baseline revision: what production is when the chain is new.
	setup := connect(t, uri)
	if result, err := pgmigrate.Upgrade(ctx, setup, baseline, nil); err != nil || result.Action != "baseline_applied" {
		t.Fatalf("baseline upgrade = %+v, %v", result, err)
	}

	holder := connect(t, uri)
	if _, err := holder.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockKey); err != nil {
		t.Fatal(err)
	}
	const runs = 6
	type outcome struct {
		result pgmigrate.Result
		err    error
	}
	outcomes := make([]outcome, runs)
	var wg sync.WaitGroup
	for index := 0; index < runs; index++ {
		conn := connect(t, uri)
		wg.Add(1)
		go func() {
			defer wg.Done()
			outcomes[index].result, outcomes[index].err = pgmigrate.Upgrade(ctx, conn, baseline, chain)
		}()
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		var waiting int
		if err := holder.QueryRow(ctx, "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted").Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting == runs {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d runs are waiting on the migration lock: the test does not line them up", waiting, runs)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := holder.Exec(ctx, "SELECT pg_advisory_unlock($1)", migrationLockKey); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	var applied []string
	for index, outcome := range outcomes {
		if outcome.err != nil {
			t.Errorf("run %d failed: %v", index, outcome.err)
		}
		applied = append(applied, outcome.result.Applied...)
	}
	sort.Strings(applied)
	var want []string
	for _, file := range chain {
		want = append(want, file.Revision)
	}
	if len(applied) != len(want) {
		t.Fatalf("the runs applied %v between them, want each of %v exactly once", applied, want)
	}
	for index := range want {
		if applied[index] != want[index] {
			t.Fatalf("the runs applied %v between them, want each of %v exactly once", applied, want)
		}
	}
	recorded, err := pgmigrate.Recorded(ctx, setup)
	if err != nil {
		t.Fatal(err)
	}
	if got, heads := len(recorded), pgmigrate.Heads(baseline, chain); got != len(heads) || recorded[len(recorded)-1] != heads[len(heads)-1] {
		t.Errorf("alembic_version holds %v, want the heads %v", recorded, heads)
	}
}
