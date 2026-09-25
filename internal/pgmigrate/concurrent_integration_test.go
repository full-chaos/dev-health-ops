//go:build integration

package pgmigrate_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
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

// A migrator that takes no dho lock (the Python Alembic upgrade) may apply a chain
// revision after dho has planned it and before dho's SQL runs. dho's SQL then fails
// (0139 adds a column that is there), and the run must not fail the deploy: the
// database is where the run wanted it. The other migrator is stood in for by a
// transaction that runs the revision's own SQL and moves alembic_version, queued on a
// table lock ahead of dho's statement; both are released together.
func TestUpgradeSurvivesAnotherMigratorApplyingTheStepFirst(t *testing.T) {
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
		t.Fatalf("the chain has %d revisions: the test needs a step for the other migrator and one after it", len(chain))
	}
	setup := connect(t, uri)
	if result, err := pgmigrate.Upgrade(ctx, setup, baseline, nil); err != nil || result.Action != "baseline_applied" {
		t.Fatalf("baseline upgrade = %+v, %v", result, err)
	}
	var applicationHead string
	for _, head := range baseline.Heads {
		if head != "0066" {
			applicationHead = head
		}
	}

	// A third session holds the table the first step alters.
	blocker := connect(t, uri)
	if _, err := blocker.Exec(ctx, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	if _, err := blocker.Exec(ctx, "LOCK TABLE refunds IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	waitForRelationWaiters := func(want int) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for {
			var waiting int
			if err := setup.QueryRow(ctx, "SELECT count(*) FROM pg_locks WHERE locktype = 'relation' AND NOT granted").Scan(&waiting); err != nil {
				t.Fatal(err)
			}
			if waiting == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%d sessions are waiting on a table lock, want %d: the test does not line them up", waiting, want)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	// The other migrator: the step's SQL and the version bump, committed as one.
	other := connect(t, uri)
	otherDone := make(chan error, 1)
	go func() {
		otherDone <- func() error {
			tx, err := other.Begin(ctx)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, chain[0].SQL); err != nil {
				_ = tx.Rollback(ctx)
				return err
			}
			if _, err := tx.Exec(ctx, "UPDATE alembic_version SET version_num = $1 WHERE version_num = $2", chain[0].Revision, applicationHead); err != nil {
				_ = tx.Rollback(ctx)
				return err
			}
			return tx.Commit(ctx)
		}()
	}()
	waitForRelationWaiters(1)

	// dho plans from the baseline and queues its own first statement behind the other's.
	dho := connect(t, uri)
	type outcome struct {
		result pgmigrate.Result
		err    error
	}
	dhoDone := make(chan outcome, 1)
	go func() {
		result, err := pgmigrate.Upgrade(ctx, dho, baseline, chain)
		dhoDone <- outcome{result, err}
	}()
	waitForRelationWaiters(2)
	if _, err := blocker.Exec(ctx, "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	if err := <-otherDone; err != nil {
		t.Fatalf("the other migrator failed: %v", err)
	}
	got := <-dhoDone
	if got.err != nil {
		t.Fatalf("dho failed although the database advanced past its step: %v", got.err)
	}
	var want []string
	for _, file := range chain[1:] {
		want = append(want, file.Revision)
	}
	if fmt.Sprint(got.result.Applied) != fmt.Sprint(want) {
		t.Errorf("dho applied %v, want only the steps the other migrator did not: %v", got.result.Applied, want)
	}
	recorded, err := pgmigrate.Recorded(ctx, setup)
	if err != nil {
		t.Fatal(err)
	}
	if heads := pgmigrate.Heads(baseline, chain); fmt.Sprint(recorded) != fmt.Sprint(heads) {
		t.Errorf("alembic_version holds %v, want the heads %v", recorded, heads)
	}
}

// A step that fails for its own reason (a column it adds is there, though nothing recorded the revision) is still the run's
// failure: nothing recorded it, so the run does not carry on, retry or hang.
func TestUpgradeStillFailsWhenTheStepFailsForItsOwnReason(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
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
	conn := connect(t, uri)
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "ALTER TABLE refunds ADD COLUMN idempotency_key text"); err != nil {
		t.Fatal(err)
	}
	result, err := pgmigrate.Upgrade(ctx, conn, baseline, chain)
	if err == nil || !strings.Contains(err.Error(), chain[0].Name) {
		t.Fatalf("Upgrade = %+v, %v; want the failure of %s", result, err, chain[0].Name)
	}
	if len(result.Applied) != 0 {
		t.Errorf("Upgrade reports %v applied after a failing first step", result.Applied)
	}
	recorded, err := pgmigrate.Recorded(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(recorded) != fmt.Sprint([]string{"0066", baseline.Heads[0]}) && fmt.Sprint(recorded) != fmt.Sprint([]string{"0066", baseline.Heads[1]}) {
		t.Errorf("the failed step changed alembic_version to %v", recorded)
	}
}
