//go:build integration

package remaining

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// CHAOS-9587: FreshCoverage decides coverage of a fixed-schedule occurrence by WHEN a same-scope run was
// created, whatever day it is labelled with. Every cell of the ruling, against real Postgres.
func TestFreshCoverageCells(t *testing.T) {
	ctx := context.Background()
	pool, store, publisher := newRemainingRedriveTestStack(t)
	const orgID = "00000000-0000-4000-8000-000000009587"
	scope := json.RawMessage(`{"version":1,"window":14}`)
	otherScope := json.RawMessage(`{"version":1,"window":14,"team_id":"00000000-0000-4000-8000-0000000095aa"}`)
	cutoff := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	now := cutoff.Add(2 * time.Hour) // the occurrence's observation instant
	const budget = 2 * time.Hour

	seq := 0
	// start creates a run labelled yesterday, created at `created`; its handoff is real (deliverable)
	// when real is true and absent (a dead handoff) otherwise.
	start := func(created time.Time, scope json.RawMessage, real bool) (runID, partitionID string) {
		t.Helper()
		seq++
		store.now = func() time.Time { return created }
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var pub PartitionPublisher = nopPartitionPublisher{}
		if real {
			pub = publisher
		}
		run, err := store.StartRunTx(ctx, tx, StartRunRequest{
			OrganizationID: orgID, Family: "recommendations",
			Generation: "cell-" + string(rune('a'+seq)), ScopeKey: "2026-11-17",
			Scopes: []json.RawMessage{scope},
		}, pub)
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		return run.ID, deterministicPartitionID(run.ID, 1)
	}
	succeed := func(partitionID, evidence string) {
		t.Helper()
		claim, err := store.ClaimPartition(ctx, partitionID)
		if err != nil || claim == nil {
			t.Fatalf("claim = %v, %v", claim, err)
		}
		if err := store.CompletePartition(ctx, *claim, evidence); err != nil {
			t.Fatal(err)
		}
	}
	check := func(want FreshCoverageBranch, scope json.RawMessage, what string) FreshCoverageResult {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		got, err := store.FreshCoverage(ctx, tx, orgID, "recommendations", scope, cutoff, now, budget)
		if err != nil {
			t.Fatal(err)
		}
		if got.Branch != want || got.Covered != (want != "") {
			t.Fatalf("%s: FreshCoverage = %+v, want branch %q", what, got, want)
		}
		return got
	}
	clean := func() {
		t.Helper()
		for _, table := range []string{"remaining_metric_partitions", "remaining_metric_runs"} {
			if _, err := pool.Exec(ctx, "DELETE FROM public."+table); err != nil {
				t.Fatal(err)
			}
		}
	}

	// the real collision: a run labelled yesterday, created after D's cutoff, succeeded -> covered
	runID, p := start(cutoff.Add(30*time.Minute), scope, true)
	succeed(p, "compatibility_execution:x:rows_written=5")
	got := check(FreshCoverageSucceeded, scope, "succeeded after the cutoff")
	if got.RunID != runID || got.Status != "succeeded" || !got.CreatedAt.Equal(cutoff.Add(30*time.Minute)) {
		t.Fatalf("covering run = %+v, want %s created %s", got, runID, cutoff.Add(30*time.Minute))
	}
	check("", otherScope, "another scope never covers")
	clean()

	// succeeded before the cutoff read an older window -> run
	_, p = start(cutoff.Add(-30*time.Minute), scope, true)
	succeed(p, "compatibility_execution:x:rows_written=5")
	check("", scope, "succeeded before the cutoff")
	clean()

	// a withheld run (0 rows) never covers
	_, p = start(cutoff.Add(30*time.Minute), scope, true)
	succeed(p, "compatibility_execution:x:rows_written=0")
	check("", scope, "succeeded with 0 rows")
	clean()

	// in flight, young (created inside the budget), after the cutoff -> covered, pending and running
	runID, p = start(cutoff.Add(30*time.Minute), scope, true)
	got = check(FreshCoverageInFlight, scope, "pending, young, after the cutoff")
	if got.RunID != runID || got.Status != "pending" {
		t.Fatalf("covering run = %+v", got)
	}
	if claim, err := store.ClaimPartition(ctx, p); err != nil || claim == nil {
		t.Fatalf("claim = %v, %v", claim, err)
	}
	got = check(FreshCoverageInFlight, scope, "running, young, after the cutoff")
	if got.Status != "running" {
		t.Fatalf("covering run = %+v", got)
	}
	clean()

	// in flight but created before the cutoff -> run
	start(cutoff.Add(-time.Minute), scope, true)
	check("", scope, "in flight, before the cutoff")
	clean()

	// in flight and older than the budget -> run (cutoff moved back so only the budget decides)
	start(now.Add(-budget-time.Minute), scope, true)
	check("", scope, "in flight, older than the budget")
	clean()

	// in flight at exactly the budget -> run
	start(now.Add(-budget), scope, true)
	check("", scope, "in flight at exactly the budget")
	clean()

	// just inside the budget -> covered
	start(now.Add(-budget+time.Second), scope, true)
	check(FreshCoverageInFlight, scope, "in flight one second inside the budget")
	clean()

	// a dead handoff (no outbox row) never covers
	start(cutoff.Add(30*time.Minute), scope, false)
	check("", scope, "pending with a dead handoff")
	clean()

	// a succeeded cover wins over an in-flight run
	_, p = start(cutoff.Add(10*time.Minute), scope, true)
	succeed(p, "compatibility_execution:x:rows_written=5")
	start(cutoff.Add(20*time.Minute), scope, true)
	check(FreshCoverageSucceeded, scope, "succeeded and in flight")
}

// A manual or fixtures launch of the same scope waits for the fixed schedule's coverage read to end its
// transaction, so no run can become visible between that read and the schedule's insert.
func TestManualRecommendationsLaunchWaitsForTheScheduleScopeLock(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := newRemainingRedriveTestStack(t)
	const orgID = "00000000-0000-4000-8000-000000009588"
	scope := json.RawMessage(`{"version":1,"window":14}`)

	scheduleTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer scheduleTx.Rollback(ctx)
	cutoff := time.Now().UTC()
	if _, err := store.FreshCoverage(ctx, scheduleTx, orgID, "recommendations", scope, cutoff, cutoff, 2*time.Hour); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := store.StartManualRecommendationsTriggerRun(ctx, orgID, "2026-11-17", "manual-lock-cell", nil, 14, nopPartitionPublisher{})
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("manual launch finished while the scheduler held the scope lock: %v", err)
	case <-time.After(750 * time.Millisecond):
	}
	if err := scheduleTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("manual launch after the lock was released: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("manual launch did not proceed after the scheduler committed")
	}
}
