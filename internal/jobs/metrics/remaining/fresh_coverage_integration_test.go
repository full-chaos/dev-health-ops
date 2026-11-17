//go:build integration

package remaining

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// CHAOS-9587: FreshCoverage decides coverage of a fixed-schedule occurrence by WHEN a same-scope run was
// created, whatever day it is labelled with, and treats an in-flight run apart from a succeeded one.
func TestFreshCoverageCells(t *testing.T) {
	ctx := context.Background()
	pool, store, publisher := newRemainingRedriveTestStack(t)
	const orgID = "00000000-0000-4000-8000-000000009587"
	scope := json.RawMessage(`{"version":1,"window":14}`)
	otherScope := json.RawMessage(`{"version":1,"window":14,"team_id":"00000000-0000-4000-8000-0000000095aa"}`)
	cutoff := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	inFlightSince := cutoff.Add(-2 * time.Hour)

	// start creates a run labelled with an arbitrary day, created at `created`; the handoff is real
	// (deliverable) when real is true, and absent (a dead handoff) otherwise.
	seq := 0
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
			Generation: "cell-" + string(rune('a'+seq)), ScopeKey: "2026-11-17", // yesterday's label
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
	check := func(want FreshCoverageState, scope json.RawMessage, what string) {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		got, err := store.FreshCoverage(ctx, tx, orgID, "recommendations", scope, cutoff, inFlightSince)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s: FreshCoverage = %d, want %d", what, got, want)
		}
	}
	clean := func() {
		t.Helper()
		for _, table := range []string{"remaining_metric_partitions", "remaining_metric_runs"} {
			if _, err := pool.Exec(ctx, "DELETE FROM public."+table); err != nil {
				t.Fatal(err)
			}
		}
	}

	// the real collision: the run is labelled yesterday and created after D's cutoff -> covered
	_, p := start(cutoff.Add(30*time.Minute), scope, true)
	succeed(p, "compatibility_execution:x:rows_written=5")
	check(FreshCoverageCovered, scope, "succeeded after the cutoff")
	check(FreshCoverageNone, otherScope, "another scope never covers")
	clean()

	// the same run created before the cutoff read an older window -> recompute
	_, p = start(cutoff.Add(-30*time.Minute), scope, true)
	succeed(p, "compatibility_execution:x:rows_written=5")
	check(FreshCoverageNone, scope, "succeeded before the cutoff")
	clean()

	// a withheld run (0 rows) never covers
	_, p = start(cutoff.Add(30*time.Minute), scope, true)
	succeed(p, "compatibility_execution:x:rows_written=0")
	check(FreshCoverageNone, scope, "succeeded with 0 rows")
	clean()

	// in flight (deliverable handoff) within the bound -> in flight
	start(cutoff.Add(-30*time.Minute), scope, true)
	check(FreshCoverageInFlight, scope, "pending with a deliverable handoff")
	clean()

	// in flight but older than the bound -> not in flight
	start(inFlightSince.Add(-time.Minute), scope, true)
	check(FreshCoverageNone, scope, "pending older than the bound")
	clean()

	// a dead handoff (no outbox row) is not in flight
	start(cutoff.Add(-30*time.Minute), scope, false)
	check(FreshCoverageNone, scope, "pending with a dead handoff")
	clean()

	// covered wins over in flight
	_, p = start(cutoff.Add(10*time.Minute), scope, true)
	succeed(p, "compatibility_execution:x:rows_written=5")
	start(cutoff.Add(20*time.Minute), scope, true)
	check(FreshCoverageCovered, scope, "covered and in flight")
}
