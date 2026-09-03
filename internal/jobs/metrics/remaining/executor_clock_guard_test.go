package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// The three tests below pin CHAOS-4954: an executor built as a composite
// literal, with no clock injected, must REFUSE rather than dereference nil.
//
// # WHY EACH ONE DRIVES A FULL POSITIVE PATH
//
// Every one of these call sites already sat behind unrelated guards -- a nil
// conn, a missing organization, an unparseable scope, and for capacity a
// missing seed. Before this change the raw clock was never reached by any
// existing test, and that was NOT because the clock was safe: it was because
// every test tripped an earlier guard first. `TestCapacityRefusesARunWithoutASeed`
// constructs exactly this literal and survives only because it passes
// `Seed: nil` and returns at the seed refusal.
//
// So a test that stops at a guard proves nothing about the clock. Each test
// here therefore supplies a VALID value for every precondition -- a real seed,
// a parseable day, backfill_days >= 1 -- so the clock guard is the first thing
// it can trip. A regression that removes the guard is then observable, which
// is the property the old tests lacked.
//
// # WHY THE PANIC IS CONVERTED RATHER THAN LEFT TO ABORT
//
// Mutation-checked, and the first attempt was defective. Replacing the refusal
// with a time.Now() fallback -- the rival design -- killed all three tests, but
// by PANIC rather than on their own assertions: with a clock in hand the call
// runs on into resolveScopes and the stub conn panics there. That is a death
// for an incidental reason. Had stubConn ever been replaced by a fuller fake
// that does not panic, the same mutant would have survived silently.
//
// mustNotPanic therefore converts a panic into a failure that NAMES the
// expectation, so each test reports the clock guard in both worlds: the mutant
// that panics downstream and the mutant that quietly returns a wall clock.

// mustNotPanic runs call and converts a panic into a named failure.
//
// A nil clock dereference IS the defect under test, so a panic must be
// reported as a failure of THIS test's expectation rather than tearing down
// the binary with a stack trace that names runtime internals instead.
func mustNotPanic(t *testing.T, call func() error) error {
	t.Helper()
	var err error
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				t.Fatalf("PANICKED (%v), want errExecutorClockUnset -- an "+
					"executor with no injected clock must refuse, not "+
					"dereference nil", recovered)
			}
		}()
		err = call()
	}()
	return err
}

func TestCapacityRefusesAnUninjectedClock(t *testing.T) {
	seed := int64(7)
	// nowUTC deliberately absent -- this is the literal shape that already
	// exists at capacity_native_test.go, minus the seed that used to save it.
	executor := &CapacityExecutor{conn: stubConn{}}
	scope, err := json.Marshal(map[string]any{
		"version": 1, "history_days": 90, "simulations": 200,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = mustNotPanic(t, func() error {
		_, err := executor.ComputePartition(
			context.Background(),
			Run{ID: "r", OrganizationID: "org", Family: "capacity", Seed: &seed},
			Partition{ID: "p", RunID: "r", Scope: scope},
		)
		return err
	})

	// The clock guard must be what fired, not an upstream one. Without this the
	// test would pass for the SAME REASON the latent panic never fired: stopped
	// early by a precondition, never reaching the clock at all. The mutation
	// matrix cannot distinguish those two worlds on its own -- "only the
	// injected-clock test dies" is consistent with a working guard AND with
	// three tests that never get past a seed or scope check.
	if errors.Is(err, ErrCapacitySeedMissing) || errors.Is(err, ErrInvalidState) {
		t.Fatalf("stopped at an UPSTREAM guard (%v); this test must drive past "+
			"every precondition and reach the clock", err)
	}
	if !errors.Is(err, errExecutorClockUnset) {
		t.Fatalf("returned %v, want errExecutorClockUnset -- a capacity run past "+
			"the seed guard reaches the clock, and an uninjected clock must "+
			"refuse rather than dereference nil", err)
	}
}

func TestDORAComputePartitionRefusesAnUninjectedClock(t *testing.T) {
	executor := &DORAExecutor{conn: stubConn{}}
	// Every precondition valid, so the clock is the first guard reachable:
	// a parseable day and backfill_days >= 1.
	scope, err := json.Marshal(map[string]any{
		"version": 1, "day": "2026-08-22", "backfill_days": 1,
		"sink": "clickhouse", "interval": "daily",
	})
	if err != nil {
		t.Fatal(err)
	}

	err = mustNotPanic(t, func() error {
		_, err := executor.ComputePartition(
			context.Background(),
			Run{ID: "r", OrganizationID: "org", Family: "dora"},
			Partition{ID: "p", RunID: "r", Scope: scope},
		)
		return err
	})

	// The clock guard must be what fired, not an upstream one. Without this the
	// test would pass for the SAME REASON the latent panic never fired: stopped
	// early by a precondition, never reaching the clock at all. The mutation
	// matrix cannot distinguish those two worlds on its own -- "only the
	// injected-clock test dies" is consistent with a working guard AND with
	// three tests that never get past a seed or scope check.
	if errors.Is(err, ErrCapacitySeedMissing) || errors.Is(err, ErrInvalidState) {
		t.Fatalf("stopped at an UPSTREAM guard (%v); this test must drive past "+
			"every precondition and reach the clock", err)
	}
	if !errors.Is(err, errExecutorClockUnset) {
		t.Fatalf("returned %v, want errExecutorClockUnset -- the partition stamp "+
			"is taken before the day loop and must refuse when no clock was "+
			"injected", err)
	}
}

func TestDORALoadIncidentsRefusesAnUninjectedClock(t *testing.T) {
	// loadIncidents binds the clock into the `as_of` query argument. It is
	// covered separately because ComputePartition now refuses BEFORE the day
	// loop, so no test routed through ComputePartition can ever reach this
	// second site -- fixing one call site does not exercise the other.
	executor := &DORAExecutor{conn: stubConn{}}

	err := mustNotPanic(t, func() error {
		_, _, err := executor.loadIncidents(
			context.Background(), "org",
			time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC), doraScope{},
		)
		return err
	})

	// The clock guard must be what fired, not an upstream one. Without this the
	// test would pass for the SAME REASON the latent panic never fired: stopped
	// early by a precondition, never reaching the clock at all. The mutation
	// matrix cannot distinguish those two worlds on its own -- "only the
	// injected-clock test dies" is consistent with a working guard AND with
	// three tests that never get past a seed or scope check.
	if errors.Is(err, ErrCapacitySeedMissing) || errors.Is(err, ErrInvalidState) {
		t.Fatalf("stopped at an UPSTREAM guard (%v); this test must drive past "+
			"every precondition and reach the clock", err)
	}
	if !errors.Is(err, errExecutorClockUnset) {
		t.Fatalf("returned %v, want errExecutorClockUnset -- as_of is bound from "+
			"the clock, and an uninjected clock must refuse before the query "+
			"is built", err)
	}
}

func TestConstructedExecutorsNeverTripTheClockGuard(t *testing.T) {
	// The guard must be unreachable for a properly constructed executor,
	// otherwise it would refuse real work. Both constructors set nowUTC on the
	// only path returning a non-nil executor; this pins that directly rather
	// than trusting the reading.
	dora := &DORAExecutor{nowUTC: func() time.Time { return time.Unix(0, 0).UTC() }}
	if _, err := dora.nowOrRefuse(); err != nil {
		t.Errorf("DORA with an injected clock refused: %v", err)
	}
	capacity := &CapacityExecutor{nowUTC: func() time.Time { return time.Unix(0, 0).UTC() }}
	if _, err := capacity.nowOrRefuse(); err != nil {
		t.Errorf("capacity with an injected clock refused: %v", err)
	}
}
