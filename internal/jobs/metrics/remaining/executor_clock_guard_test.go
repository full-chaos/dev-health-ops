package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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

// constructorConn answers exactly the two schema probes the constructors make
// and returns a sentinel error for anything else.
//
// It does NOT embed the panicking driverConnStub's behaviour for Query and
// QueryRow, because here reaching a later query is EXPECTED: the point is to
// get a real executor past construction and then past the clock, and a panic
// on the next query would be indistinguishable from the clock guard firing.
type constructorConn struct {
	driverConnStub
	sortingKey string
}

var errStubExhausted = errors.New("stub: no canned answer for this query")

func (conn constructorConn) Query(
	_ context.Context, query string, _ ...any,
) (chdriver.Rows, error) {
	if strings.Contains(query, "system.columns") {
		return &stubColumnRows{names: allCapacityRequiredColumns()}, nil
	}
	return nil, errStubExhausted
}

func (conn constructorConn) QueryRow(
	_ context.Context, query string, _ ...any,
) chdriver.Row {
	// Two DIFFERENT probes hit system.tables: DORA reads sorting_key to
	// classify the ordering contract, capacity reads engine to confirm FINAL
	// collapses anything. Answering both with one value made the capacity
	// constructor refuse with the sorting key in the engine's place.
	switch {
	case strings.Contains(query, "sorting_key"):
		return stubRow{value: conn.sortingKey}
	case strings.Contains(query, "SELECT engine"):
		// From the constant, so a marker change cannot leave this stub
		// satisfying a check the production code no longer makes.
		return stubRow{value: capacityReplacingEngineMarker + "(computed_at)"}
	}
	return stubRow{err: errStubExhausted}
}

// allCapacityRequiredColumns is derived from capacityTableRequirements rather
// than hardcoded, so adding a required column cannot make this stub silently
// stop satisfying the constructor it exists to get past.
func allCapacityRequiredColumns() []string {
	var names []string
	for _, requirement := range capacityTableRequirements {
		names = append(names, requirement.columns...)
	}
	return names
}

type stubColumnRows struct {
	names []string
	index int
}

func (rows *stubColumnRows) Next() bool { rows.index++; return rows.index <= len(rows.names) }
func (rows *stubColumnRows) Scan(dest ...any) error {
	target, ok := dest[0].(*string)
	if !ok {
		return errStubExhausted
	}
	*target = rows.names[rows.index-1]
	return nil
}
func (rows *stubColumnRows) ScanStruct(any) error               { return errStubExhausted }
func (rows *stubColumnRows) ColumnTypes() []chdriver.ColumnType { return nil }
func (rows *stubColumnRows) Totals(...any) error                { return nil }
func (rows *stubColumnRows) Columns() []string                  { return []string{"name"} }
func (rows *stubColumnRows) Close() error                       { return nil }
func (rows *stubColumnRows) Err() error                         { return nil }
func (rows *stubColumnRows) HasData() bool                      { return len(rows.names) > 0 }

type stubRow struct {
	value string
	err   error
}

func (row stubRow) Err() error { return row.err }
func (row stubRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	target, ok := dest[0].(*string)
	if !ok {
		return errStubExhausted
	}
	*target = row.value
	return nil
}
func (row stubRow) ScanStruct(any) error { return errStubExhausted }

// TestTheRealConstructorsAssignAWorkingClock pins what the CONSTRUCTORS do.
//
// # WHY THIS REPLACED A TEST THAT LOOKED LIKE IT ALREADY DID
//
// The previous version built composite literals with nowUTC set by hand and
// claimed in its comment to pin the constructors. It did not: it pinned that
// nowOrRefuse returns the field when the field is set, which is the accessor's
// behaviour. Nothing else covered the constructors either -- the refusal tests
// build literals, and the two unit tests that DO call a constructor pass a nil
// conn to assert refusal, so they return (nil, err) and never yield an
// executor whose clock could be inspected. Every call that built a real
// executor was behind //go:build integration.
//
// The consequence was specific and worse than the gap it left. Deleting
// `nowUTC:` from either constructor's struct literal left the whole unit suite
// green while production refused EVERY partition of that kind -- the refusal
// this file adds converts a latent nil-panic into a total outage, and no unit
// test could see it. Making a failure mode more severe while removing nothing
// from its blast radius is the one outcome a guard must not have.
//
// Found in peer review (3092). The comment was the load-bearing part: it told
// the next reader the constructors were covered, which is exactly why nobody
// would look again.
//
// # ONE MUTANT THIS TEST DOES NOT CATCH, AND WHAT DOES
//
// Deleting the nowOrRefuse() call from ComputePartition entirely leaves this
// test GREEN: the run still reaches the stub's first unanswered query, which
// is all assertPartitionReachesPastTheClock requires. The mutant is caught by
// TestDORAComputePartitionRefusesAnUninjectedClock instead -- a literal with
// no clock would then hit a raw call, and mustNotPanic converts the panic into
// a named failure.
//
// So the guard is covered by the SUITE, not by any single test. That is worth
// stating here rather than on the refusal test, because this is the one with
// the gap: whoever moves or deletes the refusal test needs to know this test
// was leaning on it, and a reader of the refusal test already has the cover.
func TestTheRealConstructorsAssignAWorkingClock(t *testing.T) {
	// Resolved from the environment rather than assumed, so the stub agrees
	// with whatever contract this process is configured for.
	contract, err := ConfiguredOperationalOrderingContract()
	if err != nil {
		t.Fatalf("resolve ordering contract: %v", err)
	}
	sortingKey := legacySortingKey
	if contract == OperationalOrderingRevision {
		sortingKey = revisionSortingKey
	}
	conn := constructorConn{sortingKey: sortingKey}
	ctx := context.Background()

	t.Run("DORA", func(t *testing.T) {
		executor, err := NewDORAExecutor(ctx, conn, nil, nil)
		if err != nil {
			t.Fatalf("construct: %v", err)
		}
		if _, err := executor.nowOrRefuse(); err != nil {
			t.Fatalf("the constructor did not assign a clock: %v", err)
		}
		assertPartitionReachesPastTheClock(t, func() error {
			scope, marshalErr := json.Marshal(map[string]any{
				"version": 1, "day": "2026-08-22", "backfill_days": 1,
				"sink": "clickhouse", "interval": "daily",
			})
			if marshalErr != nil {
				return marshalErr
			}
			_, err := executor.ComputePartition(ctx,
				Run{ID: "r", OrganizationID: "org", Family: "dora"},
				Partition{ID: "p", RunID: "r", Scope: scope})
			return err
		})
	})

	t.Run("Capacity", func(t *testing.T) {
		executor, err := NewCapacityExecutor(ctx, conn, nil, nil)
		if err != nil {
			t.Fatalf("construct: %v", err)
		}
		if _, err := executor.nowOrRefuse(); err != nil {
			t.Fatalf("the constructor did not assign a clock: %v", err)
		}
		seed := int64(7)
		assertPartitionReachesPastTheClock(t, func() error {
			scope, marshalErr := json.Marshal(map[string]any{
				"version": 1, "history_days": 90, "simulations": 200,
			})
			if marshalErr != nil {
				return marshalErr
			}
			_, err := executor.ComputePartition(ctx,
				Run{ID: "r", OrganizationID: "org", Family: "capacity", Seed: &seed},
				Partition{ID: "p", RunID: "r", Scope: scope})
			return err
		})
	})
}

// assertPartitionReachesPastTheClock drives a real partition and requires that
// whatever stopped it was NOT the clock guard.
//
// Asserting only that nowOrRefuse succeeds would leave the end-to-end path
// unpinned: a constructor could assign the clock while some later edit made
// ComputePartition consult a different, unset one. The run is expected to fail
// at the stub's first unanswered query -- that is the proof it got past the
// clock, since the clock guard returns an error rather than reaching a query.
func assertPartitionReachesPastTheClock(t *testing.T, run func() error) {
	t.Helper()
	err := mustNotPanic(t, run)
	if errors.Is(err, errExecutorClockUnset) {
		t.Fatalf("a CONSTRUCTED executor refused its own clock: %v", err)
	}
	if !errors.Is(err, errStubExhausted) {
		// TWO CAUSES, AND THIS NAMES BOTH. The likelier one a year from now is
		// NOT a broken clock guard: it is an unrelated edit making
		// ComputePartition return before it reaches any query. Naming only the
		// clock would send that reader hunting a problem that is not there,
		// and as a Fatalf this is the last line they see. (3092's degeneration
		// guard had the same shape and the same fix -- widen the message, keep
		// the positive assertion.)
		t.Fatalf("the run stopped before the stub's first unanswered query "+
			"(got %v). EITHER an unrelated change made ComputePartition return "+
			"early -- likelier, and not a clock problem -- OR the clock guard "+
			"broke. If this is nil the partition somehow completed, and the "+
			"test is no longer proving the clock was reached at all", err)
	}
}
