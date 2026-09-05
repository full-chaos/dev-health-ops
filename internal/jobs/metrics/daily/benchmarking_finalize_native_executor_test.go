package daily

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// poisonedConn is a driver.Conn whose embedded interface is nil, so ANY
// method call on it panics with a nil-pointer dereference. Used to prove
// BenchmarkingFinalizeExecutor never touches ClickHouse when it refuses on
// an incomplete partition barrier -- a refusal that accidentally reached the
// ClickHouse loader would crash this test immediately rather than silently
// pass, which is the point: a mock that merely RETURNS an error on Query
// could not tell "never called" apart from "called and handled," but a
// panic can.
type poisonedConn struct{ driver.Conn }

func benchmarkingFinalizeRun(id, orgID string, day time.Time) Run {
	return Run{ID: id, OrganizationID: orgID, TargetDay: day}
}

// TestBenchmarkingFinalizeRefusesWhenAPartitionIsStillOpen is the race test
// team-lead's design ruling asked for as the red/green proof (CHAOS-5194,
// astra F3): with the partition-barrier check REMOVED, an executor computing
// benchmarking while a partition is still open is exactly the bug F3 found.
// This test proves the check that prevents it, without needing a real
// ClickHouse container: the barrier is verified against Store BEFORE any
// ClickHouse read, so a fake Store reporting an incomplete run is sufficient
// to red/green the exact invariant -- RED would be "the executor proceeds to
// compute anyway" (this test's poisoned conn would then panic instead of
// the test failing cleanly, which is itself informative), GREEN is what
// this test actually asserts: refusal, the correct sentinel error, and zero
// rows, with the poisoned ClickHouse conn never touched.
func TestBenchmarkingFinalizeRefusesWhenAPartitionIsStillOpen(t *testing.T) {
	store := &fakeStore{partitionTotal: 3, partitionSucceeded: 2}
	executor, err := NewBenchmarkingFinalizeExecutor(store, poisonedConn{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	run := benchmarkingFinalizeRun("run-race", "org-race", time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC))

	rows, err := executor.ComputeFinalizeFamily(context.Background(), run)

	if rows != 0 {
		t.Errorf("rows = %d, want 0 -- a refused run must not report any rows written", rows)
	}
	if err == nil {
		t.Fatal("ComputeFinalizeFamily succeeded with a partition still open (2/3 succeeded) -- " +
			"the barrier check did not fire; this is exactly the race astra's F3 finding identified")
	}
	if !errors.Is(err, ErrBenchmarkingPartitionsIncomplete) {
		t.Errorf("error does not wrap ErrBenchmarkingPartitionsIncomplete: %v", err)
	}
	// Reaching this line without a panic from poisonedConn IS the proof the
	// refusal happened before any ClickHouse read was attempted.
}

// TestBenchmarkingFinalizeProceedsOnceAllPartitionsSucceed is the GREEN
// complement to the race test above: the SAME executor, the SAME run,
// differing only in what the barrier reports, must NOT refuse. It cannot
// reach real output (that needs ClickHouse, covered by the integration
// test), but it must get PAST the barrier check and reach the loader
// construction step -- proven by the failure changing shape (a ClickHouse
// loader error, from the poisoned conn actually being invoked) rather than
// staying ErrBenchmarkingPartitionsIncomplete.
func TestBenchmarkingFinalizeProceedsOnceAllPartitionsSucceed(t *testing.T) {
	store := &fakeStore{partitionTotal: 3, partitionSucceeded: 3}
	executor, err := NewBenchmarkingFinalizeExecutor(store, poisonedConn{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	run := benchmarkingFinalizeRun("run-complete", "org-complete", time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC))

	defer func() {
		// The poisoned conn panics the moment the ClickHouse loader is
		// actually used -- which is what SHOULD happen once the barrier is
		// satisfied. Recovering here converts that expected panic into the
		// positive proof that execution proceeded past the barrier check,
		// instead of letting it fail this test as an unexpected crash.
		if r := recover(); r == nil {
			t.Fatal("expected the poisoned ClickHouse conn to be invoked once the partition " +
				"barrier is satisfied (3/3 succeeded) -- it was never touched, meaning " +
				"ComputeFinalizeFamily returned before reaching the compute step")
		}
	}()
	_, _ = executor.ComputeFinalizeFamily(context.Background(), run)
	t.Fatal("unreachable: expected a panic from the poisoned conn")
}

// TestBenchmarkingFinalizeRefusesOnZeroPartitions guards the same "empty is
// an error, not a silent no-op" discipline the removed anchor mechanism had
// (anchorFromDiscoveredSet): a finalize job for a run with ZERO partitions
// is a caller bug (ClaimFinalize's own NOT EXISTS check is vacuously true
// with no partition rows at all), and must not be read as "nothing to do."
func TestBenchmarkingFinalizeRefusesOnZeroPartitions(t *testing.T) {
	store := &fakeStore{partitionTotal: 0, partitionSucceeded: 0}
	executor, err := NewBenchmarkingFinalizeExecutor(store, poisonedConn{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	run := benchmarkingFinalizeRun("run-empty", "org-empty", time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC))

	rows, err := executor.ComputeFinalizeFamily(context.Background(), run)
	if rows != 0 {
		t.Errorf("rows = %d, want 0", rows)
	}
	if err == nil {
		t.Fatal("ComputeFinalizeFamily succeeded for a run with zero partitions -- " +
			"expected a refusal (ErrInvalidState), not a silent no-op")
	}
	if !errors.Is(err, ErrInvalidState) {
		t.Errorf("error does not wrap ErrInvalidState: %v", err)
	}
}

// TestBenchmarkingFinalizePropagatesAPartitionCountLookupFailure is
// CHAOS-4290's finalize-scope NO-FAIL-OPEN policy applied to this new
// failure mode: a Store error checking the barrier must propagate (so the
// run retries), never degrade to a silent success or a silent skip.
func TestBenchmarkingFinalizePropagatesAPartitionCountLookupFailure(t *testing.T) {
	store := &fakeStore{partitionCompletionCountErr: errors.New("postgres: connection reset")}
	executor, err := NewBenchmarkingFinalizeExecutor(store, poisonedConn{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	run := benchmarkingFinalizeRun("run-lookup-fail", "org-lookup-fail", time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC))

	rows, err := executor.ComputeFinalizeFamily(context.Background(), run)
	if rows != 0 {
		t.Errorf("rows = %d, want 0", rows)
	}
	if err == nil {
		t.Fatal("ComputeFinalizeFamily succeeded despite a partition-count lookup failure -- " +
			"a Postgres error here must propagate, not fail open")
	}
}

// TestNewBenchmarkingFinalizeExecutorRefusesOnNilStoreOrConn matches every
// other native family's construction-time policy: fail closed on a missing
// dependency, never construct something that would panic or silently no-op
// later.
func TestNewBenchmarkingFinalizeExecutorRefusesOnNilStoreOrConn(t *testing.T) {
	if _, err := NewBenchmarkingFinalizeExecutor(nil, poisonedConn{}, nil); err == nil {
		t.Error("expected a refusal on a nil store")
	}
	if _, err := NewBenchmarkingFinalizeExecutor(&fakeStore{}, nil, nil); err == nil {
		t.Error("expected a refusal on a nil conn")
	}
}

var _ NativeFinalizeFamilyExecutor = (*BenchmarkingFinalizeExecutor)(nil)
