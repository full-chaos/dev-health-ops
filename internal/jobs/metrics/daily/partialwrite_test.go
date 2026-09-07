package daily

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

type partialWriteExecutor struct {
	rows int
	err  error
}

func (e partialWriteExecutor) ComputeFamily(context.Context, Run, Partition) (int, error) {
	return e.rows, e.err
}

type recordingObserver struct {
	family  string
	outcome jobruntime.DailyMetricsNativeFamilyOutcome
	rows    int
	calls   int
}

func (o *recordingObserver) ObserveDailyMetricsNativeFamily(
	family string, outcome jobruntime.DailyMetricsNativeFamilyOutcome, rows int, _ time.Duration,
) error {
	o.family, o.outcome, o.rows = family, outcome, rows
	o.calls++
	return nil
}

// TestPartialWriteReportsItsTrueRowCountAndHoldsThePartition pins the
// distinction codex r1 on #2235 (F2) found missing.
//
// The two failure modes used to DIVERGE at the partition level: a family
// that wrote NOTHING fell open to the Python compatibility bridge, while a
// family that had already written rows could not, because the output tables
// are append-only MergeTrees with no version column -- a bridge write would
// duplicate rather than replace, undetectably (argMax-style reads still
// return a sane latest value, only the row count grows).
//
// CHAOS-5243 first collapsed that divergence at the partition level (both
// hold the partition incomplete), and CHAOS-3092 (PR-A) then deleted the
// bridge entirely, so there is nothing left to fall open to in either case.
// What still diverges, and what this test pins in both directions, is the
// OBSERVED outcome and row count: PartialWrite reports the executor's TRUE
// count, an ordinary Refused reports 0.
func TestPartialWriteReportsItsTrueRowCountAndHoldsThePartition(t *testing.T) {
	t.Run("partial write is skipped and reports its TRUE row count", func(t *testing.T) {
		observer := &recordingObserver{}
		handler := &PartitionHandler{
			nativeFamilies: map[string]NativeFamilyExecutor{
				"benchmarking": partialWriteExecutor{
					rows: 1234,
					err:  fmt.Errorf("table 3 of 6: %w", ErrPartialWrite),
				},
			},
			nativeFamilyNames: []string{"benchmarking"},
			nativeObserver:    observer,
			nativeFamiliesNow: time.Now,
		}

		err := handler.computeNativeFamilies(context.Background(), Run{ID: "r"}, Partition{ID: "p"})

		if observer.outcome != jobruntime.DailyMetricsNativeFamilyOutcomePartialWrite {
			t.Errorf("outcome = %q, want %q", observer.outcome,
				jobruntime.DailyMetricsNativeFamilyOutcomePartialWrite)
		}
		if observer.rows != 1234 {
			t.Errorf("rows = %d, want 1234 -- reporting 0 understates what landed, which is "+
				"exactly the number needed to judge duplication", observer.rows)
		}
		// CHAOS-5078 codex round 3: a partial write must hold the PARTITION
		// incomplete.
		if !errors.Is(err, ErrPreBridgeFamilyIncomplete) {
			t.Fatalf("err = %v, want it to wrap ErrPreBridgeFamilyIncomplete", err)
		}
	})

	t.Run("an ordinary failure now also fails the partition (CHAOS-5243)", func(t *testing.T) {
		observer := &recordingObserver{}
		handler := &PartitionHandler{
			nativeFamilies: map[string]NativeFamilyExecutor{
				"benchmarking": partialWriteExecutor{rows: 0, err: errors.New("connection refused")},
			},
			nativeFamilyNames: []string{"benchmarking"},
			nativeObserver:    observer,
			nativeFamiliesNow: time.Now,
		}

		err := handler.computeNativeFamilies(context.Background(), Run{ID: "r"}, Partition{ID: "p"})

		// CHAOS-5243 (chris, "it should fail loudly so we find it"): the
		// CHAOS-4276 fail-open-to-Python-bridge path for an ORDINARY
		// (non-partial) refusal is DELETED, and CHAOS-3092 (PR-A) deleted
		// the bridge it pointed at. A family that wrote nothing is treated
		// the same as one that wrote SOMETHING and then failed: the
		// partition is held incomplete.
		if observer.outcome != jobruntime.DailyMetricsNativeFamilyOutcomeRefused {
			t.Errorf("outcome = %q, want %q -- the OUTCOME stays distinguished from PartialWrite "+
				"(nothing was written), only the partition-level disposition changed",
				observer.outcome, jobruntime.DailyMetricsNativeFamilyOutcomeRefused)
		}
		if observer.rows != 0 {
			t.Errorf("rows = %d, want 0 -- genuinely nothing was written", observer.rows)
		}
		if !errors.Is(err, ErrPreBridgeFamilyIncomplete) {
			t.Fatalf("err = %v, want it to wrap ErrPreBridgeFamilyIncomplete -- an ordinary "+
				"refusal now holds the partition incomplete, exactly like a partial write does "+
				"(CHAOS-5243)", err)
		}
	})
}
