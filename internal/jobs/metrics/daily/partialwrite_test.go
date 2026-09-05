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

// TestPartialWriteIsSkippedNotFailedOpen pins the distinction codex r1 on #2235
// (F2) found missing.
//
// Fail-open is correct when a native family wrote NOTHING: the compatibility
// bridge computes it instead. It is wrong when the family already wrote rows,
// because the output tables are append-only MergeTrees with no version column,
// so a bridge write does not replace those rows -- it duplicates them. And the
// duplication is undetectable downstream: argMax-style reads still return a
// sane latest value, only the row count grows.
//
// So the two failure modes must diverge, and this test pins both directions
// rather than only the new one -- a test that checked only ErrPartialWrite
// would pass even if ordinary failures had accidentally stopped failing open.
func TestPartialWriteIsSkippedNotFailedOpen(t *testing.T) {
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

		skip, err := handler.computeNativeFamilies(context.Background(), Run{ID: "r"}, Partition{ID: "p"})

		if len(skip) != 1 || skip[0] != "benchmarking" {
			t.Fatalf("skip = %v, want [benchmarking] -- the bridge must NOT recompute a family "+
				"whose rows are already in append-only tables", skip)
		}
		if observer.outcome != jobruntime.DailyMetricsNativeFamilyOutcomePartialWrite {
			t.Errorf("outcome = %q, want %q", observer.outcome,
				jobruntime.DailyMetricsNativeFamilyOutcomePartialWrite)
		}
		if observer.rows != 1234 {
			t.Errorf("rows = %d, want 1234 -- reporting 0 understates what landed, which is "+
				"exactly the number needed to judge duplication", observer.rows)
		}
		// CHAOS-5078 codex round 3: a partial write must also hold the
		// PARTITION incomplete, not only skip the family from the bridge.
		if !errors.Is(err, ErrPreBridgeFamilyIncomplete) {
			t.Fatalf("err = %v, want it to wrap ErrPreBridgeFamilyIncomplete", err)
		}
	})

	t.Run("an ordinary failure still fails open", func(t *testing.T) {
		observer := &recordingObserver{}
		handler := &PartitionHandler{
			nativeFamilies: map[string]NativeFamilyExecutor{
				"benchmarking": partialWriteExecutor{rows: 0, err: errors.New("connection refused")},
			},
			nativeFamilyNames: []string{"benchmarking"},
			nativeObserver:    observer,
			nativeFamiliesNow: time.Now,
		}

		skip, err := handler.computeNativeFamilies(context.Background(), Run{ID: "r"}, Partition{ID: "p"})

		if len(skip) != 0 {
			t.Fatalf("skip = %v, want empty -- a family that wrote NOTHING must fail open to "+
				"the bridge, which is the whole native-family contract", skip)
		}
		if observer.outcome != jobruntime.DailyMetricsNativeFamilyOutcomeRefused {
			t.Errorf("outcome = %q, want %q", observer.outcome,
				jobruntime.DailyMetricsNativeFamilyOutcomeRefused)
		}
		// CHAOS-5078 codex round 3: an ordinary refusal must NOT hold the
		// partition -- only a partial write does.
		if err != nil {
			t.Fatalf("err = %v, want nil -- an ordinary refusal stays fail-open at the partition level too", err)
		}
	})
}
