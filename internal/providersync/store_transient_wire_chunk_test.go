package providersync

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type wireChunkRun struct {
	err      error
	refunded bool
}

// runWireChunkWrite runs one chunk-continuation write ("continuation" or "refund")
// against a storeWire. The wire server answers a unit UPDATE sent outside BEGIN
// through commitPoint, so a "commit" fault of class 08 applies the write and then
// drops the connection: a reply lost after the commit.
func runWireChunkWrite(t *testing.T, w *storeWire, write string) wireChunkRun {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	claim := nativeTestClaim("github", "commits")
	w.sourceID = claim.SourceID
	repository, err := NewPostgresRepository(w.pool(t, ctx), providerfoundation.NewMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	repository.storeRetrySleep = func(context.Context, int) error { return nil }
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	defer slog.SetDefault(previous)
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	availableAt := now.Add(5 * time.Second)
	var run wireChunkRun
	switch write {
	case "continuation":
		run.err = repository.DeferChunkContinuation(ctx, claim, availableAt, now)
	case "refund":
		run.refunded, run.err = repository.RefundChunkProgress(ctx, claim, 3, availableAt, now)
	default:
		t.Fatalf("unknown write %q", write)
	}
	return run
}

// C1: the continuation committed and its reply was lost: the write is read back
// and reported as done, not run a second time.
func TestChunkContinuationWithALostReplyAfterTheCommitIsReadBack(t *testing.T) {
	t.Parallel()
	w := &storeWire{faults: []*wireFault{{step: "commit", code: "08006", times: 1}}}
	run := runWireChunkWrite(t, w, "continuation")
	if run.err != nil || w.updates != 1 || w.commits != 1 || w.reads != 1 {
		t.Fatalf("err=%v updates=%d commits=%d reads=%d, want nil, 1, 1, 1 (read back once, not written twice)",
			run.err, w.updates, w.commits, w.reads)
	}
}

// C2: the same for the progress refund.
func TestProgressRefundWithALostReplyAfterTheCommitIsReadBack(t *testing.T) {
	t.Parallel()
	w := &storeWire{faults: []*wireFault{{step: "commit", code: "08006", times: 1}}}
	run := runWireChunkWrite(t, w, "refund")
	if run.err != nil || !run.refunded || w.updates != 1 || w.commits != 1 || w.reads != 1 {
		t.Fatalf("refunded=%v err=%v updates=%d commits=%d reads=%d, want true, nil, 1, 1, 1",
			run.refunded, run.err, w.updates, w.commits, w.reads)
	}
}

// C3: controls that hold before and after. No row for this owner: the plain
// continuation is ErrLeaseLost and the refund is (false, nil). A lost reply when
// another owner holds the unit is read back as lease lost.
func TestChunkContinuationWritesThatDidNotLandKeepTheirAnswers(t *testing.T) {
	t.Parallel()
	t.Run("no row, continuation", func(t *testing.T) {
		t.Parallel()
		w := &storeWire{done: true}
		if run := runWireChunkWrite(t, w, "continuation"); !errors.Is(run.err, ErrLeaseLost) {
			t.Fatalf("err=%v, want ErrLeaseLost", run.err)
		}
	})
	t.Run("no row, refund", func(t *testing.T) {
		t.Parallel()
		w := &storeWire{done: true}
		if run := runWireChunkWrite(t, w, "refund"); run.err != nil || run.refunded {
			t.Fatalf("refunded=%v err=%v, want false, nil", run.refunded, run.err)
		}
	})
	t.Run("lost reply, another owner holds the unit", func(t *testing.T) {
		t.Parallel()
		w := &storeWire{firstCommitState: "other", faults: []*wireFault{{step: "commit", code: "08006", times: 1}}}
		if run := runWireChunkWrite(t, w, "continuation"); !errors.Is(run.err, ErrLeaseLost) {
			t.Fatalf("err=%v, want ErrLeaseLost", run.err)
		}
	})
}

// C6: a deadlock refused the write before COMMIT: nothing applied, the write is
// retried in place and lands.
func TestChunkContinuationRetriesADeadlockInPlace(t *testing.T) {
	t.Parallel()
	w := &storeWire{faults: []*wireFault{{step: "commit", code: "40P01", times: 1}}}
	run := runWireChunkWrite(t, w, "continuation")
	if run.err != nil || w.updates != 2 || w.commits != 1 {
		t.Fatalf("err=%v updates=%d commits=%d, want nil, 2, 1 (retried in place after the deadlock)", run.err, w.updates, w.commits)
	}
}
