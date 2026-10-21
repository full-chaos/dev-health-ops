package sync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// prometheusMaterializer is a minimal Materializer double that also exposes
// WritePrometheus, so tests can drive the type assertion in
// OccurrenceReconciler.WritePrometheus without a real NativeMaterializer.
type prometheusMaterializer struct{ text string }

func (m prometheusMaterializer) Materialize(context.Context, pgx.Tx, PendingOccurrence) (PlanResult, error) {
	return PlanResult{}, errors.New("not implemented")
}

func (m prometheusMaterializer) WritePrometheus(output io.Writer) error {
	_, err := io.WriteString(output, m.text)
	return err
}

// plainMaterializer is a Materializer double that does NOT implement
// WritePrometheus, standing in for a test double or a future alternate
// implementation.
type plainMaterializer struct{}

func (plainMaterializer) Materialize(context.Context, pgx.Tx, PendingOccurrence) (PlanResult, error) {
	return PlanResult{}, errors.New("not implemented")
}

func TestOccurrenceReconcilerWritePrometheusDelegatesWhenMaterializerSupportsIt(t *testing.T) {
	reconciler, err := NewOccurrenceReconciler(&pgxpool.Pool{}, prometheusMaterializer{text: "probe_metric 1\n"})
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := reconciler.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	if output.String() != "probe_metric 1\n" {
		t.Fatalf("output = %q, want delegated materializer output", output.String())
	}
}

func TestOccurrenceReconcilerWritePrometheusIsANoOpWhenMaterializerDoesNotSupportIt(t *testing.T) {
	reconciler, err := NewOccurrenceReconciler(&pgxpool.Pool{}, plainMaterializer{})
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := reconciler.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	if output.Len() != 0 {
		t.Fatalf("output = %q, want empty: materializer does not implement WritePrometheus", output.String())
	}
}

func TestOccurrenceReconcilerWritePrometheusHandlesNilReceiver(t *testing.T) {
	var reconciler *OccurrenceReconciler
	if err := reconciler.WritePrometheus(&bytes.Buffer{}); err != nil {
		t.Fatalf("nil receiver: err = %v, want nil (a nil reconciler has no metrics to report)", err)
	}
}

type deferralTx struct {
	pgx.Tx
	execArgs  []any
	commits   int
	rollbacks int
}

func (tx *deferralTx) Exec(_ context.Context, _ string, args ...any) (pgconn.CommandTag, error) {
	tx.execArgs = args
	return pgconn.NewCommandTag("UPDATE 1"), nil
}
func (tx *deferralTx) Commit(context.Context) error   { tx.commits++; return nil }
func (tx *deferralTx) Rollback(context.Context) error { tx.rollbacks++; return nil }

// beginRecorder stands for the pool: it fails like pgxpool does on a dead ctx.
type beginRecorder struct {
	calls int
	tx    *deferralTx
}

func (b *beginRecorder) begin(ctx context.Context) (pgx.Tx, error) {
	b.calls++
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return b.tx, nil
}

func newDeferralReconciler(b *beginRecorder) *OccurrenceReconciler {
	return &OccurrenceReconciler{beginTx: b.begin}
}

func TestRecordMaterializeFailureOnStepDeadlineConsumesNoAttemptAndOpensNoTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	b := &beginRecorder{tx: &deferralTx{}}
	outcome, err := newDeferralReconciler(b).recordMaterializeFailure(
		ctx, time.Now(), PendingOccurrence{ID: "o", AttemptCount: 1}, fmt.Errorf("plan: %w", context.DeadlineExceeded))
	if !errors.Is(err, context.DeadlineExceeded) || outcome != "" {
		t.Fatalf("outcome=%q err=%v", outcome, err)
	}
	if b.calls != 0 || b.tx.commits != 0 {
		t.Fatalf("begin calls=%d commits=%d; a step deadline must not open a deferral", b.calls, b.tx.commits)
	}
}

func TestRecordMaterializeFailureRealPlannerErrorCountsAttemptEvenWhenStepContextJustEnded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b := &beginRecorder{tx: &deferralTx{}}
	outcome, err := newDeferralReconciler(b).recordMaterializeFailure(
		ctx, time.Now(), PendingOccurrence{ID: "o", AttemptCount: 1}, errors.New("planner exploded"))
	if err != nil || outcome != OccurrenceReconcileRetry {
		t.Fatalf("outcome=%q err=%v", outcome, err)
	}
	if b.tx.commits != 1 || len(b.tx.execArgs) != 7 || b.tx.execArgs[0] != 2 {
		t.Fatalf("commits=%d args=%v; attempt must be counted to 2", b.tx.commits, b.tx.execArgs)
	}
	if b.tx.execArgs[2] != OccurrenceErrorPlannerError {
		t.Fatalf("code = %v", b.tx.execArgs[2])
	}
}
