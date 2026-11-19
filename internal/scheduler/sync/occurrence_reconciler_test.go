package sync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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

func TestBudgetEndMarksOnlyTheCallersOwnContextError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	if err := budgetEnd(ctx, fmt.Errorf("lock: %w", context.DeadlineExceeded)); !errors.Is(err, ErrReconcileBudgetEnded) {
		t.Fatalf("own deadline not marked: %v", err)
	}
	if err := budgetEnd(ctx, markRecordWrite(context.DeadlineExceeded)); errors.Is(err, ErrReconcileBudgetEnded) {
		t.Fatalf("a failure-record write timeout was marked as the budget ending: %v", err)
	}
	if err := budgetEnd(ctx, errors.New("boom")); errors.Is(err, ErrReconcileBudgetEnded) {
		t.Fatalf("a non-context error was marked: %v", err)
	}
	if err := budgetEnd(context.Background(), context.DeadlineExceeded); errors.Is(err, ErrReconcileBudgetEnded) {
		t.Fatalf("a deadline with a live ctx was marked: %v", err)
	}
}

func TestDeferOccurrenceBeginFailureIsMarkedAsAFailureRecordWrite(t *testing.T) {
	r := &OccurrenceReconciler{beginTx: func(context.Context) (pgx.Tx, error) { return nil, context.DeadlineExceeded }}
	_, err := r.deferOccurrence(context.Background(), time.Now(), PendingOccurrence{ID: "o"}, OccurrenceErrorPlannerError)
	if !errors.Is(err, errFailureRecordWrite) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}

func TestDeferOccurrenceExhaustionLogsOneQuarantineWarn(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	b := &beginRecorder{tx: &deferralTx{}}
	occurrence := PendingOccurrence{
		ID: "occ-1", OrgID: "org-1", ConfigIntegrationID: "int-1",
		AttemptCount: occurrenceReconcileMaxAttempts - 1,
	}
	outcome, err := newDeferralReconciler(b).deferOccurrence(
		context.Background(), time.Now(), occurrence, OccurrenceErrorPlannerError)
	if err != nil || outcome != OccurrenceReconcileQuarantined {
		t.Fatalf("outcome=%q err=%v", outcome, err)
	}
	var line map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &line); err != nil {
		t.Fatalf("want exactly one JSON log line, got %q: %v", buf.String(), err)
	}
	want := map[string]any{
		"level": "WARN", "msg": "sync.occurrence.quarantined", "org_id": "org-1",
		"integration_id": "int-1", "occurrence_id": "occ-1",
		"failure_class": OccurrenceErrorPlannerError,
		"attempt_count": float64(occurrenceReconcileMaxAttempts),
	}
	for k, v := range want {
		if line[k] != v {
			t.Fatalf("%s = %v, want %v (line %v)", k, line[k], v, line)
		}
	}
}

func TestDeferOccurrenceRetryLogsNoQuarantineLine(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	b := &beginRecorder{tx: &deferralTx{}}
	_, err := newDeferralReconciler(b).deferOccurrence(
		context.Background(), time.Now(), PendingOccurrence{ID: "o"}, OccurrenceErrorPlannerError)
	if err != nil || buf.Len() != 0 {
		t.Fatalf("err=%v log=%q", err, buf.String())
	}
}
