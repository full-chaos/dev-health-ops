package sync

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/jackc/pgx/v5"
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
