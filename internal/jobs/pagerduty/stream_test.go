package pagerduty

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

func TestHandlerCompletesReceiptOnlyAfterReconcile(t *testing.T) {
	receipts := &receiptStore{state: ReceiptClaimed}
	reconciler := &reconciler{}
	handler, err := NewHandler(receipts, reconciler)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Handle(context.Background(), validMessage())
	if err != nil || !reconciler.called || !receipts.completed {
		t.Fatalf("handle=%v called=%t completed=%t", err, reconciler.called, receipts.completed)
	}
}

func TestHandlerLeavesReceiptIncompleteForStreamRetry(t *testing.T) {
	receipts := &receiptStore{state: ReceiptClaimed}
	handler, err := NewHandler(receipts, &reconciler{err: errors.New("clickhouse unavailable")})
	if err != nil || handler.Handle(context.Background(), validMessage()) == nil || receipts.completed {
		t.Fatalf("transient failure must remain retryable: new=%v complete=%t", err, receipts.completed)
	}
	// The claim must be released, or the next delivery reads the abandoned
	// lease as in-flight work and can never retry until it expires.
	if !receipts.released {
		t.Fatal("failed reconciliation left its receipt claimed")
	}
}

// TestHandlerNeverAcknowledgesAnInFlightReceipt is the CUT-04 codex HIGH-2
// regression. A live lease held by another consumer is not durable success:
// that consumer may still fail, and acknowledging here drops the event with no
// canonical effect and no dead-letter record.
func TestHandlerNeverAcknowledgesAnInFlightReceipt(t *testing.T) {
	receipts := &receiptStore{state: ReceiptInFlight}
	reconciler := &reconciler{}
	handler, err := NewHandler(receipts, reconciler)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Handle(context.Background(), validMessage())
	if err == nil {
		t.Fatal("in-flight receipt was acknowledged")
	}
	if streamrunner.IsPermanent(err) {
		t.Fatal("in-flight receipt must not spend the permanent budget")
	}
	if reconciler.called || receipts.completed || receipts.released {
		t.Fatalf("in-flight receipt touched another consumer's work: called=%t completed=%t released=%t",
			reconciler.called, receipts.completed, receipts.released)
	}
}

func TestHandlerTreatsDuplicateReceiptAsDurableSuccess(t *testing.T) {
	receipts := &receiptStore{state: ReceiptCompleted}
	reconciler := &reconciler{}
	handler, err := NewHandler(receipts, reconciler)
	if err != nil || handler.Handle(context.Background(), validMessage()) != nil || reconciler.called {
		t.Fatalf("duplicate receipt outcome err=%v called=%t", err, reconciler.called)
	}
}

func TestHandlerRejectsMalformedPayloadPermanently(t *testing.T) {
	handler, err := NewHandler(&receiptStore{state: ReceiptClaimed}, &reconciler{})
	message := validMessage()
	message.Fields["payload"] = "not-json"
	err = handler.Handle(context.Background(), message)
	if err == nil || !streamrunner.IsPermanent(err) {
		t.Fatalf("malformed err=%v", err)
	}
}

func validMessage() streamrunner.Message {
	return streamrunner.Message{Stream: "pagerduty-webhooks:binding", ID: "1-0", Fields: map[string]string{"binding_id": "binding", "received_at": "2026-07-23T00:00:00Z", "payload": `{"event":{"id":"evt-1"}}`}}
}

type receiptStore struct {
	state               ReceiptState
	completed, released bool
}

func (s *receiptStore) Begin(_ context.Context, receipt string) (ReceiptClaim, error) {
	return ReceiptClaim{ReceiptID: receipt, Token: "claim", State: s.state}, nil
}
func (s *receiptStore) Complete(_ context.Context, _ ReceiptClaim) error {
	s.completed = true
	return nil
}
func (s *receiptStore) Release(_ context.Context, _ ReceiptClaim) error {
	s.released = true
	return nil
}

type reconciler struct {
	called bool
	err    error
}

func (r *reconciler) Reconcile(context.Context, Event) error { r.called = true; return r.err }
