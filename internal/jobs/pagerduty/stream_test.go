package pagerduty

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

func TestHandlerCompletesReceiptOnlyAfterReconcile(t *testing.T) {
	receipts := &receiptStore{proceed: true}
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
	receipts := &receiptStore{proceed: true}
	handler, err := NewHandler(receipts, &reconciler{err: errors.New("clickhouse unavailable")})
	if err != nil || handler.Handle(context.Background(), validMessage()) == nil || receipts.completed {
		t.Fatalf("transient failure must remain retryable: new=%v complete=%t", err, receipts.completed)
	}
}

func TestHandlerTreatsDuplicateReceiptAsDurableSuccess(t *testing.T) {
	receipts := &receiptStore{}
	reconciler := &reconciler{}
	handler, err := NewHandler(receipts, reconciler)
	if err != nil || handler.Handle(context.Background(), validMessage()) != nil || reconciler.called {
		t.Fatalf("duplicate receipt outcome err=%v called=%t", err, reconciler.called)
	}
}

func TestHandlerRejectsMalformedPayloadPermanently(t *testing.T) {
	handler, err := NewHandler(&receiptStore{proceed: true}, &reconciler{})
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

type receiptStore struct{ proceed, completed bool }

func (s *receiptStore) Begin(_ context.Context, receipt string) (ReceiptClaim, error) {
	return ReceiptClaim{ReceiptID: receipt, Token: "claim", Proceed: s.proceed}, nil
}
func (s *receiptStore) Complete(_ context.Context, _ ReceiptClaim) error {
	s.completed = true
	return nil
}

type reconciler struct {
	called bool
	err    error
}

func (r *reconciler) Reconcile(context.Context, Event) error { r.called = true; return r.err }
