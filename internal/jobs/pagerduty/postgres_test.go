package pagerduty

import (
	"context"
	"testing"
)

// The receipt store is the crash-safe half of the stream ACK boundary. Its
// guards must fail closed with a stable sanitized error rather than panic or
// leak a DSN when the process is started without domain storage.
func TestPostgresReceiptStoreFailsClosedWithoutDurableStorage(t *testing.T) {
	if _, err := NewPostgresReceiptStore(nil); err != errUnavailable {
		t.Fatalf("nil pool constructor err = %v", err)
	}
	var store *PostgresReceiptStore
	if _, err := store.Begin(context.Background(), "pagerduty:binding:evt-1"); err != errUnavailable {
		t.Fatalf("nil store Begin = %v", err)
	}
	if err := store.Complete(context.Background(), ReceiptClaim{
		ReceiptID: "pagerduty:binding:evt-1", Token: "token",
	}); err != errUnavailable {
		t.Fatalf("nil store Complete = %v", err)
	}
	unclaimed := &PostgresReceiptStore{}
	if err := unclaimed.Complete(context.Background(), ReceiptClaim{
		ReceiptID: "pagerduty:binding:evt-1",
	}); err != errUnavailable {
		t.Fatalf("untokened Complete = %v", err)
	}
}
