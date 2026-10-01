//go:build integration

package streamrunner

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	valkeystore "github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestValkeyDiscoveryGroupReadAndReclaim(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate Valkey: %v", err)
		}
	})
	client, err := valkeystore.Open(ctx, valkeystore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	transport, err := NewValkeyTransport(client)
	if err != nil {
		t.Fatal(err)
	}

	for _, stream := range []string{"ingest:org-b:commits", "not-a-stream", "ingest:org-a:commits"} {
		if stream == "not-a-stream" {
			if err := client.Do(ctx, client.B().Set().Key(stream).Value("value").Build()).Error(); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := client.Do(ctx, client.B().Xadd().Key(stream).Id("*").FieldValue().FieldValue("org_id", stream).Build()).Error(); err != nil {
			t.Fatal(err)
		}
	}
	found, err := transport.Discover(ctx, []string{"ingest:*:commits"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"ingest:org-a:commits", "ingest:org-b:commits"}; !slices.Equal(found, want) {
		t.Fatalf("discovered = %v want %v", found, want)
	}
	if _, err := transport.Discover(ctx, []string{"ingest:*:commits"}, 1); !errors.Is(err, ErrDiscoveryLimit) {
		t.Fatalf("bounded discovery error = %v", err)
	}

	for _, stream := range found {
		if err := transport.EnsureGroup(ctx, stream, "go-test"); err != nil {
			t.Fatal(err)
		}
	}
	messages, err := transport.ReadNew(ctx, found, "go-test", "consumer-a", 1, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 {
		t.Fatalf("multi-lane read count = %d", len(messages))
	}
	for _, message := range messages {
		pending, err := transport.Pending(ctx, message.Stream, "go-test", 1, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(pending) != 1 {
			t.Fatalf("pending %s = %#v", message.Stream, pending)
		}
	}
}

func TestExternalQuarantineIsIdempotentAndRepairsTrimmedRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate Valkey: %v", err)
		}
	})
	client, err := valkeystore.Open(ctx, valkeystore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	transport, err := NewValkeyTransport(client)
	if err != nil {
		t.Fatal(err)
	}
	message := Message{
		Stream: "external-ingest:org-a:batches", ID: "1-0",
		Fields: map[string]string{"org_id": "org-a", "ingestion_id": "batch-a"},
	}
	if err := transport.Quarantine(ctx, message, "schema_invalid"); err != nil {
		t.Fatal(err)
	}
	if err := transport.Quarantine(ctx, message, "schema_invalid"); err != nil {
		t.Fatal(err)
	}
	dlq := "external-ingest:org-a:dlq"
	entries, err := client.Do(ctx, client.B().Xrange().Key(dlq).Start("-").End("+").Build()).AsXRange()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("duplicate quarantine rows = %d", len(entries))
	}
	firstID := entries[0].ID
	if err := client.Do(ctx, client.B().Xdel().Key(dlq).Id(firstID).Build()).Error(); err != nil {
		t.Fatal(err)
	}
	if err := transport.Quarantine(ctx, message, "schema_invalid"); err != nil {
		t.Fatal(err)
	}
	entries, err = client.Do(ctx, client.B().Xrange().Key(dlq).Start("-").End("+").Build()).AsXRange()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].ID == firstID {
		t.Fatalf("trimmed DLQ row not repaired: %#v", entries)
	}
}

// The Valkey transport writes the product-telemetry dead-letter row with the
// payload, and the row read back from Valkey rebuilds the original entry.
func TestProductTelemetryQuarantineRowIsReplayableFromValkey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate Valkey: %v", err)
		}
	})
	client, err := valkeystore.Open(ctx, valkeystore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	transport, err := NewValkeyTransport(client)
	if err != nil {
		t.Fatal(err)
	}
	message := telemetryMessage(`[{"name":"page_viewed","eventId":"e1"}]`)
	if err := transport.Quarantine(ctx, message, "invalid_telemetry_event"); err != nil {
		t.Fatal(err)
	}
	entries, err := client.Do(ctx, client.B().Xrange().Key("product-telemetry:dlq").Start("-").End("+").Build()).AsXRange()
	if err != nil || len(entries) != 1 {
		t.Fatalf("dead-letter rows = %d, err %v", len(entries), err)
	}
	replay, ok := MessageFromDeadLetter(entries[0].FieldValues)
	if !ok || replay.Fields[eventsField] != message.Fields[eventsField] || replay.Stream != telemetryStream {
		t.Fatalf("replay from Valkey row = %#v ok=%v", replay, ok)
	}
}
