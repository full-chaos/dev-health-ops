package streamhandlers

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

type productSink struct {
	query string
	batch *productBatch
	err   error
}

func (s *productSink) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	s.query = query
	if s.err != nil {
		return nil, s.err
	}
	return s.batch, nil
}

type productBatch struct {
	rows    [][]any
	sent    bool
	sendErr error
}

func (b *productBatch) Abort() error { return nil }
func (b *productBatch) Append(v ...any) error {
	b.rows = append(b.rows, append([]any(nil), v...))
	return nil
}
func (b *productBatch) AppendStruct(any) error        { return errors.New("unused") }
func (b *productBatch) Column(int) driver.BatchColumn { return nil }
func (b *productBatch) Flush() error                  { return nil }
func (b *productBatch) Send() error                   { b.sent = true; return b.sendErr }
func (b *productBatch) IsSent() bool                  { return b.sent }
func (b *productBatch) Rows() int                     { return len(b.rows) }
func (b *productBatch) Columns() []column.Interface   { return nil }
func (b *productBatch) Close() error                  { return nil }

func TestProductTelemetryPersistsValidatedEventsThroughOneReusedSink(t *testing.T) {
	sink := &productSink{batch: &productBatch{}}
	handler, err := NewProductTelemetryHandler(sink)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Handle(context.Background(), streamrunner.Message{Fields: map[string]string{"events": `[{"name":"page_viewed","schemaVersion":"v1","eventId":"event-1","ts":"2026-07-23T12:00:00Z","sessionId":"session-1","anonymousUserId":"anon-1","payload":{"feature":"home"}}]`}})
	if err != nil {
		t.Fatal(err)
	}
	if !sink.batch.sent || len(sink.batch.rows) != 1 {
		t.Fatalf("durable product sink = %#v", sink.batch)
	}
	if len(sink.batch.rows[0]) != len(ProductTelemetryColumns) {
		t.Fatalf("columns = %d", len(sink.batch.rows[0]))
	}
	if got := sink.batch.rows[0][1]; got != "event-1" {
		t.Fatalf("event id = %v", got)
	}
}

func TestProductTelemetryRejectsPIIPayloadBeforeSinkWrite(t *testing.T) {
	sink := &productSink{batch: &productBatch{}}
	handler, err := NewProductTelemetryHandler(sink)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Handle(context.Background(), streamrunner.Message{Fields: map[string]string{"events": `[{"name":"page_viewed","schemaVersion":"v1","eventId":"event-1","ts":"2026-07-23T12:00:00Z","sessionId":"session-1","anonymousUserId":"anon-1","payload":{"email":"nope@example.test"}}]`}})
	if !streamrunner.IsPermanent(err) || len(sink.batch.rows) != 0 {
		t.Fatalf("PII result = %v rows=%v", err, sink.batch.rows)
	}
}

func TestInternalIngestCommitsUsesReplacingKeySinkBeforeAcknowledgement(t *testing.T) {
	sink := &productSink{batch: &productBatch{}}
	handler, err := NewInternalIngestHandler(sink)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Handle(context.Background(), streamrunner.Message{Stream: "ingest:org-1:commits", Fields: map[string]string{"payload": `{"org_id":"org-1","repo_url":"https://example.test/acme/repo","items":[{"hash":"abc","message":"message","author_name":"Ada","author_email":"ada@example.test","author_when":"2026-07-23T12:00:00Z"}]}`}})
	if err != nil {
		t.Fatal(err)
	}
	if !sink.batch.sent || len(sink.batch.rows) != 1 {
		t.Fatalf("internal sink = %#v", sink.batch)
	}
	if sink.batch.rows[0][1] != "abc" {
		t.Fatalf("commit hash = %v", sink.batch.rows[0][1])
	}
}
