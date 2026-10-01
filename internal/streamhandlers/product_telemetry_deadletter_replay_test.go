package streamhandlers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/producttelemetry"
	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

type capturedStreams struct {
	stream string
	fields map[string]string
}

func (c *capturedStreams) Append(_ context.Context, stream string, fields [][2]string) error {
	c.stream, c.fields = stream, map[string]string{}
	for _, field := range fields {
		c.fields[field[0]] = field[1]
	}
	return nil
}

// intakeEntry posts body to the real intake route and returns the exact stream
// entry it writes, as the consumer receives it.
func intakeEntry(t *testing.T, body string) streamrunner.Message {
	t.Helper()
	streams := &capturedStreams{}
	route := producttelemetry.Routes(streams, slog.New(slog.NewTextHandler(io.Discard, nil)))[0]
	request := httptest.NewRequest(http.MethodPost, route.Pattern, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	route.Handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted || streams.fields == nil {
		t.Fatalf("intake status %d, entry %v: %s", recorder.Code, streams.fields, recorder.Body.String())
	}
	return streamrunner.Message{Stream: streams.stream, ID: "1-0", Fields: streams.fields}
}

const replayBatch = `{"orgIdHash":"h1","events":[` +
	`{"name":"page_viewed","schemaVersion":"1","eventId":"e1","ts":"2026-09-23T02:00:00Z","sessionId":"s","anonymousUserId":"a","payload":{"n":1.5,"ok":true}},` +
	`{"name":"session_ended","schemaVersion":"1","eventId":"e2","ts":"2026-09-23T02:00:01Z","sessionId":"s","anonymousUserId":"a","payload":{}}]}`

func storedEventIDs(t *testing.T, message streamrunner.Message) []string {
	t.Helper()
	sink := &productSink{batch: &productBatch{}}
	handler, err := NewProductTelemetryHandler(sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Handle(context.Background(), message); err != nil {
		t.Fatalf("handler refused the message: %v", err)
	}
	if !sink.batch.sent {
		t.Fatal("handler did not send the batch")
	}
	var ids []string
	for _, row := range sink.batch.rows {
		ids = append(ids, row[1].(string))
	}
	return ids
}

// A product-telemetry entry that was quarantined (here: delivery limit hit
// while ClickHouse was down) can be stored from its dead-letter row alone.
// The entry is the one the real intake writes, so the encoder under test is
// the producer's.
func TestQuarantinedProductTelemetryEntryIsReplayableFromItsDeadLetterRow(t *testing.T) {
	entry := intakeEntry(t, replayBatch)
	row := streamrunner.DeadLetterFields(entry, "max_deliveries_exceeded", "2026-10-01T00:00:00Z")
	replay, ok := streamrunner.MessageFromDeadLetter(row)
	if !ok {
		t.Fatalf("row is not replayable: %v", row)
	}
	original, replayed := storedEventIDs(t, entry), storedEventIDs(t, replay)
	if len(replayed) != 2 || strings.Join(replayed, ",") != strings.Join(original, ",") || replayed[0] != "e1" {
		t.Fatalf("replay stored %v, direct delivery stored %v", replayed, original)
	}
}

// An entry refused for a banned payload key is quarantined without its text.
func TestEntryRefusedForABannedPayloadKeyIsNotCopiedIntoTheDeadLetterRow(t *testing.T) {
	entry := intakeEntry(t, strings.Replace(replayBatch, `"payload":{}`, `"payload":{"email":"person@example.test"}`, 1))
	handler, err := NewProductTelemetryHandler(&productSink{batch: &productBatch{}})
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Handle(context.Background(), entry)
	var permanent *streamrunner.PermanentError
	if !errors.As(err, &permanent) || permanent.Reason != "blocked_telemetry_payload" {
		t.Fatalf("handler error = %v, want blocked_telemetry_payload", err)
	}
	row := streamrunner.DeadLetterFields(entry, permanent.Reason, "t")
	for key, value := range row {
		if strings.Contains(value, "person@example.test") {
			t.Fatalf("dead-letter field %s carries the banned value", key)
		}
	}
	if _, ok := streamrunner.MessageFromDeadLetter(row); ok {
		t.Fatal("a withheld row claimed to be replayable")
	}
}
