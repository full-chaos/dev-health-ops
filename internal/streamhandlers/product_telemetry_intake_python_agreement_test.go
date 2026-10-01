package streamhandlers

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/producttelemetry"
	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

func intakeRawBody(t *testing.T, body string) (int, streamrunner.Message) {
	t.Helper()
	streams := &parityStreams{}
	route := producttelemetry.Routes(streams, slog.New(slog.NewTextHandler(io.Discard, nil)))[0]
	request := httptest.NewRequest(http.MethodPost, route.Pattern, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	route.Handler.ServeHTTP(recorder, request)
	return recorder.Code, streamrunner.Message{Stream: "product-telemetry:h:events", ID: "1-0", Fields: streams.fields}
}

func batchWithPayload(payload string) string {
	return `{"events":[{"name":"page_viewed","schemaVersion":"1","eventId":"e","ts":"2026-09-23T02:00:00Z","sessionId":"s","anonymousUserId":"a","payload":` + payload + `}]}`
}

func storeEntry(t *testing.T, message streamrunner.Message) (string, error) {
	t.Helper()
	sink := &productSink{batch: &productBatch{}}
	handler, err := NewProductTelemetryHandler(sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Handle(context.Background(), message); err != nil {
		return "", err
	}
	return sink.batch.rows[0][7].(string), nil
}

// Through the real intake, with the JSON text as a client writes it (the corpus helper marshals a Go map,
// which would sort the keys first), the consumer's stored payload_json agrees with what the old Python
// consumer stored for the same text. The expected values were produced by executing that consumer's
// own json.loads and persist.py `_payload_json` (sort_keys=True) at git 61634a3f8e^:
//
//	{"z":1,"a":2,"m":3}  -> {"a":2,"m":3,"z":1}     keys are stored sorted
//	{"f":1e999}          -> {"f":Infinity}          Python stored the word; chris's CHAOS-6299 ruling stores null
//	9999...(4301 digits) -> json.loads raises ValueError; the intake answers 400 (4300-digit limit)
func TestIntakeAndConsumerAgreeWithPythonOnKeyOrderOverflowAndHugeIntegers(t *testing.T) {
	code, entry := intakeRawBody(t, batchWithPayload(`{"z":1,"a":2,"m":3}`))
	if got, err := storeEntry(t, entry); code != http.StatusAccepted || err != nil || got != `{"a":2,"m":3,"z":1}` {
		t.Fatalf("key order: status %d, stored %q, err %v", code, got, err)
	}
	code, entry = intakeRawBody(t, batchWithPayload(`{"f":1e999}`))
	if got, err := storeEntry(t, entry); code != http.StatusAccepted || err != nil || got != `{"f":null}` {
		t.Fatalf("float overflow: status %d, stored %q, err %v (Python stored the word Infinity; CHAOS-6299 stores null)", code, got, err)
	}
	if code, _ := intakeRawBody(t, batchWithPayload(`{"i":`+strings.Repeat("9", 4301)+`}`)); code != http.StatusBadRequest {
		t.Fatalf("4301-digit integer: intake status %d, want 400 (Python's json.loads refuses it too)", code)
	}
	huge := strings.Repeat("9", 4300)
	code, entry = intakeRawBody(t, batchWithPayload(`{"i":`+huge+`}`))
	if got, err := storeEntry(t, entry); code != http.StatusAccepted || err != nil || got != `{"i":`+huge+`}` {
		t.Fatalf("4300-digit integer: status %d, stored %d bytes, err %v", code, len(got), err)
	}
}
