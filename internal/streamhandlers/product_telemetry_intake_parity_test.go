package streamhandlers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/producttelemetry"
	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

// product_telemetry_python_stored.json is what the Python consumer this port replaced stored for each
// event shape: the corpus event, and the row its own validation and row builder produced
// (api/product_telemetry/schemas.py and persist.py at git 61634a3f8e^, executed, not retyped).
type pythonStored struct {
	Python string         `json:"python"`
	Raw    map[string]any `json:"raw"`
	Row    struct {
		OrgIDHash       string  `json:"org_id_hash"`
		EventID         string  `json:"event_id"`
		Name            string  `json:"name"`
		SchemaVersion   string  `json:"schema_version"`
		SessionID       string  `json:"session_id"`
		AnonymousUserID string  `json:"anonymous_user_id"`
		RoutePattern    *string `json:"route_pattern"`
		PayloadJSON     string  `json:"payload_json"`
		OccurredAt      string  `json:"occurred_at"`
	} `json:"row"`
}

// payloadNamedDivergences are corpus shapes whose payload_json TEXT differs from Python's although the
// stored value is the same JSON: the Go consumer re-encodes with encoding/json (non-ASCII is kept raw, <, > and & are escaped). They are accepted divergences: the only
// reader of the column (queryapi/producttelemetry) parses it with JSONExtract*, which reads both
// spellings as the same value. Pinned so a change on either side fails this test.
var payloadNamedDivergences = map[string]struct{ goText, pythonText string }{
	"payload_unicode": {`{"s":"héllo ☃ \u003c\u0026\u003e"}`, `{"s":"h\u00e9llo \u2603 <&>"}`},
}

// refusedBelowClickHouseMin are corpus shapes whose Python row holds the timestamp but whose Python
// insert raised (ValueError, year 0001), so the Python consumer dead-lettered them: the Go consumer
// refuses them as well (it is below the range the driver encodes, see TestConsumerStoresTimestampsOnly...).
var refusedBelowClickHouseMin = map[string]struct{}{"ts_year1": {}}

// refusedByTheStricterIntake are corpus shapes the intake no longer accepts, so the entry the consumer
// reads is built by hand (an older entry of the stream, or any writer other than this intake). The
// intake answer is pinned; the consumer still stores them as Python stored them. The intake is
// stricter than Python here, which D6129 allows.
var refusedByTheStricterIntake = map[string]struct {
	status int
	reason string
}{
	"org_hash_and_route": {http.StatusUnprocessableEntity, "the org hash is bounded to the shape the web client sends; the value h1 is refused"},
	"payload_unicode":    {http.StatusAccepted, "a payload string outside the value charset drops its event (202, nothing written)"},
}

type parityStreams struct{ fields map[string]string }

func (p *parityStreams) Append(_ context.Context, _ string, fields [][2]string) error {
	p.fields = map[string]string{}
	for _, field := range fields {
		p.fields[field[0]] = field[1]
	}
	return nil
}

// Every event shape the public intake accepts, as the exact stream entry the intake writes, must be
// stored by the consumer with the values the Python consumer stored. Before this, an entry whose event
// had an empty id or a zone-less timestamp was refused for good and its whole batch (up to 500 events)
// was dead-lettered.
func TestConsumerStoresEveryIntakeAcceptedShapeAsPythonStoredIt(t *testing.T) {
	data, err := os.ReadFile("testdata/product_telemetry_python_stored.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus map[string]pythonStored
	// Numbers stay as written: a float64 here would round an integer above 2^53 before the intake saw it.
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&corpus); err != nil {
		t.Fatal(err)
	}
	stored := 0
	for name, want := range corpus {
		if want.Python != "stored" {
			continue
		}
		stored++
		t.Run(name, func(t *testing.T) {
			if _, refused := refusedBelowClickHouseMin[name]; refused {
				entry := intakeEntryFor(t, want.Raw)
				handler, err := NewProductTelemetryHandler(&productSink{batch: &productBatch{}})
				if err != nil {
					t.Fatal(err)
				}
				if err := handler.Handle(context.Background(), entry); !streamrunner.IsPermanent(err) {
					t.Fatalf("a timestamp before ClickHouse's DateTime64 range: err = %v, want a permanent refusal", err)
				}
				return
			}
			var message streamrunner.Message
			if refusal, listed := refusedByTheStricterIntake[name]; listed {
				body, err := json.Marshal(map[string]any{"events": []any{want.Raw}})
				if err != nil {
					t.Fatal(err)
				}
				code, refused := intakeRawBody(t, string(body))
				if code != refusal.status || refused.Fields != nil {
					t.Fatalf("intake: status %d, entry %v; want status %d and no entry (%s)", code, refused.Fields, refusal.status, refusal.reason)
				}
				message = handBuiltEntry(t, want.Raw)
			} else {
				message = intakeEntryFor(t, want.Raw)
			}
			sink := &productSink{batch: &productBatch{}}
			handler, err := NewProductTelemetryHandler(sink)
			if err != nil {
				t.Fatal(err)
			}
			if err := handler.Handle(context.Background(), message); err != nil {
				t.Fatalf("consumer refused an entry the intake wrote: %v", err)
			}
			if len(sink.batch.rows) != 1 || !sink.batch.sent {
				t.Fatalf("rows=%d sent=%v, want one stored row", len(sink.batch.rows), sink.batch.sent)
			}
			row := sink.batch.rows[0]
			got := map[string]any{
				"org_id_hash": row[0], "event_id": row[1], "name": row[2], "schema_version": row[3],
				"session_id": row[4], "anonymous_user_id": row[5],
			}
			wantCols := map[string]any{
				"org_id_hash": want.Row.OrgIDHash, "event_id": want.Row.EventID, "name": want.Row.Name,
				"schema_version": want.Row.SchemaVersion, "session_id": want.Row.SessionID, "anonymous_user_id": want.Row.AnonymousUserID,
			}
			if !reflect.DeepEqual(got, wantCols) {
				t.Fatalf("stored columns %v, Python stored %v", got, wantCols)
			}
			if route, _ := row[6].(*string); (route == nil) != (want.Row.RoutePattern == nil) || (route != nil && *route != *want.Row.RoutePattern) {
				t.Fatalf("route_pattern %v, Python stored %v", row[6], want.Row.RoutePattern)
			}
			if occurred := row[8].(interface{ Format(string) string }).Format("2006-01-02T15:04:05.000000"); occurred != want.Row.OccurredAt {
				t.Fatalf("occurred_at %s, Python stored %s", occurred, want.Row.OccurredAt)
			}
			for kind, table := range map[string]map[string]struct{ goText, pythonText string }{"named divergence": payloadNamedDivergences} {
				if pinned, listed := table[name]; listed {
					if row[7].(string) != pinned.goText || want.Row.PayloadJSON != pinned.pythonText {
						t.Fatalf("%s changed: Go %s (pinned %s), Python %s (pinned %s)", kind, row[7], pinned.goText, want.Row.PayloadJSON, pinned.pythonText)
					}
					return
				}
			}
			if row[7].(string) != want.Row.PayloadJSON {
				t.Fatalf("payload_json %s, Python stored %s", row[7], want.Row.PayloadJSON)
			}
		})
	}
	if stored < 20 {
		t.Fatalf("corpus holds %d stored shapes, want at least 20: the guard would walk too little", stored)
	}
}

// A required field that is missing or null is still refused, as pydantic refused it: only the empty
// string became acceptable.
func TestConsumerStillRefusesAMissingOrNullRequiredField(t *testing.T) {
	good := map[string]any{"name": "page_viewed", "schemaVersion": "1", "eventId": "e1", "ts": "2026-09-23T02:00:00Z", "sessionId": "s", "anonymousUserId": "a", "payload": map[string]any{}}
	for _, field := range []string{"schemaVersion", "eventId", "ts", "sessionId", "anonymousUserId", "payload"} {
		for _, mode := range []string{"missing", "null"} {
			event := map[string]any{}
			for key, value := range good {
				event[key] = value
			}
			if mode == "missing" {
				delete(event, field)
			} else {
				event[field] = nil
			}
			raw, _ := json.Marshal([]any{event})
			handler, err := NewProductTelemetryHandler(&productSink{batch: &productBatch{}})
			if err != nil {
				t.Fatal(err)
			}
			err = handler.Handle(context.Background(), streamrunner.Message{Stream: "product-telemetry:h:events", ID: "1-0", Fields: map[string]string{"events": string(raw)}})
			if !streamrunner.IsPermanent(err) {
				t.Fatalf("%s %s: err = %v, want a permanent refusal", field, mode, err)
			}
		}
	}
}

// handBuiltEntry is the stream entry of a writer other than this intake: the entry of a valid empty
// event from the intake with its events text replaced by the corpus event.
func handBuiltEntry(t *testing.T, raw map[string]any) streamrunner.Message {
	t.Helper()
	code, base := intakeRawBody(t, batchWithPayload(`{}`))
	if code != http.StatusAccepted || base.Fields == nil {
		t.Fatalf("intake did not accept the base event: status %d", code)
	}
	events, err := json.Marshal([]any{raw})
	if err != nil {
		t.Fatal(err)
	}
	fields := map[string]string{}
	for key, value := range base.Fields {
		fields[key] = value
	}
	fields["events"] = string(events)
	return streamrunner.Message{Stream: base.Stream, ID: base.ID, Fields: fields}
}

// An org hash of the shape the web client sends and a route pattern stay stored exactly as Python stored
// them: the corpus case with the old value h1 can no longer pass the intake.
func TestConsumerStoresAnIntakeAcceptedOrgHashAndRoute(t *testing.T) {
	const hash = "fnv1a-0123abcd"
	entry := intakeEntryFor(t, map[string]any{
		"anonymousUserId": "a", "eventId": "e1", "name": "page_viewed", "orgIdHash": hash, "payload": map[string]any{},
		"routePattern": "/a/[id]", "schemaVersion": "1", "sessionId": "s", "ts": "2026-09-23T02:00:00Z",
	})
	sink := &productSink{batch: &productBatch{}}
	handler, err := NewProductTelemetryHandler(sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Handle(context.Background(), entry); err != nil || len(sink.batch.rows) != 1 {
		t.Fatalf("handler error %v, rows %d, want one stored row", err, len(sink.batch.rows))
	}
	row := sink.batch.rows[0]
	if row[0] != hash {
		t.Fatalf("org_id_hash %v, want %s", row[0], hash)
	}
	if route, _ := row[6].(*string); route == nil || *route != "/a/[id]" {
		t.Fatalf("route_pattern %v, want /a/[id]", row[6])
	}
}

// intakeEntryFor posts one corpus event to the real intake route and returns the exact stream entry it
// writes.
func intakeEntryFor(t *testing.T, raw map[string]any) streamrunner.Message {
	t.Helper()
	body, err := json.Marshal(map[string]any{"events": []any{raw}})
	if err != nil {
		t.Fatal(err)
	}
	streams := &parityStreams{}
	route := producttelemetry.Routes(streams, slog.New(slog.NewTextHandler(io.Discard, nil)))[0]
	request := httptest.NewRequest(http.MethodPost, route.Pattern, strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	route.Handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted || streams.fields == nil {
		t.Fatalf("intake did not accept the event: status %d fields %v", recorder.Code, streams.fields)
	}
	return streamrunner.Message{Stream: "product-telemetry:h:events", ID: "1-0", Fields: streams.fields}
}

// The consumer stores a timestamp only where the ClickHouse Go driver encodes it correctly (int64
// nanoseconds, 1677-09-21 to 2262-04-11); outside it the stored time would silently differ (the
// reviewer saw year 2500 stored as 1915). Inside it, the time handed to ClickHouse is the UTC instant sent.
func TestConsumerStoresTimestampsOnlyInsideTheDriverRange(t *testing.T) {
	base := map[string]any{"name": "page_viewed", "schemaVersion": "1", "eventId": "e1", "sessionId": "s", "anonymousUserId": "a", "payload": map[string]any{}}
	for ts, want := range map[string]string{
		"1900-01-01T00:00:00":         "1900-01-01T00:00:00",
		"1899-12-31T00:00:00Z":        "1899-12-31T00:00:00",
		"1678-01-01T00:00:00Z":        "1678-01-01T00:00:00",
		"2262-04-11T23:47:16Z":        "2262-04-11T23:47:16",
		"2262-04-11T23:47:16.854775Z": "2262-04-11T23:47:16",
		"2026-09-23T02:00:00+05:30":   "2026-09-22T20:30:00",
	} {
		t.Run("stored/"+ts, func(t *testing.T) {
			event := map[string]any{"ts": ts}
			for k, v := range base {
				event[k] = v
			}
			sink := &productSink{batch: &productBatch{}}
			handler, err := NewProductTelemetryHandler(sink)
			if err != nil {
				t.Fatal(err)
			}
			if err := handler.Handle(context.Background(), intakeEntryFor(t, event)); err != nil {
				t.Fatalf("refused a timestamp inside the driver range: %v", err)
			}
			if got := sink.batch.rows[0][8].(time.Time).Format("2006-01-02T15:04:05"); got != want {
				t.Fatalf("handed %s to ClickHouse, want %s", got, want)
			}
		})
	}
	for _, ts := range []string{"2262-04-11T23:47:17Z", "2300-01-01T00:00:00Z", "2500-01-01T00:00:00Z", "9999-12-31T23:59:59Z", "1677-09-21T00:12:43Z", "1600-01-01T00:00:00Z", "0001-01-01T00:00:00"} {
		t.Run("refused/"+ts, func(t *testing.T) {
			event := map[string]any{"ts": ts}
			for k, v := range base {
				event[k] = v
			}
			handler, err := NewProductTelemetryHandler(&productSink{batch: &productBatch{}})
			if err != nil {
				t.Fatal(err)
			}
			if err := handler.Handle(context.Background(), intakeEntryFor(t, event)); !streamrunner.IsPermanent(err) {
				t.Fatalf("a timestamp outside the driver range: err = %v, want a permanent refusal", err)
			}
		})
	}
}
