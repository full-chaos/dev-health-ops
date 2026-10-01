package streamhandlers

import (
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
// stored value is the same JSON: the Go consumer re-encodes with encoding/json (a float's ".0" is
// dropped, non-ASCII is kept raw, <, > and & are escaped). They are accepted divergences: the only
// reader of the column (queryapi/producttelemetry) parses it with JSONExtract*, which reads both
// spellings as the same value. Pinned so a change on either side fails this test.
var payloadNamedDivergences = map[string]struct{ goText, pythonText string }{
	"payload_float_integral": {`{"f":100}`, `{"f":100.0}`},
	"payload_unicode":        {`{"s":"héllo ☃ \u003c\u0026\u003e"}`, `{"s":"h\u00e9llo \u2603 <&>"}`},
}

// payloadKnownDefects are shapes where the Go consumer stores a DIFFERENT VALUE than Python: it decodes
// every payload number through float64, so an integer above 2^53 changes. This is a defect, pinned at
// its current wrong output so the fix flips this entry to the exact Python text (KNOWN DEFECT, ticket
// CHAOS-7472).
var payloadKnownDefects = map[string]struct{ goText, pythonText string }{
	"payload_big_int": {`{"i":9007199254740992}`, `{"i":9007199254740993}`},
}

// refusedBelowClickHouseMin are corpus shapes whose Python row holds the timestamp but whose Python
// insert raised (ValueError, year 0001), so the Python consumer dead-lettered them: the Go consumer
// refuses them as well, because its driver would store 1970 silently.
var refusedBelowClickHouseMin = map[string]struct{}{"ts_year1": {}}

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
	if err := json.Unmarshal(data, &corpus); err != nil {
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
			message := intakeEntryFor(t, want.Raw)
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
			for kind, table := range map[string]map[string]struct{ goText, pythonText string }{"named divergence": payloadNamedDivergences, "KNOWN DEFECT": payloadKnownDefects} {
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
