package streamhandlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

// pythonProducedEvents is the `events` field text the Python producer writes for a batch
// whose first event carries non-finite payload numbers and whose second is entirely
// ordinary. It is the output of the producer's own expression
// (api/product_telemetry/streams.py: json.dumps([event.model_dump(mode="json",
// by_alias=True) ...])) executed against ProductTelemetryEvent.model_validate with payload
// {"x": nan, "y": inf, "w": -inf, "z": 1.5}, not hand-written JSON.
const pythonProducedEvents = `[{"name": "page_viewed", "schemaVersion": "1", "eventId": "nonfinite", "ts": "2026-01-01T00:00:00Z", "sessionId": "s", "anonymousUserId": "a", "orgIdHash": null, "routePattern": null, "payload": {"x": NaN, "y": Infinity, "w": -Infinity, "z": 1.5}}, {"name": "feature_viewed", "schemaVersion": "1", "eventId": "sibling", "ts": "2026-01-01T00:00:01Z", "sessionId": "s", "anonymousUserId": "a", "orgIdHash": null, "routePattern": "/home", "payload": {"feature": "home"}}]`

func handleEvents(t *testing.T, events string) (*productSink, error) {
	t.Helper()
	sink := &productSink{batch: &productBatch{}}
	handler, err := NewProductTelemetryHandler(sink)
	if err != nil {
		t.Fatal(err)
	}
	return sink, handler.Handle(context.Background(), streamrunner.Message{Fields: map[string]string{"events": events}})
}

// CHAOS-6299: an event with non-finite payload numbers no longer quarantines the whole
// entry; it and its valid sibling are persisted.
func TestProductTelemetryPersistsNonFinitePayloadsWithTheirSiblings(t *testing.T) {
	sink, err := handleEvents(t, pythonProducedEvents)
	if err != nil {
		t.Fatalf("Handle: %v (permanent=%v): the entry must not be refused", err, streamrunner.IsPermanent(err))
	}
	if !sink.batch.sent || len(sink.batch.rows) != 2 {
		t.Fatalf("sent=%v rows=%d, want both events persisted", sink.batch.sent, len(sink.batch.rows))
	}
	ids := []any{sink.batch.rows[0][1], sink.batch.rows[1][1]}
	if !reflect.DeepEqual(ids, []any{"nonfinite", "sibling"}) {
		t.Fatalf("event ids = %v", ids)
	}
	const payloadColumn = 7
	// A non-finite number is stored as JSON null: strict JSON the readers' JSONExtract*
	// parse (bare NaN makes ClickHouse's isValidJSON false and every extract the default).
	if got, want := sink.batch.rows[0][payloadColumn], `{"w":null,"x":null,"y":null,"z":1.5}`; got != want {
		t.Fatalf("payload_json = %v, want %v", got, want)
	}
	if got, want := sink.batch.rows[1][payloadColumn], `{"feature":"home"}`; got != want {
		t.Fatalf("sibling payload_json = %v, want %v", got, want)
	}
}

// The refusals that were right stay refusals, each permanent with its own reason.
func TestProductTelemetryStillRefusesWhatIsInvalid(t *testing.T) {
	event := func(name, payload string) string {
		return `[{"name":"` + name + `","schemaVersion":"1","eventId":"e","ts":"2026-01-01T00:00:00Z","sessionId":"s","anonymousUserId":"a","payload":` + payload + `}]`
	}
	// event2 is a valid event whose field `field` carries the non-finite literal instead.
	event2 := func(field, literal string) string {
		values := map[string]string{"name": `"page_viewed"`, "schemaVersion": `"1"`, "eventId": `"e"`, "ts": `"2026-01-01T00:00:00Z"`, "sessionId": `"s"`, "anonymousUserId": `"a"`, "payload": `{}`}
		values[field] = literal
		var parts []string
		for _, key := range []string{"name", "schemaVersion", "eventId", "ts", "sessionId", "anonymousUserId", "payload", "orgIdHash", "routePattern"} {
			if value, ok := values[key]; ok {
				parts = append(parts, `"`+key+`":`+value)
			}
		}
		return "[{" + strings.Join(parts, ",") + "}]"
	}
	cases := map[string]struct{ events, reason string }{
		"truncated document":                   {`[{"name": "page_viewed", "payload": {"x": NaN}`, "invalid_events_json"},
		"garbage":                              {`not json NaN`, "invalid_events_json"},
		"non-finite as a bare top-level":       {`NaN`, "invalid_events_json"},
		"empty list":                           {`[]`, "invalid_event_count"},
		"non-finite event name":                {`[{"name": NaN, "schemaVersion":"1","eventId":"e","ts":"2026-01-01T00:00:00Z","sessionId":"s","anonymousUserId":"a","payload":{}}]`, "invalid_events_json"},
		"non-finite timestamp":                 {`[{"name":"page_viewed","schemaVersion":"1","eventId":"e","ts":Infinity,"sessionId":"s","anonymousUserId":"a","payload":{}}]`, "invalid_events_json"},
		"non-finite org hash (optional field)": {event2("orgIdHash", "NaN"), "invalid_events_json"},
		"non-finite route pattern":             {event2("routePattern", "Infinity"), "invalid_events_json"},
		"non-finite session id":                {event2("sessionId", "-Infinity"), "invalid_events_json"},
		"non-finite next to a blocked key":     {event("page_viewed", `{"x": NaN, "email": "a@example.test"}`), "blocked_telemetry_payload"},
		"non-finite inside a nested payload":   {event("page_viewed", `{"x": [NaN]}`), "invalid_events_json"},
		"non-finite inside an object payload":  {event("page_viewed", `{"x": {"y": Infinity}}`), "invalid_events_json"},
		"unknown event name":                   {event("nope", `{"x": NaN}`), "invalid_telemetry_event"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			before := nonFiniteCounterValue(t)
			sink, err := handleEvents(t, tc.events)
			if moved := nonFiniteCounterValue(t) - before; moved != 0 {
				t.Fatalf("a refused entry moved the nulled-values counter by %d", moved)
			}
			var permanent *streamrunner.PermanentError
			if !streamrunner.IsPermanent(err) {
				t.Fatalf("err = %v, want a permanent %q", err, tc.reason)
			}
			permanent = err.(*streamrunner.PermanentError)
			if permanent.Reason != tc.reason {
				t.Fatalf("reason = %q, want %q", permanent.Reason, tc.reason)
			}
			if len(sink.batch.rows) != 0 && sink.batch.sent {
				t.Fatalf("rows were sent for a refused entry: %v", sink.batch.rows)
			}
		})
	}
	t.Run("more than 500 events with a non-finite one", func(t *testing.T) {
		one := `{"name":"page_viewed","schemaVersion":"1","eventId":"e","ts":"2026-01-01T00:00:00Z","sessionId":"s","anonymousUserId":"a","payload":{"x":NaN}}`
		_, err := handleEvents(t, "["+strings.TrimSuffix(strings.Repeat(one+",", 501), ",")+"]")
		if !streamrunner.IsPermanent(err) || err.(*streamrunner.PermanentError).Reason != "invalid_event_count" {
			t.Fatalf("err = %v, want permanent invalid_event_count", err)
		}
	})
}

// For every finite document, decodeProductEvents reads exactly what encoding/json read
// before this change: the re-encode through Python's scanner may not change a single
// decision (types, key case, duplicate keys, number kinds, escapes, unknown fields, nulls).
func TestDecodeProductEventsAgreesWithEncodingJSONOnFiniteDocuments(t *testing.T) {
	const wrap = `[{"name":"page_viewed","schemaVersion":"1","eventId":"e","ts":"2026-01-01T00:00:00Z","sessionId":"s","anonymousUserId":"a","payload":%s}]`
	docs := map[string]string{
		"ints and floats":       strings.Replace(wrap, "%s", `{"i":3,"f":1.5,"e":1e3,"neg":-2,"zero":0,"big":123456789012345678}`, 1),
		"strings and escapes":   strings.Replace(wrap, "%s", `{"s":"héllo \"q\" \\ \n","u":"😀","html":"<a&b>"}`, 1),
		"bool and null":         strings.Replace(wrap, "%s", `{"t":true,"f":false,"n":null}`, 1),
		"duplicate payload key": strings.Replace(wrap, "%s", `{"k":1,"k":2}`, 1),
		"unknown fields":        `[{"name":"page_viewed","schemaVersion":"1","eventId":"e","ts":"2026-01-01T00:00:00Z","sessionId":"s","anonymousUserId":"a","payload":{},"extra":{"a":[1,2]}}]`,
		"key case":              `[{"NAME":"page_viewed","SchemaVersion":"1","EVENTID":"e","Ts":"2026-01-01T00:00:00Z","sessionid":"s","anonymoususerid":"a","payload":{}}]`,
		"null fields":           `[{"name":"page_viewed","schemaVersion":null,"eventId":"e","ts":"2026-01-01T00:00:00Z","sessionId":"s","anonymousUserId":"a","routePattern":null,"payload":null}]`,
		"route pattern":         `[{"name":"page_viewed","schemaVersion":"1","eventId":"e","ts":"2026-01-01T00:00:00.123456Z","sessionId":"s","anonymousUserId":"a","routePattern":"/x/[id]","payload":{}}]`,
		"empty":                 `[]`,
		"null document":         `null`,
		"whitespace":            "  [ { \"name\" : \"page_viewed\" , \"schemaVersion\":\"1\",\"eventId\":\"e\",\"ts\":\"2026-01-01T00:00:00Z\",\"sessionId\":\"s\",\"anonymousUserId\":\"a\",\"payload\":{ } } ]  \n",
	}
	for name, doc := range docs {
		t.Run(name, func(t *testing.T) {
			var want []productEvent
			wantErr := json.Unmarshal([]byte(doc), &want)
			got, gotErr := decodeProductEventsOnly(doc)
			if (wantErr == nil) != (gotErr == nil) {
				t.Fatalf("error mismatch: encoding/json %v, decodeProductEvents %v", wantErr, gotErr)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("decoded events differ:\n encoding/json:       %#v\n decodeProductEvents: %#v", want, got)
			}
		})
	}
	// A document encoding/json refuses for a reason other than non-finite numbers is
	// still refused.
	for name, doc := range map[string]string{
		"wrong type for name":  `[{"name": 3}]`,
		"object not a list":    `{"name":"page_viewed"}`,
		"trailing garbage":     `[] x`,
		"unterminated string":  `[{"name":"page`,
		"bad timestamp string": `[{"name":"page_viewed","ts":"yesterday"}]`,
	} {
		t.Run("refused/"+name, func(t *testing.T) {
			if _, err := decodeProductEventsOnly(doc); err == nil {
				t.Fatalf("decodeProductEvents accepted %s", doc)
			}
		})
	}
}

// decodeProductEventsOnly is decodeProductEvents without the non-finite count.
func decodeProductEventsOnly(raw string) ([]productEvent, error) {
	events, _, err := decodeProductEvents(raw)
	return events, err
}

func nonFiniteCounterValue(t *testing.T) int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := testMetricReader.Collect(context.Background(), &collected); err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "dev_health_stream_product_telemetry_nonfinite_total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("counter data is %T", m.Data)
			}
			var total int64
			for _, point := range sum.DataPoints {
				total += point.Value
			}
			return total
		}
	}
	return 0
}

// A nulled value is observable: the counter counts every non-finite number and one debug
// line per entry names the entry (never the payload); an ordinary entry counts nothing.
func TestProductTelemetryCountsAndLogsNulledNonFiniteValues(t *testing.T) {
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previous)

	before := nonFiniteCounterValue(t)
	sink := &productSink{batch: &productBatch{}}
	handler, err := NewProductTelemetryHandler(sink)
	if err != nil {
		t.Fatal(err)
	}
	message := streamrunner.Message{Stream: "product-telemetry:org-1:events", ID: "1-0", Fields: map[string]string{"events": pythonProducedEvents}}
	if err := handler.Handle(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if got := nonFiniteCounterValue(t) - before; got != 3 {
		t.Fatalf("counter grew by %d, want 3 (NaN, Infinity, -Infinity)", got)
	}
	out := logged.String()
	if !strings.Contains(out, "non-finite numbers stored as null") || !strings.Contains(out, "count=3") || !strings.Contains(out, "entry_id=1-0") || !strings.Contains(out, "stream=product-telemetry:org-1:events") {
		t.Fatalf("debug line missing or incomplete:\n%s", out)
	}
	if strings.Contains(out, "nonfinite") || strings.Contains(out, "sibling") || strings.Contains(out, `"z"`) {
		t.Fatalf("the debug line carries payload content:\n%s", out)
	}

	before = nonFiniteCounterValue(t)
	logged.Reset()
	sink2 := &productSink{batch: &productBatch{}}
	handler2, _ := NewProductTelemetryHandler(sink2)
	ordinary := `[{"name":"page_viewed","schemaVersion":"1","eventId":"e","ts":"2026-01-01T00:00:00Z","sessionId":"s","anonymousUserId":"a","payload":{"z":1.5}}]`
	if err := handler2.Handle(context.Background(), streamrunner.Message{Fields: map[string]string{"events": ordinary}}); err != nil {
		t.Fatal(err)
	}
	if got := nonFiniteCounterValue(t) - before; got != 0 {
		t.Fatalf("an ordinary entry moved the counter by %d", got)
	}
	if strings.Contains(logged.String(), "non-finite") {
		t.Fatalf("an ordinary entry logged a non-finite line:\n%s", logged.String())
	}
}

// The counter and the log say "stored": they move only for an entry whose whole batch was
// validated and sent (r1 review), never for one refused mid-way or whose write failed.
func TestProductTelemetryCountsNulledValuesOnlyWhenTheEntryIsPersisted(t *testing.T) {
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previous)

	nan := `{"name":"page_viewed","schemaVersion":"1","eventId":"nan","ts":"2026-01-01T00:00:00Z","sessionId":"s","anonymousUserId":"a","payload":{"x":NaN}}`
	invalid := `{"name":"not_a_telemetry_event","schemaVersion":"1","eventId":"bad","ts":"2026-01-01T00:00:00Z","sessionId":"s","anonymousUserId":"a","payload":{}}`
	cases := map[string]struct {
		events  string
		sendErr error
		wantErr func(error) bool
		want    int64
	}{
		"refused after a non-finite event":     {"[" + nan + "," + invalid + "]", nil, streamrunner.IsPermanent, 0},
		"write fails after the batch is built": {"[" + nan + "]", errors.New("clickhouse unavailable"), func(err error) bool { return err != nil && !streamrunner.IsPermanent(err) }, 0},
		"persisted":                            {"[" + nan + "]", nil, func(err error) bool { return err == nil }, 1},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			logged.Reset()
			before := nonFiniteCounterValue(t)
			sink := &productSink{batch: &productBatch{sendErr: tc.sendErr}}
			handler, err := NewProductTelemetryHandler(sink)
			if err != nil {
				t.Fatal(err)
			}
			err = handler.Handle(context.Background(), streamrunner.Message{Fields: map[string]string{"events": tc.events}})
			if !tc.wantErr(err) {
				t.Fatalf("err = %v", err)
			}
			if got := nonFiniteCounterValue(t) - before; got != tc.want {
				t.Fatalf("counter moved by %d, want %d", got, tc.want)
			}
			if said := strings.Contains(logged.String(), "stored as null"); said != (tc.want > 0) {
				t.Fatalf("log says stored=%v, want %v:\n%s", said, tc.want > 0, logged.String())
			}
		})
	}
}
