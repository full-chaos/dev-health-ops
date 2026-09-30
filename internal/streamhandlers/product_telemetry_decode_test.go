package streamhandlers

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

// Round 3 of the non-finite change found that the decoder, not the non-finite rule, had
// widened what the consumer accepts: re-encoding the entry through an intermediate object
// collapsed duplicate keys, refused ignored unknown fields, and repaired lone surrogates.
// The decoder now rewrites text, so for a document with no non-finite word in a payload
// value position it must be byte-for-byte the decision encoding/json makes. Each case
// below fails on the intermediate-object decoder (the merged main at 9d74aa6cc) and passes now.
func TestDecodeProductEventsEqualsEncodingJSONWhereNoPayloadValueIsNonFinite(t *testing.T) {
	const fields = `"name":"page_viewed","schemaVersion":"1","eventId":"e","ts":"2026-09-29T00:00:00Z","sessionId":"s","anonymousUserId":"a"`
	wrap := func(tail string) string { return `[{` + fields + `,` + tail + `}]` }
	cases := map[string]string{
		"duplicate case reordering":     `[{"name":"page_viewed","NAME":"feature_viewed",` + strings.Replace(fields, `"name":"page_viewed",`, "", 1) + `,"name":"chart_interacted","payload":{}}]`,
		"duplicate payload merges keys": wrap(`"payload":{"feature":"home"},"payload":{"surface":"nav"}`),
		"duplicate payload hides email": wrap(`"payload":{"email":"a@example.test"},"payload":{"surface":"nav"}`),
		"duplicate name wrong type":     `[{"name":3,` + fields + `,"payload":{}}]`,
		"duplicate ts parse error":      `[{"ts":"yesterday",` + fields + `,"payload":{}}]`,
		"duplicate payload wrong type":  wrap(`"payload":[],"payload":{}`),
		"name then name null":           wrap(`"payload":{},"name":null`),
		"unknown NaN then 1":            wrap(`"payload":{},"extra":NaN,"extra":1`),
		"unknown huge exponent":         wrap(`"payload":{},"extra":1e999`),
		"unknown nested huge exponent":  wrap(`"payload":{},"extra":{"k":1e999}`),
		"unknown array huge exponent":   wrap(`"payload":{},"extra":[1e999]`),
		"unknown huge integer":          wrap(`"payload":{},"extra":` + strings.Repeat("1", 4301)),
		"payload huge exponent":         wrap(`"payload":{"k":1e999}`),
		"lone surrogate event id":       strings.Replace(wrap(`"payload":{}`), `"eventId":"e"`, `"eventId":"\ud800"`, 1),
		"lone surrogate payload value":  wrap(`"payload":{"feature":"\ud800"}`),
		"lone surrogate payload key":    wrap(`"payload":{"\ud800":"home"}`),
		"payload key with escape":       wrap(`"payload":{"k":1}`),
		"payload key other case":        wrap(`"Payload":{"k":1}`),
		"string that holds a bare word": wrap(`"payload":{"k":"NaN","j":"Infinity"}`),
		"escaped quote before a word":   wrap(`"payload":{"k":"a\"NaN"}`),
		"NaN as a nested payload value": wrap(`"payload":{"k":{"a":NaN}}`),
		"NaN in a payload array":        wrap(`"payload":{"k":[NaN]}`),
		"payload itself NaN":            wrap(`"payload":NaN`),
		"NaN as a payload key":          wrap(`"payload":{NaN:1}`),
		"NaN event id":                  strings.Replace(wrap(`"payload":{}`), `"eventId":"e"`, `"eventId":NaN`, 1),
	}
	one := wrap(`"payload":{}`)
	cases["500 events, one with an ignored exponent"] = "[" + strings.TrimSuffix(strings.Repeat(strings.Trim(one, "[]")+",", 499), ",") + "," + strings.Trim(wrap(`"payload":{},"extra":1e999`), "[]") + "]"
	for _, key := range []string{"schemaVersion", "sessionId", "anonymousUserId", "orgIdHash", "routePattern"} {
		cases["lone surrogate "+key] = wrap(`"payload":{},"` + key + `":"\ud800"`)
	}
	for key, good := range map[string]string{"name": `"page_viewed"`, "schemaVersion": `"1"`, "eventId": `"e"`, "ts": `"2026-09-29T00:00:00Z"`, "sessionId": `"s"`, "anonymousUserId": `"a"`, "orgIdHash": `"org"`, "routePattern": `"/home"`} {
		cases["wrong type then "+key] = `[{"` + key + `":3,` + fields + `,"payload":{},"` + key + `":` + good + `}]`
		cases["NaN then "+key] = `[{"` + key + `":NaN,` + fields + `,"payload":{},"` + key + `":` + good + `}]`
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			var want []productEvent
			wantErr := json.Unmarshal([]byte(raw), &want)
			got, nonFinite, gotErr := decodeProductEvents(raw)
			if (wantErr == nil) != (gotErr == nil) {
				t.Fatalf("encoding/json err=%v, decodeProductEvents err=%v", wantErr, gotErr)
			}
			if nonFinite != 0 {
				t.Fatalf("counted %d non-finite values in a document with none in a payload value position", nonFinite)
			}
			if wantErr == nil && !reflect.DeepEqual(got, want) {
				t.Fatalf("decoded events differ:\n encoding/json: %#v\n decoder:       %#v", want, got)
			}
			// Through the handler, too: the same accept/refuse decision and the same stored row.
			sink, err := handleEvents(t, raw)
			if wantErr != nil {
				if err == nil || sink.batch.sent {
					t.Fatalf("encoding/json refused this document, Handle err=%v sent=%v", err, sink.batch.sent)
				}
				return
			}
			_, validateErr := validateProductEvent(want[0])
			if (validateErr == nil) != (err == nil) {
				t.Fatalf("validate err=%v, Handle err=%v", validateErr, err)
			}
		})
	}
}

// The non-finite words that ARE nulled, each site: only a direct value of a key of an
// event's payload object, in every event of the list, in either sign, whatever the
// whitespace, and never a word inside a string or a key.
func TestNullPayloadNonFiniteSites(t *testing.T) {
	cases := []struct {
		name, in, want string
		count          int
	}{
		{"NaN", `[{"payload":{"a":NaN}}]`, `[{"payload":{"a":null}}]`, 1},
		{"Infinity", `[{"payload":{"a":Infinity}}]`, `[{"payload":{"a":null}}]`, 1},
		{"negative Infinity", `[{"payload":{"a":-Infinity}}]`, `[{"payload":{"a":null}}]`, 1},
		{"spaced", "[ { \"payload\" : { \"a\" :\n NaN , \"b\":1 } } ]", "[ { \"payload\" : { \"a\" :\n null , \"b\":1 } } ]", 1},
		{"two events", `[{"payload":{"a":NaN}},{"payload":{"b":Infinity,"c":-Infinity}}]`, `[{"payload":{"a":null}},{"payload":{"b":null,"c":null}}]`, 3},
		{"payload before other fields", `[{"payload":{"a":NaN},"name":"x"}]`, `[{"payload":{"a":null},"name":"x"}]`, 1},
		{"other field before payload", `[{"orgIdHash":"NaN","payload":{"a":NaN}}]`, `[{"orgIdHash":"NaN","payload":{"a":null}}]`, 1},
		{"key spelled like a word", `[{"payload":{"NaN":NaN}}]`, `[{"payload":{"NaN":null}}]`, 1},
		{"word in a string", `[{"payload":{"a":"NaN"}}]`, `[{"payload":{"a":"NaN"}}]`, 0},
		{"not a payload value", `[{"x":NaN,"payload":{}}]`, `[{"x":NaN,"payload":{}}]`, 0},
		{"a sibling object of payload", `[{"extra":{"a":NaN},"payload":{}}]`, `[{"extra":{"a":NaN},"payload":{}}]`, 0},
		{"payload key inside a non-event object", `[{"a":{"payload":{"b":NaN}}}]`, `[{"a":{"payload":{"b":NaN}}}]`, 0},
		{"payload key in a nested list", `[[{"payload":{"b":NaN}}]]`, `[[{"payload":{"b":NaN}}]]`, 0},
		{"escaped payload key", `[{"pay\u006coad":{"k":NaN}}]`, `[{"pay\u006coad":{"k":NaN}}]`, 0},
		{"other-case payload key", `[{"Payload":{"k":NaN}}]`, `[{"Payload":{"k":NaN}}]`, 0},
		{"NaN where a key belongs", `[{"payload":{NaN:1}}]`, `[{"payload":{NaN:1}}]`, 0},
		{"nested object", `[{"payload":{"a":{"b":NaN}}}]`, `[{"payload":{"a":{"b":NaN}}}]`, 0},
		{"nested array", `[{"payload":{"a":[NaN]}}]`, `[{"payload":{"a":[NaN]}}]`, 0},
		{"a longer word", `[{"payload":{"a":NaNx}}]`, `[{"payload":{"a":NaNx}}]`, 0},
		{"second payload key", `[{"payload":{"a":NaN},"payload":{"b":Infinity}}]`, `[{"payload":{"a":null},"payload":{"b":null}}]`, 2},
		{"unterminated", `[{"payload":{"a":NaN`, `[{"payload":{"a":null`, 1},
		{"unterminated string", `[{"payload":{"a":"NaN`, `[{"payload":{"a":"NaN`, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, count := nullPayloadNonFinite(tc.in)
			if got != tc.want || count != tc.count {
				t.Fatalf("nullPayloadNonFinite(%q) = %q, %d; want %q, %d", tc.in, got, count, tc.want, tc.count)
			}
		})
	}
}

// An entry that is refused, or whose write fails, must not hold a ClickHouse connection: a
// batch that was opened and never sent keeps its connection until it is aborted, so four
// refused entries used to exhaust the default pool of four and stall every valid entry behind
// them (r3). Every refusal is checked: none opens a batch, and every failure after one aborts.
func TestProductTelemetryRefusedOrFailedEntryReleasesItsBatch(t *testing.T) {
	ok := `{"name":"page_viewed","schemaVersion":"1","eventId":"e","ts":"2026-01-01T00:00:00Z","sessionId":"s","anonymousUserId":"a","payload":{"k":1}}`
	bad := func(payload string) string {
		return `{"name":"page_viewed","schemaVersion":"1","eventId":"b","ts":"2026-01-01T00:00:00Z","sessionId":"s","anonymousUserId":"a","payload":` + payload + `}`
	}
	refused := map[string]string{
		"blocked key":            "[" + ok + "," + bad(`{"email":"a@example.test"}`) + "]",
		"blocked key beside NaN": "[" + bad(`{"x":NaN,"email":"a@example.test"}`) + "]",
		"nested payload value":   "[" + bad(`{"x":[1]}`) + "]",
		"unknown name":           `[{"name":"nope","schemaVersion":"1","eventId":"e","ts":"2026-01-01T00:00:00Z","sessionId":"s","anonymousUserId":"a","payload":{}}]`,
		"last event invalid":     "[" + ok + "," + ok + `,{"name":"page_viewed"}]`,
	}
	for name, events := range refused {
		t.Run("refused/"+name, func(t *testing.T) {
			batch := &productBatch{}
			sink := &productSink{batch: batch}
			handler, _ := NewProductTelemetryHandler(sink)
			err := handler.Handle(context.Background(), streamrunner.Message{Fields: map[string]string{"events": events}})
			if !streamrunner.IsPermanent(err) {
				t.Fatalf("err = %v, want a permanent refusal", err)
			}
			if len(sink.queries) != 0 && batch.aborts == 0 {
				t.Fatalf("a batch was opened (%d) for a refused entry and never aborted", len(sink.queries))
			}
			if batch.sent {
				t.Fatal("a refused entry was sent")
			}
		})
	}
	t.Run("Send fails", func(t *testing.T) {
		batch := &productBatch{sendErr: errors.New("boom")}
		handler, _ := NewProductTelemetryHandler(&productSink{batch: batch})
		if err := handler.Handle(context.Background(), streamrunner.Message{Fields: map[string]string{"events": "[" + ok + "]"}}); err == nil || streamrunner.IsPermanent(err) {
			t.Fatalf("err = %v, want a retryable error", err)
		}
		if batch.aborts != 1 {
			t.Fatalf("aborts = %d after a failed Send, want 1", batch.aborts)
		}
	})
	t.Run("a sent batch is not aborted", func(t *testing.T) {
		batch := &productBatch{}
		handler, _ := NewProductTelemetryHandler(&productSink{batch: batch})
		if err := handler.Handle(context.Background(), streamrunner.Message{Fields: map[string]string{"events": "[" + ok + "]"}}); err != nil {
			t.Fatal(err)
		}
		if !batch.sent || batch.aborts != 0 {
			t.Fatalf("sent=%v aborts=%d, want sent and never aborted", batch.sent, batch.aborts)
		}
	})
}
