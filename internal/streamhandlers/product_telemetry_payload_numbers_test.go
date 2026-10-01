package streamhandlers

import (
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

func payloadEntry(payload string) string {
	return `[{"name":"page_viewed","schemaVersion":"1","eventId":"e","ts":"2026-09-23T02:00:00Z","sessionId":"s","anonymousUserId":"a","payload":` + payload + `}]`
}

func storedPayload(t *testing.T, payload string) (string, error) {
	t.Helper()
	sink := &productSink{batch: &productBatch{}}
	handler, err := NewProductTelemetryHandler(sink)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Handle(context.Background(), streamrunner.Message{Stream: "product-telemetry:h:events", ID: "1-0", Fields: map[string]string{"events": payloadEntry(payload)}})
	if err != nil {
		return "", err
	}
	return sink.batch.rows[0][7].(string), nil
}

// A payload number is stored as the text the producer wrote, whatever its size.
func TestPayloadNumbersAreStoredExactly(t *testing.T) {
	huge := strings.Repeat("9", 4301)
	for name, tc := range map[string]struct{ in, want string }{
		"above 2^53":        {`{"i":9007199254740993}`, `{"i":9007199254740993}`},
		"below -2^53":       {`{"i":-9007199254740993}`, `{"i":-9007199254740993}`},
		"int64 max":         {`{"i":9223372036854775807}`, `{"i":9223372036854775807}`},
		"beyond int64":      {`{"i":123456789012345678901234567890}`, `{"i":123456789012345678901234567890}`},
		"4301 digit int":    {`{"i":` + huge + `}`, `{"i":` + huge + `}`},
		"float keeps .0":    {`{"f":100.0}`, `{"f":100.0}`},
		"small":             {`{"f":0.1}`, `{"f":0.1}`},
		"negative zero":     {`{"f":-0.0}`, `{"f":-0.0}`},
		"keys sorted":       {`{"b":2,"a":1}`, `{"a":1,"b":2}`},
		"scalars unchanged": {`{"s":"x","t":true,"n":null}`, `{"n":null,"s":"x","t":true}`},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := storedPayload(t, tc.in)
			if err != nil || got != tc.want {
				t.Fatalf("stored %q (err %v), want %q", got, err, tc.want)
			}
		})
	}
}

// A number that does not fit a float64 as a float is still refused, as before; an integer of any
// length is not a float and is kept.
func TestPayloadFloatOverflowIsStillRefused(t *testing.T) {
	for name, payload := range map[string]string{
		"exponent overflow":        `{"f":1e999}`,
		"negative exponent over":   `{"f":-1e999}`,
		"fraction with 400 digits": `{"f":1.` + strings.Repeat("0", 400) + `e999}`,
		"nested overflow":          `{"k":{"a":1e999}}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := storedPayload(t, payload); !streamrunner.IsPermanent(err) {
				t.Fatalf("err = %v, want a permanent refusal", err)
			}
		})
	}
}
