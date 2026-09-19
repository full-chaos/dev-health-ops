package logging_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// TestProviderErrorAndJSONBytesInEveryStructuredCarrier: a ProviderError
// logged directly, inside a map, or as a struct value, and JSON held as
// bytes or as json.RawMessage, never put the provider body or a protected
// value in the line.
func TestProviderErrorAndJSONBytesInEveryStructuredCarrier(t *testing.T) {
	raw := []byte(`{"\u0074oken":"CANARY_BYTE_SECRET","status":400}`)
	e := &providerfoundation.ProviderError{Class: providerfoundation.ErrorPermanent, StatusCode: 400, Path: "/repos/a/b", Body: `{"message":"CANARY_PROVIDER_CONTENT"}`}
	for _, c := range []struct {
		name  string
		value any
	}{
		{"bytes", raw}, {"raw_message_control", json.RawMessage(raw)},
		{"direct_error_control", e}, {"nested_error", map[string]any{"cause": e}}, {"value_error", *e},
	} {
		t.Run(c.name, func(t *testing.T) {
			var b bytes.Buffer
			logging.NewJSON(&b, slog.LevelInfo).Info("review", "value", c.value)
			t.Log(b.String())
			if !json.Valid(bytes.TrimSpace(b.Bytes())) {
				t.Fatal("invalid log JSON")
			}
			if strings.Contains(b.String(), "CANARY_") {
				t.Fatal("protected value or provider body reached log")
			}
		})
	}
}
