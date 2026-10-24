package emitscan_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// sentinels are built at run time, so no secret-shaped literal sits in the source. Each one is the random part of a
// value in a shape the shared redactor must recognise; a test fails when the random part reaches an output.
func sentinels(t *testing.T) (secrets map[string]string, shaped map[string]string) {
	t.Helper()
	random := func() string {
		raw := make([]byte, 12)
		if _, err := rand.Read(raw); err != nil {
			t.Fatal(err)
		}
		return hex.EncodeToString(raw)
	}
	secrets = map[string]string{"bearer": random(), "provider": random(), "userinfo": random(), "keyvalue": random()}
	shaped = map[string]string{
		"bearer":   "Authorization: Bearer " + secrets["bearer"],
		"provider": "token ghp_" + secrets["provider"] + "AB",
		"userinfo": "postgres://svc:" + secrets["userinfo"] + "@db.internal/app",
		"keyvalue": "password=" + secrets["keyvalue"],
	}
	return secrets, shaped
}

func assertNone(t *testing.T, where, output string, secrets map[string]string) {
	t.Helper()
	for name, secret := range secrets {
		if strings.Contains(output, secret) {
			t.Errorf("%s: sentinel %q reached the output", where, name)
		}
	}
}

// The real process handler, installed as the default, is the path every logger takes, River's own client logger
// (slog.Default) and the standard log package included.
func TestSentinelsDoNotSurviveTheRealLogChain(t *testing.T) {
	secrets, shaped := sentinels(t)
	var out bytes.Buffer
	restore := logging.InstallDefault(logging.NewJSON(&out, slog.LevelDebug))
	t.Cleanup(restore)

	for _, value := range shaped {
		err := errors.New("upstream said: " + value)
		slog.Error("message "+value, "detail", value, "err", err)
		slog.Default().With("scope", value).Warn("with-bound", slog.Group("g", slog.String("inner", value)))
		slog.Default().Error("river-style job failure", "error", err)
		log.Print("standard log " + value)
	}
	assertNone(t, "log chain", out.String(), secrets)
	if out.Len() == 0 {
		t.Fatal("the log chain wrote nothing")
	}
}

// Error text reaches a span only through ErrorSpanAttributes, which names the class of the error and never its text.
func TestSentinelsDoNotReachASpanThroughErrorAttributes(t *testing.T) {
	secrets, shaped := sentinels(t)
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	_, span := provider.Tracer("emitscan-chain").Start(context.Background(), "job")
	for _, value := range shaped {
		err := fmt.Errorf("call failed: %s: %w", value, errors.New(value))
		span.AddEvent("exception", oteltrace.WithAttributes(logging.ErrorSpanAttributes(err)...))
	}
	span.End()
	var seen strings.Builder
	for _, exported := range exporter.GetSpans() {
		for _, event := range exported.Events {
			for _, kv := range event.Attributes {
				seen.WriteString(string(kv.Key) + "=" + kv.Value.Emit() + "\n")
			}
		}
	}
	assertNone(t, "span events", seen.String(), secrets)
	if seen.Len() == 0 {
		t.Fatal("no span event attribute was exported")
	}
}

// Control: the same sentinels through a handler with no redactor DO reach the output, so the assertions above cannot pass
// by accident of the sentinel shapes.
func TestSentinelShapesAreDetectableWithoutTheRedactor(t *testing.T) {
	secrets, shaped := sentinels(t)
	var out bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&out, nil))
	for _, value := range shaped {
		logger.Error("x", "detail", value)
	}
	for name, secret := range secrets {
		if !strings.Contains(out.String(), secret) {
			t.Errorf("control: sentinel %q was not visible without the redactor", name)
		}
	}
}
