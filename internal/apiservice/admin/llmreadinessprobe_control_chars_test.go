package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"unicode"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// controlRunes is C0 (U+0000-U+001F), DEL and the 32 C1 code points (U+0080-U+009F).
func controlRunes() []rune {
	var out []rune
	for r := rune(0); r <= 0x9f; r++ {
		if r < 0x20 || r >= 0x7f {
			out = append(out, r)
		}
	}
	return out
}

func hasControl(s string) bool {
	return strings.IndexFunc(s, unicode.IsControl) >= 0
}

func controlBody(t *testing.T, message, errType, param, code string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"error": map[string]any{
		"message": message, "type": errType, "param": param, "code": code,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestControlRunesCoverC0DelAndAllC1(t *testing.T) {
	if got := len(controlRunes()); got != 32+1+32 {
		t.Fatalf("control rune count = %d, want 65", got)
	}
}

func TestPlainProviderTextReplacesEveryControlRune(t *testing.T) {
	for _, r := range controlRunes() {
		if got := plainProviderText("a" + string(r) + "b"); got != "a b" {
			t.Errorf("plainProviderText(U+%04X) = %q, want %q", r, got, "a b")
		}
	}
}

func TestReadinessFailureMessageHasNoControlRune(t *testing.T) {
	invalid := errCodeInvalidRequest
	for _, r := range controlRunes() {
		record := agentReadinessRecord{SafeErrorCode: &invalid, ErrorMessage: "a" + string(r) + "b"}
		got := readinessFailureMessage(&record)
		if hasControl(got) || !strings.HasSuffix(got, ": a b") {
			t.Errorf("status message for U+%04X = %q", r, got)
		}
	}
}

func TestProviderControlRunesReachNoSink(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(previous) })

	for _, r := range controlRunes() {
		t.Run(fmt.Sprintf("U+%04X", r), func(t *testing.T) {
			exporter.Reset()
			c := string(r)
			body := controlBody(t, "a"+c+"b", "t"+c+"y", "p"+c+"q", "c"+c+"d")
			f := newParamFakeOpenAI(t, map[string]string{"tools": body})
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
			res := detailProberOver(logger).probeDetail(context.Background(), "openai", "future-model-x", f.server.URL, "test-key")
			wantInvalidRequest(t, res.Outcome, res.SafeErrorCode)

			if res.ErrorMessage != "a b" {
				t.Errorf("result ErrorMessage = %q, want %q", res.ErrorMessage, "a b")
			}
			if hasControl(res.ErrorCode) || hasControl(res.ErrorParam) {
				t.Errorf("result code/param carry a control rune: %q %q", res.ErrorCode, res.ErrorParam)
			}
			record := agentReadinessRecord{SafeErrorCode: res.SafeErrorCode, ErrorMessage: res.ErrorMessage, ErrorCode: res.ErrorCode, ErrorParam: res.ErrorParam}
			if msg := readinessFailureMessage(&record); hasControl(msg) {
				t.Errorf("status message carries a control rune: %q", msg)
			}

			seen := map[string]int{}
			for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
				var entry map[string]any
				if err := json.Unmarshal([]byte(line), &entry); err != nil {
					t.Fatal(err)
				}
				level, _ := entry["level"].(string)
				seen[level]++
				for field, v := range entry {
					if s, ok := v.(string); ok && hasControl(s) {
						t.Errorf("%s log field %s carries a control rune: %q", level, field, s)
					}
				}
				if msg, ok := entry["error_message"].(string); ok && msg != "a b" {
					t.Errorf("%s log error_message = %q, want %q", level, msg, "a b")
				}
			}
			if seen["INFO"] != 1 || seen["WARN"] != 1 {
				t.Errorf("log lines = %v, want one INFO and one WARN", seen)
			}

			for _, span := range exporter.GetSpans() {
				if hasControl(span.Status.Description) {
					t.Errorf("span %s status description carries a control rune: %q", span.Name, span.Status.Description)
				}
				for _, kv := range span.Attributes {
					if hasControl(kv.Value.Emit()) {
						t.Errorf("span %s attribute %s carries a control rune", span.Name, kv.Key)
					}
				}
				for _, ev := range span.Events {
					if hasControl(ev.Name) {
						t.Errorf("span %s event name carries a control rune", span.Name)
					}
					for _, kv := range ev.Attributes {
						if hasControl(kv.Value.Emit()) {
							t.Errorf("span %s event attribute %s carries a control rune", span.Name, kv.Key)
						}
					}
				}
			}
		})
	}
}
