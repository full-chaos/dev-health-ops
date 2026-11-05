package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func sortedStrings(in []string) []string {
	out := append([]string(nil), in...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// A model that refuses two bounded parameters passes once both are dropped.
func TestReadinessProbeDropsEveryRefusedParam(t *testing.T) {
	f := newParamFakeOpenAI(t, map[string]string{
		"reasoning_effort":    paramRefusal("reasoning_effort", "unsupported_value"),
		"parallel_tool_calls": paramRefusal("parallel_tool_calls", "unsupported_parameter"),
	})
	res := detailProberOver(nil).probeDetail(context.Background(), "openai", "gpt-5-mini", f.server.URL, "test-key")
	wantReady(t, res.Outcome, res.SafeErrorCode)
	if got := sortedStrings(res.DroppedParams); !reflect.DeepEqual(got, []string{"parallel_tool_calls", "reasoning_effort"}) {
		t.Fatalf("DroppedParams = %v, want both refused parameters", res.DroppedParams)
	}
}

// Three refused parameters in a row, then accepted: the loop is not a single retry.
func TestReadinessProbeDropsThreeRefusedParamsInOneRound(t *testing.T) {
	f := newParamFakeOpenAI(t, map[string]string{
		"temperature":           paramRefusal("temperature", "unsupported_value"),
		"parallel_tool_calls":   paramRefusal("parallel_tool_calls", "unsupported_parameter"),
		"max_completion_tokens": paramRefusal("max_completion_tokens", "unsupported_parameter"),
	})
	res := detailProberOver(nil).probeDetail(context.Background(), "openai", "future-model-x", f.server.URL, "test-key")
	wantReady(t, res.Outcome, res.SafeErrorCode)
	if len(res.DroppedParams) != 3 {
		t.Fatalf("DroppedParams = %v, want 3", res.DroppedParams)
	}
}

// The catalog knows gpt-6-luna accepts reasoning_effort none only: the first request sends none and is not refused.
func TestReadinessProbeGPT6LunaFirstRequestSendsNone(t *testing.T) {
	f := newParamFakeOpenAI(t, nil)
	res := detailProberOver(nil).probeDetail(context.Background(), "openai", "gpt-6-luna", f.server.URL, "test-key")
	wantReady(t, res.Outcome, res.SafeErrorCode)
	if calls := f.snapshot(); len(calls) != 2 {
		t.Fatalf("provider requests = %d, want 2 (no refused request)", len(calls))
	}
	if n := f.countKey("reasoning_effort", false); n != 1 {
		t.Fatalf("gpt-6-luna round 1 requests carrying reasoning_effort = %d, want 1", n)
	}
	for _, c := range f.snapshot() {
		if c.body["reasoning_effort"] != "none" {
			t.Fatalf("reasoning_effort = %v, want none", c.body["reasoning_effort"])
		}
	}
	if len(res.DroppedParams) != 0 {
		t.Fatalf("DroppedParams = %v, want none", res.DroppedParams)
	}
}

// The failure answer carries the provider's error text, not only the code.
func TestReadinessFailureAnswerCarriesProviderErrorText(t *testing.T) {
	f := newParamFakeOpenAI(t, map[string]string{"tools": paramRefusal("tools", "unsupported_parameter")})
	res := detailProberOver(nil).probeDetail(context.Background(), "openai", "future-model-x", f.server.URL, "test-key")
	wantInvalidRequest(t, res.Outcome, res.SafeErrorCode)
	raw, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if msg, _ := fields["ErrorMessage"].(string); !strings.Contains(msg, "Unsupported value: 'tools'") {
		t.Fatalf("probe result carries no provider error text: %s", raw)
	}
	var record agentReadinessRecord
	recordJSON := `{"outcome":"failed","safe_error_code":"invalid_request","error_param":"tools","error_code":"unsupported_parameter","error_message":"Unsupported value: 'tools' is not allowed"}`
	if err := json.Unmarshal([]byte(recordJSON), &record); err != nil {
		t.Fatal(err)
	}
	got := readinessFailureMessage(&record)
	if !strings.Contains(got, "unsupported_parameter") || !strings.Contains(got, "is not allowed") {
		t.Fatalf("failure message = %q, want the provider error code and text", got)
	}
}

// A failed probe logs exactly one WARN line: provider, model, dropped params, provider error, trace id; no key.
func TestReadinessFailureLogsOneWarnLine(t *testing.T) {
	const key = "canary-key-for-warn-test"
	f := newParamFakeOpenAI(t, map[string]string{
		"temperature": paramRefusal("temperature", "unsupported_value"),
		"tools":       paramRefusal("tools", "unsupported_parameter"),
	})
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	prober := detailProberOver(logger)
	res := prober.probeDetail(context.Background(), "openai", "future-model-x", f.server.URL, key)
	wantInvalidRequest(t, res.Outcome, res.SafeErrorCode)
	var warns []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatal(err)
		}
		if entry["level"] == "WARN" {
			warns = append(warns, entry)
		}
	}
	if len(warns) != 1 {
		t.Fatalf("WARN lines = %d, want 1:\n%s", len(warns), buf.String())
	}
	w := warns[0]
	if w["provider"] != "openai" || w["model"] != "future-model-x" || w["error_code"] != "unsupported_parameter" {
		t.Fatalf("WARN fields = %v", w)
	}
	if msg, _ := w["error_message"].(string); !strings.Contains(msg, "Unsupported value: 'tools'") {
		t.Fatalf("WARN error_message = %q", msg)
	}
	if !strings.Contains(buf.String(), "temperature") {
		t.Fatalf("WARN line names no dropped parameter: %v", w)
	}
	if strings.Contains(buf.String(), key) || strings.Contains(buf.String(), f.server.URL) {
		t.Fatalf("log carries the key or the base URL: %s", buf.String())
	}
}

func stringAttr(attrs []attribute.KeyValue, key string) (string, bool) {
	for _, kv := range attrs {
		if string(kv.Key) == key {
			return kv.Value.Emit(), true
		}
	}
	return "", false
}

// The probe span carries provider, model, rounds, dropped parameters and outcome; each provider round is a child span.
func TestReadinessProbeEmitsSpanWithRoundChildren(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(previous) })

	f := newParamFakeOpenAI(t, map[string]string{"temperature": paramRefusal("temperature", "unsupported_value")})
	res := detailProberOver(nil).probeDetail(context.Background(), "openai", "future-model-x", f.server.URL, "test-key")
	wantReady(t, res.Outcome, res.SafeErrorCode)

	spans := exporter.GetSpans()
	if len(spans) != 3 {
		t.Fatalf("spans = %d, want 3 (probe + 2 rounds)", len(spans))
	}
	var parent *tracetest.SpanStub
	children := 0
	for i := range spans {
		if !spans[i].Parent.IsValid() {
			parent = &spans[i]
		} else {
			children++
		}
	}
	if parent == nil || children != 2 {
		t.Fatalf("want one root span and two children, got parent=%v children=%d", parent != nil, children)
	}
	for key, want := range map[string]string{"llm.provider": "openai", "llm.model": "future-model-x", "llm.probe.outcome": "ready"} {
		if got, ok := stringAttr(parent.Attributes, key); !ok || got != want {
			t.Errorf("probe span %s = %q (present %v), want %q", key, got, ok, want)
		}
	}
	if got, _ := stringAttr(parent.Attributes, "llm.probe.dropped_params"); !strings.Contains(got, "temperature") {
		t.Errorf("probe span dropped params = %q, want temperature", got)
	}
	if got, _ := stringAttr(parent.Attributes, "llm.probe.rounds"); got != "2" {
		t.Errorf("probe span rounds = %q, want 2", got)
	}
}
