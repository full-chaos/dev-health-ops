package categorize

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func captureLLMCallLines(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buf
}

func llmCallRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		if record["msg"] == "llm.call" {
			records = append(records, record)
		}
	}
	return records
}

func requireLLMCallRecord(t *testing.T, record map[string]any, want map[string]any) {
	t.Helper()
	if record["level"] != "INFO" {
		t.Fatalf("level = %v, want INFO", record["level"])
	}
	for key, value := range want {
		if record[key] != value {
			t.Fatalf("%s = %v, want %v (record %v)", key, record[key], value, record)
		}
	}
}

func TestSharedProviderLogsOneLLMCallLineWithTheFormatPathOfTheResult(t *testing.T) {
	buf := captureLLMCallLines(t)
	wire := newWireModels(t)
	shared, err := NewSharedProvider(SharedProviderConfig{Kind: ProviderKindOpenAI, APIKey: secrets.NewHidden("test-key-value"), BaseURL: wire.server.URL, Model: "gpt-5-nano"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := shared.Complete(context.Background(), CategorizationRequest("SECRET_PROMPT_SENTINEL"))
	if err != nil {
		t.Fatal(err)
	}
	if result.FormatPath != "schema" {
		t.Fatalf("result.FormatPath = %q, want schema copied from the shared result", result.FormatPath)
	}
	records := llmCallRecords(t, buf)
	if len(records) != 1 {
		t.Fatalf("llm.call lines = %d, want 1; log:\n%s", len(records), buf.String())
	}
	requireLLMCallRecord(t, records[0], map[string]any{
		"model": "gpt-5-nano", "input_tokens": float64(3), "output_tokens": float64(2),
		"format_path": "schema", "format_reason": "",
	})
	if strings.Contains(buf.String(), "SECRET_PROMPT_SENTINEL") || strings.Contains(buf.String(), `{\"ok\":true}`) {
		t.Fatalf("log carries prompt or output text:\n%s", buf.String())
	}
}

func TestSharedProviderFailedCallLogsNoLLMCallLine(t *testing.T) {
	buf := captureLLMCallLines(t)
	server, _ := statusOnce(t, http.StatusUnauthorized, `{"error":{"message":"bad key"}}`)
	shared, err := NewSharedProvider(SharedProviderConfig{Kind: ProviderKindOpenAI, APIKey: secrets.NewHidden("test-key-value"), BaseURL: server.URL, Model: "gpt-5-nano"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := shared.Complete(context.Background(), CategorizationRequest("p")); err == nil {
		t.Fatal("want an error")
	}
	if records := llmCallRecords(t, buf); len(records) != 0 {
		t.Fatalf("llm.call lines = %d, want 0 for a failed call", len(records))
	}
}

func TestCategorizeWithRepairLogsExactlyOneLineForEachProviderCall(t *testing.T) {
	buf := captureLLMCallLines(t)
	wire := newWireModels(t)
	shared, err := NewSharedProvider(SharedProviderConfig{Kind: ProviderKindOpenAI, APIKey: secrets.NewHidden("test-key-value"), BaseURL: wire.server.URL, Model: "gpt-5-nano"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CategorizeTextBundle(context.Background(), seamBundle(), CategorizeOptions{Provider: shared}); err != nil {
		t.Fatal(err)
	}
	if len(wire.sent()) != 2 {
		t.Fatalf("provider calls = %d, want 2 (first call and repair call)", len(wire.sent()))
	}
	if records := llmCallRecords(t, buf); len(records) != 2 {
		t.Fatalf("llm.call lines = %d, want 2; log:\n%s", len(records), buf.String())
	}
}

func TestNoneAndMockProvidersLogNoLLMCallLine(t *testing.T) {
	for name, provider := range map[string]Provider{"none": NoneProvider{}, "mock": MockProvider{}} {
		t.Run(name, func(t *testing.T) {
			buf := captureLLMCallLines(t)
			if _, err := CategorizeTextBundle(context.Background(), seamBundle(), CategorizeOptions{Provider: provider}); err != nil {
				t.Fatal(err)
			}
			if records := llmCallRecords(t, buf); len(records) != 0 {
				t.Fatalf("llm.call lines = %d, want 0: no provider request is sent", len(records))
			}
		})
	}
}

func TestOllamaProviderLogsTheSchemaFormatPath(t *testing.T) {
	buf := captureLLMCallLines(t)
	provider, _ := newTestOllamaProvider(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"model":"gemma3","message":{"role":"assistant","content":"{\"ok\":true}"},"done":true,"prompt_eval_count":7,"eval_count":5}`))
	})
	result, err := provider.Complete(context.Background(), CategorizationRequest("SECRET_PROMPT_SENTINEL"))
	if err != nil {
		t.Fatal(err)
	}
	if result.FormatPath != "schema" {
		t.Fatalf("result.FormatPath = %q, want schema", result.FormatPath)
	}
	records := llmCallRecords(t, buf)
	if len(records) != 1 {
		t.Fatalf("llm.call lines = %d, want 1; log:\n%s", len(records), buf.String())
	}
	requireLLMCallRecord(t, records[0], map[string]any{
		"model": "gemma3", "input_tokens": float64(7), "output_tokens": float64(5), "format_path": "schema",
	})
}

func TestOllamaProviderLogsNoFormatPathWhenTheSchemaIsDropped(t *testing.T) {
	buf := captureLLMCallLines(t)
	calls := 0
	provider, _ := newTestOllamaProvider(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			http.Error(w, "format unsupported", http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"model":"gemma3","message":{"role":"assistant","content":"{}"},"done":true,"prompt_eval_count":1,"eval_count":1}`))
	})
	if _, err := provider.Complete(context.Background(), CategorizationRequest("p")); err != nil {
		t.Fatal(err)
	}
	records := llmCallRecords(t, buf)
	if len(records) != 1 {
		t.Fatalf("llm.call lines = %d, want 1", len(records))
	}
	requireLLMCallRecord(t, records[0], map[string]any{"format_path": "", "format_reason": "schema_rejected"})
}
