package categorize

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

type formatProvider struct {
	text string
	path string
	why  string
}

func (p *formatProvider) Complete(context.Context, CompletionRequest) (CompletionResult, error) {
	in, out := 100, 20
	return CompletionResult{
		Text: p.text, InputTokens: &in, OutputTokens: &out, Model: "model-returned",
		FormatPath: p.path, FormatReason: p.why,
	}, nil
}
func (p *formatProvider) Close() error  { return nil }
func (p *formatProvider) Model() string { return "model-requested" }

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

func TestEveryLLMCallLogsOneInfoLineWithFormatPathAndReason(t *testing.T) {
	buf := captureLLMCallLines(t)
	provider := &formatProvider{text: seamBadQuoteText, path: "tool", why: "strict_schema_unsupported"}
	if _, err := CategorizeTextBundle(context.Background(), seamBundle(), CategorizeOptions{Provider: provider}); err != nil {
		t.Fatal(err)
	}
	records := llmCallRecords(t, buf)
	if len(records) != 2 {
		t.Fatalf("llm.call lines = %d, want 2 (first call and repair call); log:\n%s", len(records), buf.String())
	}
	for _, record := range records {
		if record["level"] != "INFO" {
			t.Fatalf("level = %v, want INFO", record["level"])
		}
		want := map[string]any{
			"model": "model-returned", "input_tokens": float64(100), "output_tokens": float64(20),
			"format_path": "tool", "format_reason": "strict_schema_unsupported",
		}
		for key, value := range want {
			if record[key] != value {
				t.Fatalf("%s = %v, want %v", key, record[key], value)
			}
		}
	}
	if strings.Contains(buf.String(), "stream the CSV export") || strings.Contains(buf.String(), "this text is in no source") {
		t.Fatalf("log carries prompt or output text:\n%s", buf.String())
	}
}
