package investment

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestBatchModeLogsOneLLMCallLineForEachCompletedItem(t *testing.T) {
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	pending := batchPending(t, 3)
	_, server := newFakeOpenAI(t)
	got := runCategorize(t, context.Background(), fakeProvider(server.URL), batchTestConfig(LLMBatchModeProvider), pending)
	if got.err != nil || len(got.outcomes) != 3 {
		t.Fatalf("result %+v", got)
	}
	count := 0
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var record map[string]any
		if line != "" && json.Unmarshal([]byte(line), &record) == nil && record["msg"] == "llm.call" {
			count++
			if record["level"] != "INFO" {
				t.Fatalf("level = %v", record["level"])
			}
			if record["format_path"] != "schema" {
				t.Fatalf("format_path = %v, want schema (batch requests send a strict json_schema)", record["format_path"])
			}
		}
	}
	if count != 3 {
		t.Fatalf("llm.call lines = %d, want 3 (one per completed batch item); log:\n%s", count, buf.String())
	}
}
