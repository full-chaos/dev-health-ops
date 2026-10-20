package investment

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"
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

func captureInfoLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buf
}

func countLLMCallLines(buf *bytes.Buffer) int {
	return strings.Count(buf.String(), `"msg":"llm.call"`)
}

func TestBatchModeLogsOneLLMCallLineForABilledLineWithNoText(t *testing.T) {
	buf := captureInfoLog(t)
	pending := batchPending(t, 2)
	fake, server := newFakeOpenAI(t)
	fake.lineFor = func(customID, prompt string) string {
		if customID == "run-batch-test-1" {
			return `{"id":"l1","custom_id":"run-batch-test-1","response":{"status_code":200,"body":{"usage":{"input_tokens":7,"output_tokens":3}}},"error":null}`
		}
		return okLine(customID, mockAnswer(t, prompt))
	}
	got := runCategorize(t, context.Background(), fakeProvider(server.URL), batchTestConfig(LLMBatchModeProvider), pending)
	if got.err != nil || got.stats.LLMCalls != 2 {
		t.Fatalf("result %+v", got)
	}
	if n := countLLMCallLines(buf); n != got.stats.LLMCalls {
		t.Fatalf("llm.call lines = %d, want %d (one per billed call); log:\n%s", n, got.stats.LLMCalls, buf.String())
	}
}

func TestBatchModeLogsOneLLMCallLineForEachBilledLineOfACancelledBatch(t *testing.T) {
	buf := captureInfoLog(t)
	pending := batchPending(t, 2)
	fake, server := newFakeOpenAI(t)
	fake.statuses = []string{"in_progress"}
	fake.outputOnCancel = okLine("run-batch-test-0", "{}") + "\n"
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			fake.mu.Lock()
			polled := fake.polls
			fake.mu.Unlock()
			if polled >= 2 {
				cancel()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	got := runCategorize(t, ctx, fakeProvider(server.URL), batchTestConfig(LLMBatchModeProvider), pending)
	if got.stats.LLMCalls != 1 {
		t.Fatalf("result %+v", got)
	}
	if n := countLLMCallLines(buf); n != 1 {
		t.Fatalf("llm.call lines = %d, want 1 (the billed line of the cancelled batch); log:\n%s", n, buf.String())
	}
}
