package decision

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestASuccessfulClassificationLogsOneLLMCallLine(t *testing.T) {
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	got, err := newTestCompleter(t, staticTransport(readResponse(t, "real-ok"))).Classify(context.Background(), syntheticBundle(t))
	if err != nil || got.State != StateOK {
		t.Fatalf("classification = %+v err %v", got, err)
	}
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var record map[string]any
		if line != "" && json.Unmarshal([]byte(line), &record) == nil && record["msg"] == "llm.call" {
			records = append(records, record)
		}
	}
	if len(records) != 1 {
		t.Fatalf("llm.call lines = %d, want 1; log:\n%s", len(records), buf.String())
	}
	record := records[0]
	if record["level"] != "INFO" || record["model"] == "" || record["model"] == nil {
		t.Fatalf("record = %v", record)
	}
	if in, ok := record["input_tokens"].(float64); !ok || in <= 0 {
		t.Fatalf("input_tokens = %v, want the response usage", record["input_tokens"])
	}
}

func TestABilledResponseTheParserRejectsStillLogsOneLLMCallLine(t *testing.T) {
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	body := `{"model":"jev-1.13.0","usage":{"input_tokens":40,"output_tokens":7},"answers":[]}`
	got, err := newTestCompleter(t, staticTransport(body)).Classify(context.Background(), syntheticBundle(t))
	if err != nil || got.State != StateRequestFailed {
		t.Fatalf("classification = %+v err %v", got, err)
	}
	count := strings.Count(buf.String(), `"msg":"llm.call"`)
	if count != 1 {
		t.Fatalf("llm.call lines = %d, want 1 for a billed response; log:\n%s", count, buf.String())
	}
	if !strings.Contains(buf.String(), `"input_tokens":40`) || !strings.Contains(buf.String(), `"output_tokens":7`) {
		t.Fatalf("line lacks the response usage: %s", buf.String())
	}
}

func TestAResponseTheParserRejectsLogsOneLLMCallLineEvenWithNoUsage(t *testing.T) {
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	got, err := newTestCompleter(t, staticTransport(`{"model":"jev-1.13.0","answers":[]}`)).Classify(context.Background(), syntheticBundle(t))
	if err != nil || got.State != StateRequestFailed {
		t.Fatalf("classification = %+v err %v", got, err)
	}
	if n := strings.Count(buf.String(), `"msg":"llm.call"`); n != 1 {
		t.Fatalf("llm.call lines = %d, want 1 for a response the transport returned; log:\n%s", n, buf.String())
	}
}
