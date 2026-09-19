package logging

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
)

// lifecycleLine logs the shape of the provider-unit lifecycle line: nine
// identifying attributes, a result and an error detail.
func lifecycleLine(logger *slog.Logger) {
	logger.InfoContext(context.Background(), "sync_provider_unit_finished",
		"provider", "github", "dataset", "pull-requests", "mode", "incremental",
		"kind", "sync_provider_unit", "queue", "provider-sync", "job_id", int64(912345),
		"attempt", 2, "sync_run_id", "3f1c1f7e-9a51-4b8e-9d7e-6f0a3c2b1d10",
		"sync_unit_id", "0c7d5c52-1e0f-4a8e-8b61-2f7d9e3a4b21", "result", "retrying",
		"error_detail", errors.New("provider request failed: rate_limited status=429 path=/repos/octo/hello/pulls"),
	)
}

// BenchmarkLifecycleLineRedacting is the line through NewJSON.
func BenchmarkLifecycleLineRedacting(b *testing.B) {
	logger := NewJSON(io.Discard, slog.LevelInfo)
	b.ReportAllocs()
	for b.Loop() {
		lifecycleLine(logger)
	}
}

// BenchmarkLifecycleLinePlain is the same line through an unredacted JSON
// handler, the floor the redactor's cost is measured against.
func BenchmarkLifecycleLinePlain(b *testing.B) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo}))
	b.ReportAllocs()
	for b.Loop() {
		lifecycleLine(logger)
	}
}

// structuredLine logs a line whose attribute is a structured value (a map
// with a nested object), which the redactor always decodes and walks.
func structuredLine(logger *slog.Logger) {
	logger.Info("provider_sync.batch_committed", "result", map[string]any{
		"rows": 120, "provider": "github", "dataset": "pull-requests",
		"watermark": map[string]any{"cursor": "Y3Vyc29yOnYyOpK5", "updated_at": "2026-01-02T03:04:05Z"},
	})
}

// BenchmarkStructuredAttributeRedacting is the structured line through
// NewJSON.
func BenchmarkStructuredAttributeRedacting(b *testing.B) {
	logger := NewJSON(io.Discard, slog.LevelInfo)
	b.ReportAllocs()
	for b.Loop() {
		structuredLine(logger)
	}
}

// BenchmarkStructuredAttributePlain is the floor.
func BenchmarkStructuredAttributePlain(b *testing.B) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo}))
	b.ReportAllocs()
	for b.Loop() {
		structuredLine(logger)
	}
}
