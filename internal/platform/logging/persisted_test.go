package logging

import (
	"context"
	"log/slog"
	"strings"
	"testing"
)

type recordedLines struct {
	slog.Handler
	lines []string
}

func (h *recordedLines) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordedLines) Handle(_ context.Context, r slog.Record) error {
	line := r.Message
	r.Attrs(func(a slog.Attr) bool { line += " " + a.Key + "=" + a.Value.String(); return true })
	h.lines = append(h.lines, line)
	return nil
}

func TestRedactPersistedRemovesACredentialAndLogsCountsOnly(t *testing.T) {
	marker := "ghp_aaaaaaaaaaaaaaaaaaaaaaaa"
	handler := &recordedLines{}
	stored := RedactPersisted(context.Background(), slog.New(handler), "t.col", "decode failed near "+marker)
	if strings.Contains(stored, marker) || !strings.Contains(stored, "decode failed near") {
		t.Fatalf("stored = %q", stored)
	}
	if len(handler.lines) != 1 {
		t.Fatalf("debug lines = %d, want 1", len(handler.lines))
	}
	if strings.Contains(handler.lines[0], marker) || strings.Contains(handler.lines[0], "decode failed") {
		t.Fatalf("debug line carries text: %q", handler.lines[0])
	}
}

func TestRedactPersistedLeavesPlainTextAndStaysQuiet(t *testing.T) {
	handler := &recordedLines{}
	const plain = "external recompute: permanent rejection: inconsistent bridge scope"
	if stored := RedactPersisted(context.Background(), slog.New(handler), "t.col", plain); stored != plain {
		t.Fatalf("plain text changed: %q", stored)
	}
	if len(handler.lines) != 0 {
		t.Fatalf("unexpected debug lines: %v", handler.lines)
	}
}

func TestRedactPersistedKeepsEveryOtherByte(t *testing.T) {
	const plain = "diagnostic q=project%3Dfoo&note=visible"
	if stored := RedactPersisted(context.Background(), slog.New(&recordedLines{}), "t.col", plain); stored != plain {
		t.Fatalf("stored = %q, want the bytes unchanged", stored)
	}
}
