package externalrecompute

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/credentialshapes"
)

func TestPersistedFailureTextRedactsCredentialShapes(t *testing.T) {
	var token string
	for _, shape := range credentialshapes.Shapes() {
		if shape.ID == "github ghp" {
			token = shape.Value()
		}
	}
	if token == "" {
		t.Fatal("github ghp shape missing from the shared corpus")
	}
	cause := fmt.Errorf("%w: decode bridge scope: unexpected value %s", ErrPermanent, token)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	stored := persistedFailureText(context.Background(), logger, "bridge-1", cause)

	if strings.Contains(stored, token) {
		t.Fatalf("stored recompute_error still carries the credential: %q", stored)
	}
	if !strings.Contains(stored, "decode bridge scope") {
		t.Fatalf("stored recompute_error lost its diagnostic text: %q", stored)
	}
}

func TestPersistedFailureTextKeepsPlainText(t *testing.T) {
	cause := errors.New("external recompute: permanent rejection: inconsistent external recompute bridge scope")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if stored := persistedFailureText(context.Background(), logger, "bridge-1", cause); stored != cause.Error() {
		t.Fatalf("plain text changed: %q", stored)
	}
}

type countingHandler struct {
	slog.Handler
	messages []string
	attrs    []string
}

func (h *countingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *countingHandler) Handle(_ context.Context, r slog.Record) error {
	h.messages = append(h.messages, r.Message)
	r.Attrs(func(a slog.Attr) bool { h.attrs = append(h.attrs, a.Key+"="+a.Value.String()); return true })
	return nil
}

func TestPersistedFailureTextDebugLineCarriesCountsOnly(t *testing.T) {
	var token string
	for _, shape := range credentialshapes.Shapes() {
		if shape.ID == "github ghp" {
			token = shape.Value()
		}
	}
	handler := &countingHandler{}
	persistedFailureText(context.Background(), slog.New(handler), "bridge-1", errors.New("boom "+token))
	if len(handler.messages) != 1 {
		t.Fatalf("debug lines = %d, want 1", len(handler.messages))
	}
	for _, text := range append(handler.attrs, handler.messages...) {
		if strings.Contains(text, token) || strings.Contains(text, "boom") {
			t.Fatalf("debug line carries error text: %q", text)
		}
	}
}
