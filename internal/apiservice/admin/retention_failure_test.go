package admin

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestRetentionRunFailureHidesDriverText(t *testing.T) {
	var logs bytes.Buffer
	h := &handlers{logger: slog.New(slog.NewTextHandler(&logs, nil))}
	p := &retentionPolicy{ID: uuid.New(), OrgID: uuid.New(), ResourceType: "audit_logs"}
	driver := errors.New(`ERROR: relation "audit_logs" secret detail (SQLSTATE 42P01)`)

	text := h.retentionRunFailure(context.Background(), p, false, driver)

	if strings.Contains(text, "audit_logs") || strings.Contains(text, "SQLSTATE") {
		t.Fatalf("response text leaks the driver error: %q", text)
	}
	reference := strings.TrimPrefix(text, "Retention run failed. Reference: ")
	if _, err := uuid.Parse(reference); err != nil {
		t.Fatalf("text %q carries no reference: %v", text, err)
	}
	if !strings.Contains(logs.String(), "SQLSTATE 42P01") || !strings.Contains(logs.String(), reference) {
		t.Fatalf("log lacks the driver error or the reference: %s", logs.String())
	}
}
