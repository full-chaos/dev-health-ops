package admin

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// A target whose superuser flag cannot be read is refused with a 500, never
// served as "not a superuser".
func TestRefuseSuperuserWriteFailsOnAFailedTargetRead(t *testing.T) {
	h := &handlers{logger: slog.New(slog.DiscardHandler)}
	failing := func() (bool, error) { return false, errors.New("read failed") }

	recorder := httptest.NewRecorder()
	if !h.refuseSuperuserWrite(context.Background(), recorder, &policy.User{Role: "admin"}, nil, failing, "test") {
		t.Fatal("an org admin's write was served after the target read failed")
	}
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", recorder.Code)
	}
}
