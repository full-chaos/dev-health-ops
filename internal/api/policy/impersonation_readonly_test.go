package policy

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestRefuseImpersonationLogsAndRefuses(t *testing.T) {
	var logs bytes.Buffer
	guard := NewGuard(nil, slog.New(slog.NewTextHandler(&logs, nil)))
	reached := false
	handler := guard.RefuseImpersonation(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached = true }))

	req := httptest.NewRequest(http.MethodPost, "/x", nil)
	req = req.WithContext(WithImpersonation(req.Context(), &Impersonation{AdminUserID: uuid.New()}))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden || reached || !strings.Contains(w.Body.String(), "Not allowed in an impersonation session") {
		t.Fatalf("session: status %d reached %v body %s", w.Code, reached, w.Body.String())
	}
	if !strings.Contains(logs.String(), "write refused in an impersonation session") {
		t.Fatalf("no warn line: %s", logs.String())
	}

	w = httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/x", nil))
	if !reached {
		t.Fatalf("no session: handler not reached, status %d", w.Code)
	}
}
