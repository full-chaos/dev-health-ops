package policy

import (
	"bytes"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// An admin with an ACTIVE impersonation session whose session lookup errors
// must be refused, not served as though no session existed.
func TestImpersonationSessionLookupErrorIsRefused(t *testing.T) {
	store := newStore()
	store.users[userID] = UserState{IsActive: true, IsSuperuser: true}
	store.sessions[userID] = &Impersonation{ID: uuid.New(), AdminUserID: userID, TargetUserID: targetUserID,
		TargetOrgID: targetOrgID, TargetRole: "member", ExpiresAt: time.Now().Add(time.Hour)}
	store.errSession = errors.New("store down")
	var logs bytes.Buffer
	scope := NewScope(authenticator(t, store), slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	token := sign(t, claims(func(c jwt.MapClaims) { c["is_superuser"] = true }))
	mux := http.NewServeMux()
	mux.Handle("GET /x/{id}", scope.Impersonation(echo))
	got := serve(mux, http.MethodGet, "/x/an-unbounded-id", map[string]string{"Authorization": "Bearer " + token})
	if got.status != http.StatusInternalServerError {
		t.Fatalf("status %d body %q, want 500: the active-session lookup failed", got.status, got.body)
	}
	if got.header.Get("X-Impersonating") != "" || strings.Contains(got.body, "org=") {
		t.Fatalf("a refused request reached the route or stamped impersonation: %q %v", got.body, got.header)
	}
	if store.sessionCalls != 1 {
		t.Fatalf("%d session lookups, want 1", store.sessionCalls)
	}
	// The refusal must be visible at the default (Info) level, with the store
	// error and the route template (never the raw path), so a fault that turns admin requests into
	// 500s is not silent.
	for _, want := range []string{"level=ERROR", "session lookup failed; refusing the request", "path=\"GET /x/{id}\"", "store down"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log %q lacks %q", logs.String(), want)
		}
	}
	if strings.Contains(logs.String(), "an-unbounded-id") {
		t.Fatal("the log carries the raw request path")
	}
}
