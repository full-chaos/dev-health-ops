package policy

import (
	"errors"
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
	scope := NewScope(authenticator(t, store), quiet())
	token := sign(t, claims(func(c jwt.MapClaims) { c["is_superuser"] = true }))
	got := serve(scope.Impersonation(echo), http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + token})
	if got.status != http.StatusInternalServerError {
		t.Fatalf("status %d body %q, want 500: the active-session lookup failed", got.status, got.body)
	}
	if got.header.Get("X-Impersonating") != "" || strings.Contains(got.body, "org=") {
		t.Fatalf("a refused request reached the route or stamped impersonation: %q %v", got.body, got.header)
	}
	if store.sessionCalls != 1 {
		t.Fatalf("%d session lookups, want 1", store.sessionCalls)
	}
}
