package policy

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"
)

// TestAnAuthenticatorWithoutAnAccessCheckRefusesBeforeAnyRead pins the
// unrouted path HS256 access verify left behind: with no RouteAccess a token
// that the retired HS256 path accepted (the go-api key, a live users row) is
// refused with reason no_access_check, before any store read, so no legacy
// claim branch (CheckClaims, the claim role, the superuser claim) runs.
func TestAnAuthenticatorWithoutAnAccessCheckRefusesBeforeAnyRead(t *testing.T) {
	store := newStore()
	var logs bytes.Buffer
	auth, err := NewAuthenticator(store, slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if err != nil {
		t.Fatal(err)
	}
	token := sign(t, claims(nil))

	user, err := auth.Authenticate(context.Background(), token)
	if user != nil || !IsRefusal(err) {
		t.Fatalf("unrouted Authenticate: user %v err %v, want the refusal", user, err)
	}
	if !strings.Contains(logs.String(), "reason=no_access_check") {
		t.Fatalf("log %q: want reason=no_access_check", logs.String())
	}
	if strings.Contains(logs.String(), token) {
		t.Fatal("the refusal log quotes the token")
	}

	got := serve(wrapLevel(NewGuard(auth, quiet()), AdminOrg, echo), http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + token})
	if got.status != http.StatusUnauthorized {
		t.Fatalf("guard: %d %q, want 401", got.status, got.body)
	}
	if store.userCalls != 0 || store.sessionCalls != 0 || len(store.membershipSeen) != 0 {
		t.Fatalf("store reads users=%d sessions=%d memberships=%v, want none", store.userCalls, store.sessionCalls, store.membershipSeen)
	}
}
