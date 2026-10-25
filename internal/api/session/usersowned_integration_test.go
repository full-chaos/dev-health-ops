//go:build integration

package session_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sessionscenario"
)

func tokenVersionOf(t *testing.T, ctx context.Context, st stack, id string) int64 {
	t.Helper()
	var version int64
	if err := st.pool.QueryRow(ctx, `SELECT token_version FROM users WHERE id = $1::uuid`, id).Scan(&version); err != nil {
		t.Fatal(err)
	}
	return version
}

// With the users-owned switch on, a correct change-password request is
// refused with the moved-operation answer and the stored row is untouched:
// no new hash, no token_version bump (the auth side would otherwise hold a
// stale verifier).
func TestChangePasswordRefusedWhileUsersOwnedLeavesTheRowUntouched(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	t.Setenv("AUTH_USERS_OWNED", "true")
	st := startStack(t, ctx)
	orgA := st.specs["alice"]["org_id"].(string)
	alice := changeToken(t, st, changeAlice, "alice@example.com", orgA, "owner", 0)
	hashBefore := passwordHashOf(t, ctx, st, changeAlice)
	versionBefore := tokenVersionOf(t, ctx, st, changeAlice)

	status, body := changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/change-password", alice, changeBody(sessionscenario.Password, newPassword))
	if status != http.StatusConflict || !strings.Contains(body, `"reason":"operation_moved"`) {
		t.Fatalf("status %d body %s, want 409 operation_moved", status, body)
	}
	if passwordHashOf(t, ctx, st, changeAlice) != hashBefore || tokenVersionOf(t, ctx, st, changeAlice) != versionBefore {
		t.Fatal("a refused change moved the hash or the token version")
	}
}
