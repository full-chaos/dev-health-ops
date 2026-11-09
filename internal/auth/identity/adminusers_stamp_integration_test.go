//go:build integration

package identity

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionrule"
)

// After the admin password set, a session the user held before it is refused: the session rule reads it revoked
// (the password stamp is after its creation), and both pins moved in the same commit (auth legacy_token_version
// above the session's pin, public token_version + 1), so an access token minted before the change fails too.
func TestAdminSetPasswordEndsTheOlderSession(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	none := func(context.Context, audit.TxOps) error { return nil }
	actor := e.seedUser(t, ctx, uniqueEmail("stamp-actor"), true, true)
	e.project(t, ctx, actor)
	first := projectionHash(t, "stamp-first-material")
	created, err := e.owned.AdminCreateUser(ctx, AdminCreateRequest{Email: uniqueEmail("stamp"), PasswordHash: &first, Actor: actor, Authorize: none})
	if err != nil {
		t.Fatal(err)
	}
	session := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.sessions (id, principal_id, token_hash, expires_at, legacy_token_version, created_at)
VALUES ($1, $2, $3, now() + interval '1 hour', 0, now() - interval '1 minute')`, session, created.ID, []byte(session.String())); err != nil {
		t.Fatal(err)
	}
	rule, err := sessionrule.New(`"auth"`, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	revoked := func() bool {
		var ended bool
		if err := e.admin.QueryRow(ctx, `SELECT (`+rule.Revoked("s")+`) IS NOT NULL FROM auth.sessions s WHERE s.id = $1`, session).Scan(&ended); err != nil {
			t.Fatal(err)
		}
		return ended
	}
	if revoked() {
		t.Fatal("the session is revoked before the password set")
	}
	second := projectionHash(t, "stamp-second-material")
	if err := e.owned.AdminSetPassword(ctx, AdminPasswordRequest{Principal: created.ID, Hash: second, Actor: actor, Authorize: none}); err != nil {
		t.Fatalf("AdminSetPassword: %v", err)
	}
	if !revoked() {
		t.Error("the session the user held before the admin password set is still live")
	}
	var authPin, sessionPin int64
	if err := e.admin.QueryRow(ctx, `SELECT u.legacy_token_version, s.legacy_token_version FROM auth.users u JOIN auth.sessions s ON s.principal_id = u.principal_id
WHERE s.id = $1`, session).Scan(&authPin, &sessionPin); err != nil {
		t.Fatal(err)
	}
	if authPin <= sessionPin {
		t.Errorf("auth legacy_token_version %d is not above the session's pin %d: the pin was not bumped", authPin, sessionPin)
	}
	if row, _ := e.publicUser(t, ctx, created.ID); row.TokenVersion != 1 {
		t.Errorf("public token_version = %d, want 1: the public pin was not bumped", row.TokenVersion)
	}
}
