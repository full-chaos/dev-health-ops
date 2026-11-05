package venueoracle

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// AuthSchema is the auth-owned schema WriteAuthSessions creates on the Go copy: the api's default
// (AUTH_DATABASE_SCHEMA unset).
const AuthSchema = "auth"

// WriteAuthSessions gives the Go copy the auth-service session of every token the venue minted, for an api
// that reads a user access token's sid in Postgres as production does (policy.PGStore.SessionState): it
// applies the auth schema (auth-migrate) with the api role as a session reader, as a deploy grants it, and
// writes one principal and one live session row per minted session, pinned at the token's token_version.
// A venue without minted tokens writes nothing but the schema.
func (v *Venue) WriteAuthSessions(t *testing.T, ctx context.Context) {
	t.Helper()
	if v.authRuntimeRole == "" {
		t.Fatal("venue: WriteAuthSessions needs a venue built by Start")
	}
	admin, err := pgxpool.New(ctx, v.AdminURI(t, v.GoDB))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, ok := v.Roles["auth_runtime"]; !ok {
		id, err := authschema.NewValidatedIdentifier(v.authRuntimeRole)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Exec(ctx, "CREATE ROLE "+authschema.Quote(id)+" NOLOGIN"); err != nil {
			t.Fatalf("venue: auth runtime role: %v", err)
		}
		// Start's cleanup drops every role in Roles once the Go copy is gone.
		v.Roles["auth_runtime"] = v.authRuntimeRole
		if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: AuthSchema, RuntimeRole: v.authRuntimeRole,
			SessionReaderRoles: []string{v.Roles["api"]}}); err != nil {
			t.Fatalf("venue: auth schema: %v", err)
		}
	}
	if v.TokenIssuer == nil {
		return
	}
	for _, session := range v.TokenIssuer.Sessions() {
		if _, err := admin.Exec(ctx, `INSERT INTO auth.principals (id, kind) VALUES ($1, 'user') ON CONFLICT (id) DO NOTHING`, session.Principal); err != nil {
			t.Fatalf("venue: principal %s: %v", session.Principal, err)
		}
		hash := sha256.Sum256([]byte(fmt.Sprintf("venue-session:%s", session.ID)))
		if _, err := admin.Exec(ctx, `INSERT INTO auth.sessions (id, principal_id, token_hash, expires_at, legacy_token_version)
			VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO NOTHING`, session.ID, session.Principal, hash[:], session.ExpiresAt, session.TokenVersion); err != nil {
			t.Fatalf("venue: session %s: %v", session.ID, err)
		}
	}
}

// authRuntimeRoleName is the auth runtime role WriteAuthSessions provisions, named for instance.
func authRuntimeRoleName(instance *containers.Instance) (string, error) {
	return containers.RoleName("venue_auth", instance)
}
