//go:build integration

package session

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

const readerStorePassword = "reader-store-test-only-not-a-credential"

func readerStoreLogin(t *testing.T, ctx context.Context, admin *pgxpool.Pool, role string) *pgxpool.Pool {
	t.Helper()
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User, cfg.ConnConfig.Password = role, readerStorePassword
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// oneUser is a policy.Store with one active principal; sessions are read
// through the real policy.PGStore it embeds.
type oneUser struct {
	policy.PGStore
	user uuid.UUID
}

func (o oneUser) UserState(_ context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	return policy.UserState{IsActive: true}, id == o.user, nil
}

func (o oneUser) Membership(context.Context, uuid.UUID, uuid.UUID) (string, bool, error) {
	return "", false, nil
}

func (o oneUser) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	return nil, nil
}

// go-api's policy.PGStore reads an auth-service session as a session reader
// role, in the auth schema AUTH_DATABASE_SCHEMA names (here "acp"), with only
// the column grant auth-migrate gives that role; a revoke there ends the
// session's access token through Authenticator.
func TestPolicyStoreReadsTheSessionAsAReaderRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	// A session pins public.users.token_version, so Create reads go-api's tables.
	pgschema.Apply(ctx, t, admin)
	roles := map[string]string{}
	for _, base := range []string{"store_runtime", "store_reader"} {
		role, err := containers.RoleName(base, instance)
		if err != nil {
			t.Fatal(err)
		}
		roleID, err := authschema.NewValidatedIdentifier(role)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD '%s'`, authschema.Quote(roleID), readerStorePassword)); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
		roles[base] = role
	}
	const schemaName = "acp"
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: schemaName, RuntimeRole: roles["store_runtime"],
		SessionReaderRoles: []string{roles["store_reader"]}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	schema, err := authschema.NewValidatedIdentifier(schemaName)
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	sessions, err := New(Config{Pool: readerStoreLogin(t, ctx, admin, roles["store_runtime"]), Schema: schema,
		SuccessorKey: contracts.NewSecretString(base64.RawURLEncoding.EncodeToString(key))})
	if err != nil {
		t.Fatal(err)
	}
	principal := uuid.New()
	if _, err := admin.Exec(ctx, `INSERT INTO acp.principals (id, kind, display_name) VALUES ($1, 'user', 'reader-store-test')`, principal.String()); err != nil {
		t.Fatal(err)
	}
	issued, err := sessions.Create(ctx, principal, nil, []string{"pwd"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	reader := policy.PGStore{Pool: readerStoreLogin(t, ctx, admin, roles["store_reader"]), AuthSchema: schemaName}
	row, found, err := reader.SessionState(ctx, issued.Session.ID)
	if err != nil || !found || row.PrincipalID != principal || row.Revoked {
		t.Fatalf("live session: row=%+v found=%v err=%v", row, found, err)
	}
	if _, found, err := reader.SessionState(ctx, uuid.New()); err != nil || found {
		t.Fatalf("unknown session: found=%v err=%v", found, err)
	}

	keyText := strings.Repeat("reader-store-signing-", 2)
	verifier, err := edgetoken.New(keyText, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(verifier, oneUser{PGStore: reader, user: principal}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": principal.String(), "type": "access", "iss": "dev-health-ops", "aud": "dev-health-api",
		"exp": now.Add(time.Hour).Unix(), "iat": now.Unix(), "tv": 0, "sid": WireSessionID(issued.Session.ID),
	}).SignedString([]byte(keyText))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := auth.Authenticate(ctx, token); err != nil {
		t.Fatalf("a live session's token: %v", err)
	}
	if _, err := sessions.Revoke(ctx, issued.Session.ID, Actor{}); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if row, _, err := reader.SessionState(ctx, issued.Session.ID); err != nil || !row.Revoked {
		t.Fatalf("revoked session: row=%+v err=%v", row, err)
	}
	if _, err := auth.Authenticate(ctx, token); !policy.IsRefusal(err) {
		t.Fatalf("a revoked session's token: err = %v, want a refusal", err)
	}

	// The default schema does not hold these sessions, and a name that is not
	// an identifier never reaches SQL: both are errors, never a pass.
	for _, name := range []string{"", `acp"; --`} {
		if _, found, err := (policy.PGStore{Pool: reader.Pool, AuthSchema: name}).SessionState(ctx, issued.Session.ID); err == nil || found {
			t.Errorf("AuthSchema %q: found=%v err=%v, want an error", name, found, err)
		}
	}
	// Without the reader grant the read is a missing-grant error naming it,
	// never a pass and never "unavailable".
	_, _, err = (policy.PGStore{Pool: readerStoreLogin(t, ctx, admin, roles["store_runtime"]), AuthSchema: "public"}).SessionState(ctx, issued.Session.ID)
	if err == nil || errors.Is(err, policy.ErrUnavailable) {
		t.Errorf("a schema without sessions: err = %v, want a non-unavailable error", err)
	}
}
