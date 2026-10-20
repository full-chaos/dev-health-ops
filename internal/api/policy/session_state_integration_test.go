//go:build integration

package policy

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const sessionStatePassword = "session-state-test-only-not-a-credential"

func loginPool(t *testing.T, ctx context.Context, admin *pgxpool.Pool, role string) *pgxpool.Pool {
	t.Helper()
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User, cfg.ConnConfig.Password = role, sessionStatePassword
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// PGStore reads an auth-service session as a session reader role, in the auth
// schema AUTH_DATABASE_SCHEMA names (not always "auth"), with only the column
// grant auth-migrate gives that role; a revoke there ends the session's
// tokens through CheckClaims.
func TestPGStoreReadsTheSessionAsAReaderRole(t *testing.T) {
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
	roles := map[string]string{}
	for _, base := range []string{"state_runtime", "state_reader"} {
		role, err := containers.RoleName(base, instance)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, sessionStatePassword)); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
		roles[base] = role
	}
	const schemaName = "acp"
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: schemaName, RuntimeRole: roles["state_runtime"],
		SessionReaderRoles: []string{roles["state_reader"]}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	schema, err := authschema.NewValidatedIdentifier(schemaName)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := session.New(session.Config{Pool: loginPool(t, ctx, admin, roles["state_runtime"]), Schema: schema,
		SuccessorKey: contracts.NewSecretString("session-state-successor-key-not-a-credential-0123456789")})
	if err != nil {
		t.Fatal(err)
	}
	principal := uuid.New()
	if _, err := admin.Exec(ctx, `INSERT INTO acp.principals (id, kind, display_name) VALUES ($1, 'user', 'state-test')`, principal.String()); err != nil {
		t.Fatal(err)
	}
	issued, err := sessions.Create(ctx, principal, nil, []string{"pwd"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	reader := PGStore{Pool: loginPool(t, ctx, admin, roles["state_reader"]), AuthSchema: schemaName}
	row, found, err := reader.SessionState(ctx, issued.Session.ID)
	if err != nil || !found || row.PrincipalID != principal || row.Revoked {
		t.Fatalf("live session: row=%+v found=%v err=%v", row, found, err)
	}
	if _, found, err := reader.SessionState(ctx, uuid.New()); err != nil || found {
		t.Fatalf("unknown session: found=%v err=%v", found, err)
	}

	// The same token is good, then refused once its session is revoked.
	store := &readerStore{PGStore: reader, user: principal}
	token := sign(t, claims(func(c jwt.MapClaims) {
		c["sub"] = principal.String()
		c["sid"] = session.WireSessionID(issued.Session.ID)
	}))
	if _, err := authenticator(t, store).Authenticate(ctx, token); err != nil {
		t.Fatalf("a live session's token: %v", err)
	}
	if _, err := sessions.Revoke(ctx, issued.Session.ID, session.Actor{}); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if row, _, err := reader.SessionState(ctx, issued.Session.ID); err != nil || !row.Revoked {
		t.Fatalf("revoked session: row=%+v err=%v", row, err)
	}
	if _, err := authenticator(t, store).Authenticate(ctx, token); !isRefusal(err) {
		t.Fatalf("a revoked session's token: err = %v, want a refusal", err)
	}

	// The default schema name does not hold these sessions, and a schema name
	// that is not an identifier is never put into SQL: both are errors, never
	// a pass.
	for _, schemaName := range []string{"", `acp"; --`} {
		if _, found, err := (PGStore{Pool: reader.Pool, AuthSchema: schemaName}).SessionState(ctx, issued.Session.ID); err == nil || found {
			t.Errorf("AuthSchema %q: found=%v err=%v, want an error", schemaName, found, err)
		}
	}
	if _, _, err := (PGStore{Pool: loginPool(t, ctx, admin, roles["state_runtime"]), AuthSchema: "public"}).SessionState(ctx, issued.Session.ID); err == nil || errors.Is(err, ErrUnavailable) {
		t.Errorf("a schema without sessions: err = %v, want a non-unavailable error", err)
	}
}

// readerStore answers the users row of one active principal and reads
// sessions through the real PGStore.
type readerStore struct {
	PGStore
	user uuid.UUID
}

func (r *readerStore) UserState(_ context.Context, id uuid.UUID) (UserState, bool, error) {
	return UserState{IsActive: true}, id == r.user, nil
}

func (r *readerStore) Membership(context.Context, uuid.UUID, uuid.UUID) (string, bool, error) {
	return "", false, nil
}

func (r *readerStore) ActiveImpersonation(context.Context, uuid.UUID) (*Impersonation, error) {
	return nil, nil
}
