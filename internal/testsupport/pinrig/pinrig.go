package pinrig

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const pinRigPassword = "pin-rig-test-only-not-a-credential"

// PinRig is an auth schema next to an already migrated public schema, with a
// session store that signs users in as auth-service does and the go-api read of
// those sessions as the session reader role holds it.
type PinRig struct {
	admin    *pgxpool.Pool
	sessions *session.Store
	auth     *policy.Authenticator
	reader   policy.PGStore
	// base is the session store's configuration, for SignInWith.
	base session.Config
}

// NewPinRig applies the auth schema on admin's database.
func NewPinRig(t *testing.T, ctx context.Context, admin *pgxpool.Pool, instance *containers.Instance) *PinRig {
	t.Helper()
	roles := map[string]string{}
	for _, base := range []string{"pin_runtime", "pin_reader"} {
		role, err := containers.RoleName(base, instance)
		if err != nil {
			t.Fatal(err)
		}
		id, err := authschema.NewValidatedIdentifier(role)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD '%s'`, authschema.Quote(id), pinRigPassword)); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
		roles[base] = role
	}
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: roles["pin_runtime"],
		SessionReaderRoles: []string{roles["pin_reader"]}}); err != nil {
		t.Fatalf("auth schema: %v", err)
	}
	login := func(role string) *pgxpool.Pool {
		cfg := admin.Config().Copy()
		cfg.ConnConfig.User, cfg.ConnConfig.Password = role, pinRigPassword
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		return pool
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	base := session.Config{Pool: login(roles["pin_runtime"]), Schema: schema,
		SuccessorKey: contracts.NewSecretString(base64.RawURLEncoding.EncodeToString(key))}
	sessions, err := session.New(base)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := edgetoken.New(base64.RawURLEncoding.EncodeToString(key), "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	reader := policy.PGStore{Pool: login(roles["pin_reader"]), AuthSchema: "auth"}
	auth, err := policy.NewAuthenticator(verifier, pinStore{PGStore: reader, users: admin},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return &PinRig{admin: admin, sessions: sessions, auth: auth, reader: reader, base: base}
}

// Reader is go-api's session read as the session reader role holds it: its
// SessionState reads only the columns auth-migrate grants a reader.
func (r *PinRig) Reader() policy.PGStore { return r.reader }

// Revoke ends sid as auth-service's session revoke does.
func (r *PinRig) Revoke(t *testing.T, ctx context.Context, sid uuid.UUID) {
	t.Helper()
	if revoked, err := r.sessions.Revoke(ctx, sid, session.Actor{}); err != nil || !revoked {
		t.Fatalf("revoke %s: %v %v", sid, revoked, err)
	}
}

// SignIn creates a session of user, pinned to the user's token_version now.
func (r *PinRig) SignIn(t *testing.T, ctx context.Context, user uuid.UUID) uuid.UUID {
	t.Helper()
	if _, err := r.admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'pin-rig') ON CONFLICT (id) DO NOTHING`, user); err != nil {
		t.Fatalf("principal: %v", err)
	}
	issued, err := r.sessions.Create(ctx, user, nil, []string{"pwd"})
	if err != nil {
		t.Fatalf("sign in: %v", err)
	}
	return issued.Session.ID
}

// SignInWith creates a session of user as auth-service does with a session
// store configured with the given organization binding, absolute session
// lifetime and refresh (idle) lifetime; a zero lifetime keeps the store's
// default (no absolute cap; a 7-day refresh life).
func (r *PinRig) SignInWith(t *testing.T, ctx context.Context, user uuid.UUID, organization *uuid.UUID, sessionLifetime, refreshTTL time.Duration) uuid.UUID {
	t.Helper()
	if _, err := r.admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'pin-rig') ON CONFLICT (id) DO NOTHING`, user); err != nil {
		t.Fatalf("principal: %v", err)
	}
	if organization != nil {
		// auth.sessions.organization_id references auth.organizations; the
		// rig's organization carries the public organization's id.
		// Security tables are written only through audit.Commit.
		id := *organization
		if err := audit.Commit(ctx, r.admin, r.base.Schema, audit.Mutation{
			Apply: func(ctx context.Context, tx audit.TxOps) error {
				_, err := tx.Exec(ctx, `INSERT INTO auth.organizations (id, name, slug) VALUES ($1, $2, $2) ON CONFLICT (id) DO NOTHING`,
					id, "pin-rig-"+id.String())
				return err
			},
			Audit: audit.AuditEvent{EventType: "organization.created", Outcome: audit.OutcomeAllowed},
			Events: []audit.OutboxEvent{{AggregateType: "organization", AggregateID: "org_" + id.String(),
				EventType: "organization.created"}},
		}); err != nil {
			t.Fatalf("auth organization: %v", err)
		}
	}
	cfg := r.base
	cfg.SessionLifetime, cfg.RefreshTTL = sessionLifetime, refreshTTL
	store, err := session.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	issued, err := store.Create(ctx, user, organization, []string{"pwd"})
	if err != nil {
		t.Fatalf("sign in: %v", err)
	}
	return issued.Session.ID
}

// ClearPin removes the session's token_version pin, as a session from before
// the pin existed has none.
func (r *PinRig) ClearPin(t *testing.T, ctx context.Context, sid uuid.UUID) {
	t.Helper()
	for _, statement := range []string{
		`ALTER TABLE auth.sessions DROP CONSTRAINT sessions_pin_present`,
		`ALTER TABLE auth.sessions DISABLE TRIGGER sessions_legacy_token_version_write_once`,
	} {
		if _, err := r.admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	_, err := r.admin.Exec(ctx, `UPDATE auth.sessions SET legacy_token_version = NULL WHERE id = $1`, sid)
	_, e2 := r.admin.Exec(ctx, `ALTER TABLE auth.sessions ENABLE TRIGGER sessions_legacy_token_version_write_once`)
	_, e3 := r.admin.Exec(ctx, `ALTER TABLE auth.sessions ADD CONSTRAINT sessions_pin_present
		CHECK (legacy_token_version IS NOT NULL OR revoked_at IS NOT NULL) NOT VALID`)
	if err != nil || e2 != nil || e3 != nil {
		t.Fatalf("clear pin: %v %v %v", err, e2, e3)
	}
}

// Check is go-api's decision for a token of user naming sid, carrying the
// user's current token_version.
func (r *PinRig) Check(ctx context.Context, user, sid uuid.UUID) error {
	var version int64
	if err := r.admin.QueryRow(ctx, `SELECT token_version FROM users WHERE id = $1`, user).Scan(&version); err != nil {
		return err
	}
	_, err := r.auth.CheckClaims(ctx, jwt.MapClaims{"sub": user.String(), "sid": session.WireSessionID(sid), "tv": float64(version)})
	return err
}

// Refused reports whether err is the generic refusal of a token.
func Refused(err error) bool { return policy.IsRefusal(err) }

type pinStore struct {
	policy.PGStore
	users *pgxpool.Pool
}

func (s pinStore) UserState(ctx context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	return policy.PGStore{Pool: s.users}.UserState(ctx, id)
}
