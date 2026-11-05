//go:build integration

package decisionstate

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// Parity: the session store and decisionstate read one effective liveness rule.
// Each fixture kind below is asserted on BOTH readers against the same wantLive,
// so a change on one side alone fails a cell.

func (e *env) sessionStore(t *testing.T) *session.Store {
	t.Helper()
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	store, err := session.New(session.Config{
		Pool: e.runtime, Schema: e.schema,
		SuccessorKey: contracts.NewSecretString(base64.RawURLEncoding.EncodeToString(raw)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func (e *env) stamp(t *testing.T, ctx context.Context, principal uuid.UUID, ago string) {
	t.Helper()
	err := audit.Commit(ctx, e.runtime, e.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `INSERT INTO auth.users (principal_id, email, email_lower, password_updated_at)
VALUES ($1, $2, $2, now() - interval '`+ago+`')
ON CONFLICT (principal_id) DO UPDATE SET password_updated_at = EXCLUDED.password_updated_at`, principal, principal.String()+"@example.com")
			return err
		},
		Audit:     audit.AuditEvent{EventType: "test.session_setup", Outcome: audit.OutcomeAllowed},
		OffStream: audit.OffStreamSessionLifecycle,
	})
	if err != nil {
		t.Fatalf("stamp: %v", err)
	}
}

// seedAt seeds a session created createdAgo ago that expires expiresIn from now
// (a negative interval is in the past). A non-nil web links it to its web session.
func (e *env) seedAt(t *testing.T, ctx context.Context, principal, org uuid.UUID, createdAgo, expiresIn string, revoked bool, web *uuid.UUID) uuid.UUID {
	t.Helper()
	id := uuid.New()
	revokedAt := "NULL"
	if revoked {
		revokedAt = "now()"
	}
	e.armed(t, ctx, `INSERT INTO auth.sessions (id, principal_id, organization_id, token_hash, issued_at, created_at, updated_at, expires_at,
auth_time, amr, legacy_token_version, revoked_at, web_session_id)
VALUES ($1, $2, $3, $4, now() - interval '`+createdAgo+`', now() - interval '`+createdAgo+`', now() - interval '`+createdAgo+`',
now() + interval '`+expiresIn+`', now() - interval '`+createdAgo+`', ARRAY['pwd'], 0, `+revokedAt+`, $5)`,
		id, principal, org, []byte("ds-"+id.String()), web)
	return id
}

// credential seeds a refresh credential of sid issued issuedAgo ago that expires expiresIn from now.
func (e *env) credential(t *testing.T, ctx context.Context, sid, principal uuid.UUID, issuedAgo, expiresIn string) {
	t.Helper()
	e.armed(t, ctx, `INSERT INTO auth.refresh_credentials (session_id, principal_id, token_hash, issued_at, created_at, expires_at)
VALUES ($1, $2, decode(md5($3::text) || md5($3::text || 'b'), 'hex'), now() - interval '`+issuedAgo+`', now() - interval '`+issuedAgo+`', now() + interval '`+expiresIn+`')`,
		sid, principal, uuid.NewString())
}

func storeSaysEnded(t *testing.T, ctx context.Context, store *session.Store, id uuid.UUID) bool {
	t.Helper()
	r, err := store.Current(ctx, id)
	if err != nil {
		t.Fatalf("store.Current: %v", err)
	}
	return r.RevokedAt != nil || !r.ExpiresAt.After(time.Now())
}

type seedFunc func(t *testing.T, ctx context.Context, e *env, p, org uuid.UUID) uuid.UUID

type livenessCell struct {
	name string
	// seed returns the session to read; wantLive is both the store's and decisionstate's expected view.
	seed     seedFunc
	wantLive bool
}

func livenessCells() []livenessCell {
	plain := func(createdAgo, expiresIn, stampAgo string, creds ...[2]string) seedFunc {
		return func(t *testing.T, ctx context.Context, e *env, p, org uuid.UUID) uuid.UUID {
			id := e.seedAt(t, ctx, p, org, createdAgo, expiresIn, false, nil)
			for _, c := range creds {
				e.credential(t, ctx, id, p, c[0], c[1])
			}
			if stampAgo != "" {
				e.stamp(t, ctx, p, stampAgo)
			}
			return id
		}
	}
	client := func(parentRevoked bool, parentExpiresIn, stampAgo string, parentCreds ...[2]string) seedFunc {
		return func(t *testing.T, ctx context.Context, e *env, p, org uuid.UUID) uuid.UUID {
			parent := e.seedAt(t, ctx, p, org, "3 hours", parentExpiresIn, parentRevoked, nil)
			for _, c := range parentCreds {
				e.credential(t, ctx, parent, p, c[0], c[1])
			}
			child := e.seedAt(t, ctx, p, org, "1 hour", "1 hour", false, &parent)
			if stampAgo != "" {
				e.stamp(t, ctx, p, stampAgo)
			}
			return child
		}
	}
	return []livenessCell{
		{"plain/control live, stamp before created_at", plain("2 hours", "1 hour", "3 hours"), true},
		{"plain/control live, no stamp", plain("2 hours", "1 hour", ""), true},
		{"plain/stamp after created_at", plain("2 hours", "1 hour", "1 hour"), false},
		{"plain/expired", plain("2 hours", "-1 minute", ""), false},
		{"plain/newest credential expired, cap far", plain("2 hours", "30 days", "", [2]string{"2 hours", "-1 minute"}), false},
		{"plain/newest credential live beats an older expired one", plain("2 hours", "30 days", "", [2]string{"2 hours", "-1 minute"}, [2]string{"1 hour", "1 hour"}), true},
		{"plain/no credential, idle past the refresh lifetime, cap far", plain("8 days", "30 days", ""), false},
		{"client/control live", client(false, "1 hour", ""), true},
		{"client/web parent revoked", client(true, "1 hour", ""), false},
		{"client/web parent voided by stamp after parent created_at", client(false, "1 hour", "2 hours"), false},
		{"client/web parent expired first", client(false, "-1 minute", ""), false},
		{"client/web parent credential expired, parent cap far", client(false, "30 days", "", [2]string{"3 hours", "-1 minute"}), false},
	}
}

func TestSessionLivenessParitySessionRead(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	store := e.sessionStore(t)
	for _, c := range livenessCells() {
		t.Run(c.name, func(t *testing.T) {
			p, org := e.user(t, ctx, true, false), e.org(t, ctx)
			id := c.seed(t, ctx, e, p, org)
			if ended := storeSaysEnded(t, ctx, store, id); ended == c.wantLive {
				t.Fatalf("fixture: store ended = %v, want %v", ended, !c.wantLive)
			}
			if s := e.readSession(t, ctx, id); !s.Found || s.Live != c.wantLive {
				t.Errorf("store says live=%v, decisionstate Session Live=%v", c.wantLive, s.Live)
			}
		})
	}
}

// The delegated read: Session on the delegated row reports BaseLive of its delegating session.
func TestSessionLivenessParityDelegatingSession(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	store := e.sessionStore(t)
	for _, c := range livenessCells() {
		t.Run(c.name, func(t *testing.T) {
			actor, target, org := e.user(t, ctx, true, false), e.user(t, ctx, true, false), e.org(t, ctx)
			base := c.seed(t, ctx, e, actor, org)
			if ended := storeSaysEnded(t, ctx, store, base); ended == c.wantLive {
				t.Fatalf("fixture: store ended = %v, want %v", ended, !c.wantLive)
			}
			delegated := e.delegatedSession(t, ctx, target, actor, base, org, []string{"git.read"})
			if s := e.readSession(t, ctx, delegated); !s.Found || !s.Live || !s.HasDelegatedFrom || s.BaseLive != c.wantLive {
				t.Errorf("delegated session: store says base live=%v, decisionstate BaseLive=%v (Live=%v)", c.wantLive, s.BaseLive, s.Live)
			}
		})
	}
}
