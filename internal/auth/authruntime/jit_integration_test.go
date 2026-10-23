//go:build integration

package authruntime

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/oidc"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

type jitStack struct {
	env      *routesEnv
	schema   authschema.ValidatedIdentifier
	owned    *identity.Store
	sessions authapi.SSOSessions
	jit      oidc.PGJIT
}

func newJITStack(t *testing.T, ctx context.Context) *jitStack {
	t.Helper()
	env := newRoutesEnv(t, ctx)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	owned, err := identity.New(identity.Config{Pool: env.runtime, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := authapi.NewIssuer(testAccessSecret, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := authapi.NewSSOSessions(authapi.CompatDeps{
		Sessions: env.sessions, Users: authapi.PGUsers{Pool: env.runtime, UsersOwned: true, Schema: schema}, Issuer: issuer,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Projector: owned,
		AppendAudit: func(ctx context.Context, event audit.AuditEvent) error {
			return audit.Append(ctx, env.runtime, schema, event)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &jitStack{env: env, schema: schema, owned: owned, sessions: sessions,
		jit: oidc.PGJIT{Identities: owned, Sessions: env.sessions, Mint: sessions.MintOIDC}}
}

func (s *jitStack) count(t *testing.T, ctx context.Context, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := s.env.admin.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", sql, err)
	}
	return n
}

func (s *jitStack) newOrg(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	org := uuid.New()
	if _, err := s.env.admin.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, is_active, settings, managed_by, created_at, updated_at)
VALUES ($1, $2, 'Jit Org', 'community', true, '{}', 'self', now(), now())`, org, "jit-"+org.String()[:8]); err != nil {
		t.Fatal(err)
	}
	return org
}

// A known account without a membership joins the provider's organization and
// starts its session in one commit; the membership is written to the auth
// schema only.
func TestJoinAndIssueWritesOneMembershipAndOneSession(t *testing.T) {
	ctx := context.Background()
	s := newJITStack(t, ctx)
	user, _ := s.env.seed(t, ctx)
	org := s.newOrg(t, ctx)
	orgRef := org
	tokens, err := s.jit.JoinAndIssue(ctx, user, org, oidc.SessionRequest{PrincipalID: user, OrgID: &orgRef, Role: "member", AMR: []string{"fed"}})
	if err != nil {
		t.Fatalf("JoinAndIssue: %v", err)
	}
	if tokens.AccessToken == "" || tokens.RefreshToken == "" {
		t.Fatal("no tokens")
	}
	if n := s.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2 AND status = 'active' AND role = 'member'`, org, user); n != 1 {
		t.Fatalf("auth memberships = %d, want 1", n)
	}
	if n := s.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1 AND organization_id = $2`, user, org); n != 1 {
		t.Fatalf("sessions in the organization = %d, want 1", n)
	}
	if n := s.count(t, ctx, `SELECT count(*) FROM public.memberships WHERE user_id = $1 AND org_id = $2`, user, org); n != 0 {
		t.Fatal("the join wrote public.memberships")
	}
	if n := s.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1 AND event_type = 'auth.membership.added'`, user); n != 1 {
		t.Fatalf("membership audit rows = %d, want 1", n)
	}
}

// A failure in the session write rolls the membership back with it.
func TestJoinAndIssueFailureLeavesNoMembershipAndNoSession(t *testing.T) {
	ctx := context.Background()
	s := newJITStack(t, ctx)
	user, _ := s.env.seed(t, ctx)
	org := s.newOrg(t, ctx)
	orgRef := org
	before := s.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, user)
	if _, err := s.jit.JoinAndIssue(ctx, user, org, oidc.SessionRequest{PrincipalID: user, OrgID: &orgRef, Role: "member", AMR: []string{"no-such-method"}}); err == nil {
		t.Fatal("a session with an unknown authentication method was started")
	}
	if n := s.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1`, org); n != 0 {
		t.Fatalf("memberships after the rollback = %d", n)
	}
	if n := s.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, user); n != before {
		t.Fatalf("sessions after the rollback = %d, before %d", n, before)
	}
	if n := s.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1 AND event_type = 'auth.membership.added'`, user); n != 0 {
		t.Fatal("a membership audit row survived the rollback")
	}
}

// A new account's failed session write leaves no account, identity,
// membership or session.
func TestProvisionJoinAndIssueFailureLeavesNothing(t *testing.T) {
	ctx := context.Background()
	s := newJITStack(t, ctx)
	org := s.newOrg(t, ctx)
	orgRef := org
	subject := "jit-" + uuid.NewString()
	email := "jit-" + uuid.NewString()[:8] + "@example.com"
	key := oidc.ExternalKey{Issuer: "https://idp.example.com/jit", Provider: uuid.NewString(), Subject: subject}
	if _, _, err := s.jit.ProvisionJoinAndIssue(ctx, oidc.FederatedAccount{Key: key, Email: email, FullName: "Jit"}, org,
		oidc.SessionRequest{OrgID: &orgRef, Role: "member", AMR: []string{"no-such-method"}}); err == nil {
		t.Fatal("a session with an unknown authentication method was started")
	}
	lower, _ := identity.NormalizeEmail(email)
	if n := s.count(t, ctx, `SELECT count(*) FROM auth.users WHERE email_lower = $1`, lower); n != 0 {
		t.Fatalf("users after the rollback = %d", n)
	}
	if n := s.count(t, ctx, `SELECT count(*) FROM auth.external_identities WHERE subject = $1`, subject); n != 0 {
		t.Fatalf("identities after the rollback = %d", n)
	}
	if n := s.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1`, org); n != 0 {
		t.Fatalf("memberships after the rollback = %d", n)
	}
}

// With the users cutover off the readers see public.memberships only; with it
// on they also see the auth memberships and an auth row wins on role.
func TestMembershipReadersFollowTheUsersCutoverSwitch(t *testing.T) {
	ctx := context.Background()
	s := newJITStack(t, ctx)
	user, orgA := s.env.seed(t, ctx) // public membership: owner
	orgB := s.newOrg(t, ctx)
	for _, join := range []identity.OrgJoin{{OrgID: orgA, Role: "viewer"}, {OrgID: orgB, Role: "admin"}} {
		if _, err := s.owned.JoinOrg(ctx, user, join, identity.SelfSignUp(nil), nil); err != nil {
			t.Fatalf("join %v: %v", join, err)
		}
	}
	off := authapi.PGUsers{Pool: s.env.runtime}
	on := authapi.PGUsers{Pool: s.env.runtime, UsersOwned: true, Schema: s.schema}
	if role, found, err := off.Role(ctx, user, orgB); err != nil || found || role != nil {
		t.Fatalf("off: auth-only membership visible: %v %v %v", role, found, err)
	}
	if role, found, err := off.Role(ctx, user, orgA); err != nil || !found || role == nil || *role != "owner" {
		t.Fatalf("off: public role = %v %v %v, want owner", role, found, err)
	}
	if role, found, err := on.Role(ctx, user, orgB); err != nil || !found || role == nil || *role != "admin" {
		t.Fatalf("on: auth-only role = %v %v %v, want admin", role, found, err)
	}
	if role, found, err := on.Role(ctx, user, orgA); err != nil || !found || role == nil || *role != "viewer" {
		t.Fatalf("on: auth must win on role: %v %v %v, want viewer", role, found, err)
	}
	if list, err := off.Memberships(ctx, user); err != nil || len(list) != 1 {
		t.Fatalf("off memberships = %d, %v; want 1", len(list), err)
	}
	if list, err := on.Memberships(ctx, user); err != nil || len(list) != 2 {
		t.Fatalf("on memberships = %d, %v; want 2 (one per organization)", len(list), err)
	}
	for _, mode := range []struct {
		owned bool
		wantA string
		wantB bool
	}{{false, "owner", false}, {true, "viewer", true}} {
		reader := oidc.PGMemberships{Pool: s.env.runtime, UsersOwned: mode.owned, Schema: s.schema}
		role, found, err := reader.Role(ctx, user, orgA)
		if err != nil || !found || role != mode.wantA {
			t.Fatalf("oidc reader owned=%v orgA = %q %v %v, want %q", mode.owned, role, found, err, mode.wantA)
		}
		if _, found, err := reader.Role(ctx, user, orgB); err != nil || found != mode.wantB {
			t.Fatalf("oidc reader owned=%v orgB found=%v %v, want %v", mode.owned, found, err, mode.wantB)
		}
		store, err := saml.NewPGStore(s.env.runtime, s.schema)
		if err != nil {
			t.Fatal(err)
		}
		member, err := store.WithUsersOwned(mode.owned).Member(ctx, user, orgB)
		if err != nil || member != mode.wantB {
			t.Fatalf("saml reader owned=%v orgB member=%v %v, want %v", mode.owned, member, err, mode.wantB)
		}
	}
}
