//go:build integration

package local

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// TestAServiceProviderAccountGetsNoResetLinkPasswordOrLocalLoginUnderUsersOwned runs Forgot -> Reset -> Login under
// users owned, against live Postgres, for a verified service-provider account with a deliverable address and the
// placeholder shape but not a proof principal id. Each step refuses with the answer of an unknown account; a local
// account on the same rig goes through all three, so the chain itself works.
func TestAServiceProviderAccountGetsNoResetLinkPasswordOrLocalLoginUnderUsersOwned(t *testing.T) {
	ctx := context.Background()
	rig := newEndRig(t, ctx)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	store, err := identity.New(identity.Config{Pool: rig.env.runtime, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	login, err := New(Config{Users: IdentityUsers{Store: store}, Sessions: rig.sessions,
		Attempts: NewPGAttempts(rig.env.runtime, schema), LockoutKey: contracts.NewSecretString(testKey), Compare: bcryptMatches})
	if err != nil {
		t.Fatal(err)
	}
	mail, ok := rig.flows.mail.(*liveMail)
	if !ok {
		t.Fatalf("the rig mailer is %T, want *liveMail", rig.flows.mail)
	}
	const newPassword = "A-new-pass-7788"

	// Control: a local account resets and signs in.
	local := rig.account(t, ctx, "Old-pass-12345")
	if err := rig.accounts.SetEmailVerified(ctx, local); err != nil {
		t.Fatal(err)
	}
	var localEmail string
	if err := rig.env.admin.QueryRow(ctx, `SELECT email FROM public.users WHERE id = $1`, local).Scan(&localEmail); err != nil {
		t.Fatal(err)
	}
	if err := rig.flows.Forgot(ctx, localEmail); err != nil {
		t.Fatal(err)
	}
	sent := mail.last(t)
	if !sent.Deliver {
		t.Fatal("control: Forgot did not deliver a reset link to a local account")
	}
	if out, err := rig.flows.Reset(ctx, sent.Secret.Reveal(), newPassword); err != nil || out.Kind != FlowOK {
		t.Fatalf("control: Reset = %+v, %v", out, err)
	}
	if out, err := login.Login(ctx, Credentials{Email: localEmail, Password: newPassword}); err != nil || out.Kind != KindSuccess {
		t.Fatalf("control: Login = %+v, %v", out, err)
	}

	// The service-provider account: placeholder shape, verified, deliverable address, not a proof principal id.
	svc := uuid.New()
	svcEmail := "svc-" + svc.String() + "@example.com"
	if _, err := rig.env.admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, auth_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'service', NULL, true, true, false, 0, now(), now())`, svc, svcEmail); err != nil {
		t.Fatal(err)
	}
	// Its auth rows as the users backfill writes them.
	if _, err := store.BackfillUser(ctx, identity.BackfillTarget{ID: svc, Email: svcEmail, Active: true, Verified: true}, false); err != nil {
		t.Fatalf("backfill the service account: %v", err)
	}
	unknown, err := login.Login(ctx, Credentials{Email: "nobody-" + svc.String() + "@example.com", Password: newPassword})
	if err != nil {
		t.Fatal(err)
	}
	links := func() int {
		return rig.env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials WHERE principal_id = $1`, svc)
	}

	// Step 1: Forgot stores no link and delivers nothing (the unknown-address decoy).
	if err := rig.flows.Forgot(ctx, svcEmail); err != nil {
		t.Fatal(err)
	}
	if got := mail.last(t); got.Deliver || got.To != svcEmail {
		t.Errorf("Forgot for the service account: deliver=%v to=%q; want the decoy (deliver=false) to the address", got.Deliver, got.To)
	}
	if n := links(); n != 0 {
		t.Errorf("Forgot stored %d link credential(s) for the service account, want 0", n)
	}

	// Step 2: a reset link that exists anyway reads as an unknown link and sets no password.
	if err := rig.links.IssueBound(ctx, IssueRequest{PrincipalID: svc, Purpose: PurposePasswordReset, TokenHash: linkHash("svc-reset-secret"), TTL: ResetTTL}); err != nil {
		t.Fatal(err)
	}
	if out, err := rig.flows.Reset(ctx, "svc-reset-secret", newPassword); err != nil || out.Kind != FlowInvalidToken {
		t.Errorf("Reset with the service account's link = %+v, %v; want %s", out, err, FlowInvalidToken)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.users WHERE principal_id = $1 AND password_hash IS NOT NULL`, svc); n != 0 {
		t.Errorf("the service account has a password in auth.users after Reset")
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM public.users WHERE id = $1 AND password_hash IS NOT NULL`, svc); n != 0 {
		t.Errorf("the service account has a password in public.users after Reset")
	}

	// Step 3: even with a password in auth.users, the owned lookup gives no local login.
	hash, err := cheapHash(newPassword)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.BackfillUser(ctx, identity.BackfillTarget{ID: svc, Email: svcEmail, Active: true, Verified: true, PasswordHash: &hash}, false); err != nil {
		t.Fatalf("give the service account a password in auth.users: %v", err)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.users WHERE principal_id = $1 AND password_hash IS NOT NULL`, svc); n != 1 {
		t.Fatalf("the service account has no password in auth.users after the seed (%d)", n)
	}
	out, err := login.Login(ctx, Credentials{Email: svcEmail, Password: newPassword})
	if err != nil {
		t.Fatal(err)
	}
	if out.Kind == KindSuccess || out.Reason != unknown.Reason {
		t.Errorf("Login of the service account = kind %s reason %s; want a refusal with the unknown-account reason %s", out.Kind, out.Reason, unknown.Reason)
	}
	user, err := IdentityUsers{Store: store}.ByEmail(ctx, svcEmail)
	if err != nil || user == nil || user.PasswordHash != nil || !identity.IsServiceProvider(user.AuthProvider) {
		t.Errorf("owned lookup of the service account = %+v, %v; want no password and the service provider", user, err)
	}
}
