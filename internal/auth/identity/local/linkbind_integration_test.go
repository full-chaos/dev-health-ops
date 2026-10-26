//go:build integration

package local

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

// goAPIPasswordWrite changes the password the way go-api's change-password does
// (internal/api/session/changepassword.go): hash and token_version in one
// statement, outside auth-service.
func goAPIPasswordWrite(t *testing.T, ctx context.Context, rig *endRig, id uuid.UUID) {
	t.Helper()
	hash, err := cheapHash("Out-of-band-pass-99")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rig.env.admin.Exec(ctx, `UPDATE public.users SET password_hash = $2, token_version = token_version + 1, updated_at = now() WHERE id = $1`, id, hash); err != nil {
		t.Fatalf("go-api style password write: %v", err)
	}
}

func forgotSecret(t *testing.T, ctx context.Context, rig *endRig, id uuid.UUID) string {
	t.Helper()
	var email string
	if err := rig.env.admin.QueryRow(ctx, `SELECT email FROM public.users WHERE id = $1`, id).Scan(&email); err != nil {
		t.Fatal(err)
	}
	if err := rig.flows.Forgot(ctx, email); err != nil {
		t.Fatal(err)
	}
	sent := rig.flows.mail.(*liveMail).last(t)
	if !sent.Deliver {
		t.Fatal("Forgot delivered no reset link")
	}
	return sent.Secret.Reveal()
}

// TestAResetLinkIsUnknownAfterAPasswordWriteOutsideAuthService: a reset link
// issued under users owned does not redeem after the password was written by
// go-api (token_version raised), and the password and the link stay as they
// are; without that write the same flow redeems.
func TestAResetLinkIsUnknownAfterAPasswordWriteOutsideAuthService(t *testing.T) {
	ctx := context.Background()
	rig := newEndRig(t, ctx)

	kept := rig.account(t, ctx, "Old-pass-12345")
	secret := forgotSecret(t, ctx, rig, kept)
	goAPIPasswordWrite(t, ctx, rig, kept)
	before := rig.passwordHash(t, ctx, kept)
	if out, err := rig.flows.Reset(ctx, secret, "A-new-pass-7788"); err != nil || out.Kind != FlowInvalidToken {
		t.Fatalf("Reset after an outside password write = %+v, %v; want %s", out, err, FlowInvalidToken)
	}
	if after := rig.passwordHash(t, ctx, kept); after != before {
		t.Error("the refused reset changed the auth.users password")
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials WHERE principal_id = $1 AND consumed_at IS NOT NULL`, kept); n != 0 {
		t.Errorf("the refused reset consumed %d link(s)", n)
	}

	hash, err := cheapHash("Old-pass-12345")
	if err != nil {
		t.Fatal(err)
	}
	control, err := rig.accounts.ProvisionLocal(ctx, ProvisionLocalRequest{Email: uuid.NewString() + "@example.com", PasswordHash: hash})
	if err != nil {
		t.Fatalf("ProvisionLocal: %v", err)
	}
	if out, err := rig.flows.Reset(ctx, forgotSecret(t, ctx, rig, control), "A-new-pass-7788"); err != nil || out.Kind != FlowOK {
		t.Fatalf("control Reset with no password write in between = %+v, %v; want ok", out, err)
	}
}

// peekThenWrite is the account store with a go-api style password write right
// after Reset has read the link: the consume in the password transaction must
// then refuse.
type peekThenWrite struct {
	IdentityAccounts
	write func()
}

func (p peekThenWrite) PeekResetLink(ctx context.Context, tokenHash [32]byte) (uuid.UUID, bool, error) {
	id, ok, err := p.IdentityAccounts.PeekResetLink(ctx, tokenHash)
	if ok && err == nil {
		p.write()
	}
	return id, ok, err
}

// TestAResetLinkIsUnusableWhenThePasswordChangesBetweenItsReadAndItsConsume:
// the binding is checked again in the password transaction.
func TestAResetLinkIsUnusableWhenThePasswordChangesBetweenItsReadAndItsConsume(t *testing.T) {
	ctx := context.Background()
	rig := newEndRig(t, ctx)
	id := rig.account(t, ctx, "Old-pass-12345")
	secret := forgotSecret(t, ctx, rig, id)
	flows, err := NewFlows(FlowsConfig{Accounts: peekThenWrite{IdentityAccounts: rig.accounts, write: func() { goAPIPasswordWrite(t, ctx, rig, id) }},
		Links: rig.links, Sessions: rig.sessions, Mail: &liveMail{}, Locker: rig.links, UsersOwnedByAuth: true, Hash: cheapHash})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := flows.Reset(ctx, secret, "A-new-pass-7788"); err != nil || out.Kind != FlowInvalidToken {
		t.Fatalf("Reset with a password write between read and consume = %+v, %v; want %s", out, err, FlowInvalidToken)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials WHERE principal_id = $1 AND consumed_at IS NOT NULL`, id); n != 0 {
		t.Errorf("the refused reset consumed %d link(s)", n)
	}
}

// TestAResetLinkWithoutAnIssuedTokenVersionNeverRedeems: a row from before the
// column (NULL) is an unknown link.
func TestAResetLinkWithoutAnIssuedTokenVersionNeverRedeems(t *testing.T) {
	ctx := context.Background()
	rig := newEndRig(t, ctx)
	id := rig.account(t, ctx, "Old-pass-12345")
	unbound := linkHash("unbound-secret")
	if _, err := rig.env.admin.Exec(ctx, `INSERT INTO auth.link_credentials (principal_id, purpose, token_hash, expires_at, created_at)
VALUES ($1, $2, $3, now() + interval '1 hour', now())`, id, string(PurposePasswordReset), unbound[:]); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := rig.links.PeekResetLink(ctx, linkHash("unbound-secret")); err != nil || ok {
		t.Fatalf("PeekResetLink of an unbound row = ok %v, %v; want unknown", ok, err)
	}
	if out, err := rig.flows.Reset(ctx, "unbound-secret", "A-new-pass-7788"); err != nil || out.Kind != FlowInvalidToken {
		t.Fatalf("Reset with an unbound row = %+v, %v; want %s", out, err, FlowInvalidToken)
	}
}

// TestIssueBoundStoresTheCounterAndNoPasswordMaterial: the row holds the
// user's token_version at issue; no column of the row holds the password hash.
func TestIssueBoundStoresTheCounterAndNoPasswordMaterial(t *testing.T) {
	ctx := context.Background()
	rig := newEndRig(t, ctx)
	id := rig.account(t, ctx, "Old-pass-12345")
	forgotSecret(t, ctx, rig, id)
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials l JOIN public.users u ON u.id = l.principal_id
WHERE l.principal_id = $1 AND l.consumed_at IS NULL AND l.issued_token_version = u.token_version`, id); n != 1 {
		t.Fatalf("live reset links bound to the current token_version = %d, want 1", n)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials l JOIN public.users u ON u.id = l.principal_id
WHERE l.principal_id = $1 AND position(u.password_hash IN l::text) > 0`, id); n != 0 {
		t.Fatal("a link row carries the password hash")
	}
}
