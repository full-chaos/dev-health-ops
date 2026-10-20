//go:build integration

package local

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// revokeDown is the B1 store whose RevokeAll always fails.
type revokeDown struct{ *session.Store }

func (revokeDown) RevokeAll(context.Context, uuid.UUID, session.Actor) (int, error) {
	return 0, errors.New("session store unavailable")
}

func cheapHash(password string) (string, error) {
	h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	return strings.Replace(string(h), "$2a$", "$2b$", 1), err
}

type stampRig struct {
	env      *pgEnv
	links    *PGLinks
	sessions *session.Store
	accounts IdentityAccounts
	flows    *Flows
}

func newStampRig(t *testing.T, ctx context.Context) *stampRig {
	t.Helper()
	env, links := linkEnv(t, ctx)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	store, err := identity.New(identity.Config{Pool: env.runtime, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := session.New(session.Config{Pool: env.runtime, Schema: schema,
		SuccessorKey: contracts.NewSecretString("AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8")})
	if err != nil {
		t.Fatal(err)
	}
	rig := &stampRig{env: env, links: links, sessions: sessions, accounts: IdentityAccounts{Store: store}}
	rig.flows, err = NewFlows(FlowsConfig{Accounts: rig.accounts, Links: links, Sessions: revokeDown{sessions}, Mail: &liveMail{},
		Locker: links, UsersOwnedByAuth: true, Hash: cheapHash})
	if err != nil {
		t.Fatal(err)
	}
	return rig
}

func (r *stampRig) account(t *testing.T, ctx context.Context, password string) uuid.UUID {
	t.Helper()
	hash, err := cheapHash(password)
	if err != nil {
		t.Fatal(err)
	}
	id, err := r.accounts.ProvisionLocal(ctx, ProvisionLocalRequest{Email: uuid.NewString() + "@example.com", PasswordHash: hash})
	if err != nil {
		t.Fatalf("ProvisionLocal: %v", err)
	}
	return id
}

func (r *stampRig) live(t *testing.T, ctx context.Context, issued session.Issued) bool {
	t.Helper()
	record, err := r.sessions.Current(ctx, issued.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	return record.RevokedAt == nil
}

// A reset whose RevokeAll fails still ends the older sessions: the password
// stamp voids them at verify, and a session created afterwards is live.
func TestResetWithRevokeAllDownRefusesTheOldSession(t *testing.T) {
	ctx := context.Background()
	rig := newStampRig(t, ctx)
	id := rig.account(t, ctx, "Old-pass-12345")
	old, err := rig.sessions.Create(ctx, id, nil, []string{"pwd"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.links.Issue(ctx, IssueRequest{PrincipalID: id, Purpose: PurposePasswordReset, TokenHash: linkHash("reset-secret"), TTL: ResetTTL}); err != nil {
		t.Fatal(err)
	}
	out, err := rig.flows.Reset(ctx, "reset-secret", "A-new-pass-77")
	if err != nil || out.Kind != FlowOK || !errors.Is(out.RevokeErr, ErrSessionsNotRevoked) {
		t.Fatalf("Reset = %+v, %v", out, err)
	}
	if rig.live(t, ctx, old) {
		t.Fatal("the pre-reset session is still live after the password changed")
	}
	if res, err := rig.sessions.Exchange(ctx, old.Credential); err != nil || res.Reason != session.ReasonSessionRevoked {
		t.Errorf("Exchange of the old credential = %+v, %v; want %s", res, err, session.ReasonSessionRevoked)
	}
	time.Sleep(20 * time.Millisecond)
	after, err := rig.sessions.Create(ctx, id, nil, []string{"pwd"})
	if err != nil || !rig.live(t, ctx, after) {
		t.Errorf("a session created after the reset is not live: %v", err)
	}
}

// A change keeps the caller's own new session and a later second device live,
// and voids the older device, even with RevokeAll down.
func TestChangeKeepsTheWinnersSessionAndALaterDeviceAndVoidsTheOlderOne(t *testing.T) {
	ctx := context.Background()
	rig := newStampRig(t, ctx)
	id := rig.account(t, ctx, "Old-pass-12345")
	older, err := rig.sessions.Create(ctx, id, nil, []string{"pwd"})
	if err != nil {
		t.Fatal(err)
	}
	out, err := rig.flows.Change(ctx, ChangeRequest{PrincipalID: id, Current: "Old-pass-12345", New: "Brand-new-pass-42"})
	if err != nil || out.Kind != FlowOK || out.Issued == nil {
		t.Fatalf("Change = %+v, %v", out, err)
	}
	if rig.live(t, ctx, older) {
		t.Error("the older device's session is live after the change")
	}
	if !rig.live(t, ctx, *out.Issued) {
		t.Error("the session the change issued to the caller is void")
	}
	time.Sleep(20 * time.Millisecond)
	second, err := rig.sessions.Create(ctx, id, nil, []string{"pwd"})
	if err != nil || !rig.live(t, ctx, second) {
		t.Errorf("a second device's session after the change is not live: %v", err)
	}
}
