//go:build integration

package session

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

func signInEvent(principal uuid.UUID) audit.AuditEvent {
	text := principal.String()
	return audit.AuditEvent{
		EventType: "auth.login.succeeded", Outcome: audit.OutcomeAllowed, ActorPrincipalID: &text, SubjectPrincipalID: &text,
		Attributes: map[string]any{"reason_code": "accepted", "target_kind": "session", "provider": "local"},
	}
}

// A sign-in's session and its audit row commit together, as the runtime role,
// and both carry the request's client address; the row falls back to the
// context's request id.
func TestCreateAuditedCommitsTheSessionWithItsAuditRow(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	principal := e.principal(t, ctx)
	id := uuid.New()
	ctx = clientaddr.With(requestid.With(ctx, id), netip.MustParseAddr("198.51.100.7"))
	issued, err := e.store.CreateAudited(ctx, principal, nil, []string{"pwd"}, signInEvent(principal))
	if err != nil {
		t.Fatalf("CreateAudited: %v", err)
	}
	if e.liveCredentials(t, ctx, issued.Session.ID) != 1 {
		t.Fatal("the session has no live credential")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events
		WHERE event_type = 'auth.login.succeeded' AND request_id = $1 AND host(client_ip) = '198.51.100.7'
		  AND subject_principal_id = $2`, id.String(), principal.String()); n != 1 {
		t.Fatalf("%d audit rows with the request id and client address, want 1", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE id = $1 AND host(client_ip) = '198.51.100.7'`,
		issued.Session.ID.String()); n != 1 {
		t.Fatal("the session does not carry the client address")
	}
	if e.outboxRows(t, ctx, principal.String()) != 0 {
		t.Fatal("a sign-in wrote a provisioning event")
	}
}

func TestCreateAuditedForADisabledPrincipalWritesNothing(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	principal := e.principal(t, ctx)
	e.guarded(t, ctx, `UPDATE auth.principals SET disabled_at = now() WHERE id = $1`, principal.String())
	_, err := e.store.CreateAudited(ctx, principal, nil, []string{"pwd"}, signInEvent(principal))
	if !errors.Is(err, ErrPrincipalUnavailable) {
		t.Fatalf("err = %v, want ErrPrincipalUnavailable", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.login.succeeded'`); n != 0 {
		t.Fatalf("%d audit rows for a sign-in that started no session", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, principal.String()); n != 0 {
		t.Fatal("a session was written")
	}
}

func TestRevokeByCredentialEndsOnlyTheSessionOfALiveCredential(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	principal := e.principal(t, ctx)
	issued := e.create(t, ctx, principal)
	rotated := e.exchange(t, ctx, e.store, issued.Credential)

	revoked, err := e.store.RevokeByCredential(ctx, issued.Credential, Actor{})
	if err != nil || revoked {
		t.Fatalf("a consumed credential: revoked %v err %v, want nothing", revoked, err)
	}
	for _, credential := range []contracts.SecretString{
		contracts.NewSecretString("garbage"),
		contracts.NewSecretString(credentialPrefix + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
	} {
		if revoked, err := e.store.RevokeByCredential(ctx, credential, Actor{}); err != nil || revoked {
			t.Fatalf("an unknown credential: revoked %v err %v", revoked, err)
		}
	}
	if e.sessionRevoked(t, ctx, issued.Session.ID) {
		t.Fatal("a credential that no longer works revoked the session")
	}

	id := uuid.New()
	revoked, err = e.store.RevokeByCredential(requestid.With(ctx, id), rotated.Credential, Actor{})
	if err != nil || !revoked {
		t.Fatalf("the live credential: revoked %v err %v", revoked, err)
	}
	if !e.sessionRevoked(t, ctx, issued.Session.ID) || e.liveCredentials(t, ctx, issued.Session.ID) != 0 {
		t.Fatal("the session or a credential of it is still live")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events
		WHERE event_type = 'auth.session.revoked' AND attributes->>'target_id' = $1 AND request_id = $2`,
		issued.Session.ID.String(), id.String()); n != 1 {
		t.Fatalf("%d revocation audit rows with the request id, want 1", n)
	}
	if revoked, err := e.store.RevokeByCredential(ctx, rotated.Credential, Actor{}); err != nil || revoked {
		t.Fatalf("a second sign-out: revoked %v err %v", revoked, err)
	}
	if e.auditRows(t, ctx, "auth.session.revoked", issued.Session.ID.String()) != 1 {
		t.Fatal("a second sign-out wrote a second audit row")
	}
}
