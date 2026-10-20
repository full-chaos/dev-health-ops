//go:build integration

package session

import (
	"context"
	"errors"
	"net/netip"
	"reflect"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/useragent"
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
	ctx = useragent.With(ctx, "Mozilla/5.0 (sign-in test) \u00e9")
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
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE id = $1 AND user_agent = $2`,
		issued.Session.ID.String(), "Mozilla/5.0 (sign-in test) \u00e9"); n != 1 {
		t.Fatal("the session does not carry the request's user agent")
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

// signInRowsQuery reads the columns of a session and its first credential
// that a sign-in decides; ids, hashes and instants differ per row by
// construction and are compared as lifetimes in whole seconds.
const signInRowsQuery = `
	SELECT s.principal_id::text, s.organization_id::text, s.amr, s.revoked_at IS NULL, s.revoked_reason,
	       s.client_ip::text, s.user_agent, s.last_seen_at IS NULL, s.revision,
	       isfinite(s.expires_at), CASE WHEN isfinite(s.expires_at) THEN round(extract(epoch FROM s.expires_at - s.issued_at)) END,
	       s.auth_time = s.issued_at, s.created_at = s.issued_at,
	       r.principal_id::text, r.parent_id IS NULL, r.consumed_at IS NULL, r.revoked_at IS NULL,
	       r.superseded_by IS NULL, round(extract(epoch FROM r.expires_at - r.issued_at)),
	       (SELECT count(*) FROM auth.refresh_credentials c WHERE c.session_id = s.id)
	FROM auth.sessions s JOIN auth.refresh_credentials r ON r.session_id = s.id
	WHERE s.id = $1`

func (e *env) signInRows(t *testing.T, ctx context.Context, id uuid.UUID) []any {
	t.Helper()
	rows, err := e.admin.Query(ctx, signInRowsQuery, id.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("session %s has no credential row", id)
	}
	values, err := rows.Values()
	if err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Fatalf("session %s has more than one credential row", id)
	}
	return values
}

// Create and CreateAudited are two writers of the same rows until CHAOS-10411
// makes Create call insertSession: for the same input they write the same
// session and credential, column for column.
func TestCreateAndCreateAuditedWriteTheSameRows(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	principal := e.principal(t, ctx)
	for _, amr := range [][]string{{"pwd"}, {"fed", "mfa"}} {
		plain, err := e.store.Create(ctx, principal, nil, amr)
		if err != nil {
			t.Fatal(err)
		}
		audited, err := e.store.CreateAudited(ctx, principal, nil, amr, signInEvent(principal))
		if err != nil {
			t.Fatal(err)
		}
		got, want := e.signInRows(t, ctx, audited.Session.ID), e.signInRows(t, ctx, plain.Session.ID)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("amr %v: CreateAudited wrote %v, Create wrote %v", amr, got, want)
		}
		if !reflect.DeepEqual(plain.Session.AMR, audited.Session.AMR) || plain.Session.Revision != audited.Session.Revision {
			t.Fatalf("amr %v: records differ: %+v vs %+v", amr, plain.Session, audited.Session)
		}
	}
}
