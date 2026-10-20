//go:build integration

package session

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// stampUser stores the password stamp of a principal on the database clock;
// clause is the SQL expression of the stamp (NULL for none).
func (e *env) stampUser(t *testing.T, ctx context.Context, principal uuid.UUID, clause string) {
	t.Helper()
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.users (principal_id, email, email_lower, password_updated_at)
		VALUES ($1, $2, $2, `+clause+`)
		ON CONFLICT (principal_id) DO UPDATE SET password_updated_at = EXCLUDED.password_updated_at`,
		principal.String(), principal.String()+"@example.com"); err != nil {
		t.Fatalf("stamp: %v", err)
	}
}

// A session issued before the principal's password stamp is void: Current
// reports it revoked at the stamp and Exchange refuses its credential, with
// no RevokeAll having run.
func TestSessionIssuedBeforeThePasswordStampIsVoid(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	p := e.principal(t, ctx)
	old := e.create(t, ctx, p)
	e.stampUser(t, ctx, p, "clock_timestamp()")

	record, err := e.store.Current(ctx, old.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if record.RevokedAt == nil {
		t.Fatal("Current reports the pre-stamp session as live")
	}
	if got := e.exchange(t, ctx, e.store, old.Credential); got.Reason != ReasonSessionRevoked {
		t.Errorf("Exchange reason = %s, want %s", got.Reason, ReasonSessionRevoked)
	}
}

// A session issued after the stamp, by the same change or by another device
// later, stays live.
func TestSessionsIssuedAfterThePasswordStampStayLive(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	p := e.principal(t, ctx)
	e.stampUser(t, ctx, p, "clock_timestamp()")
	first := e.create(t, ctx, p)
	time.Sleep(20 * time.Millisecond)
	second := e.create(t, ctx, p)
	for _, issued := range []Issued{first, second} {
		record, err := e.store.Current(ctx, issued.Session.ID)
		if err != nil || record.RevokedAt != nil {
			t.Fatalf("Current = %+v, %v; want a live session", record, err)
		}
		if got := e.exchange(t, ctx, e.store, issued.Credential); got.Reason != ReasonAccepted {
			t.Errorf("Exchange reason = %s, want %s", got.Reason, ReasonAccepted)
		}
	}
}

// No stamp (a principal that never set a password here) voids nothing, and a
// stamp equal to the session's creation time does not either: the comparison
// is strict.
func TestPasswordStampVoidsOnlyStrictlyOlderSessions(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)

	none := e.principal(t, ctx)
	e.stampUser(t, ctx, none, "NULL")
	noneSession := e.create(t, ctx, none)
	if record, err := e.store.Current(ctx, noneSession.Session.ID); err != nil || record.RevokedAt != nil {
		t.Errorf("NULL stamp: Current = %+v, %v; want live", record, err)
	}

	equal := e.principal(t, ctx)
	equalSession := e.create(t, ctx, equal)
	e.stampUser(t, ctx, equal, "(SELECT created_at FROM auth.sessions WHERE id = '"+equalSession.Session.ID.String()+"')")
	if record, err := e.store.Current(ctx, equalSession.Session.ID); err != nil || record.RevokedAt != nil {
		t.Errorf("equal stamp: Current = %+v, %v; want live", record, err)
	}
}

// An explicit revoke keeps its own revoked_at; the stamp only fills the gap.
func TestExplicitRevokeKeepsItsTimestampUnderAStamp(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	p := e.principal(t, ctx)
	issued := e.create(t, ctx, p)
	if ok, err := e.store.Revoke(ctx, issued.Session.ID, Actor{}); err != nil || !ok {
		t.Fatalf("Revoke = %v, %v", ok, err)
	}
	before, _ := e.store.Current(ctx, issued.Session.ID)
	e.stampUser(t, ctx, p, "clock_timestamp()")
	after, _ := e.store.Current(ctx, issued.Session.ID)
	if before.RevokedAt == nil || after.RevokedAt == nil || !before.RevokedAt.Equal(*after.RevokedAt) {
		t.Errorf("revoked_at moved: %v -> %v", before.RevokedAt, after.RevokedAt)
	}
}

// A stamp-void session answers the plain session_revoked refusal. The derived
// revoked_at is not an event: no reuse incident, no family revoke and no
// revoke audit row are written, whether the presented credential is live or
// already consumed.
func TestStampVoidSessionIsNotRefreshReuse(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	p := e.principal(t, ctx)
	issued := e.create(t, ctx, p)
	rotated := e.exchange(t, ctx, e.store, issued.Credential)
	if rotated.Reason != ReasonAccepted {
		t.Fatalf("rotation reason = %s", rotated.Reason)
	}
	e.stampUser(t, ctx, p, "clock_timestamp()")

	audits := func() int {
		return e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type IN ('auth.session.refresh_reuse_revoked', 'auth.session.revoked', 'auth.session.revoked_all')`)
	}
	before := audits()
	for name, credential := range map[string]Issued{"live credential": {Credential: rotated.Issued.Credential}, "consumed credential": issued} {
		if got := e.exchange(t, ctx, e.store, credential.Credential); got.Reason != ReasonSessionRevoked {
			t.Errorf("%s: reason = %s, want %s", name, got.Reason, ReasonSessionRevoked)
		}
	}
	if after := audits(); after != before {
		t.Errorf("revoke or reuse audit rows went from %d to %d", before, after)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE id = $1 AND revoked_at IS NOT NULL`, issued.Session.ID.String()); n != 0 {
		t.Error("the derived revocation was written to sessions.revoked_at")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials WHERE session_id = $1 AND revoked_at IS NOT NULL`, issued.Session.ID.String()); n != 0 {
		t.Error("a family revoke was written")
	}
}
