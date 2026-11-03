//go:build integration

package session

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// CHAOS-11400 (acp answers-11400-plan-3): an accepted refresh exchange commits the auth.session.refreshed event in its
// own transaction (audit.Commit; the new refresh_credentials create count makes the rotation a counted state change).
// A refusal and a grace replay write no event, and a refused refresh is not a failed commit in the log.

const refreshedEventType = "auth.session.refreshed"

func (e *env) refreshedEvents(t *testing.T, ctx context.Context, session uuid.UUID) int {
	t.Helper()
	return e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1 AND attributes->>'target_id' = $2`,
		refreshedEventType, session.String())
}

// lockedBuffer is a log sink a test reads while the store writes to it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureDefaultLog routes the process default logger (the one audit.Commit logs to) into a buffer for the test.
func captureDefaultLog(t *testing.T) *lockedBuffer {
	t.Helper()
	sink := &lockedBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(sink, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return sink
}

func TestAnAcceptedRefreshCommitsOneRefreshedEvent(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	principal := e.principal(t, ctx)
	issued := e.create(t, ctx, principal)
	child := e.exchange(t, ctx, e.store, issued.Credential)
	if child.Reason != ReasonAccepted {
		t.Fatalf("Exchange = %s, want accepted", child.Reason)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1 AND outcome = 'allowed'
		AND subject_principal_id = $2 AND attributes->>'reason_code' = 'accepted' AND attributes->>'target_kind' = 'session'
		AND attributes->>'target_id' = $3`, refreshedEventType, principal.String(), issued.Session.ID.String()); n != 1 {
		t.Fatalf("%s events of the session = %d, want 1", refreshedEventType, n)
	}
	if n := e.outboxRows(t, ctx, principal.String()); n != 0 {
		t.Errorf("outbox rows = %d, want 0: session lifecycle is off the provisioning stream", n)
	}
	if next := e.exchange(t, ctx, e.store, child.Credential); next.Reason != ReasonAccepted {
		t.Fatalf("the committed successor exchanges as %s, want accepted", next.Reason)
	}
	if n := e.refreshedEvents(t, ctx, issued.Session.ID); n != 2 {
		t.Fatalf("%s events after two refreshes = %d, want 2", refreshedEventType, n)
	}
}

func TestRefusedRefreshesWriteNoEventAndNoCommitFailureLine(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	logs := captureDefaultLog(t)
	unknown, err := NewUnstoredCredential()
	if err != nil {
		t.Fatal(err)
	}
	expiredCredential := e.create(t, ctx, e.principal(t, ctx))
	e.guarded(t, ctx, `UPDATE auth.refresh_credentials SET expires_at = now() - interval '1 second' WHERE token_hash = $1`, hashCredential(expiredCredential.Credential))
	expiredSession := e.create(t, ctx, e.principal(t, ctx))
	e.guarded(t, ctx, `UPDATE auth.sessions SET expires_at = now() - interval '1 second' WHERE id = $1`, expiredSession.Session.ID.String())
	disabledPrincipal := e.principal(t, ctx)
	disabled := e.create(t, ctx, disabledPrincipal)
	e.guarded(t, ctx, `UPDATE auth.principals SET disabled_at = now() WHERE id = $1`, disabledPrincipal.String())
	revoked := e.create(t, ctx, e.principal(t, ctx))
	if ok, err := e.store.Revoke(ctx, revoked.Session.ID, Actor{}); !ok || err != nil {
		t.Fatalf("Revoke: %v %v", ok, err)
	}
	for _, c := range []struct {
		name       string
		credential contracts.SecretString
		want       Reason
	}{
		{"unstored credential", unknown, ReasonRefreshUnknown},
		{"expired credential", expiredCredential.Credential, ReasonRefreshExpired},
		{"expired session", expiredSession.Credential, ReasonSessionExpired},
		{"disabled principal", disabled.Credential, ReasonPrincipalDeactivated},
		{"revoked session", revoked.Credential, ReasonSessionRevoked},
	} {
		if got := e.exchange(t, ctx, e.store, c.credential); got.Reason != c.want || got.Credential.Reveal() != "" {
			t.Errorf("%s: %s, want %s and no credential", c.name, got.Reason, c.want)
		}
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1`, refreshedEventType); n != 0 {
		t.Errorf("%s events after refusals = %d, want 0", refreshedEventType, n)
	}
	if strings.Contains(logs.String(), "audit: Commit failed") {
		t.Errorf("a refused refresh logged a failed commit:\n%s", logs.String())
	}
}

// A grace replay rotates nothing: it answers the same successor and writes no event.
func TestAGraceReplayWritesNoRefreshedEvent(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	issued := e.create(t, ctx, e.principal(t, ctx))
	child := e.rotatedAgo(t, ctx, issued, GraceWindow-15*time.Second)
	replay := e.exchange(t, ctx, e.store, issued.Credential)
	if replay.Reason != ReasonGraceReplay || replay.Credential.Reveal() != child.Credential.Reveal() {
		t.Fatalf("replay = %s (same successor %v), want refresh_grace_replay", replay.Reason, replay.Credential.Reveal() == child.Credential.Reveal())
	}
	if n := e.refreshedEvents(t, ctx, issued.Session.ID); n != 0 {
		t.Fatalf("%s events after a grace replay = %d, want 0", refreshedEventType, n)
	}
}

// The legacy exchange of a go-api refresh token is a refresh too: one event on its first exchange, none on a replay.
func TestTheLegacyExchangeCommitsOneRefreshedEvent(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	user := e.goUser(t, ctx, 0)
	e.goRefresh(t, ctx, user, uuid.New(), "jti-refreshed", time.Hour)
	first, err := e.store.ExchangeLegacy(ctx, legacy(user, "jti-refreshed"))
	if err != nil || first.Reason != ReasonAccepted {
		t.Fatalf("ExchangeLegacy = %s, %v", first.Reason, err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1 AND subject_principal_id = $2
		AND attributes->>'target_id' = $3 AND attributes->>'reason_code' = 'accepted'`, refreshedEventType, user.String(), first.Session.ID.String()); n != 1 {
		t.Fatalf("%s events of the legacy exchange = %d, want 1", refreshedEventType, n)
	}
	if second, err := e.store.ExchangeLegacy(ctx, legacy(user, "jti-refreshed")); err != nil || second.Reason != ReasonGraceReplay {
		t.Fatalf("replay = %s, %v; want refresh_grace_replay", second.Reason, err)
	}
	if n := e.refreshedEvents(t, ctx, first.Session.ID); n != 1 {
		t.Fatalf("%s events after a legacy replay = %d, want still 1", refreshedEventType, n)
	}
}

// The statements a build without the change runs outside audit.Commit (Create's session and credential insert, and
// rotate's successor insert) are unchanged on the schema with the refresh_credentials create count.
func TestTheOldBuildsWritesStillRunOutsideCommit(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	issued := e.create(t, ctx, e.principal(t, ctx))
	child := e.rotatedAgo(t, ctx, issued, time.Second)
	if child.Credential.Reveal() == "" {
		t.Fatal("no successor")
	}
	if n := e.liveCredentials(t, ctx, issued.Session.ID); n != 1 {
		t.Fatalf("live credentials = %d after an outside-Commit rotation, want 1", n)
	}
}
