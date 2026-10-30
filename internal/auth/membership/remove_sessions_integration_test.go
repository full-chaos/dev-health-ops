//go:build integration

package membership

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

var removeSessionsKey = base64.RawURLEncoding.EncodeToString([]byte("membership-remove-successor-0032"))

// sessionEnv is a membership store that ends a removed member's sessions, the
// session store behind it, and the store's replan counter.
type sessionEnv struct {
	*env
	members  *Store
	sessions *session.Store
	reader   *sdkmetric.ManualReader
}

func newSessionEnv(t *testing.T) *sessionEnv {
	t.Helper()
	e := getEnv(t)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := session.New(session.Config{Pool: e.runtime, Schema: schema, SuccessorKey: contracts.NewSecretString(removeSessionsKey)})
	if err != nil {
		t.Fatal(err)
	}
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("membership-remove-sessions")
	members, err := New(Config{Pool: e.runtime, Schema: schema, OwnsUsers: true, Sessions: sessions, Meter: meter})
	if err != nil {
		t.Fatal(err)
	}
	return &sessionEnv{env: e, members: members, sessions: sessions, reader: reader}
}

// replanned is how many remove attempts the store planned again.
func (s *sessionEnv) replanned(t *testing.T) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := s.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	var n int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			data, ok := m.Data.(metricdata.Sum[int64])
			if !ok || m.Name != "auth.membership.mutations" {
				continue
			}
			for _, p := range data.DataPoints {
				verb, _ := p.Attributes.Value("verb")
				outcome, _ := p.Attributes.Value("outcome")
				if verb.AsString() == string(verbRemove) && outcome.AsString() == "replanned" {
					n += p.Value
				}
			}
		}
	}
	return n
}

func (s *sessionEnv) open(t *testing.T, ctx context.Context, principal uuid.UUID, org *uuid.UUID) uuid.UUID {
	t.Helper()
	issued, err := s.sessions.Create(ctx, principal, org, []string{"pwd"})
	if err != nil {
		t.Fatalf("session Create: %v", err)
	}
	return issued.Session.ID
}

// goToken seeds a live go-api refresh_tokens row and returns its id.
func (s *sessionEnv) goToken(t *testing.T, ctx context.Context, user uuid.UUID, org *uuid.UUID) uuid.UUID {
	t.Helper()
	id := uuid.New()
	digest := sha256.Sum256([]byte(id.String()))
	if _, err := s.admin.Exec(ctx, `INSERT INTO public.refresh_tokens (id, user_id, org_id, token_hash, family_id, expires_at, created_at)
		VALUES ($1, $2, $3, $4, gen_random_uuid(), now() + interval '1 day', now())`, id, user, org, hex.EncodeToString(digest[:])); err != nil {
		t.Fatalf("seed go-api refresh token: %v", err)
	}
	return id
}

// sessionEnded reports whether the session is revoked and no credential of it
// is left unrevoked.
func (s *sessionEnv) sessionEnded(t *testing.T, ctx context.Context, id uuid.UUID) bool {
	t.Helper()
	revoked := s.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE id = $1 AND revoked_at IS NOT NULL AND revoked_reason = 'session_revoked'`, id)
	live := s.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials WHERE session_id = $1 AND revoked_at IS NULL`, id)
	return revoked == 1 && live == 0
}

// sessionLive reports whether the session and every credential of it are unrevoked.
func (s *sessionEnv) sessionLive(t *testing.T, ctx context.Context, id uuid.UUID) bool {
	t.Helper()
	unrevoked := s.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE id = $1 AND revoked_at IS NULL`, id)
	revokedCreds := s.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials WHERE session_id = $1 AND revoked_at IS NOT NULL`, id)
	creds := s.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials WHERE session_id = $1`, id)
	return unrevoked == 1 && revokedCreds == 0 && creds > 0
}

func (s *sessionEnv) goTokenRevoked(t *testing.T, ctx context.Context, id uuid.UUID) bool {
	t.Helper()
	return s.count(t, ctx, `SELECT count(*) FROM public.refresh_tokens WHERE id = $1 AND revoked_at IS NOT NULL`, id) == 1
}

func (s *sessionEnv) sessionAuditRows(t *testing.T, ctx context.Context, principal uuid.UUID) int {
	return s.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type LIKE 'auth.session.%' AND subject_principal_id = $1`, principal.String())
}

// A removal ends the member's sessions and go-api refresh tokens of that
// organization in its own commit; its sessions of other organizations, its
// sessions with no organization and other members' sessions stay live. The
// removal's audit row is the only audit row.
func TestRemoveEndsTheMembersSessionsOfThatOrganizationOnly(t *testing.T) {
	s := newSessionEnv(t)
	ctx := context.Background()
	org, other := s.seedOrg(t, ctx), s.seedOrg(t, ctx)
	user, peer, owner := s.seedUser(t, ctx), s.seedUser(t, ctx), s.seedUser(t, ctx)
	s.add(t, ctx, org, owner, "owner")
	m := s.add(t, ctx, org, user, "member")
	s.add(t, ctx, org, peer, "member")
	s.add(t, ctx, other, owner, "owner")
	s.add(t, ctx, other, user, "member")
	inOrg := []uuid.UUID{s.open(t, ctx, user, &org), s.open(t, ctx, user, &org)}
	inOther, noOrg, peerInOrg := s.open(t, ctx, user, &other), s.open(t, ctx, user, nil), s.open(t, ctx, peer, &org)
	goInOrg := []uuid.UUID{s.goToken(t, ctx, user, &org), s.goToken(t, ctx, user, &org)}
	goInOther, goNoOrg, goPeer := s.goToken(t, ctx, user, &other), s.goToken(t, ctx, user, nil), s.goToken(t, ctx, peer, &org)
	sessionAudit := s.sessionAuditRows(t, ctx, user)

	if _, err := s.members.Remove(ctx, Change{OrgID: org, PrincipalID: user, Authorize: allowAll}); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	for _, id := range inOrg {
		if !s.sessionEnded(t, ctx, id) {
			t.Errorf("session %s of the removed member in the organization is not ended", id)
		}
	}
	for name, id := range map[string]uuid.UUID{"other organization": inOther, "no organization": noOrg, "other member": peerInOrg} {
		if !s.sessionLive(t, ctx, id) {
			t.Errorf("session (%s) %s was touched", name, id)
		}
	}
	for _, id := range goInOrg {
		if !s.goTokenRevoked(t, ctx, id) {
			t.Errorf("go-api refresh token %s of the removed member in the organization is not revoked", id)
		}
	}
	for name, id := range map[string]uuid.UUID{"other organization": goInOther, "no organization": goNoOrg, "other member": goPeer} {
		if s.goTokenRevoked(t, ctx, id) {
			t.Errorf("go-api refresh token (%s) %s was revoked", name, id)
		}
	}
	if got := s.rows(t, ctx, org, user); got != (rows{"", "removed", "member"}) {
		t.Fatalf("rows after remove = %+v", got)
	}
	if n := s.auditRows(t, ctx, "auth.membership.removed", org); n != 1 {
		t.Fatalf("removal audit rows = %d, want 1", n)
	}
	if n := s.outboxRows(t, ctx, "membership.removed", m.ID); n != 1 {
		t.Fatalf("removal outbox rows = %d, want 1", n)
	}
	if n := s.sessionAuditRows(t, ctx, user); n != sessionAudit {
		t.Fatalf("session audit rows %d -> %d, want unchanged (the removal row is the commit's audit row)", sessionAudit, n)
	}
}

// A refused removal ends nothing.
func TestARefusedRemoveEndsNoSession(t *testing.T) {
	s := newSessionEnv(t)
	ctx := context.Background()
	org, user, owner := s.seedOrg(t, ctx), s.seedUser(t, ctx), s.seedUser(t, ctx)
	s.add(t, ctx, org, owner, "owner")
	s.add(t, ctx, org, user, "member")
	ownerSession, userSession := s.open(t, ctx, owner, &org), s.open(t, ctx, user, &org)
	ownerGo, userGo := s.goToken(t, ctx, owner, &org), s.goToken(t, ctx, user, &org)
	if _, err := s.members.Remove(ctx, Change{OrgID: org, PrincipalID: owner, Authorize: allowAll}); !errors.Is(err, ErrLastOwner) {
		t.Fatalf("Remove of the last owner = %v, want ErrLastOwner", err)
	}
	refuse := func(context.Context, audit.TxOps, Locked) error { return ErrDenied }
	if _, err := s.members.Remove(ctx, Change{OrgID: org, PrincipalID: user, Authorize: refuse}); !errors.Is(err, ErrDenied) {
		t.Fatalf("Remove with a refusing Authorize = %v, want ErrDenied", err)
	}
	for _, id := range []uuid.UUID{ownerSession, userSession} {
		if !s.sessionLive(t, ctx, id) {
			t.Errorf("session %s was touched by a refused removal", id)
		}
	}
	for _, id := range []uuid.UUID{ownerGo, userGo} {
		if s.goTokenRevoked(t, ctx, id) {
			t.Errorf("go-api refresh token %s was revoked by a refused removal", id)
		}
	}
}

// The sessions end in the removal's transaction: a failure of the membership
// write, of the session write or of the commit's audit row leaves the
// membership and every session as they were.
func TestAFailedRemovalCommitLeavesTheSessionsLive(t *testing.T) {
	for _, table := range []string{"auth.memberships", "auth.sessions", "auth.security_audit_events"} {
		t.Run(table, func(t *testing.T) {
			s := newSessionEnv(t)
			ctx := context.Background()
			org, user, owner := s.seedOrg(t, ctx), s.seedUser(t, ctx), s.seedUser(t, ctx)
			s.add(t, ctx, org, owner, "owner")
			m := s.add(t, ctx, org, user, "member")
			sess, goTok := s.open(t, ctx, user, &org), s.goToken(t, ctx, user, &org)
			s.plantFailure(t, ctx, table)
			if _, err := s.members.Remove(ctx, Change{OrgID: org, PrincipalID: user, Authorize: allowAll}); err == nil {
				t.Fatal("Remove with a planted failure succeeded")
			}
			if got := s.rows(t, ctx, org, user); got != (rows{"member", "active", "member"}) {
				t.Fatalf("rows = %+v, want unchanged", got)
			}
			if !s.sessionLive(t, ctx, sess) {
				t.Fatal("the session ended although the removal rolled back")
			}
			if s.goTokenRevoked(t, ctx, goTok) {
				t.Fatal("the go-api refresh token was revoked although the removal rolled back")
			}
			if n := s.outboxRows(t, ctx, "membership.removed", m.ID); n != 0 {
				t.Fatalf("outbox rows = %d, want 0", n)
			}
		})
	}
}

// holdSession plays an Exchange: it locks the session row FOR UPDATE, as
// session.Store.Exchange does, and returns the open transaction.
func (s *sessionEnv) holdSession(t *testing.T, ctx context.Context, id uuid.UUID) func(insertCredential bool) error {
	t.Helper()
	tx, err := s.runtime.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	var principal uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT principal_id FROM auth.sessions WHERE id = $1 FOR UPDATE`, id).Scan(&principal); err != nil {
		t.Fatalf("lock the session: %v", err)
	}
	return func(insertCredential bool) error {
		if insertCredential {
			// The successor credential: its principal foreign key takes a
			// key-share lock on the principal row the removal locks FOR UPDATE.
			hash := sha256.Sum256([]byte(uuid.NewString()))
			if _, err := tx.Exec(ctx, `INSERT INTO auth.refresh_credentials (session_id, principal_id, token_hash, expires_at)
				VALUES ($1, $2, $3, now() + interval '1 day')`, id, principal, hash[:]); err != nil {
				return err
			}
		}
		return tx.Commit(ctx)
	}
}

// A session another writer holds is never waited for while the removal holds
// the principal: the removal rolls back and plans again, and after its last
// attempt it is ErrStore (503) with nothing written.
func TestARemovalThatFindsTheSessionLockedGivesUpWithNothingWritten(t *testing.T) {
	s := newSessionEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	org, user, owner := s.seedOrg(t, ctx), s.seedUser(t, ctx), s.seedUser(t, ctx)
	s.add(t, ctx, org, owner, "owner")
	m := s.add(t, ctx, org, user, "member")
	sess := s.open(t, ctx, user, &org)
	rev := s.revision(t, ctx, m.ID)
	release := s.holdSession(t, ctx, sess)
	_, err := s.members.Remove(ctx, Change{OrgID: org, PrincipalID: user, Authorize: allowAll})
	if !errors.Is(err, ErrStore) || !errors.Is(err, errSessionsBusy) {
		t.Fatalf("Remove with the session held = %v, want ErrStore after the busy attempts", err)
	}
	if n := s.replanned(t); n != planAttempts {
		t.Fatalf("replanned attempts = %d, want %d", n, planAttempts)
	}
	if got := s.rows(t, ctx, org, user); got != (rows{"member", "active", "member"}) {
		t.Fatalf("rows = %+v, want unchanged", got)
	}
	if n := s.auditRows(t, ctx, "auth.membership.removed", org); n != 0 {
		t.Fatalf("audit rows = %d, want 0", n)
	}
	if n := s.outboxRows(t, ctx, "membership.removed", m.ID); n != 0 {
		t.Fatalf("outbox rows = %d, want 0", n)
	}
	if after := s.revision(t, ctx, m.ID); after != rev {
		t.Fatalf("revision %d -> %d, want unchanged", rev, after)
	}
	if err := release(false); err != nil {
		t.Fatal(err)
	}
	if !s.sessionLive(t, ctx, sess) {
		t.Fatal("the session ended although the removal wrote nothing")
	}
}

// The race of an Exchange and a removal: the Exchange holds the session and
// inserts its new credential while the removal backs off; it commits; the
// removal's next attempt ends the session with that credential. No deadlock.
func TestARemovalRacingAnExchangeEndsTheNewCredentialWithoutADeadlock(t *testing.T) {
	s := newSessionEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	org, user, owner := s.seedOrg(t, ctx), s.seedUser(t, ctx), s.seedUser(t, ctx)
	s.add(t, ctx, org, owner, "owner")
	s.add(t, ctx, org, user, "member")
	sess := s.open(t, ctx, user, &org)
	saved := busyBackoff
	busyBackoff = [planAttempts - 1]time.Duration{2 * time.Second, 2 * time.Second}
	t.Cleanup(func() { busyBackoff = saved })
	release := s.holdSession(t, ctx, sess)
	done := make(chan error, 1)
	go func() {
		_, err := s.members.Remove(ctx, Change{OrgID: org, PrincipalID: user, Authorize: allowAll})
		done <- err
	}()
	// The removal's first attempt has met the held session: it was planned
	// again (or, without NOWAIT, it waits on the session lock).
	deadline := time.Now().Add(20 * time.Second)
	for s.replanned(t) == 0 && s.count(t, ctx, `SELECT count(*) FROM pg_stat_activity
		WHERE wait_event_type = 'Lock' AND query LIKE '%FOR UPDATE OF s%'`) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the removal never reached the held session")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := release(true); err != nil {
		t.Fatalf("the Exchange's commit: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if !s.sessionEnded(t, ctx, sess) {
		t.Fatal("the session or the credential the Exchange committed is not ended")
	}
	if got := s.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials WHERE session_id = $1`, sess); got != 2 {
		t.Fatalf("credentials of the session = %d, want 2 (the first and the Exchange's)", got)
	}
	if got := s.rows(t, ctx, org, user); got != (rows{"", "removed", "member"}) {
		t.Fatalf("rows after remove = %+v", got)
	}
}
