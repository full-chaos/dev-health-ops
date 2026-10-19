//go:build integration

package session

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// raceMarker is what `check_go.sh auth race-integration` counts: a race test
// prints it only when it ran with -race against a live database.
const raceMarker = "AUTH_RACE_EXECUTED"

func markRace(t *testing.T) {
	t.Helper()
	if raceEnabled {
		t.Logf("%s %s", raceMarker, t.Name())
	}
}

type env struct {
	admin   *pgxpool.Pool
	runtime *pgxpool.Pool
	schema  authschema.ValidatedIdentifier
	keyText contracts.SecretString
	store   *Store
}

// newEnv applies the auth schema to a container of its own and builds a Store
// that connects AS the runtime role, so every write below runs with the
// production grants and through the 0006/0010 guard triggers.
func newEnv(t *testing.T, ctx context.Context) *env {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatalf("admin pool: %v", err)
	}
	t.Cleanup(admin.Close)
	role, err := containers.RoleName("session_runtime", instance)
	if err != nil {
		t.Fatalf("derive role: %v", err)
	}
	const password = "session-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		t.Fatalf("create role: %v", err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User = role
	cfg.ConnConfig.Password = password
	cfg.MaxConns = 48
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("runtime pool: %v", err)
	}
	t.Cleanup(runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	keyText, _ := testKey(t)
	e := &env{admin: admin, runtime: runtime, schema: schema, keyText: keyText}
	e.store = e.newStore(t, keyText, Config{})
	return e
}

func (e *env) newStore(t *testing.T, key contracts.SecretString, cfg Config) *Store {
	t.Helper()
	cfg.Pool, cfg.Schema, cfg.SuccessorKey = e.runtime, e.schema, key
	store, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func (e *env) principal(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'session-test')`, id.String()); err != nil {
		t.Fatalf("seed principal: %v", err)
	}
	return id
}

func (e *env) create(t *testing.T, ctx context.Context, principal uuid.UUID) Issued {
	t.Helper()
	issued, err := e.store.Create(ctx, principal, nil, []string{"pwd"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return issued
}

func (e *env) exchange(t *testing.T, ctx context.Context, store *Store, credential contracts.SecretString) Result {
	t.Helper()
	result, err := store.Exchange(ctx, credential)
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	return result
}

// guarded sets up a state the guard triggers refuse outside audit.Commit
// (an expired credential or session, a disabled principal) through
// audit.Commit itself, as the runtime role, with its audit row and event.
func (e *env) guarded(t *testing.T, ctx context.Context, sql string, args ...any) {
	t.Helper()
	err := audit.Commit(ctx, e.runtime, e.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, sql, args...)
			return err
		},
		Audit: audit.AuditEvent{EventType: "test.session_setup", Outcome: audit.OutcomeAllowed},
		Event: audit.OutboxEvent{
			AggregateType: "test", AggregateID: uuid.NewString(), EventType: "test.session_setup",
			IdempotencyKey: "test.session_setup:" + uuid.NewString(),
		},
	})
	if err != nil {
		t.Fatalf("guarded %q: %v", sql, err)
	}
}

// rotatedAgo performs the rotation Exchange performs, but with the parent's
// consumption stamped `ago` in the past on the database clock: the successor
// row is inserted (derived with the store's key, so a grace replay can match
// it) and the parent's FIRST and only consumed_at write is backdated. It runs
// as the runtime role through the ordinary triggers.
func (e *env) rotatedAgo(t *testing.T, ctx context.Context, parent Issued, ago time.Duration) Issued {
	t.Helper()
	childID := uuid.New()
	successor := deriveSuccessor(e.store.key, childID, parent.Credential)
	tx, err := e.runtime.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var expires time.Time
	if err := tx.QueryRow(ctx, `
		INSERT INTO auth.refresh_credentials (id, session_id, principal_id, token_hash, family_id, parent_id, expires_at)
		SELECT $1, p.session_id, p.principal_id, $2, p.family_id, p.id, clock_timestamp() + interval '7 days'
		FROM auth.refresh_credentials p WHERE p.token_hash = $3
		RETURNING expires_at`, childID.String(), hashCredential(successor), hashCredential(parent.Credential)).Scan(&expires); err != nil {
		t.Fatalf("insert the successor: %v", err)
	}
	tag, err := tx.Exec(ctx, `UPDATE auth.refresh_credentials
		SET consumed_at = clock_timestamp() - make_interval(secs => $2), superseded_by = $3
		WHERE token_hash = $1 AND consumed_at IS NULL`, hashCredential(parent.Credential), ago.Seconds(), childID.String())
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("consume the parent: %v, %v", tag, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return Issued{Session: parent.Session, Credential: successor, ExpiresAt: expires}
}

func (e *env) count(t *testing.T, ctx context.Context, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", sql, err)
	}
	return n
}

func (e *env) liveCredentials(t *testing.T, ctx context.Context, session uuid.UUID) int {
	return e.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials
		WHERE session_id = $1 AND consumed_at IS NULL AND revoked_at IS NULL`, session.String())
}

func (e *env) sessionRevoked(t *testing.T, ctx context.Context, session uuid.UUID) bool {
	return e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE id = $1 AND revoked_at IS NOT NULL`, session.String()) == 1
}

func (e *env) auditRows(t *testing.T, ctx context.Context, eventType, target string) int {
	return e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events
		WHERE event_type = $1 AND attributes->>'target_id' = $2`, eventType, target)
}

func (e *env) outboxRows(t *testing.T, ctx context.Context, eventType, aggregate string) int {
	return e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events
		WHERE event_type = $1 AND aggregate_id = $2`, eventType, aggregate)
}

// parallel starts n calls at once and waits for all of them.
func parallel(n int, call func(i int)) {
	var start, done sync.WaitGroup
	start.Add(1)
	done.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer done.Done()
			start.Wait()
			call(i)
		}(i)
	}
	start.Done()
	done.Wait()
}

func testContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// R1: one live credential presented by 32 clients at once rotates exactly
// once, and every client gets the same successor.
func TestRaceParallelExchangeOfOneCredentialRotatesOnce(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	issued := e.create(t, ctx, e.principal(t, ctx))

	const n = 32
	results := make([]Result, n)
	errs := make([]error, n)
	parallel(n, func(i int) { results[i], errs[i] = e.store.Exchange(ctx, issued.Credential) })

	reasons := map[Reason]int{}
	successors := map[string]bool{}
	for i := range results {
		if errs[i] != nil {
			t.Fatalf("exchange %d: %v", i, errs[i])
		}
		reasons[results[i].Reason]++
		successors[results[i].Credential.Reveal()] = true
	}
	if reasons[ReasonAccepted] != 1 || reasons[ReasonGraceReplay] != n-1 {
		t.Fatalf("reasons = %v, want 1 accepted and %d grace replays", reasons, n-1)
	}
	if len(successors) != 1 {
		t.Fatalf("%d different successors were handed out, want 1", len(successors))
	}
	if children := e.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials c
		JOIN auth.refresh_credentials p ON c.parent_id = p.id WHERE p.token_hash = $1`, hashCredential(issued.Credential)); children != 1 {
		t.Fatalf("the parent has %d children, want 1", children)
	}
	if live := e.liveCredentials(t, ctx, issued.Session.ID); live != 1 {
		t.Fatalf("%d live credentials, want 1", live)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events`); n != 0 {
		t.Fatalf("%d audit rows after a clean rotation, want 0", n)
	}
	if next := e.exchange(t, ctx, e.store, results[0].Credential); next.Reason != ReasonAccepted {
		t.Fatalf("the successor's own exchange = %s, want accepted", next.Reason)
	}
	markRace(t)
}

// R2: a reused parent and its live child presented at the same time. Whatever
// the order, a revoked family leaves no live credential behind: a grandchild
// minted by the child's rotation cannot survive the revoke.
func TestRaceReuseAgainstChildRotationLeavesNoLiveCredential(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	principal := e.principal(t, ctx)
	for iteration := 0; iteration < 100; iteration++ {
		issued := e.create(t, ctx, principal)
		child := e.rotatedAgo(t, ctx, issued, GraceWindow+time.Second)

		var reuse, rotation Result
		var reuseErr, rotationErr error
		parallel(2, func(i int) {
			if i == 0 {
				reuse, reuseErr = e.store.Exchange(ctx, issued.Credential)
			} else {
				rotation, rotationErr = e.store.Exchange(ctx, child.Credential)
			}
		})
		if reuseErr != nil || rotationErr != nil {
			t.Fatalf("iteration %d: errors %v / %v", iteration, reuseErr, rotationErr)
		}
		if reuse.Reason != ReasonRefreshReused {
			t.Fatalf("iteration %d: reuse = %s, want refresh_reused", iteration, reuse.Reason)
		}
		if rotation.Reason != ReasonAccepted && rotation.Reason != ReasonSessionRevoked {
			t.Fatalf("iteration %d: child rotation = %s", iteration, rotation.Reason)
		}
		if live := e.liveCredentials(t, ctx, issued.Session.ID); live != 0 || !e.sessionRevoked(t, ctx, issued.Session.ID) {
			t.Fatalf("iteration %d: %d live credentials after the reuse revoke (session revoked: %v)",
				iteration, live, e.sessionRevoked(t, ctx, issued.Session.ID))
		}
		if rotation.Reason == ReasonAccepted {
			if after := e.exchange(t, ctx, e.store, rotation.Credential); after.Reason != ReasonSessionRevoked {
				t.Fatalf("iteration %d: the grandchild still exchanges (%s)", iteration, after.Reason)
			}
		}
	}
	markRace(t)
}

// R3: 16 clients replay the same stale credential. The family is revoked once,
// with exactly one audit row and one outbox event.
func TestRaceConcurrentReuseRevokesAndAuditsOnce(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	issued := e.create(t, ctx, e.principal(t, ctx))
	e.rotatedAgo(t, ctx, issued, GraceWindow+time.Second)

	const n = 16
	results := make([]Result, n)
	errs := make([]error, n)
	parallel(n, func(i int) { results[i], errs[i] = e.store.Exchange(ctx, issued.Credential) })
	reasons := map[Reason]int{}
	for i := range results {
		if errs[i] != nil {
			t.Fatalf("exchange %d: %v", i, errs[i])
		}
		reasons[results[i].Reason]++
	}
	if reasons[ReasonRefreshReused] != 1 || reasons[ReasonSessionRevoked] != n-1 {
		t.Fatalf("reasons = %v, want 1 refresh_reused and %d session_revoked", reasons, n-1)
	}
	id := issued.Session.ID.String()
	if a, o := e.auditRows(t, ctx, "auth.session.refresh_reuse_revoked", id), e.outboxRows(t, ctx, "session.revoked", id); a != 1 || o != 1 {
		t.Fatalf("audit rows %d, outbox events %d; want 1 and 1", a, o)
	}
	if live := e.liveCredentials(t, ctx, issued.Session.ID); live != 0 || !e.sessionRevoked(t, ctx, issued.Session.ID) {
		t.Fatalf("%d live credentials after reuse", live)
	}
	markRace(t)
}

// R4: revoke and exchange of the same session at the same time. After Revoke
// returns, no credential of the session is live, whichever ran first.
func TestRaceRevokeAgainstExchange(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	principal := e.principal(t, ctx)
	for iteration := 0; iteration < 100; iteration++ {
		issued := e.create(t, ctx, principal)
		var exchanged Result
		var revoked bool
		var exchangeErr, revokeErr error
		parallel(2, func(i int) {
			if i == 0 {
				exchanged, exchangeErr = e.store.Exchange(ctx, issued.Credential)
			} else {
				revoked, revokeErr = e.store.Revoke(ctx, issued.Session.ID, Actor{})
			}
		})
		if exchangeErr != nil || revokeErr != nil || !revoked {
			t.Fatalf("iteration %d: exchange %v, revoke %v (%v)", iteration, exchangeErr, revoked, revokeErr)
		}
		if exchanged.Reason != ReasonAccepted && exchanged.Reason != ReasonSessionRevoked {
			t.Fatalf("iteration %d: exchange = %s", iteration, exchanged.Reason)
		}
		if live := e.liveCredentials(t, ctx, issued.Session.ID); live != 0 {
			t.Fatalf("iteration %d: %d live credentials after Revoke", iteration, live)
		}
		if exchanged.Reason == ReasonAccepted {
			if after := e.exchange(t, ctx, e.store, exchanged.Credential); after.Reason != ReasonSessionRevoked {
				t.Fatalf("iteration %d: a credential issued before the revoke still exchanges (%s)", iteration, after.Reason)
			}
		}
	}
	markRace(t)
}

// R5: revoke-all while every session of the principal is rotating in a loop.
// No deadlock, no survivor, one audit row and one outbox event.
func TestRaceRevokeAllAgainstRotations(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	principal := e.principal(t, ctx)
	other := e.create(t, ctx, e.principal(t, ctx))
	const sessions = 8
	issued := make([]Issued, sessions)
	for i := range issued {
		issued[i] = e.create(t, ctx, principal)
	}
	var revokedCount int
	var revokeErr error
	loopErrs := make([]error, sessions)
	parallel(sessions+1, func(i int) {
		if i == sessions {
			time.Sleep(20 * time.Millisecond)
			revokedCount, revokeErr = e.store.RevokeAll(ctx, principal, Actor{})
			return
		}
		credential := issued[i].Credential
		for {
			result, err := e.store.Exchange(ctx, credential)
			if err != nil {
				loopErrs[i] = err
				return
			}
			if result.Reason != ReasonAccepted {
				if result.Reason != ReasonSessionRevoked {
					loopErrs[i] = fmt.Errorf("rotation loop ended with %s", result.Reason)
				}
				return
			}
			credential = result.Credential
		}
	})
	if revokeErr != nil || revokedCount != sessions {
		t.Fatalf("RevokeAll = %d, %v; want %d", revokedCount, revokeErr, sessions)
	}
	for i, err := range loopErrs {
		if err != nil {
			t.Fatalf("session %d: %v", i, err)
		}
	}
	if live := e.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials
		WHERE principal_id = $1 AND consumed_at IS NULL AND revoked_at IS NULL`, principal.String()); live != 0 {
		t.Fatalf("%d live credentials after RevokeAll", live)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1 AND revoked_at IS NULL`, principal.String()); n != 0 {
		t.Fatalf("%d live sessions after RevokeAll", n)
	}
	if a, o := e.auditRows(t, ctx, "auth.session.revoked_all", principal.String()),
		e.outboxRows(t, ctx, "principal.sessions_revoked", principal.String()); a != 1 || o != 1 {
		t.Fatalf("audit rows %d, outbox events %d; want 1 and 1", a, o)
	}
	if e.sessionRevoked(t, ctx, other.Session.ID) {
		t.Fatal("RevokeAll revoked another principal's session")
	}
	markRace(t)
}

// R6: concurrent revision bumps lose no update.
func TestRaceConcurrentRevisionBumpsLoseNothing(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	issued := e.create(t, ctx, e.principal(t, ctx))
	const n = 32
	revisions := make([]int64, n)
	errs := make([]error, n)
	parallel(n, func(i int) { revisions[i], errs[i] = e.store.BumpRevision(ctx, issued.Session.ID, Actor{}) })
	seen := map[int64]bool{}
	for i := range revisions {
		if errs[i] != nil {
			t.Fatalf("bump %d: %v", i, errs[i])
		}
		seen[revisions[i]] = true
	}
	for want := int64(2); want <= n+1; want++ {
		if !seen[want] {
			t.Fatalf("revision %d was never returned; returned %v", want, revisions)
		}
	}
	current, err := e.store.Current(ctx, issued.Session.ID)
	if err != nil || current.Revision != n+1 {
		t.Fatalf("Current revision = %d, %v; want %d", current.Revision, err, n+1)
	}
	if a := e.auditRows(t, ctx, "auth.session.revision_bumped", issued.Session.ID.String()); a != n {
		t.Fatalf("%d audit rows, want %d", a, n)
	}
	markRace(t)
}

func TestGraceIsMeasuredOnTheDatabaseClock(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	principal := e.principal(t, ctx)

	inside := e.create(t, ctx, principal)
	child := e.rotatedAgo(t, ctx, inside, GraceWindow-15*time.Second)
	replay := e.exchange(t, ctx, e.store, inside.Credential)
	if replay.Reason != ReasonGraceReplay || replay.Credential.Reveal() != child.Credential.Reveal() {
		t.Fatalf("inside the grace: %s (same successor %v)", replay.Reason, replay.Credential.Reveal() == child.Credential.Reveal())
	}
	if !replay.ExpiresAt.Equal(child.ExpiresAt) {
		t.Errorf("grace replay expiry %v, the successor's is %v", replay.ExpiresAt, child.ExpiresAt)
	}

	outside := e.create(t, ctx, principal)
	e.rotatedAgo(t, ctx, outside, GraceWindow+15*time.Second)
	if reuse := e.exchange(t, ctx, e.store, outside.Credential); reuse.Reason != ReasonRefreshReused {
		t.Fatalf("past the grace: %s, want refresh_reused", reuse.Reason)
	}
	if !e.sessionRevoked(t, ctx, outside.Session.ID) || e.sessionRevoked(t, ctx, inside.Session.ID) {
		t.Fatal("reuse past the grace must revoke its own session and only that one")
	}
}

func TestGraceAfterTheSuccessorWasUsedIsReuse(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	issued := e.create(t, ctx, e.principal(t, ctx))
	child := e.exchange(t, ctx, e.store, issued.Credential)
	e.exchange(t, ctx, e.store, child.Credential)
	if reuse := e.exchange(t, ctx, e.store, issued.Credential); reuse.Reason != ReasonRefreshReused {
		t.Fatalf("parent replayed after its successor rotated: %s, want refresh_reused", reuse.Reason)
	}
	if live := e.liveCredentials(t, ctx, issued.Session.ID); live != 0 {
		t.Fatalf("%d live credentials", live)
	}
}

// A grace replay whose successor cannot be recomputed (the key changed) is a
// retryable error and revokes nothing.
func TestGraceWithAChangedKeyIsRetryableAndRevokesNothing(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	issued := e.create(t, ctx, e.principal(t, ctx))
	child := e.exchange(t, ctx, e.store, issued.Credential)
	otherKey, _ := testKey(t)
	rotated := e.newStore(t, otherKey, Config{})

	result, err := rotated.Exchange(ctx, issued.Credential)
	if !errors.Is(err, ErrGraceUnavailable) || result.Reason != ReasonDependency || result.Credential.Reveal() != "" {
		t.Fatalf("Exchange = %s, %v; want %s and ErrGraceUnavailable with no credential", result.Reason, err, ReasonDependency)
	}
	if e.sessionRevoked(t, ctx, issued.Session.ID) || e.liveCredentials(t, ctx, issued.Session.ID) != 1 {
		t.Fatal("a key mismatch revoked the session or its successor")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events`); n != 0 {
		t.Fatalf("%d audit rows after a key mismatch, want 0", n)
	}
	if next := e.exchange(t, ctx, e.store, child.Credential); next.Reason != ReasonAccepted {
		t.Fatalf("the successor no longer exchanges: %s", next.Reason)
	}
}

func TestRefusalsWriteNothing(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	snapshot := func() string {
		var doc string
		if err := e.admin.QueryRow(ctx, `SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY r.id), '[]')::text || (SELECT coalesce(jsonb_agg(to_jsonb(s) ORDER BY s.id), '[]')::text FROM auth.sessions s)
			|| (SELECT count(*)::text FROM auth.security_audit_events) FROM auth.refresh_credentials r`).Scan(&doc); err != nil {
			t.Fatal(err)
		}
		return doc
	}
	unknown, _ := newCredential()

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
		{"malformed jwt", contracts.NewSecretString("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2ln"), ReasonMalformed},
		{"unknown", unknown, ReasonRefreshUnknown},
		{"expired credential", expiredCredential.Credential, ReasonRefreshExpired},
		{"expired session", expiredSession.Credential, ReasonSessionExpired},
		{"disabled principal", disabled.Credential, ReasonPrincipalDeactivated},
		{"revoked session", revoked.Credential, ReasonSessionRevoked},
	} {
		before := snapshot()
		if got := e.exchange(t, ctx, e.store, c.credential); got.Reason != c.want || got.Credential.Reveal() != "" {
			t.Errorf("%s: %s, want %s and no credential", c.name, got.Reason, c.want)
		}
		if after := snapshot(); after != before {
			t.Errorf("%s: the refusal wrote to the database", c.name)
		}
	}
}

func TestOnlyHashesAreAtRest(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	issued := e.create(t, ctx, e.principal(t, ctx))
	child := e.exchange(t, ctx, e.store, issued.Credential)
	for _, credential := range []contracts.SecretString{issued.Credential, child.Credential} {
		sum := sha256.Sum256([]byte(credential.Reveal()))
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials WHERE token_hash = $1`, sum[:]); n != 1 {
			t.Fatalf("token_hash = SHA-256(credential) matched %d rows, want 1", n)
		}
	}
	var dump string
	if err := e.admin.QueryRow(ctx, `SELECT string_agg(to_jsonb(t)::text, '') FROM (
		SELECT to_jsonb(r) AS t FROM auth.refresh_credentials r
		UNION ALL SELECT to_jsonb(s) FROM auth.sessions s
		UNION ALL SELECT to_jsonb(a) FROM auth.security_audit_events a
		UNION ALL SELECT to_jsonb(o) FROM auth.auth_outbox_events o) x`).Scan(&dump); err != nil {
		t.Fatal(err)
	}
	for _, credential := range []contracts.SecretString{issued.Credential, child.Credential, e.keyText} {
		v := credential.Reveal()
		if strings.Contains(dump, v) || strings.Contains(dump, strings.TrimPrefix(v, credentialPrefix)) {
			t.Fatal("a credential or the successor key is stored in plain form")
		}
	}
}

func TestCreateRefusesDisabledPrincipalsAndBadAMR(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	principal := e.principal(t, ctx)
	for _, amr := range [][]string{nil, {}, {"password"}, {"sms"}, {"pwd", "pwd"}} {
		if _, err := e.store.Create(ctx, principal, nil, amr); !errors.Is(err, ErrInvalidAMR) {
			t.Errorf("amr %v: %v, want ErrInvalidAMR", amr, err)
		}
	}
	if _, err := e.store.Create(ctx, uuid.New(), nil, []string{"pwd"}); !errors.Is(err, ErrPrincipalUnavailable) {
		t.Errorf("unknown principal: %v", err)
	}
	e.guarded(t, ctx, `UPDATE auth.principals SET disabled_at = now() WHERE id = $1`, principal.String())
	if _, err := e.store.Create(ctx, principal, nil, []string{"pwd"}); !errors.Is(err, ErrPrincipalUnavailable) {
		t.Errorf("disabled principal: %v", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.sessions`); n != 0 {
		t.Errorf("%d sessions after refused creates", n)
	}
	federated, err := e.store.Create(ctx, e.principal(t, ctx), nil, []string{"fed"})
	if err != nil {
		t.Fatalf("a federated sign-in was refused: %v", err)
	}
	doc, _ := json.Marshal(federated.Session.Wire())
	if _, err := contracts.ParseSession(repoRoot(t), doc); err != nil {
		t.Fatalf("session.v1 refused amr fed: %v\n%s", err, doc)
	}
}

func TestCredentialNeverOutlivesItsSession(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	short := e.newStore(t, e.keyText, Config{SessionLifetime: time.Hour})
	issued, err := short.Create(ctx, e.principal(t, ctx), nil, []string{"pwd"})
	if err != nil {
		t.Fatal(err)
	}
	child := e.exchange(t, ctx, short, issued.Credential)
	for _, got := range []time.Time{issued.ExpiresAt, child.ExpiresAt} {
		if got.After(issued.Session.ExpiresAt) {
			t.Fatalf("credential expires %v, after its session (%v)", got, issued.Session.ExpiresAt)
		}
	}
}

// ACP-ADR-03 keeps go-api's 7-day ROLLING refresh with no absolute cap: by
// default a session lives as long as it keeps refreshing, and its reported
// expiry moves with each rotation.
func TestDefaultSessionHasNoAbsoluteCapAndRolls(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	issued := e.create(t, ctx, e.principal(t, ctx))
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE id = $1 AND expires_at = 'infinity'`, issued.Session.ID.String()); n != 1 {
		t.Fatal("a default session stored an absolute expiry")
	}
	if !issued.Session.ExpiresAt.Equal(issued.ExpiresAt) {
		t.Fatalf("session expiry %v, its only credential expires %v", issued.Session.ExpiresAt, issued.ExpiresAt)
	}
	if got := issued.ExpiresAt.Sub(issued.Session.CreatedAt); got < defaultRefreshTTL-time.Minute || got > defaultRefreshTTL+time.Minute {
		t.Fatalf("first credential lives %v, want about %v", got, defaultRefreshTTL)
	}
	time.Sleep(10 * time.Millisecond)
	child := e.exchange(t, ctx, e.store, issued.Credential)
	if !child.ExpiresAt.After(issued.ExpiresAt) || !child.Session.ExpiresAt.Equal(child.ExpiresAt) {
		t.Fatalf("rotation did not roll the expiry: credential %v -> %v, session %v", issued.ExpiresAt, child.ExpiresAt, child.Session.ExpiresAt)
	}
	current, err := e.store.Current(ctx, issued.Session.ID)
	if err != nil || !current.ExpiresAt.Equal(child.ExpiresAt) {
		t.Fatalf("Current expiry %v, %v; want %v", current.ExpiresAt, err, child.ExpiresAt)
	}
	doc, _ := json.Marshal(current.Wire())
	if _, err := contracts.ParseSession(repoRoot(t), doc); err != nil {
		t.Fatalf("session.v1 refused an uncapped session: %v\n%s", err, doc)
	}

	// Every credential swept: the record still reads, expiring at the last
	// rotation plus the refresh lifetime.
	if _, err := e.admin.Exec(ctx, `DELETE FROM auth.refresh_credentials WHERE session_id = $1`, issued.Session.ID.String()); err != nil {
		t.Fatal(err)
	}
	swept, err := e.store.Current(ctx, issued.Session.ID)
	if err != nil {
		t.Fatalf("Current after a sweep: %v", err)
	}
	if d := swept.ExpiresAt.Sub(child.ExpiresAt); d < -time.Second || d > time.Second {
		t.Fatalf("swept session expiry %v, want about %v", swept.ExpiresAt, child.ExpiresAt)
	}
}

func TestCurrentIsASessionV1Document(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	issued := e.create(t, ctx, e.principal(t, ctx))
	root := repoRoot(t)
	for _, revoke := range []bool{false, true} {
		if revoke {
			if ok, err := e.store.Revoke(ctx, issued.Session.ID, Actor{}); !ok || err != nil {
				t.Fatalf("Revoke: %v %v", ok, err)
			}
		}
		current, err := e.store.Current(ctx, issued.Session.ID)
		if err != nil {
			t.Fatal(err)
		}
		doc, err := json.Marshal(current.Wire())
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := contracts.ParseSession(root, doc)
		if err != nil {
			t.Fatalf("session.v1 refused Current: %v\n%s", err, doc)
		}
		if (parsed.RevokedAt != nil) != revoke || int64(parsed.Revision) != 1 || parsed.AMR[0] != "pwd" {
			t.Fatalf("Current lost state: %s", doc)
		}
	}
	if _, err := e.store.Current(ctx, uuid.New()); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("unknown session: %v", err)
	}
}

func TestRevokeIsAuditedOnceAndIdempotent(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	actor := e.principal(t, ctx)
	issued := e.create(t, ctx, e.principal(t, ctx))
	requestID := uuid.NewString()
	for i, want := range []bool{true, false} {
		ok, err := e.store.Revoke(ctx, issued.Session.ID, Actor{PrincipalID: &actor, RequestID: &requestID})
		if err != nil || ok != want {
			t.Fatalf("Revoke %d = %v, %v; want %v", i, ok, err, want)
		}
	}
	id := issued.Session.ID.String()
	if a, o := e.auditRows(t, ctx, "auth.session.revoked", id), e.outboxRows(t, ctx, "session.revoked", id); a != 1 || o != 1 {
		t.Fatalf("audit %d, outbox %d; want 1, 1", a, o)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE actor_principal_id = $1 AND request_id = $2`, actor.String(), requestID); n != 1 {
		t.Fatalf("the audit row does not name the actor and request (%d rows)", n)
	}
	if _, err := e.store.Revoke(ctx, uuid.New(), Actor{}); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("unknown session: %v", err)
	}
	if _, err := e.store.BumpRevision(ctx, issued.Session.ID, Actor{}); !errors.Is(err, ErrSessionNotLive) {
		t.Fatalf("BumpRevision on a revoked session: %v", err)
	}
}

// The store relies on the 0006/0010 guard: a revocation written outside
// audit.Commit by the runtime role itself is refused by PostgreSQL.
func TestRevocationOutsideAuditCommitIsRefused(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	issued := e.create(t, ctx, e.principal(t, ctx))
	for _, sql := range []string{
		`UPDATE auth.refresh_credentials SET revoked_at = now() WHERE session_id = $1`,
		`UPDATE auth.sessions SET revoked_at = now() WHERE id = $1`,
		`UPDATE auth.sessions SET revision = revision + 1 WHERE id = $1`,
	} {
		_, err := e.runtime.Exec(ctx, sql, issued.Session.ID.String())
		if err == nil || !strings.Contains(err.Error(), "audit.Commit") {
			t.Errorf("%q outside audit.Commit: %v, want the guard's refusal", sql, err)
		}
	}
}

// A consumed credential is never legitimately presented after its successor
// was issued, expired or not: an expired consumed credential is reuse too.
func TestConsumedExpiredCredentialIsReuse(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	issued := e.create(t, ctx, e.principal(t, ctx))
	e.rotatedAgo(t, ctx, issued, GraceWindow+15*time.Second)
	e.guarded(t, ctx, `UPDATE auth.refresh_credentials SET expires_at = now() - interval '1 second' WHERE token_hash = $1`, hashCredential(issued.Credential))
	if got := e.exchange(t, ctx, e.store, issued.Credential); got.Reason != ReasonRefreshReused {
		t.Fatalf("expired consumed credential: %s, want refresh_reused", got.Reason)
	}
	if live := e.liveCredentials(t, ctx, issued.Session.ID); live != 0 || !e.sessionRevoked(t, ctx, issued.Session.ID) {
		t.Fatalf("%d live credentials after reuse of an expired consumed credential", live)
	}
}

// Liveness is the EFFECTIVE expiry Current reports, for every writer: a
// session whose last credential expired cannot have its revision bumped.
func TestBumpRevisionRefusesAnEffectivelyExpiredSession(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	short := e.newStore(t, e.keyText, Config{RefreshTTL: 100 * time.Millisecond})
	issued, err := short.Create(ctx, e.principal(t, ctx), nil, []string{"pwd"})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	if rev, err := short.BumpRevision(ctx, issued.Session.ID, Actor{}); !errors.Is(err, ErrSessionNotLive) {
		t.Fatalf("BumpRevision on an effectively expired session = %d, %v; want ErrSessionNotLive", rev, err)
	}
	current, err := short.Current(ctx, issued.Session.ID)
	if err != nil || current.Revision != 1 {
		t.Fatalf("revision %d, %v; want 1", current.Revision, err)
	}
}

// The swept-session fallback adds the configured refresh lifetime exactly,
// sub-second part included.
func TestSweptFallbackKeepsAFractionalRefreshTTL(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	const ttl = 1500 * time.Millisecond
	store := e.newStore(t, e.keyText, Config{RefreshTTL: ttl})
	issued, err := store.Create(ctx, e.principal(t, ctx), nil, []string{"pwd"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `DELETE FROM auth.refresh_credentials WHERE session_id = $1`, issued.Session.ID.String()); err != nil {
		t.Fatal(err)
	}
	current, err := store.Current(ctx, issued.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if want := issued.Session.CreatedAt.Add(ttl); !current.ExpiresAt.Equal(want) {
		t.Fatalf("swept fallback expiry %v, want created_at + %v = %v", current.ExpiresAt, ttl, want)
	}
}

// Revoke and RevokeAll judge liveness by the effective expiry like every other
// writer: a session whose last credential expired is already dead, and is
// neither revoked again nor audited.
func TestRevokeLeavesAnEffectivelyExpiredSessionAlone(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	short := e.newStore(t, e.keyText, Config{RefreshTTL: 100 * time.Millisecond})
	principal := e.principal(t, ctx)
	expired, err := short.Create(ctx, principal, nil, []string{"pwd"})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	live := e.create(t, ctx, principal)

	if ok, err := short.Revoke(ctx, expired.Session.ID, Actor{}); ok || err != nil {
		t.Fatalf("Revoke of an effectively expired session = %v, %v; want false, nil", ok, err)
	}
	n, err := e.store.RevokeAll(ctx, principal, Actor{})
	if err != nil || n != 1 {
		t.Fatalf("RevokeAll = %d, %v; want 1 (only the live session)", n, err)
	}
	if e.sessionRevoked(t, ctx, expired.Session.ID) {
		t.Fatal("an effectively expired session was revoked")
	}
	if !e.sessionRevoked(t, ctx, live.Session.ID) {
		t.Fatal("RevokeAll left the live session")
	}
	if a := e.auditRows(t, ctx, "auth.session.revoked", expired.Session.ID.String()); a != 0 {
		t.Fatalf("%d audit rows for revoking a dead session, want 0", a)
	}
}

// The effective expiry is the expiry of the NEWEST credential (the head of the
// chain), not the latest expiry of any credential the session ever had: a
// shorter-lived successor ends the session when it expires, even though its
// consumed parent was issued with a longer lifetime.
func TestEffectiveExpiryFollowsTheNewestCredential(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	long := e.newStore(t, e.keyText, Config{RefreshTTL: 24 * time.Hour})
	short := e.newStore(t, e.keyText, Config{RefreshTTL: 100 * time.Millisecond})
	issued, err := long.Create(ctx, e.principal(t, ctx), nil, []string{"pwd"})
	if err != nil {
		t.Fatal(err)
	}
	child := e.exchange(t, ctx, short, issued.Credential)
	if child.Reason != ReasonAccepted {
		t.Fatalf("rotation: %s", child.Reason)
	}
	time.Sleep(250 * time.Millisecond)
	current, err := long.Current(ctx, issued.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !current.ExpiresAt.Equal(child.ExpiresAt) {
		t.Fatalf("Current effective expiry %v, newest credential expires %v", current.ExpiresAt, child.ExpiresAt)
	}
	if rev, err := long.BumpRevision(ctx, issued.Session.ID, Actor{}); !errors.Is(err, ErrSessionNotLive) {
		t.Fatalf("BumpRevision after the newest credential expired = %d, %v; want ErrSessionNotLive", rev, err)
	}
}
