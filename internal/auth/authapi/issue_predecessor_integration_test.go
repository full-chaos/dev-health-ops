//go:build integration

package authapi

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// switchLive is the session issue route over a live auth schema and the real
// session store, with two organizations the user belongs to, and go-api's own
// session read (policy.PGStore.SessionState) on the same database.
type switchLive struct {
	*preflightStore
	server     http.Handler
	user       uuid.UUID
	orgA, orgB uuid.UUID
	reader     policy.PGStore
}

func newSwitchLive(t *testing.T, ctx context.Context) *switchLive {
	t.Helper()
	p := newPreflightStore(t, ctx)
	signer, _, _ := fileSigner(t)
	user := p.principal(t, ctx)
	orgA, orgB := uuid.New(), uuid.New()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	for _, org := range []uuid.UUID{orgA, orgB} {
		// Organizations are a guarded table: written only through audit.Commit.
		if err := audit.Commit(ctx, p.admin, schema, audit.Mutation{
			Apply: func(ctx context.Context, tx audit.TxOps) error {
				_, err := tx.Exec(ctx, `INSERT INTO auth.organizations (id, name, slug) VALUES ($1, $2, $2)`, org, "switch-"+org.String())
				return err
			},
			Audit:  audit.AuditEvent{EventType: "organization.created", Outcome: audit.OutcomeAllowed},
			Events: []audit.OutboxEvent{{AggregateType: "organization", AggregateID: "org_" + org.String(), EventType: "organization.updated"}},
		}); err != nil {
			t.Fatalf("seed organization: %v", err)
		}
	}
	role := "member"
	users := &fakeUsers{
		users: map[uuid.UUID]UserState{user: {Email: "ada@example.com", IsActive: true}},
		roles: map[[2]uuid.UUID]*string{{user, orgA}: &role, {user, orgB}: &role},
	}
	routes, err := CompatRoutes(CompatDeps{
		Sessions: p.store, Users: users, Issuer: newEdDSAIssuer(t, signer, testAudience),
		Logger:             slog.New(slog.DiscardHandler),
		Projector:          &fakeProjector{orgs: map[uuid.UUID]bool{orgA: true, orgB: true}, gone: map[uuid.UUID]bool{}},
		AppendAudit:        func(context.Context, audit.AuditEvent) error { return nil },
		ServiceCredentials: &fakeServiceCredentials{id: uuid.NewString()},
		Meter:              sdkmetric.NewMeterProvider().Meter("test"),
		Limits:             mustLimits(t, httpapi.NewMemoryCounters(nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return &switchLive{preflightStore: p, server: serveRoutes(routes), user: user, orgA: orgA, orgB: orgB,
		reader: policy.PGStore{Pool: p.admin, AuthSchema: "auth"}}
}

// issue posts the issue route for org, superseding predecessor when set, and
// returns the status and the issued session id (from the access token).
func (l *switchLive) issue(t *testing.T, org uuid.UUID, predecessor *uuid.UUID) (int, uuid.UUID) {
	t.Helper()
	body := issueBody(l.user, org.String(), "pwd")
	if predecessor != nil {
		body = issueBodyWithPredecessor(l.user, org.String(), `"`+predecessor.String()+`"`)
	}
	response := post(l.server, PatternIssueSession, body, issuerBearer)
	if response.Code != http.StatusOK {
		return response.Code, uuid.Nil
	}
	var answer struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &answer); err != nil {
		t.Fatal(err)
	}
	var id uuid.UUID
	if err := l.admin.QueryRow(context.Background(),
		`SELECT id FROM auth.sessions WHERE principal_id = $1 ORDER BY created_at DESC, issued_at DESC LIMIT 1`, l.user).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return response.Code, id
}

// live is go-api's verdict on a token naming sid: the session reader's row,
// as policy.checkSession decides it (found, same principal, not revoked).
func (l *switchLive) live(t *testing.T, sid uuid.UUID) bool {
	t.Helper()
	row, found, err := l.reader.SessionState(context.Background(), sid)
	if err != nil {
		t.Fatal(err)
	}
	return found && row.PrincipalID == l.user && !row.Revoked
}

func (l *switchLive) liveCredentials(t *testing.T, sid uuid.UUID) int {
	t.Helper()
	return l.count(t, context.Background(), `SELECT count(*) FROM auth.refresh_credentials WHERE session_id = $1 AND revoked_at IS NULL`, sid)
}

// The negative proof of CHAOS-12085: sign in to org A, keep the A session,
// switch to org B, sign out in B. The A session ended at the switch, in the
// switch's own transaction, so the A token is refused before and after the
// sign-out; the B token ends at the sign-out; another device's session lives.
func TestSwitchRevokesThePredecessorAndSignOutLeavesNothingLive(t *testing.T) {
	ctx := context.Background()
	l := newSwitchLive(t, ctx)
	status, sessionA := l.issue(t, l.orgA, nil)
	if status != http.StatusOK || !l.live(t, sessionA) {
		t.Fatalf("sign in to A: %d, live %v", status, l.live(t, sessionA))
	}
	_, otherDevice := l.issue(t, l.orgA, nil)
	auditsBefore := l.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1`, l.user.String())

	status, sessionB := l.issue(t, l.orgB, &sessionA)
	if status != http.StatusOK || sessionB == sessionA {
		t.Fatalf("switch to B: %d, session %s", status, sessionB)
	}
	if l.live(t, sessionA) || l.liveCredentials(t, sessionA) != 0 {
		t.Fatalf("after the switch the A session is live=%v with %d live credentials, want ended", l.live(t, sessionA), l.liveCredentials(t, sessionA))
	}
	if !l.live(t, sessionB) || !l.live(t, otherDevice) {
		t.Fatalf("B live=%v, other device live=%v, want both live", l.live(t, sessionB), l.live(t, otherDevice))
	}
	var org string
	if err := l.admin.QueryRow(ctx, `SELECT organization_id::text FROM auth.sessions WHERE id = $1`, sessionB).Scan(&org); err != nil || org != l.orgB.String() {
		t.Fatalf("B session organization %q %v, want %s", org, err, l.orgB)
	}
	// The switch wrote exactly two audit rows: the successor's issue and the
	// predecessor's revoke, which names the A session.
	if n := l.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1`, l.user.String()); n != auditsBefore+2 {
		t.Fatalf("%d audit rows after the switch, want %d", n, auditsBefore+2)
	}
	if n := l.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1 AND subject_principal_id = $2 AND attributes->>'target_id' = $3 AND attributes->>'reason_code' = 'session_revoked'`,
		session.EventSessionRevoked, l.user.String(), sessionA.String()); n != 1 {
		t.Fatalf("%d predecessor revoke rows, want 1", n)
	}
	if n := l.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE id = $1 AND revoked_reason = 'session_revoked'`, sessionA); n != 1 {
		t.Fatal("the A session has no revoked_reason")
	}

	// Sign out in B (auth-service's session revoke of the bearer's own session).
	if revoked, err := l.store.Revoke(ctx, sessionB, session.Actor{}); err != nil || !revoked {
		t.Fatalf("sign out in B: %v %v", revoked, err)
	}
	if l.live(t, sessionA) || l.live(t, sessionB) {
		t.Fatalf("after sign-out A live=%v B live=%v, want neither", l.live(t, sessionA), l.live(t, sessionB))
	}
	if !l.live(t, otherDevice) {
		t.Fatal("the sign-out ended another device's session")
	}

	// A second switch from the ended A session issues nothing.
	sessionsBefore := l.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, l.user)
	if status, _ := l.issue(t, l.orgB, &sessionA); status != http.StatusUnauthorized {
		t.Fatalf("switch from an ended session: %d, want 401", status)
	}
	if n := l.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, l.user); n != sessionsBefore {
		t.Fatalf("a refused switch created %d sessions", n-sessionsBefore)
	}
}

// Two concurrent switches from one session leave at most one live successor:
// the second waits on the predecessor's lock, finds it revoked and issues
// nothing.
func TestConcurrentSwitchesFromOneSessionLeaveOneSuccessor(t *testing.T) {
	ctx := context.Background()
	l := newSwitchLive(t, ctx)
	for round := 0; round < 5; round++ {
		_, predecessor := l.issue(t, l.orgA, nil)
		before := l.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, l.user)
		const racers = 4
		statuses := make([]int, racers)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < racers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				response := post(l.server, PatternIssueSession, issueBodyWithPredecessor(l.user, l.orgB.String(), `"`+predecessor.String()+`"`), issuerBearer)
				statuses[i] = response.Code
			}(i)
		}
		close(start)
		wg.Wait()
		ok, refused := 0, 0
		for _, status := range statuses {
			switch status {
			case http.StatusOK:
				ok++
			case http.StatusUnauthorized:
				refused++
			default:
				t.Fatalf("round %d: status %d", round, status)
			}
		}
		created := l.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, l.user) - before
		if ok != 1 || refused != racers-1 || created != 1 {
			t.Fatalf("round %d: %d issued, %d refused, %d sessions created; want 1, %d, 1", round, ok, refused, created, racers-1)
		}
		if l.live(t, predecessor) {
			t.Fatalf("round %d: the predecessor is still live", round)
		}
	}
}

// A predecessor of another principal is refused 403 and nothing is written:
// the other principal's session stays live, and no session or audit row is
// created for the caller.
func TestSwitchRefusesAnotherPrincipalsSession(t *testing.T) {
	ctx := context.Background()
	l := newSwitchLive(t, ctx)
	stranger := l.principal(t, ctx)
	theirs, err := l.store.Create(ctx, stranger, nil, []string{"pwd"})
	if err != nil {
		t.Fatal(err)
	}
	sessions := l.count(t, ctx, `SELECT count(*) FROM auth.sessions`)
	audits := l.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events`)
	predecessor := theirs.Session.ID
	if status, _ := l.issue(t, l.orgB, &predecessor); status != http.StatusForbidden {
		t.Fatalf("status %d, want 403", status)
	}
	row, found, err := l.reader.SessionState(ctx, predecessor)
	if err != nil || !found || row.Revoked {
		t.Fatalf("the other principal's session: found %v revoked %v %v, want live", found, row.Revoked, err)
	}
	if n := l.count(t, ctx, `SELECT count(*) FROM auth.sessions`); n != sessions {
		t.Fatalf("%d sessions created by a refused switch", n-sessions)
	}
	if n := l.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events`); n != audits {
		t.Fatalf("%d audit rows written by a refused switch", n-audits)
	}
}

// An expired predecessor (its absolute session lifetime passed) refuses the
// switch like a revoked one: 401 and nothing written.
func TestSwitchRefusesAnExpiredPredecessor(t *testing.T) {
	ctx := context.Background()
	l := newSwitchLive(t, ctx)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	short, err := session.New(session.Config{Pool: l.admin, Schema: schema, SessionLifetime: time.Second,
		SuccessorKey: contracts.NewSecretString(base64.RawURLEncoding.EncodeToString(key))})
	if err != nil {
		t.Fatal(err)
	}
	org := l.orgA
	issued, err := short.Create(ctx, l.user, &org, []string{"pwd"})
	if err != nil {
		t.Fatal(err)
	}
	expired := issued.Session.ID
	time.Sleep(1500 * time.Millisecond)
	if current, err := l.store.Current(ctx, expired); err != nil || current.ExpiresAt.After(time.Now()) {
		t.Fatalf("the predecessor is not expired yet: %+v %v", current, err)
	}
	sessions := l.count(t, ctx, `SELECT count(*) FROM auth.sessions`)
	audits := l.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events`)
	if status, _ := l.issue(t, l.orgB, &expired); status != http.StatusUnauthorized {
		t.Fatalf("switch from an expired session: %d, want 401", status)
	}
	if n := l.count(t, ctx, `SELECT count(*) FROM auth.sessions`); n != sessions {
		t.Fatalf("%d sessions created by a refused switch", n-sessions)
	}
	if n := l.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events`); n != audits {
		t.Fatalf("%d audit rows written by a refused switch", n-audits)
	}
}

// A refresh of the predecessor racing its supersede: whichever takes the
// session lock first, the switch issues its successor, the predecessor ends
// with no live credential, and a credential the refresh handed out is refused
// afterwards.
func TestARefreshRacingASupersedeLeavesNoLivePredecessor(t *testing.T) {
	ctx := context.Background()
	l := newSwitchLive(t, ctx)
	for round := 0; round < 8; round++ {
		response := post(l.server, PatternIssueSession, issueBody(l.user, l.orgA.String(), "pwd"), issuerBearer)
		if response.Code != http.StatusOK {
			t.Fatalf("round %d: sign in %d", round, response.Code)
		}
		var answer struct {
			RefreshToken string `json:"refresh_token"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &answer); err != nil {
			t.Fatal(err)
		}
		var predecessor uuid.UUID
		if err := l.admin.QueryRow(ctx, `SELECT session_id FROM auth.refresh_credentials WHERE token_hash IS NOT NULL ORDER BY created_at DESC LIMIT 1`).Scan(&predecessor); err != nil {
			t.Fatal(err)
		}
		var (
			wg       sync.WaitGroup
			result   session.Result
			exErr    error
			switched int
		)
		start := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			result, exErr = l.store.Exchange(ctx, contracts.NewSecretString(answer.RefreshToken))
		}()
		go func() {
			defer wg.Done()
			<-start
			switched = post(l.server, PatternIssueSession, issueBodyWithPredecessor(l.user, l.orgB.String(), `"`+predecessor.String()+`"`), issuerBearer).Code
		}()
		close(start)
		wg.Wait()
		if exErr != nil {
			t.Fatalf("round %d: exchange error %v", round, exErr)
		}
		if switched != http.StatusOK {
			t.Fatalf("round %d: switch %d, want 200 whichever order the lock gave", round, switched)
		}
		if l.live(t, predecessor) || l.liveCredentials(t, predecessor) != 0 {
			t.Fatalf("round %d: predecessor live=%v with %d live credentials", round, l.live(t, predecessor), l.liveCredentials(t, predecessor))
		}
		if result.Reason.Success() {
			again, err := l.store.Exchange(ctx, result.Credential)
			if err != nil || again.Reason.Success() {
				t.Fatalf("round %d: the refresh's successor still exchanges after the supersede: %+v %v", round, again.Reason, err)
			}
		}
		t.Logf("round %d: refresh %s, switch %d", round, result.Reason, switched)
	}
}
