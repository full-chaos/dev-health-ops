//go:build integration

package authapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/servicecred"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// workloadIssueLive is the session issue route over a live auth schema, the
// real session store and audit writer, and a file-backed signer.
type workloadIssueLive struct {
	*preflightStore
	server    http.Handler
	signer    *token.Signer
	keys      *issueCountingKeys
	creds     *fakeServiceCredentials
	user      uuid.UUID
	workload  uuid.UUID
	subject   string
	userState func(UserState)
}

func newWorkloadIssueLive(t *testing.T, ctx context.Context) *workloadIssueLive {
	t.Helper()
	p := newPreflightStore(t, ctx)
	signer, _, _ := fileSigner(t)
	keys := &issueCountingKeys{inner: signer}
	user := p.principal(t, ctx)
	workload := uuid.New()
	if _, err := p.admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'workload', 'go-api-test')`, workload.String()); err != nil {
		t.Fatalf("seed workload principal: %v", err)
	}
	subject := session.WirePrincipalID(workload)
	issueWorkload, err := NewSessionIssueWorkload(SessionIssueWorkloadConfig{IssuerURL: goldenIssuer, Keys: keys, Subject: subject})
	if err != nil {
		t.Fatal(err)
	}
	live := &workloadIssueLive{preflightStore: p, signer: signer, keys: keys, creds: &fakeServiceCredentials{id: uuid.NewString()}, user: user, workload: workload, subject: subject}
	users := &fakeUsers{users: map[uuid.UUID]UserState{user: {Email: "ada@example.com", IsActive: true}}}
	routes, err := CompatRoutes(CompatDeps{
		Sessions: p.store, Users: users, Issuer: newEdDSAIssuer(t, signer, testAudience),
		Logger:        slog.New(slog.DiscardHandler),
		Projector:     &fakeProjector{orgs: map[uuid.UUID]bool{}, gone: map[uuid.UUID]bool{}},
		AppendAudit:   func(context.Context, audit.AuditEvent) error { return nil },
		IssueWorkload: issueWorkload, ServiceCredentials: live.creds,
		Meter:  sdkmetric.NewMeterProvider().Meter("test"),
		Limits: mustLimits(t, httpapi.NewMemoryCounters(nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	live.server = serveRoutes(routes)
	live.userState = func(state UserState) { users.users[user] = state }
	return live
}

func (l *workloadIssueLive) token(t *testing.T, aud string) (map[string]string, string) {
	t.Helper()
	now := time.Now()
	claims := contracts.WorkloadTokenClaims{
		Iss: goldenIssuer, Aud: aud, Sub: l.subject, PrincipalType: "workload", Jti: uuid.NewString(),
		Iat: now.Unix(), Nbf: now.Unix(), Exp: now.Unix() + 300,
	}
	payload, _ := json.Marshal(claims)
	compact, _, err := l.signer.Sign(context.Background(), contracts.WorkloadTokenTyp, payload)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]string{"Authorization": "Bearer " + compact}, claims.Jti
}

func (l *workloadIssueLive) sessions(t *testing.T, ctx context.Context) int {
	t.Helper()
	return l.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, l.user.String())
}

// I1: a workload token with audience auth-service and the configured subject
// issues a session on the live store; the audit row, written in the session's
// transaction, names the workload as actor and its jti, with no credential_id.
func TestIssueSessionWorkloadTokenOnTheLiveStore(t *testing.T) {
	ctx := context.Background()
	l := newWorkloadIssueLive(t, ctx)
	header, jti := l.token(t, "auth-service")
	response := post(l.server, PatternIssueSession, issueBody(l.user, "", "pwd"), header)
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s", response.Code, response.Body)
	}
	if n := l.sessions(t, ctx); n != 1 {
		t.Fatalf("%d session rows, want 1", n)
	}
	if n := l.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1 AND subject_principal_id = $2 AND actor_principal_id = $3 AND attributes->>'workload_jti' = $4 AND NOT (attributes ? 'credential_id')`,
		EventSessionIssued, l.user.String(), l.workload.String(), jti); n != 1 {
		t.Fatalf("%d matching %s rows, want 1", n, EventSessionIssued)
	}
	if len(l.creds.seen) != 0 {
		t.Fatalf("the workload path checked a service credential: %v", l.creds.seen)
	}
}

// I2: a token for the other workload audience is refused before any read and
// writes no row.
func TestIssueSessionRefusesAWrongAudienceTokenOnTheLiveStore(t *testing.T) {
	ctx := context.Background()
	l := newWorkloadIssueLive(t, ctx)
	header, _ := l.token(t, "dev-health-api")
	expectError(t, post(l.server, PatternIssueSession, issueBody(l.user, "", "pwd"), header), http.StatusUnauthorized, reasonInvalidCredentials)
	if l.keys.reads != 0 || len(l.creds.seen) != 0 {
		t.Fatalf("a wrong audience read the key set %d times and checked %d credentials", l.keys.reads, len(l.creds.seen))
	}
	if n := l.sessions(t, ctx); n != 0 {
		t.Fatalf("%d session rows after a refusal", n)
	}
	if n := l.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1`, EventSessionIssued); n != 0 {
		t.Fatalf("%d %s rows after a refusal", n, EventSessionIssued)
	}
}

// I3: a deactivated user is refused 403 on the workload path, and no row is
// written.
func TestIssueSessionRefusesADeactivatedUserOnTheWorkloadPath(t *testing.T) {
	ctx := context.Background()
	l := newWorkloadIssueLive(t, ctx)
	l.userState(UserState{Email: "ada@example.com", IsActive: false})
	header, _ := l.token(t, "auth-service")
	expectError(t, post(l.server, PatternIssueSession, issueBody(l.user, "", "pwd"), header), http.StatusForbidden, reasonDeactivated)
	if n := l.sessions(t, ctx); n != 0 {
		t.Fatalf("%d session rows for a deactivated user", n)
	}
}

// I4: svc_authsess_ still issues on the live store with its credential id in
// the audit row and no workload_jti, and a revoked one is refused with no row.
func TestIssueSessionKeepsTheServiceCredentialPathOnTheLiveStore(t *testing.T) {
	ctx := context.Background()
	l := newWorkloadIssueLive(t, ctx)
	l.creds.refusal = servicecred.RefusedRevoked
	expectError(t, post(l.server, PatternIssueSession, issueBody(l.user, "", "pwd"), issuerBearer), http.StatusUnauthorized, reasonInvalidCredentials)
	if n := l.sessions(t, ctx); n != 0 {
		t.Fatalf("%d session rows for a revoked credential", n)
	}
	l.creds.refusal = ""
	if response := post(l.server, PatternIssueSession, issueBody(l.user, "", "pwd"), issuerBearer); response.Code != http.StatusOK {
		t.Fatalf("status %d: %s", response.Code, response.Body)
	}
	if n := l.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1 AND attributes->>'credential_id' = $2 AND NOT (attributes ? 'workload_jti') AND actor_principal_id IS NULL`,
		EventSessionIssued, l.creds.id); n != 1 {
		t.Fatalf("%d matching %s rows, want 1", n, EventSessionIssued)
	}
}
