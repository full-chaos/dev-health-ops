//go:build integration

package authapi

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// preflightStore is a live auth schema in a container of its own, written by
// a session.Store that connects as the runtime role.
type preflightStore struct {
	admin *pgxpool.Pool
	store *session.Store
}

func newPreflightStore(t *testing.T, ctx context.Context) *preflightStore {
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
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("preflight_runtime", instance)
	if err != nil {
		t.Fatalf("derive role: %v", err)
	}
	const password = "preflight-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		t.Fatalf("create role: %v", err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User, cfg.ConnConfig.Password = role, password
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("runtime pool: %v", err)
	}
	t.Cleanup(runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	store, err := session.New(session.Config{Pool: runtime, Schema: schema,
		SuccessorKey: contracts.NewSecretString(base64.RawURLEncoding.EncodeToString(key))})
	if err != nil {
		t.Fatal(err)
	}
	return &preflightStore{admin: admin, store: store}
}

func (p *preflightStore) principal(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := p.admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'preflight-test')`, id.String()); err != nil {
		t.Fatalf("seed principal: %v", err)
	}
	return id
}

func (p *preflightStore) count(t *testing.T, ctx context.Context, query string, args ...any) int {
	t.Helper()
	var n int
	if err := p.admin.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func serveRoutes(routes []httpapi.Route) http.Handler {
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(route.Handler))
	}
	return mux
}

func post(server http.Handler, path, body string, header map[string]string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	for k, v := range header {
		request.Header.Set(k, v)
	}
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	return response
}

func TestRefreshSignerUnavailableLeavesParentLiveInTheStore(t *testing.T) {
	ctx := context.Background()
	p := newPreflightStore(t, ctx)
	principal := p.principal(t, ctx)
	issued, err := p.store.Create(ctx, principal, nil, []string{"pwd"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	users := fakeUsers{users: map[uuid.UUID]UserState{principal: {Email: "ada@example.com", IsActive: true}}}
	routes, err := Routes(Deps{Sessions: p.store, Users: users, Issuer: newEdDSAIssuer(t, unreadableKeySigner(t), testAudience),
		Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	server := serveRoutes(routes)

	const credentials = `SELECT count(*) FROM auth.refresh_credentials WHERE principal_id = $1`
	const consumed = `SELECT count(*) FROM auth.refresh_credentials WHERE principal_id = $1 AND consumed_at IS NOT NULL`
	before := p.count(t, ctx, credentials, principal.String())
	response := post(server, PatternRefresh, refreshBody(issued.Credential.Reveal()), nil)
	expectError(t, response, http.StatusServiceUnavailable, reasonDependency)
	if after := p.count(t, ctx, credentials, principal.String()); after != before {
		t.Fatalf("refresh_credentials rows %d -> %d: a successor was written before the token could be signed", before, after)
	}
	if n := p.count(t, ctx, consumed, principal.String()); n != 0 {
		t.Fatalf("%d consumed credentials: the parent was consumed before the token could be signed", n)
	}
	result, err := p.store.Exchange(ctx, issued.Credential)
	if err != nil || result.Reason != session.ReasonAccepted {
		t.Fatalf("the same credential after the refusal: %q, %v; want %q (the parent stayed live)", result.Reason, err, session.ReasonAccepted)
	}
}

func TestIssueSignerUnavailableWritesNoSessionRow(t *testing.T) {
	ctx := context.Background()
	p := newPreflightStore(t, ctx)
	principal := p.principal(t, ctx)
	users := fakeUsers{users: map[uuid.UUID]UserState{principal: {Email: "ada@example.com", IsActive: true}}}
	routes, err := CompatRoutes(CompatDeps{
		Sessions: p.store, Users: users, Issuer: newEdDSAIssuer(t, unreadableKeySigner(t), testAudience),
		Logger:             slog.New(slog.DiscardHandler),
		Projector:          &fakeProjector{orgs: map[uuid.UUID]bool{}, gone: map[uuid.UUID]bool{}},
		AppendAudit:        func(context.Context, audit.AuditEvent) error { return nil },
		ServiceCredentials: &fakeServiceCredentials{id: uuid.NewString()},
		Limits:             mustLimits(t, httpapi.NewMemoryCounters(nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	server := serveRoutes(routes)

	response := post(server, PatternIssueSession, issueBody(principal, "", "pwd"), issuerBearer)
	expectError(t, response, http.StatusServiceUnavailable, reasonDependency)
	if n := p.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, principal.String()); n != 0 {
		t.Fatalf("%d session rows written before the token could be signed", n)
	}
	if n := p.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials WHERE principal_id = $1`, principal.String()); n != 0 {
		t.Fatalf("%d refresh credential rows written before the token could be signed", n)
	}
	if n := p.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1 AND subject_principal_id = $2`,
		EventSessionIssued, principal.String()); n != 0 {
		t.Fatalf("%d %s audit rows written before the token could be signed", n, EventSessionIssued)
	}
}

// Outage behaviour (acp, class level), on the live store: an unknown
// credential and a revoked session's credential, presented while the eddsa
// signer cannot sign, answer 503 and change no row.
func TestRefreshOfARefusedCredentialChangesNoRowWhileTheSignerIsUnavailable(t *testing.T) {
	ctx := context.Background()
	p := newPreflightStore(t, ctx)
	principal := p.principal(t, ctx)
	revoked, err := p.store.Create(ctx, principal, nil, []string{"pwd"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if ok, err := p.store.Revoke(ctx, revoked.Session.ID, session.Actor{}); err != nil || !ok {
		t.Fatalf("Revoke: %v %v", ok, err)
	}
	users := fakeUsers{users: map[uuid.UUID]UserState{principal: {Email: "ada@example.com", IsActive: true}}}
	routes, err := Routes(Deps{Sessions: p.store, Users: users, Issuer: newEdDSAIssuer(t, unreadableKeySigner(t), testAudience),
		Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	server := serveRoutes(routes)
	snapshot := func() [4]int {
		return [4]int{
			p.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials`),
			p.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials WHERE consumed_at IS NOT NULL OR revoked_at IS NOT NULL`),
			p.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE revoked_at IS NOT NULL`),
			p.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events`),
		}
	}
	for name, presented := range map[string]string{
		"unknown credential": credential(),
		"revoked session":    revoked.Credential.Reveal(),
	} {
		before := snapshot()
		response := post(server, PatternRefresh, refreshBody(presented), nil)
		expectError(t, response, http.StatusServiceUnavailable, reasonDependency)
		if after := snapshot(); after != before {
			t.Fatalf("%s: rows (credentials, ended credentials, revoked sessions, audit) %v -> %v while the signer is unavailable", name, before, after)
		}
	}
}
