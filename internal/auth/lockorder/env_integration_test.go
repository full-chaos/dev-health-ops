//go:build integration

package lockorder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/authmail"
	"github.com/full-chaos/dev-health-ops/internal/api/billing"
	"github.com/full-chaos/dev-health-ops/internal/api/billing/stripeclient"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/session"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/invite"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	authsession "github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/auth/signedtoken"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

// env is one Postgres with go-api's schema and the auth schema, the auth
// stores on the runtime role and go-api's admin and session routes on the
// admin role. Both pools carry a barrier: a test holds one statement of a
// writer at its start, so each writer runs its own code up to that point.
type env struct {
	admin   *pgxpool.Pool
	runtime *pgxpool.Pool
	// goPool is go-api's pool: every statement on it passes goBarrier.
	goPool   *pgxpool.Pool
	members  *membership.Store
	invites  *invite.Store
	identity *identity.Store
	// sessions and platform are the delegation cells' stores on the runtime role.
	sessions    *authsession.Store
	platform    *platformrole.Store
	authBarrier *barrier
	goBarrier   *barrier
	server      *httptest.Server
	// authServer is auth-service's compatibility routes (go-api's refresh and
	// the session issue route) over the auth stores on the runtime role.
	authServer *httptest.Server
	// issuerServer is go-api's session routes with auth-service owning
	// sessions (AUTH_PROXY_SESSIONS): the session comes from authServer.
	issuerServer *httptest.Server
	// authServerOne is authServer's routes over runtimeOne, a one-connection
	// pool on the runtime role; issueOne is go-api's issue client for it.
	authServerOne *httptest.Server
	runtimeOne    *pgxpool.Pool
	issueOne      *sessionissue.Client
	signer        *edgetoken.Signer
	linkSecret    string
	inviteKey     string
	root          string
	// webhookKey signs the Stripe deliveries of the billing cells.
	webhookKey string
	goLog      *syncBuffer
}

var (
	shared     *env
	sharedErr  error
	sharedOnce sync.Once
	closers    []func()
)

func TestMain(m *testing.M) {
	code := m.Run()
	for i := len(closers) - 1; i >= 0; i-- {
		closers[i]()
	}
	os.Exit(code)
}

func getEnv(t *testing.T) *env {
	t.Helper()
	sharedOnce.Do(func() { shared, sharedErr = newEnv(t) })
	if sharedErr != nil {
		t.Fatal(sharedErr)
	}
	shared.authBarrier.Release()
	shared.goBarrier.Release()
	return shared
}

func newEnv(t *testing.T) (*env, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	// A private server: the cells count the lock waits of the whole server.
	instance, err := containers.StartPostgresIsolated(ctx)
	if err != nil {
		return nil, err
	}
	closers = append(closers, func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = instance.Close(closeCtx)
	})
	admin0, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		return nil, fmt.Errorf("admin pool: %w", err)
	}
	closers = append(closers, admin0.Close)
	pgschema.Apply(ctx, t, admin0)
	role, err := containers.RoleName("lockorder_runtime", instance)
	if err != nil {
		return nil, fmt.Errorf("derive role: %w", err)
	}
	const password = "lockorder-test-only-not-a-credential"
	if _, err := admin0.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		return nil, fmt.Errorf("create role: %w", err)
	}
	closers = append(closers, func() { containers.DropRole(admin0, role, func(string, ...any) {}) })
	if _, err := authschema.Apply(ctx, admin0, authschema.Options{Schema: "auth", RuntimeRole: role, UsersOwned: true}); err != nil {
		return nil, fmt.Errorf("authschema.Apply: %w", err)
	}
	e := &env{admin: admin0, authBarrier: &barrier{}, goBarrier: &barrier{}, goLog: &syncBuffer{}}

	runtimeCfg := admin0.Config().Copy()
	runtimeCfg.ConnConfig.User = role
	runtimeCfg.ConnConfig.Password = password
	runtimeCfg.ConnConfig.Tracer = e.authBarrier
	if e.runtime, err = pgxpool.NewWithConfig(ctx, runtimeCfg); err != nil {
		return nil, fmt.Errorf("runtime pool: %w", err)
	}
	closers = append(closers, e.runtime.Close)
	goCfg := admin0.Config().Copy()
	goCfg.ConnConfig.Tracer = e.goBarrier
	goPool, err := pgxpool.NewWithConfig(ctx, goCfg)
	if err != nil {
		return nil, fmt.Errorf("go-api pool: %w", err)
	}
	closers = append(closers, goPool.Close)
	e.goPool = goPool

	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		return nil, err
	}
	if e.members, err = membership.New(membership.Config{Pool: e.runtime, Schema: schema, OwnsUsers: true}); err != nil {
		return nil, err
	}
	e.inviteKey = "lockorder-invite-key-" + uuid.NewString()
	if e.invites, err = invite.New(invite.Config{Pool: e.runtime, Schema: schema, OwnsUsers: true, Memberships: e.members,
		Secret: e.inviteKey, AcceptSecrets: []string{e.inviteKey}}); err != nil {
		return nil, err
	}
	if e.identity, err = identity.New(identity.Config{Pool: e.runtime, Schema: schema, UsersOwnedByAuth: true}); err != nil {
		return nil, err
	}
	successor, err := randomText(32)
	if err != nil {
		return nil, err
	}
	if e.sessions, err = authsession.New(authsession.Config{Pool: e.runtime, Schema: schema,
		SuccessorKey: contracts.NewSecretString(successor), UsersOwned: true, PlatformRolesOwned: true}); err != nil {
		return nil, err
	}
	if e.platform, err = platformrole.New(platformrole.Config{Pool: e.runtime, Schema: schema}); err != nil {
		return nil, err
	}

	ch, err := containers.StartClickHouseIsolated(ctx)
	if err != nil {
		return nil, fmt.Errorf("start clickhouse: %w", err)
	}
	closers = append(closers, func() { _ = ch.Close(context.Background()) })
	chConn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(ch.URI))
	if err != nil {
		return nil, fmt.Errorf("clickhouse: %w", err)
	}
	// The org deletion's preflight needs one org_id table to purge.
	err = chConn.Exec(ctx, "CREATE TABLE lockorder_probe (org_id String, n UInt8) ENGINE = MergeTree ORDER BY tuple()")
	_ = chConn.Close()
	if err != nil {
		return nil, fmt.Errorf("clickhouse table: %w", err)
	}

	const jwtKey = "lockorder-test-signing-key-32-bytes-long!!"
	verifier, err := edgetoken.New(jwtKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		return nil, err
	}
	if e.signer, err = edgetoken.NewSigner(jwtKey, "dev-health-ops", "dev-health-api"); err != nil {
		return nil, err
	}
	logger := slog.New(slog.NewTextHandler(e.goLog, nil))
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: admin0}, logger)
	if err != nil {
		return nil, err
	}
	guard := policy.NewGuard(auth, logger)
	e.linkSecret = "lockorder-link-secret-" + uuid.NewString()
	mux := http.NewServeMux()
	for _, route := range admin.Routes(admin.Deps{Pool: goPool, Guard: guard, Logger: logger, ClickHouseDSN: ch.URI}) {
		if strings.HasPrefix(route.Pattern, "/api/v1/admin/orgs/") {
			mux.Handle(route.Method+" "+route.Pattern, route.Handler)
		}
	}
	for _, route := range session.Routes(session.Deps{Pool: goPool, Guard: guard, Auth: auth, Verifier: verifier, Signer: e.signer,
		Mail: authmail.Config{LinkSecret: e.linkSecret}, Limits: httpapi.NewMemoryCounters(nil), Logger: logger}) {
		switch route.Pattern {
		case "/api/v1/auth/accept-invite", "/api/v1/auth/onboard", "/api/v1/auth/refresh":
			mux.Handle(route.Method+" "+route.Pattern, route.Handler)
		}
	}
	e.webhookKey = "lockorder-webhook-" + uuid.NewString()
	for _, route := range billing.Routes(billing.Deps{Pool: goPool, Guard: guard, Logger: logger,
		Stripe: stripeclient.New(stripeclient.Options{Key: "sk_test_lockorder"}), WebhookSecret: secrets.NewValue(e.webhookKey)}) {
		if route.Pattern == stripeWebhookPath {
			mux.Handle(route.Method+" "+route.Pattern, route.Handler)
		}
	}
	e.server = httptest.NewServer(mux)
	closers = append(closers, e.server.Close)
	if err := e.startAuthPlane(ctx, schema, logger, jwtKey); err != nil {
		return nil, err
	}
	issuer, err := e.seedIssueClient(ctx)
	if err != nil {
		return nil, err
	}
	issuerMux := http.NewServeMux()
	for _, route := range session.Routes(session.Deps{Pool: goPool, Guard: guard, Auth: auth, Verifier: verifier, Signer: e.signer,
		Mail: authmail.Config{LinkSecret: e.linkSecret}, Limits: httpapi.NewMemoryCounters(nil), Logger: logger, Issuer: issuer}) {
		switch route.Pattern {
		case "/api/v1/auth/accept-invite", "/api/v1/auth/onboard":
			issuerMux.Handle(route.Method+" "+route.Pattern, route.Handler)
		}
	}
	e.issuerServer = httptest.NewServer(issuerMux)
	closers = append(closers, e.issuerServer.Close)

	rootID := uuid.New()
	if _, err := admin0.Exec(ctx, `INSERT INTO public.users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, true, 0, now(), now())`, rootID, "root-"+rootID.String()[:8]+"@example.com"); err != nil {
		return nil, fmt.Errorf("seed root: %w", err)
	}
	if e.root, err = e.token(rootID, true); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *env) token(id uuid.UUID, superuser bool) (string, error) {
	return e.signer.Access(edgetoken.AccessClaims{UserID: id.String(), Email: id.String() + "@example.com", IsSuperuser: superuser},
		time.Now(), uuid.NewString())
}

// barrier holds the first statement whose SQL starts with the armed prefix,
// at its start, until Release. Every other statement runs.
type barrier struct {
	mu      sync.Mutex
	prefix  string
	reached chan struct{}
	release chan struct{}
}

func (b *barrier) Arm(prefix string) (reached <-chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prefix, b.reached, b.release = prefix, make(chan struct{}), make(chan struct{})
	return b.reached
}

func (b *barrier) Release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prefix = ""
	if b.release != nil {
		close(b.release)
		b.release = nil
	}
}

func (b *barrier) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	b.mu.Lock()
	hit := b.prefix != "" && strings.HasPrefix(strings.TrimSpace(data.SQL), b.prefix)
	var reached, release chan struct{}
	if hit {
		b.prefix, reached, release = "", b.reached, b.release
	}
	b.mu.Unlock()
	if hit {
		close(reached)
		<-release
	}
	return ctx
}

func (b *barrier) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

// contains reports whether go-api's log holds text.
func (s *syncBuffer) contains(text string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Contains(s.buf.String(), text)
}

// deadlockLines are go-api's logged errors that name a deadlock.
func (s *syncBuffer) deadlockLines() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, line := range strings.Split(s.buf.String(), "\n") {
		if strings.Contains(line, "40P01") || strings.Contains(line, "deadlock detected") {
			out = append(out, line)
		}
	}
	return out
}

func allowAll(context.Context, audit.TxOps, membership.Locked) error { return nil }

func allowInvite(context.Context, audit.TxOps, uuid.UUID) error { return nil }

func isDeadlock(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "40P01"
}

func (e *env) seedUser(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', true, true, false, 0, now(), now())`, id, email(id)); err != nil {
		t.Fatalf("seed public.users: %v", err)
	}
	if err := e.identity.Project(ctx, id); err != nil {
		t.Fatalf("Project: %v", err)
	}
	return id
}

func email(id uuid.UUID) string { return "member-" + id.String()[:8] + "@example.com" }

// seedPair seeds two users and returns them in id order (the order Postgres
// sorts uuid values in).
func (e *env) seedPair(t *testing.T, ctx context.Context) (lo, hi uuid.UUID) {
	t.Helper()
	a, b := e.seedUser(t, ctx), e.seedUser(t, ctx)
	if bytes.Compare(a[:], b[:]) > 0 {
		a, b = b, a
	}
	return a, b
}

func (e *env) seedOrg(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'lockorder-test')`,
		id, "org-"+id.String()[:8]); err != nil {
		t.Fatalf("seed public.organizations: %v", err)
	}
	if err := e.identity.ProjectOrganization(ctx, id); err != nil {
		t.Fatalf("ProjectOrganization: %v", err)
	}
	return id
}

func (e *env) add(t *testing.T, ctx context.Context, org, user uuid.UUID, role string, inviter *uuid.UUID) {
	t.Helper()
	if _, err := e.members.Add(ctx, membership.Change{OrgID: org, PrincipalID: user, Role: role, InvitedBy: inviter, Authorize: allowAll}); err != nil {
		t.Fatalf("Add %s: %v", role, err)
	}
}

// goInvite inserts an invite only go-api holds (public.org_invites).
func (e *env) goInvite(t *testing.T, ctx context.Context, org, inviter uuid.UUID, to string) (id uuid.UUID, token string) {
	t.Helper()
	id = uuid.New()
	token, digest := signedtoken.Build(id, e.linkSecret)
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.org_invites (id, org_id, email, role, token_hash, invited_by_id, status, expires_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', $4, $5, 'pending', now() + interval '1 day', now(), now())`, id, org, to, digest, inviter); err != nil {
		t.Fatalf("seed public.org_invites: %v", err)
	}
	return id, token
}

func (e *env) scalar(t *testing.T, ctx context.Context, sql string, args ...any) string {
	t.Helper()
	var out string
	if err := e.admin.QueryRow(ctx, sql, args...).Scan(&out); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return out
}

func (e *env) publicRole(t *testing.T, ctx context.Context, org, user uuid.UUID) string {
	return e.scalar(t, ctx, `SELECT coalesce((SELECT role FROM public.memberships WHERE org_id = $1 AND user_id = $2), '-')`, org, user)
}

func (e *env) authState(t *testing.T, ctx context.Context, org, user uuid.UUID) string {
	return e.scalar(t, ctx, `SELECT coalesce((SELECT status || ':' || coalesce(role, '') FROM auth.memberships
WHERE organization_id = $1 AND principal_id = $2), '-')`, org, user)
}

// waiters counts the backends of this database that wait on a lock.
func (e *env) waiters(t *testing.T, ctx context.Context) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
WHERE NOT l.granted AND a.datname = current_database()`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// response is one go-api answer.
type response struct {
	status int
	body   map[string]any
	raw    string
}

func (e *env) call(ctx context.Context, bearer, method, path string, body any) response {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return response{status: -1, raw: err.Error()}
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, e.server.URL+path, reader)
	if err != nil {
		return response{status: -1, raw: err.Error()}
	}
	request.Header.Set("Authorization", "Bearer "+bearer)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return response{status: -1, raw: err.Error()}
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	out := response{status: resp.StatusCode, raw: string(raw)}
	_ = json.Unmarshal(raw, &out.body)
	return out
}

func (e *env) addMember(ctx context.Context, org, user uuid.UUID, inviter *uuid.UUID) response {
	body := map[string]any{"user_id": user.String(), "role": "member"}
	if inviter != nil {
		body["invited_by_id"] = inviter.String()
	}
	return e.call(ctx, e.root, http.MethodPost, "/api/v1/admin/orgs/"+org.String()+"/members", body)
}

func (e *env) setMemberRole(ctx context.Context, org, user uuid.UUID, role string) response {
	return e.call(ctx, e.root, http.MethodPatch, "/api/v1/admin/orgs/"+org.String()+"/members/"+user.String(), map[string]any{"role": role})
}

func (e *env) deleteOrg(ctx context.Context, org uuid.UUID) response {
	return e.call(ctx, e.root, http.MethodDelete, "/api/v1/admin/orgs/"+org.String(), nil)
}

func (e *env) acceptInvite(ctx context.Context, user uuid.UUID, token string) response {
	bearer, err := e.token(user, false)
	if err != nil {
		return response{status: -1, raw: err.Error()}
	}
	return e.call(ctx, bearer, http.MethodPost, "/api/v1/auth/accept-invite", map[string]any{"token": token})
}

// party is one writer of a cell, run in its own goroutine.
type party struct {
	name string
	done chan struct{}
	err  error
	resp response
}

func goAuth(name string, f func() error) *party {
	p := &party{name: name, done: make(chan struct{})}
	go func() {
		defer close(p.done)
		p.err = f()
	}()
	return p
}

func goAPI(name string, f func() response) *party {
	p := &party{name: name, done: make(chan struct{})}
	go func() {
		defer close(p.done)
		p.resp = f()
	}()
	return p
}

func (p *party) finished() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *party) String() string {
	if p.resp.status != 0 {
		return fmt.Sprintf("%s answered %d %s", p.name, p.resp.status, strings.TrimSpace(p.resp.raw))
	}
	return fmt.Sprintf("%s returned %v", p.name, p.err)
}

const choreographyBound = 20 * time.Second

// awaitReached waits for a barrier; a party that ends first, or a timeout,
// fails the cell by name.
func awaitReached(t *testing.T, cell, what string, reached <-chan struct{}, parties ...*party) {
	t.Helper()
	deadline := time.After(choreographyBound)
	for {
		select {
		case <-reached:
			return
		case <-deadline:
			t.Fatalf("%s: choreography: %s was never reached", cell, what)
		case <-time.After(10 * time.Millisecond):
		}
		for _, p := range parties {
			if p.finished() {
				t.Fatalf("%s: choreography: %s ended before %s: %s", cell, p.name, what, p)
			}
		}
	}
}

// awaitWaiters waits until exactly n backends of this database wait on a
// lock while every party still runs. More waiters, a party that ends first or
// a timeout fail the cell by name.
func (e *env) awaitWaiters(t *testing.T, ctx context.Context, cell string, n int, parties ...*party) {
	t.Helper()
	deadline := time.Now().Add(choreographyBound)
	for {
		for _, p := range parties {
			if p.finished() {
				t.Fatalf("%s: choreography: %s ended before %d lock waits: %s", cell, p.name, n, p)
			}
		}
		got := e.waiters(t, ctx)
		if got == n {
			return
		}
		if got > n {
			t.Fatalf("%s: choreography: %d backends wait on a lock, want %d", cell, got, n)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: choreography: %d backends wait on a lock after %s, want %d", cell, got, choreographyBound, n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// finish waits for every party (bounded) and fails the cell on a deadlock
// (40P01, from the auth verb or in go-api's log) or any other failure.
func (e *env) finish(t *testing.T, cell string, parties ...*party) {
	t.Helper()
	deadline := time.After(2 * choreographyBound)
	for _, p := range parties {
		select {
		case <-p.done:
		case <-deadline:
			t.Fatalf("%s: %s did not end within %s", cell, p.name, 2*choreographyBound)
		}
	}
	for _, p := range parties {
		if isDeadlock(p.err) {
			t.Errorf("%s: deadlock: SQLSTATE 40P01 in %s", cell, p)
		}
	}
	if lines := e.goLog.deadlockLines(); len(lines) > 0 {
		t.Errorf("%s: deadlock: SQLSTATE 40P01 in go-api: %s", cell, strings.Join(lines, " | "))
	}
	if t.Failed() {
		t.FailNow()
	}
}

func (e *env) resetGoLog() {
	e.goLog.mu.Lock()
	e.goLog.buf.Reset()
	e.goLog.mu.Unlock()
}
