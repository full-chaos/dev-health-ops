//go:build integration

package authruntime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// sessionRoutesMarker is logged once the session routes were driven through
// configure against a live PostgreSQL.
const sessionRoutesMarker = "AUTH_SESSION_ROUTES_EXECUTED"

// publicReadGrants is the read-only column set on go-api's tables the session
// routes need until the users cutover: CHAOS-10224 as widened for C1.
var publicReadGrants = []string{
	`GRANT SELECT (id, email, username, full_name, password_hash, is_active, is_verified, is_superuser, token_version, auth_provider, auth_provider_id) ON public.users TO %q`,
	`GRANT SELECT (user_id, org_id, role) ON public.memberships TO %q`,
	`GRANT SELECT (id, org_id) ON public.sso_providers TO %q`,
	`GRANT SELECT (id, slug, name, is_active) ON public.organizations TO %q`,
}

type routesEnv struct {
	admin    *pgxpool.Pool
	runtime  *pgxpool.Pool
	sessions *session.Store
	identity *identity.Store
	front    *httptest.Server
	client   *http.Client
	edge     *principal.EdgeVerifier
	root     string
	logs     *safeBuffer
}

// newRoutesEnv builds the real stack: the go-api public schema, the auth
// schema, a runtime role with the production posture plus the column grants
// above, and the API handler that configure builds from a loaded Config whose
// DSN logs in AS that role.
func newRoutesEnv(t *testing.T, ctx context.Context) *routesEnv {
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
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("routes_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	const password = "routes-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role}); err != nil {
		t.Fatalf("authschema.Apply: %v", err)
	}
	for _, grant := range publicReadGrants {
		if _, err := admin.Exec(ctx, fmt.Sprintf(grant, role)); err != nil {
			t.Fatalf("grant: %v", err)
		}
	}
	dsn, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	dsn.User = url.UserPassword(role, password)

	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(sessionEnvironment(map[string]string{
		authconfig.EnvDatabaseURI:     dsn.String(),
		authconfig.EnvDatabaseSchema:  "auth",
		authconfig.EnvSigningKeyFile:  "/nonexistent",
		authconfig.EnvSigningKeyID:    "routes-test",
		authconfig.EnvAPIAddress:      "127.0.0.1:0",
		authconfig.EnvOperatorAddress: "127.0.0.1:1",
	}))})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	logs := &safeBuffer{}
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	components, err := configure(ctx, cfg, logger)
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	t.Cleanup(func() {
		for _, component := range components {
			_ = component.Shutdown(context.Background())
		}
	})
	var apiHandler http.Handler
	for _, component := range components {
		if announced, ok := component.(announcedListener); ok {
			if server, ok := announced.listener.(*httpapi.Server); ok {
				apiHandler = server.Handler()
			}
		}
	}
	if apiHandler == nil {
		t.Fatal("configure built no API listener")
	}
	front := httptest.NewServer(apiHandler)
	t.Cleanup(front.Close)

	runtimeConfig, err := pgxpool.ParseConfig(dsn.String())
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := pgxpool.NewWithConfig(ctx, runtimeConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := session.New(session.Config{Pool: runtime, Schema: schema, SuccessorKey: contracts.NewSecretString(testSuccessorKey)})
	if err != nil {
		t.Fatal(err)
	}
	identities, err := identity.New(identity.Config{Pool: runtime, Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	// query-api's edge verifier over the go-api users table: the consumer
	// that must keep accepting what auth-service mints.
	edge, err := principal.NewEdgeVerifier(testAccessSecret, "dev-health-ops", "dev-health-api", policy.PGStore{Pool: admin})
	if err != nil {
		t.Fatal(err)
	}
	wd, err := contractsRoot()
	if err != nil {
		t.Fatal(err)
	}
	return &routesEnv{admin: admin, runtime: runtime, sessions: sessions, identity: identities, front: front,
		client: &http.Client{Timeout: 10 * time.Second}, edge: edge, root: wd, logs: logs}
}

func contractsRoot() (string, error) {
	return contracts.RepoRoot(".")
}

// seed inserts an organization and a user with a membership in it as go-api
// writes them, then projects the user's principal and the organization into
// the auth schema through the identity store, as the runtime role.
func (e *routesEnv) seed(t *testing.T, ctx context.Context) (user, org uuid.UUID) {
	t.Helper()
	user, org = uuid.New(), uuid.New()
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO organizations (id, slug, name, tier, is_active, settings, managed_by, created_at, updated_at)
VALUES ($1, $2, 'Routes Org', 'community', true, '{}', 'self', now(), now())`, []any{org, "routes-" + org.String()[:8]}},
		{`INSERT INTO users (id, email, username, full_name, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, 'Routes User', NULL, 'local', true, true, false, 2, now(), now())`, []any{user, "routes-" + user.String()[:8] + "@example.test", "routes-" + user.String()[:8]}},
		{`INSERT INTO memberships (id, user_id, org_id, role, joined_at, created_at) VALUES (gen_random_uuid(), $1, $2, 'owner', now(), now())`, []any{user, org}},
	} {
		if _, err := e.admin.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if err := e.identity.Project(ctx, user); err != nil {
		t.Fatalf("project the principal: %v", err)
	}
	if err := e.identity.ProjectOrganization(ctx, org); err != nil {
		t.Fatalf("project the organization: %v", err)
	}
	return user, org
}

func (e *routesEnv) start(t *testing.T, ctx context.Context, user uuid.UUID, org *uuid.UUID) session.Issued {
	t.Helper()
	issued, err := e.sessions.Create(ctx, user, org, []string{"pwd"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return issued
}

type answer struct {
	status int
	body   []byte
	header http.Header
}

func (e *routesEnv) do(t *testing.T, method, path, body, bearer string) answer {
	t.Helper()
	request, err := http.NewRequest(method, e.front.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	response, err := e.client.Do(request)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	return answer{status: response.StatusCode, body: raw, header: response.Header}
}

func refreshBody(credential string) string {
	return `{"schema_version":"refresh-request.v1","refresh_credential":"` + credential + `"}`
}

func (e *routesEnv) refresh(t *testing.T, credential string) (*contracts.RefreshResponse, answer) {
	t.Helper()
	got := e.do(t, http.MethodPost, "/v1/token/refresh", refreshBody(credential), "")
	if got.status != http.StatusOK {
		return nil, got
	}
	parsed, err := contracts.ParseRefreshResponse(e.root, got.body)
	if err != nil {
		t.Fatalf("200 is not refresh-response.v1: %v", err)
	}
	return parsed, got
}

func (e *routesEnv) refused(t *testing.T, got answer, status int, reason string) {
	t.Helper()
	if got.status != status {
		t.Fatalf("status = %d, want %d; body %s", got.status, status, got.body)
	}
	envelope, err := contracts.ParseErrorEnvelope(e.root, got.body, status, time.Time{})
	if err != nil {
		t.Fatalf("not error.v1: %v; body %s", err, got.body)
	}
	if envelope.ReasonCode != reason {
		t.Fatalf("reason_code = %q, want %q", envelope.ReasonCode, reason)
	}
	if echoed := got.header.Get("X-Auth-Request-ID"); echoed == "" || echoed != envelope.RequestID {
		t.Fatalf("X-Auth-Request-ID = %q, error.v1 request_id = %q: not the same server-minted id", echoed, envelope.RequestID)
	}
}

func (e *routesEnv) revokedAt(t *testing.T, ctx context.Context, wireID string) *time.Time {
	t.Helper()
	id, err := session.ParseWireSessionID(wireID)
	if err != nil {
		t.Fatal(err)
	}
	var at *time.Time
	if err := e.admin.QueryRow(ctx, `SELECT revoked_at FROM auth.sessions WHERE id = $1`, id).Scan(&at); err != nil {
		t.Fatal(err)
	}
	return at
}

// TestSessionRoutesThroughConfigure drives the four session routes through the
// handler configure builds, on a live PostgreSQL, as the runtime role.
func TestSessionRoutesThroughConfigure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)
	user, org := e.seed(t, ctx)

	// Refresh: a rotated credential and a go-api access token that query-api's
	// edge verifier accepts against the live users row.
	first := e.start(t, ctx, user, &org)
	rotated, got := e.refresh(t, first.Credential.Reveal())
	if rotated == nil {
		t.Fatalf("refresh = %d %s", got.status, got.body)
	}
	if rotated.RefreshCredential.Reveal() == first.Credential.Reveal() || rotated.SessionID != session.WireSessionID(first.Session.ID) {
		t.Fatal("refresh did not rotate within the session")
	}
	if got.header.Get("Cache-Control") != "no-store" {
		t.Fatal("token response is cacheable")
	}
	claims, err := e.edge.Verify(ctx, rotated.AccessToken.Reveal())
	if err != nil {
		t.Fatalf("query-api's edge verifier refused the access token: %v", err)
	}
	if claims.OrgID != org.String() {
		t.Fatalf("org_id = %q, want %s", claims.OrgID, org)
	}
	payload := decodePayload(t, rotated.AccessToken.Reveal())
	for key, want := range map[string]any{"sub": user.String(), "role": "owner", "tv": float64(2), "is_superuser": false,
		"username": "routes-" + user.String()[:8], "full_name": "Routes User", "sid": rotated.SessionID, "type": "access"} {
		if payload[key] != want {
			t.Errorf("claim %s = %v, want %v", key, payload[key], want)
		}
	}

	// Grace: the same credential again inside 30 s answers the same successor.
	again, got := e.refresh(t, first.Credential.Reveal())
	if again == nil || again.RefreshCredential.Reveal() != rotated.RefreshCredential.Reveal() {
		t.Fatalf("grace replay = %d %s, want the same successor", got.status, got.body)
	}

	// Current: the bearer's own session as session.v1.
	bearer := rotated.AccessToken.Reveal()
	got = e.do(t, http.MethodGet, "/v1/sessions/current", "", bearer)
	if got.status != http.StatusOK {
		t.Fatalf("current = %d %s", got.status, got.body)
	}
	current, err := contracts.ParseSession(e.root, got.body)
	if err != nil {
		t.Fatalf("current is not session.v1: %v", err)
	}
	if current.SessionID != rotated.SessionID || current.RevokedAt != nil {
		t.Fatalf("current = %+v", current)
	}

	// Reuse: rotate again, then present the first credential (its successor is
	// now consumed): 401 refresh_reused, the session is revoked and its bearer
	// is refused.
	if next, got := e.refresh(t, rotated.RefreshCredential.Reveal()); next == nil {
		t.Fatalf("second rotation = %d %s", got.status, got.body)
	}
	e.refused(t, e.do(t, http.MethodPost, "/v1/token/refresh", refreshBody(first.Credential.Reveal()), ""), 401, "refresh_reused")
	if e.revokedAt(t, ctx, rotated.SessionID) == nil {
		t.Fatal("reuse left the session live")
	}
	e.refused(t, e.do(t, http.MethodGet, "/v1/sessions/current", "", bearer), 401, "session_revoked")

	// Revoke: the bearer's own session ends; its credential is refused.
	second := e.start(t, ctx, user, &org)
	tokens, got := e.refresh(t, second.Credential.Reveal())
	if tokens == nil {
		t.Fatalf("refresh = %d %s", got.status, got.body)
	}
	if got := e.do(t, http.MethodPost, "/v1/sessions/revoke", "", tokens.AccessToken.Reveal()); got.status != http.StatusNoContent {
		t.Fatalf("revoke = %d %s", got.status, got.body)
	}
	e.refused(t, e.do(t, http.MethodPost, "/v1/token/refresh", refreshBody(tokens.RefreshCredential.Reveal()), ""), 401, "session_revoked")
	var auditRequestID *string
	if err := e.admin.QueryRow(ctx, `SELECT request_id FROM auth.security_audit_events
		WHERE event_type = 'auth.session.revoked' AND attributes->>'target_id' = $1`,
		mustSessionUUID(t, tokens.SessionID).String()).Scan(&auditRequestID); err != nil {
		t.Fatalf("revoke audit row: %v", err)
	}
	if auditRequestID == nil {
		t.Fatal("the revoke audit row has no request id")
	}
	if _, err := uuid.Parse(*auditRequestID); err != nil {
		t.Fatalf("audit request id %q is not a UUID", *auditRequestID)
	}

	// Revoke-all: every live session of the principal ends.
	third, fourth := e.start(t, ctx, user, &org), e.start(t, ctx, user, nil)
	thirdTokens, _ := e.refresh(t, third.Credential.Reveal())
	fourthTokens, _ := e.refresh(t, fourth.Credential.Reveal())
	if thirdTokens == nil || fourthTokens == nil {
		t.Fatal("refresh before revoke-all failed")
	}
	if got := e.do(t, http.MethodPost, "/v1/sessions/revoke-all", "", thirdTokens.AccessToken.Reveal()); got.status != http.StatusNoContent {
		t.Fatalf("revoke-all = %d %s", got.status, got.body)
	}
	for _, credential := range []string{thirdTokens.RefreshCredential.Reveal(), fourthTokens.RefreshCredential.Reveal()} {
		e.refused(t, e.do(t, http.MethodPost, "/v1/token/refresh", refreshBody(credential), ""), 401, "session_revoked")
	}

	// token_version moves (go-api's reset or admin bump): the old bearer is
	// refused here exactly as query-api refuses it.
	fifth := e.start(t, ctx, user, &org)
	fifthTokens, _ := e.refresh(t, fifth.Credential.Reveal())
	if fifthTokens == nil {
		t.Fatal("refresh failed")
	}
	if _, err := e.admin.Exec(ctx, `UPDATE users SET token_version = token_version + 1 WHERE id = $1`, user); err != nil {
		t.Fatal(err)
	}
	e.refused(t, e.do(t, http.MethodGet, "/v1/sessions/current", "", fifthTokens.AccessToken.Reveal()), 401, "revision_stale")
	if _, err := e.edge.Verify(ctx, fifthTokens.AccessToken.Reveal()); err == nil || !policy.IsRefusal(err) {
		t.Fatalf("query-api accepted a stale token (err %v): the two checks disagree", err)
	}
	// A refresh after the bump mints the new token_version, which query-api accepts.
	renewed, got := e.refresh(t, fifthTokens.RefreshCredential.Reveal())
	if renewed == nil {
		t.Fatalf("refresh after the bump = %d %s", got.status, got.body)
	}
	if _, err := e.edge.Verify(ctx, renewed.AccessToken.Reveal()); err != nil {
		t.Fatalf("query-api refused the renewed token: %v", err)
	}

	// Deactivation: refresh mints nothing and the session is revoked.
	if _, err := e.admin.Exec(ctx, `UPDATE users SET is_active = false WHERE id = $1`, user); err != nil {
		t.Fatal(err)
	}
	e.refused(t, e.do(t, http.MethodPost, "/v1/token/refresh", refreshBody(renewed.RefreshCredential.Reveal()), ""), 401, "principal_deactivated")
	if e.revokedAt(t, ctx, renewed.SessionID) == nil {
		t.Fatal("a deactivated user's session stayed live")
	}

	// Malformed bodies never reach the store; a refresh JWT is not a credential.
	e.refused(t, e.do(t, http.MethodPost, "/v1/token/refresh", `{}`, ""), 401, "malformed")
	e.refused(t, e.do(t, http.MethodPost, "/v1/token/refresh", refreshBody("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ4In0.c2lnbmF0dXJlc2lnbmF0dXJl"), ""), 401, "malformed")
	e.refused(t, e.do(t, http.MethodPost, "/v1/token/refresh", refreshBody("dhr1_"+base64.RawURLEncoding.EncodeToString(make([]byte, 32))), ""), 401, "refresh_unknown")

	t.Logf("%s refresh=200 grace=same reuse=401 revoke=204 revoke_all=204 stale=401 deactivated=401 edge_verifier=accepts", sessionRoutesMarker)
}

// TestASessionCannotNameAnOrganizationAuthHasNotProjected pins why the login
// seams project the organization before they create a session:
// auth.sessions.organization_id references auth.organizations, so an
// organization only go-api's public.organizations holds is refused by the
// foreign key until identity.ProjectOrganization has run (CHAOS-10228
// finding and ruling).
func TestASessionCannotNameAnOrganizationAuthHasNotProjected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)
	user, _ := e.seed(t, ctx)
	org := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, is_active, settings, managed_by, created_at, updated_at)
VALUES ($1, $2, 'Go API Only', 'community', true, '{}', 'self', now(), now())`, org, "goapi-"+org.String()[:8]); err != nil {
		t.Fatal(err)
	}
	_, err := e.sessions.Create(ctx, user, &org, []string{"pwd"})
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23503" || pgErr.ConstraintName != "sessions_organization_id_fkey" {
		t.Fatalf("Create with an unprojected organization = %v, want the sessions_organization_id_fkey violation", err)
	}
}

// TestAStoreFailureIsLoggedWithoutTheDatabaseText breaks the refresh store
// under the configured handler: the answer is error.v1 503, and no log line of
// the process (the route's or the session store's) carries the database's
// message, which can name relations and hold key values.
func TestAStoreFailureIsLoggedWithoutTheDatabaseText(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)
	user, org := e.seed(t, ctx)
	issued := e.start(t, ctx, user, &org)
	if _, err := e.admin.Exec(ctx, `ALTER TABLE auth.refresh_credentials RENAME TO refresh_credentials_moved`); err != nil {
		t.Fatal(err)
	}
	e.refused(t, e.do(t, http.MethodPost, "/v1/token/refresh", refreshBody(issued.Credential.Reveal()), ""), 503, "store_unavailable")
	logs := e.logs.String()
	for _, text := range []string{"does not exist", "refresh_credentials", "relation"} {
		if strings.Contains(logs, text) {
			t.Fatalf("a log line carries the database text %q:\n%s", text, logs)
		}
	}
	if !strings.Contains(logs, `"code":"42P01"`) {
		t.Fatalf("the failure is not logged with its SQLSTATE:\n%s", logs)
	}
}

// TestReuseRevocationAuditCarriesTheRequestID: the audit row of a reuse
// revocation carries the request id the answer carries in X-Auth-Request-ID.
func TestReuseRevocationAuditCarriesTheRequestID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)
	user, org := e.seed(t, ctx)
	first := e.start(t, ctx, user, &org)
	rotated, got := e.refresh(t, first.Credential.Reveal())
	if rotated == nil {
		t.Fatalf("refresh = %d %s", got.status, got.body)
	}
	if next, got := e.refresh(t, rotated.RefreshCredential.Reveal()); next == nil {
		t.Fatalf("second rotation = %d %s", got.status, got.body)
	}
	reuse := e.do(t, http.MethodPost, "/v1/token/refresh", refreshBody(first.Credential.Reveal()), "")
	e.refused(t, reuse, 401, "refresh_reused")
	wire := reuse.header.Get("X-Auth-Request-ID")
	var auditRequestID *string
	if err := e.admin.QueryRow(ctx, `SELECT request_id FROM auth.security_audit_events
		WHERE event_type = 'auth.session.refresh_reuse_revoked' AND attributes->>'target_id' = $1`,
		mustSessionUUID(t, rotated.SessionID).String()).Scan(&auditRequestID); err != nil {
		t.Fatalf("reuse audit row: %v", err)
	}
	if auditRequestID == nil {
		t.Fatalf("the reuse revocation audit row has no request id; the answer carried %s", wire)
	}
	id, err := uuid.Parse(*auditRequestID)
	if err != nil || requestid.WireOf(id) != wire {
		t.Fatalf("audit request_id %q is not the id of X-Auth-Request-ID %s", *auditRequestID, wire)
	}
}

// auditRequestIDOf reads the request id of the newest audit row of eventType
// on targetID.
func (e *routesEnv) auditRequestIDOf(t *testing.T, ctx context.Context, eventType, targetID string) *string {
	t.Helper()
	var id *string
	if err := e.admin.QueryRow(ctx, `SELECT request_id FROM auth.security_audit_events
		WHERE event_type = $1 AND attributes->>'target_id' = $2 ORDER BY occurred_at DESC LIMIT 1`,
		eventType, targetID).Scan(&id); err != nil {
		t.Fatalf("audit row %s on %s: %v", eventType, targetID, err)
	}
	return id
}

// TestEveryAuditRowARouteWritesCarriesItsRequestID drives every handler path
// that writes an audit row (refresh reuse, refresh of a deactivated user,
// refresh of a user that is gone, revoke, revoke-all) through the configured
// handler, and asserts each row carries the request id of the answer that
// wrote it (the UUID behind X-Auth-Request-ID).
func TestEveryAuditRowARouteWritesCarriesItsRequestID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)
	check := func(t *testing.T, got answer, eventType, targetID string) {
		t.Helper()
		wire := got.header.Get("X-Auth-Request-ID")
		id := e.auditRequestIDOf(t, ctx, eventType, targetID)
		if id == nil {
			t.Fatalf("%s audit row has no request id; the answer carried %q", eventType, wire)
		}
		parsed, err := uuid.Parse(*id)
		if err != nil || requestid.WireOf(parsed) != wire {
			t.Fatalf("%s audit request_id %q is not the id of X-Auth-Request-ID %q", eventType, *id, wire)
		}
	}

	t.Run("refresh reuse", func(t *testing.T) {
		user, org := e.seed(t, ctx)
		first := e.start(t, ctx, user, &org)
		rotated, got := e.refresh(t, first.Credential.Reveal())
		if rotated == nil {
			t.Fatalf("refresh = %d %s", got.status, got.body)
		}
		if next, got := e.refresh(t, rotated.RefreshCredential.Reveal()); next == nil {
			t.Fatalf("second rotation = %d %s", got.status, got.body)
		}
		reuse := e.do(t, http.MethodPost, "/v1/token/refresh", refreshBody(first.Credential.Reveal()), "")
		e.refused(t, reuse, 401, "refresh_reused")
		check(t, reuse, "auth.session.refresh_reuse_revoked", mustSessionUUID(t, rotated.SessionID).String())
	})
	t.Run("refresh of a deactivated user", func(t *testing.T) {
		user, org := e.seed(t, ctx)
		issued := e.start(t, ctx, user, &org)
		if _, err := e.admin.Exec(ctx, `UPDATE users SET is_active = false WHERE id = $1`, user); err != nil {
			t.Fatal(err)
		}
		got := e.do(t, http.MethodPost, "/v1/token/refresh", refreshBody(issued.Credential.Reveal()), "")
		e.refused(t, got, 401, "principal_deactivated")
		check(t, got, "auth.session.revoked", issued.Session.ID.String())
	})
	t.Run("refresh of a user that is gone", func(t *testing.T) {
		user, org := e.seed(t, ctx)
		issued := e.start(t, ctx, user, &org)
		if _, err := e.admin.Exec(ctx, `DELETE FROM memberships WHERE user_id = $1`, user); err != nil {
			t.Fatal(err)
		}
		if _, err := e.admin.Exec(ctx, `DELETE FROM users WHERE id = $1`, user); err != nil {
			t.Fatal(err)
		}
		got := e.do(t, http.MethodPost, "/v1/token/refresh", refreshBody(issued.Credential.Reveal()), "")
		e.refused(t, got, 401, "principal_not_found")
		check(t, got, "auth.session.revoked", issued.Session.ID.String())
	})
	t.Run("revoke", func(t *testing.T) {
		user, org := e.seed(t, ctx)
		tokens, got := e.refresh(t, e.start(t, ctx, user, &org).Credential.Reveal())
		if tokens == nil {
			t.Fatalf("refresh = %d %s", got.status, got.body)
		}
		got = e.do(t, http.MethodPost, "/v1/sessions/revoke", "", tokens.AccessToken.Reveal())
		if got.status != http.StatusNoContent {
			t.Fatalf("revoke = %d %s", got.status, got.body)
		}
		check(t, got, "auth.session.revoked", mustSessionUUID(t, tokens.SessionID).String())
	})
	t.Run("revoke-all", func(t *testing.T) {
		user, org := e.seed(t, ctx)
		tokens, got := e.refresh(t, e.start(t, ctx, user, &org).Credential.Reveal())
		if tokens == nil {
			t.Fatalf("refresh = %d %s", got.status, got.body)
		}
		got = e.do(t, http.MethodPost, "/v1/sessions/revoke-all", "", tokens.AccessToken.Reveal())
		if got.status != http.StatusNoContent {
			t.Fatalf("revoke-all = %d %s", got.status, got.body)
		}
		check(t, got, "auth.session.revoked_all", user.String())
	})
}

func mustSessionUUID(t *testing.T, wire string) uuid.UUID {
	t.Helper()
	id, err := session.ParseWireSessionID(wire)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func decodePayload(t *testing.T, token string) map[string]any {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token has %d segments", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var claims map[string]any
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatal(err)
	}
	return claims
}
