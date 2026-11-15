package authapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-go/authverify"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	apisession "github.com/full-chaos/dev-health-ops/internal/api/session"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/accessrouting"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/acrmint"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/legacyjwt"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// validateRows is the endpoint-profile document with one auth-service row for
// the validate route accepting classes; the real row is PR-2's (D6617).
func validateRows(classes ...string) []byte {
	quoted, _ := json.Marshal(classes)
	return []byte(`{"rows":[{"service":"` + AccessService + `","method":"POST","route":"` + PatternCompatValidate +
		`","accepted_credential_classes":` + string(quoted) + `}]}`)
}

var (
	// validateRowsUser is the real row's class list: the user access token only (ops_access_token_hs256 is retired).
	validateRowsUser = validateRows("user_access_token")
	// validateRowsHS256 lists only the retired HS256 class: the route never mounts on it.
	validateRowsHS256 = validateRows("ops_access_token_hs256")
)

// sessionRows is go-api's session read (policy.SessionStore) over a map.
type sessionRows struct {
	mu   sync.Mutex
	rows map[uuid.UUID]policy.SessionRow
	err  error
}

func (s *sessionRows) SessionState(_ context.Context, id uuid.UUID) (policy.SessionRow, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return policy.SessionRow{}, false, s.err
	}
	row, ok := s.rows[id]
	return row, ok, nil
}

// goAPIValidateStore is go-api's policy.Store over the same fixture data, so
// go-api's own handler judges the same rows.
type goAPIValidateStore struct{ f *validateFixture }

func (g goAPIValidateStore) UserState(ctx context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	state, found, err := g.f.users.State(ctx, id)
	return policy.UserState{IsActive: state.IsActive, IsSuperuser: state.IsSuperuser, TokenVersion: state.TokenVersion}, found, err
}

func (g goAPIValidateStore) AccessUser(ctx context.Context, id uuid.UUID) (policy.AccessUserRow, bool, error) {
	state, found, err := g.f.users.State(ctx, id)
	return policy.AccessUserRow{
		State: policy.UserState{IsActive: state.IsActive, IsSuperuser: state.IsSuperuser, TokenVersion: state.TokenVersion},
		Email: state.Email, Username: state.Username, FullName: state.FullName,
	}, found, err
}

func (g goAPIValidateStore) Membership(ctx context.Context, user, org uuid.UUID) (string, bool, error) {
	role, found, err := g.f.users.Role(ctx, user, org)
	if role == nil {
		return "", found, err
	}
	return *role, found, err
}

func (g goAPIValidateStore) OrgMembership(context.Context, uuid.UUID, uuid.UUID) (string, bool, bool, error) {
	return "", false, false, errors.New("not read by validate")
}

func (g goAPIValidateStore) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	return nil, errors.New("not read by validate")
}

func (goAPIValidateStore) ReadsPlatformRoles() bool { return false }

func (g goAPIValidateStore) SessionState(ctx context.Context, id uuid.UUID) (policy.SessionRow, bool, error) {
	return g.f.rows.SessionState(ctx, id)
}

// validateFixture serves the compatibility route and, beside it, go-api's own
// validate handler (internal/api/session) over the same users and session
// rows: a cell compares the two planes byte for byte.
type validateFixture struct {
	t         *testing.T
	principal uuid.UUID
	org       uuid.UUID
	record    session.Record
	sessions  *fakeSessions
	users     fakeUsers
	pgUsers   Users // replaces users when set
	rows      *sessionRows
	issuer    *Issuer
	hs256     *legacyjwt.Signer
	signer    *token.Signer
	logs      *bytes.Buffer
	counters  httpapi.CounterStore
	eddsa     bool
	document  []byte
	server    http.Handler
	goAPI     http.Handler
	bodyLimit int64
	meter     metric.Meter
}

func newValidateFixture(t *testing.T, eddsa bool) *validateFixture {
	t.Helper()
	principal, org := uuid.New(), uuid.New()
	now := time.Now().UTC()
	pin := int64(4)
	record := session.Record{ID: uuid.New(), PrincipalID: principal, OrganizationID: &org, Revision: 1,
		AuthTime: now.Add(-time.Hour), AMR: []string{"pwd"}, CreatedAt: now.Add(-time.Hour), ExpiresAt: now.Add(7 * 24 * time.Hour),
		LegacyTokenVersion: &pin}
	admin, username := "admin", "ada"
	issuer := newTestIssuer(t)
	hs256, err := legacyjwt.NewSigner(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	signer, _, _ := fileSigner(t)
	f := &validateFixture{
		t: t, principal: principal, org: org, record: record,
		sessions: &fakeSessions{records: map[uuid.UUID]session.Record{record.ID: record}},
		users: fakeUsers{
			users: map[uuid.UUID]UserState{principal: {Email: "ada@example.com", Username: &username, IsActive: true, TokenVersion: 4}},
			roles: map[[2]uuid.UUID]*string{{principal, org}: &admin},
		},
		rows:   &sessionRows{rows: map[uuid.UUID]policy.SessionRow{record.ID: {PrincipalID: principal, LegacyTokenVersion: &pin}}},
		issuer: issuer, hs256: hs256, signer: signer, logs: &bytes.Buffer{}, counters: httpapi.NewMemoryCounters(nil),
		eddsa: eddsa, document: validateRowsUser, bodyLimit: 1 << 20,
	}
	f.build()
	return f
}

func (f *validateFixture) deps() CompatDeps {
	config := &ValidateConfig{Sessions: f.rows}
	if f.eddsa {
		config.IssuerURL, config.Keys = goldenIssuer, f.signer
	}
	var users Users = f.users
	if f.pgUsers != nil {
		users = f.pgUsers
	}
	return CompatDeps{
		Sessions: f.sessions, Users: users, Issuer: f.issuer, Projector: &fakeProjector{},
		Logger:      slog.New(slog.NewJSONHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		AppendAudit: func(context.Context, audit.AuditEvent) error { return nil },
		Limits:      mustLimits(f.t, f.counters),
		Validate:    config,
		Meter:       f.meter,
	}
}

func (f *validateFixture) build() {
	f.t.Helper()
	routes, err := compatRoutes(f.deps(), f.document)
	if err != nil {
		f.t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range routes {
		route.ResponseModel = true
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(flaggedHandler(route)))
	}
	f.server = mux
	f.goAPI = f.goAPIHandler()
}

// goAPIHandler is go-api's own POST /api/v1/auth/validate over
// goAPIValidateStore: unrouted (HS256 only, prod today), or under the EdDSA
// configuration routed as go-api routes it (goAPIRouteAccess), behind its
// route-profile middleware (apiservice newServer).
func (f *validateFixture) goAPIHandler() http.Handler {
	f.t.Helper()
	logger := slog.New(slog.DiscardHandler)
	auth, err := policy.NewAuthenticator(goAPIValidateStore{f}, logger)
	if err != nil {
		f.t.Fatal(err)
	}
	pool, err := pgxpool.New(context.Background(), "postgres://validate@127.0.0.1:1/none")
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(pool.Close)
	if f.eddsa {
		f.goAPIRouteAccess(auth, logger)
	}
	for _, route := range apisession.Routes(apisession.Deps{Pool: pool, Guard: policy.NewGuard(auth, logger), Auth: auth, Logger: logger, Limits: httpapi.NewMemoryCounters(nil)}) {
		if route.Method == http.MethodPost && route.Pattern == PatternCompatValidate {
			templates, err := httpapi.NewRouteTemplates([]httpapi.Route{route})
			if err != nil {
				f.t.Fatal(err)
			}
			mux := http.NewServeMux()
			mux.Handle(route.Method+" "+route.Pattern, route.Handler)
			return policy.RouteProfiles(templates.Match)(mux)
		}
	}
	f.t.Fatal("go-api mounts no validate route")
	return nil
}

// goAPIAccessService is go-api's endpoint-profile service (apiservice
// goAPIAccessService).
const goAPIAccessService = "dev-health-ops-api"

// goAPIRouteAccess routes auth as go-api's service start does
// (apiservice buildAccessVerify, then RouteAccess): every dev-health-ops-api
// row of go-api's endpoint-profile document, the EdDSA verifier of audience
// dev-health-api pinned to the fixture's issuer over the keys the fixture
// signs with, and the HS256 path as the legacy verifier.
func (f *validateFixture) goAPIRouteAccess(auth *policy.Authenticator, logger *slog.Logger) {
	f.t.Helper()
	routeGoAPIAccess(f.t, auth, f.signer, logger)
}

// routeGoAPIAccess is goAPIRouteAccess over the keys signer signs with.
func routeGoAPIAccess(t *testing.T, auth *policy.Authenticator, signer *token.Signer, logger *slog.Logger) {
	t.Helper()
	set, err := signer.KeySet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	keys := authverify.StaticKeys{}
	for _, key := range set.Published() {
		keys[key.KeyID] = key.PublicKey
	}
	verifier, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: goldenIssuer, Audience: "dev-health-api", Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	validator, err := accessverify.New(accessverify.Config{
		Rows:   goAPIAccessRows(t),
		EdDSA:  verifier,
		Meter:  otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/accessverify"),
		Logger: logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.RouteAccess(accessrouting.Adapter{Validator: validator}, goAPIAccessService); err != nil {
		t.Fatal(err)
	}
}

// goAPIAccessRows is apiservice goAPIAccessRows: every dev-health-ops-api row
// of go-api's endpoint-profile document.
func goAPIAccessRows(t *testing.T) []accessverify.Row {
	t.Helper()
	var document struct {
		Rows []struct {
			Service string   `json:"service"`
			Method  string   `json:"method"`
			Route   string   `json:"route"`
			Classes []string `json:"accepted_credential_classes"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(authv1.GoAPIEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	var rows []accessverify.Row
	for _, entry := range document.Rows {
		if entry.Service != goAPIAccessService {
			continue
		}
		accepts := false
		for _, class := range entry.Classes {
			accepts = accepts || class == accessverify.ClassUser
		}
		rows = append(rows, accessverify.Row{Service: goAPIAccessService, Profile: entry.Method + " " + entry.Route,
			AcceptsUser: accepts})
	}
	return rows
}

func (f *validateFixture) post(server http.Handler, body string, header map[string]string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, PatternCompatValidate, strings.NewReader(body))
	for k, v := range header {
		request.Header.Set(k, v)
	}
	request.Body = http.MaxBytesReader(httptest.NewRecorder(), request.Body, f.bodyLimit)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	return response
}

func tokenBody(token string) string {
	quoted, _ := json.Marshal(token)
	return `{"token":` + string(quoted) + `}`
}

// hs256Token is a go-api HS256 access token for the fixture user, with the
// session's sid: a bearer of the retired class.
func (f *validateFixture) hs256Token() string {
	f.t.Helper()
	sid := session.WireSessionID(f.record.ID)
	return hs256AccessToken(f.t, f.principal.String(), f.org.String(), 4, &sid)
}

// hs256AccessToken is a go-api HS256 access token (the retired class) under the
// test key; no production code mints one.
func hs256AccessToken(t testing.TB, subject, org string, tv int64, sid *string) string {
	t.Helper()
	now := time.Now()
	claims := jwt.MapClaims{"sub": subject, "email": "ada@example.com", "org_id": org, "role": "admin", "is_superuser": false,
		"type": "access", "iss": testIssuer, "aud": testAudience, "exp": now.Add(time.Hour).Unix(), "iat": now.Unix(),
		"jti": uuid.NewString(), "tv": tv}
	if sid != nil {
		claims["sid"] = *sid
	}
	compact, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(testSecret))
	if err != nil {
		t.Fatal(err)
	}
	return compact
}

// eddsaToken is the user access token the eddsa class mints for the fixture
// session, at audience (dev-health-api: the token web sends; auth-service:
// the session-route token of the same mint).
func (f *validateFixture) eddsaToken(audience string) string {
	f.t.Helper()
	issuer, err := NewEdDSAIssuer(contracts.PlatformAPIAudience, f.signer, goldenIssuer)
	if err != nil {
		f.t.Fatal(err)
	}
	tokens, err := issuer.Tokens(context.Background(), Grant{PrincipalID: f.principal, OrganizationID: &f.org, Role: "admin",
		SessionID: f.record.ID, User: f.users.users[f.principal]})
	if err != nil {
		f.t.Fatal(err)
	}
	if audience == contracts.SessionRouteAudience {
		return tokens.Session
	}
	return tokens.Access
}

func (f *validateFixture) sign(typ string, claims any) string {
	f.t.Helper()
	payload, err := json.Marshal(claims)
	if err != nil {
		f.t.Fatal(err)
	}
	compact, _, err := f.signer.Sign(context.Background(), typ, payload)
	if err != nil {
		f.t.Fatal(err)
	}
	return compact
}

const nullAnswer = `{"valid":false,"user_id":null,"email":null,"org_id":null,"role":null,"expires_at":null}`

func (f *validateFixture) validAnswer(email, org, role string) string {
	return `{"valid":true,"user_id":"` + f.principal.String() + `","email":"` + email + `","org_id":"` + org + `","role":"` + role + `","expires_at":null}`
}

func expectAnswer(t *testing.T, name string, response *httptest.ResponseRecorder, status int, body string) {
	t.Helper()
	if response.Code != status || response.Body.String() != body {
		t.Errorf("%s: %d %s, want %d %s", name, response.Code, response.Body, status, body)
	}
}

// expectSamePlanes checks the compatibility route answers exactly what go-api's
// handler answers: status, content type and body bytes.
func expectSamePlanes(t *testing.T, name string, compat, goAPI *httptest.ResponseRecorder) {
	t.Helper()
	if compat.Code != goAPI.Code || compat.Body.String() != goAPI.Body.String() ||
		compat.Header().Get("Content-Type") != goAPI.Header().Get("Content-Type") {
		t.Errorf("%s: auth-service %d %q %s, go-api %d %q %s", name, compat.Code, compat.Header().Get("Content-Type"), compat.Body,
			goAPI.Code, goAPI.Header().Get("Content-Type"), goAPI.Body)
	}
}

// c3: a revoked, expired or foreign session, a deactivated or unknown user, a
// missing or stale pin and a stale token_version answer valid:false. Every
// row but the two effective-state rows is go-api's answer too; those two are
// the declared difference (acp answers-12395-2 (b)): go-api reads the
// session row's own revoke and no expiry, the compatibility route then reads
// auth-service's effective state.
func TestValidateCompatC3EndedSessionsAndStaleVersionsAreRefused(t *testing.T) {
	stale, other := int64(3), uuid.New()
	for _, tc := range []struct {
		name    string
		arrange func(*validateFixture)
		token   func(*validateFixture) string
		goAPI   string // "" = go-api answers the same refusal
	}{
		{name: "revoked", arrange: func(f *validateFixture) {
			now := time.Now()
			f.rows.rows[f.record.ID] = policy.SessionRow{PrincipalID: f.principal, Revoked: true, LegacyTokenVersion: f.record.LegacyTokenVersion}
			record := f.record
			record.RevokedAt = &now
			f.sessions.records[f.record.ID] = record
		}},
		{name: "expired (go-api passes it)", arrange: func(f *validateFixture) {
			record := f.record
			record.ExpiresAt = time.Now().Add(-time.Second)
			f.sessions.records[f.record.ID] = record
		}, goAPI: "valid"},
		{name: "effectively revoked by a password stamp (go-api passes it)", arrange: func(f *validateFixture) {
			now := time.Now()
			record := f.record
			record.RevokedAt = &now
			f.sessions.records[f.record.ID] = record
		}, goAPI: "valid"},
		{name: "deactivated", arrange: func(f *validateFixture) {
			user := f.users.users[f.principal]
			user.IsActive = false
			f.users.users[f.principal] = user
		}},
		{name: "unknown user", arrange: func(f *validateFixture) { delete(f.users.users, f.principal) }},
		{name: "stale pin", arrange: func(f *validateFixture) {
			f.rows.rows[f.record.ID] = policy.SessionRow{PrincipalID: f.principal, LegacyTokenVersion: &stale}
		}},
		{name: "no pin", arrange: func(f *validateFixture) {
			f.rows.rows[f.record.ID] = policy.SessionRow{PrincipalID: f.principal}
		}},
		{name: "unknown session", arrange: func(f *validateFixture) { delete(f.rows.rows, f.record.ID) }},
		{name: "session of another principal", arrange: func(f *validateFixture) {
			f.rows.rows[f.record.ID] = policy.SessionRow{PrincipalID: other, LegacyTokenVersion: f.record.LegacyTokenVersion}
		}},
		{name: "no effective session state (go-api passes it)", arrange: func(f *validateFixture) {
			delete(f.sessions.records, f.record.ID)
		}, goAPI: "valid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newValidateFixture(t, true)
			if tc.arrange != nil {
				tc.arrange(f)
			}
			token := f.eddsaToken(contracts.PlatformAPIAudience)
			if tc.token != nil {
				token = tc.token(f)
			}
			compat, goAPI := f.post(f.server, tokenBody(token), nil), f.post(f.goAPI, tokenBody(token), nil)
			expectAnswer(t, tc.name, compat, http.StatusOK, nullAnswer)
			if tc.goAPI == "valid" {
				expectAnswer(t, tc.name+" (go-api)", goAPI, http.StatusOK, f.validAnswer("ada@example.com", f.org.String(), "admin"))
				return
			}
			expectSamePlanes(t, tc.name, compat, goAPI)
		})
	}
}

// c4: a refresh token, garbage, an empty token, a missing member, a lone
// surrogate and every body go-api refuses answer go-api's status and bytes,
// and the recorded Python answers where the corpus has them.
func TestValidateCompatC4RefusedInputsAnswerGoAPIsStatusAndBody(t *testing.T) {
	f := newValidateFixture(t, true)
	refresh, err := f.hs256.Refresh(legacyjwt.RefreshClaims{UserID: f.principal.String(), OrgID: f.org.String()}, time.Now(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, body, golden string
		header             map[string]string
		status             int
	}{
		{name: "refresh token", body: tokenBody(refresh), golden: "validate: refresh token", status: http.StatusOK},
		{name: "garbage", body: tokenBody("not-a-token"), golden: "validate: garbage", status: http.StatusOK},
		{name: "empty", body: tokenBody(""), golden: "validate: empty", status: http.StatusOK},
		{name: "missing member", body: `{}`, golden: "validate: missing", status: http.StatusUnprocessableEntity},
		{name: "lone surrogate", body: `{"token":"\ud800"}`, golden: "validate: lone surrogate", status: http.StatusInternalServerError},
		{name: "token not a string", body: `{"token":7}`, status: http.StatusUnprocessableEntity},
		{name: "not an object", body: `["token"]`, status: http.StatusUnprocessableEntity},
		{name: "no body", body: ``, status: http.StatusUnprocessableEntity},
		{name: "invalid JSON", body: `{"token":`, status: http.StatusUnprocessableEntity},
		{name: "bytes that are not UTF-8", body: "{\"token\":\"\xff\"}", status: http.StatusBadRequest},
		{name: "over the body bound", body: tokenBody(strings.Repeat("x", 4096)), status: http.StatusRequestEntityTooLarge},
		{name: "text body", body: `token`, header: map[string]string{"Content-Type": "text/plain"}, status: http.StatusUnprocessableEntity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f.bodyLimit = 1 << 20
			if tc.status == http.StatusRequestEntityTooLarge {
				f.bodyLimit = 1024
			}
			compat, goAPI := f.post(f.server, tc.body, tc.header), f.post(f.goAPI, tc.body, tc.header)
			if compat.Code != tc.status {
				t.Errorf("%s: status %d, want %d: %s", tc.name, compat.Code, tc.status, compat.Body)
			}
			if tc.name == "lone surrogate" {
				// CHAOS-12282, a named plane difference: go-api refuses a lone surrogate at its body boundary
				// (pybody.ReadChecked), auth-service keeps pybody.Read and its recorded 500.
				goBody := goAPI.Body.String()
				if goAPI.Code != http.StatusUnprocessableEntity || !strings.Contains(goBody, `"string_unicode"`) ||
					!strings.Contains(goBody, `"loc":["body","token"]`) || strings.Contains(strings.ToLower(goBody), "ud800") {
					t.Errorf("%s: go-api %d %s, want 422 string_unicode at body.token without the input", tc.name, goAPI.Code, goBody)
				}
			} else {
				expectSamePlanes(t, tc.name, compat, goAPI)
			}
			if tc.golden != "" {
				status, want := goldenAnswer(t, tc.golden)
				var got any
				if err := json.Unmarshal(compat.Body.Bytes(), &got); err != nil || status != compat.Code || !jsonEqual(withoutEcho(got), want) {
					t.Errorf("%s: %d %s, recorded Python answer %d %v", tc.name, compat.Code, compat.Body, status, want)
				}
			}
		})
	}
}

// The EdDSA class cell (answers-12395-3 D1): under the EdDSA configuration
// the answer to a user access token of audience dev-health-api is go-api's
// routed answer (accessverify, then accessTokenUser's users row, session and
// membership reads) for the same token and rows, byte for byte; an HS256
// token beside it is go-api's routed legacy answer.
func TestValidateCompatEdDSAAnswerIsGoAPIsRoutedAnswerByteForByte(t *testing.T) {
	t.Parallel()
	stale, other := int64(3), uuid.New()
	for _, tc := range []struct {
		name    string
		arrange func(*validateFixture)
		token   func(*validateFixture) string
		want    func(*validateFixture) string
	}{
		{name: "live session of an active user", want: func(f *validateFixture) string {
			return f.validAnswer("ada@example.com", f.org.String(), "admin")
		}},
		{name: "email from the users row, role from the membership row", arrange: func(f *validateFixture) {
			user, viewer := f.users.users[f.principal], "viewer"
			user.Email = "ada.lovelace@example.com"
			f.users.users[f.principal] = user
			f.users.roles[[2]uuid.UUID{f.principal, f.org}] = &viewer
		}, want: func(f *validateFixture) string {
			return f.validAnswer("ada.lovelace@example.com", f.org.String(), "viewer")
		}},
		{name: "deactivated user", arrange: func(f *validateFixture) {
			user := f.users.users[f.principal]
			user.IsActive = false
			f.users.users[f.principal] = user
		}},
		{name: "unknown user", arrange: func(f *validateFixture) { delete(f.users.users, f.principal) }},
		{name: "unknown session", arrange: func(f *validateFixture) { delete(f.rows.rows, f.record.ID) }},
		{name: "revoked session", arrange: func(f *validateFixture) {
			now := time.Now()
			f.rows.rows[f.record.ID] = policy.SessionRow{PrincipalID: f.principal, Revoked: true, LegacyTokenVersion: f.record.LegacyTokenVersion}
			record := f.record
			record.RevokedAt = &now
			f.sessions.records[f.record.ID] = record
		}},
		{name: "session of another principal", arrange: func(f *validateFixture) {
			f.rows.rows[f.record.ID] = policy.SessionRow{PrincipalID: other, LegacyTokenVersion: f.record.LegacyTokenVersion}
		}},
		{name: "stale pin", arrange: func(f *validateFixture) {
			f.rows.rows[f.record.ID] = policy.SessionRow{PrincipalID: f.principal, LegacyTokenVersion: &stale}
		}},
		{name: "org the user left", arrange: func(f *validateFixture) { delete(f.users.roles, [2]uuid.UUID{f.principal, f.org}) },
			want: func(f *validateFixture) string { return f.validAnswer("ada@example.com", f.org.String(), "member") }},
		{name: "aud auth-service", token: func(f *validateFixture) string { return f.eddsaToken(contracts.SessionRouteAudience) }},
		{name: "HS256 token beside it (retired class, refused)", token: func(f *validateFixture) string { return f.hs256Token() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newValidateFixture(t, true)
			if tc.arrange != nil {
				tc.arrange(f)
			}
			token := f.eddsaToken(contracts.PlatformAPIAudience)
			if tc.token != nil {
				token = tc.token(f)
			}
			want := nullAnswer
			if tc.want != nil {
				want = tc.want(f)
			}
			compat, goAPI := f.post(f.server, tokenBody(token), nil), f.post(f.goAPI, tokenBody(token), nil)
			expectAnswer(t, tc.name, compat, http.StatusOK, want)
			expectSamePlanes(t, tc.name, compat, goAPI)
		})
	}
}

func jsonEqual(a, b any) bool {
	left, errLeft := json.Marshal(a)
	right, errRight := json.Marshal(b)
	return errLeft == nil && errRight == nil && bytes.Equal(left, right)
}

// c5: under an EdDSA configuration the user access token of audience
// dev-health-api is accepted, judged as go-api's routed path judges it (the
// users row, the session, the membership of the org claim); every other
// audience, an ACR token from the real acrmint producer, a workload token and
// a query-api token are refused, and so is the HS256 token (its class is
// retired). Without the EdDSA configuration the route still mounts and judges
// HS256 alone.
func TestValidateCompatC5TokenClassesAreGoAPIsAndNoWider(t *testing.T) {
	t.Parallel()
	t.Run("eddsa", func(t *testing.T) {
		f := newValidateFixture(t, true)
		expectAnswer(t, "user token, aud dev-health-api", f.post(f.server, tokenBody(f.eddsaToken(contracts.PlatformAPIAudience)), nil),
			http.StatusOK, f.validAnswer("ada@example.com", f.org.String(), "admin"))
		expectAnswer(t, "HS256 token beside it (retired class, refused)", f.post(f.server, tokenBody(f.hs256Token()), nil),
			http.StatusOK, nullAnswer)
		now := time.Now().Unix()
		refused := map[string]string{
			"aud auth-service": f.eddsaToken(contracts.SessionRouteAudience),
			"aud query-api": f.sign(accessTokenTyp, contracts.AccessTokenClaims{Iss: goldenIssuer, Aud: "query-api",
				Sub: session.WirePrincipalID(f.principal), PrincipalType: "human", Sid: session.WireSessionID(f.record.ID),
				Jti: uuid.NewString(), Iat: now, Nbf: now, Exp: now + 600, Scope: "api:access"}),
			"workload token": f.sign(contracts.WorkloadTokenTyp, contracts.WorkloadTokenClaims{Iss: goldenIssuer, Aud: contracts.PlatformAPIAudience,
				Sub: session.WirePrincipalID(f.principal), PrincipalType: "workload", Jti: uuid.NewString(), Iat: now, Nbf: now, Exp: now + 300}),
		}
		minter, err := acrmint.New(acrmint.Config{Signer: f.signer, IssuerURL: goldenIssuer, Impersonation: noImpersonation{}, Now: time.Now})
		if err != nil {
			t.Fatal(err)
		}
		for _, audience := range []string{contracts.ACRAPIAudience, contracts.ACRMCPAudience} {
			minted, _, err := minter.Mint(context.Background(), acrmint.Request{Audience: audience, PrincipalID: f.principal, SessionID: f.record.ID, OrganizationID: f.org})
			if err != nil {
				t.Fatal(err)
			}
			refused["aud "+audience] = minted
		}
		for name, token := range refused {
			expectAnswer(t, name, f.post(f.server, tokenBody(token), nil), http.StatusOK, nullAnswer)
		}
		other := uuid.New()
		user := f.users.users[f.principal]
		user.IsActive = false
		f.users.users[f.principal] = user
		expectAnswer(t, "user token of a deactivated user", f.post(f.server, tokenBody(f.eddsaToken(contracts.PlatformAPIAudience)), nil), http.StatusOK, nullAnswer)
		user.IsActive = true
		f.users.users[f.principal] = user
		delete(f.users.roles, [2]uuid.UUID{f.principal, f.org})
		expectAnswer(t, "user token for an org the user left", f.post(f.server, tokenBody(f.eddsaToken(contracts.PlatformAPIAudience)), nil),
			http.StatusOK, f.validAnswer("ada@example.com", f.org.String(), "member"))
		f.rows.rows[f.record.ID] = policy.SessionRow{PrincipalID: other, LegacyTokenVersion: f.record.LegacyTokenVersion}
		expectAnswer(t, "user token of another principal's session", f.post(f.server, tokenBody(f.eddsaToken(contracts.PlatformAPIAudience)), nil), http.StatusOK, nullAnswer)
	})
	t.Run("the route needs the user access token on its row", func(t *testing.T) {
		f := newValidateFixture(t, true)
		f.document = validateRowsHS256
		f.build()
		if r := f.post(f.server, tokenBody(f.hs256Token()), nil); r.Code != http.StatusNotFound {
			t.Fatalf("mounted on a row without user_access_token: %d %s", r.Code, r.Body)
		}
	})
}

// c6: a store fault is go-api's 503 with go-api's text, never a pass: the
// users read (a refused dial, and a real pgx pool on a dead address), go-api's
// session row read, and auth-service's effective state read; unrouted (HS256
// only) and routed under the EdDSA configuration, judged there for the user
// access token (the HS256 token is refused before any read: its class is retired).
func TestValidateCompatC6AStoreFaultIsThe503(t *testing.T) {
	t.Parallel()
	down := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	dead, err := pgxpool.New(context.Background(), "postgres://validate:none@127.0.0.1:1/none?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dead.Close)
	want := `{"detail":{"message":"` + sessionissue.UnavailableText + `"}}`
	for _, eddsa := range []bool{true} {
		for _, tc := range []struct {
			name    string
			arrange func(*validateFixture)
			token   func(*validateFixture) string
		}{
			{name: "users read", arrange: func(f *validateFixture) { f.users.err = down }},
			{name: "users read, dead pgx pool", arrange: func(f *validateFixture) { f.pgUsers = PGUsers{Pool: dead} }},
			{name: "session row read", arrange: func(f *validateFixture) { f.rows.err = down }},
			{name: "effective state read", arrange: func(f *validateFixture) { f.sessions.currentErr = errors.Join(session.ErrStore, down) }},
		} {
			if eddsa && tc.token != nil {
				continue
			}
			t.Run(fmt.Sprintf("%s eddsa=%v", tc.name, eddsa), func(t *testing.T) {
				f := newValidateFixture(t, eddsa)
				tc.arrange(f)
				f.build()
				token := f.hs256Token()
				if eddsa {
					token = f.eddsaToken(contracts.PlatformAPIAudience)
				}
				if tc.token != nil {
					token = tc.token(f)
				}
				expectAnswer(t, tc.name, f.post(f.server, tokenBody(token), nil), http.StatusServiceUnavailable, want)
			})
		}
	}
}

// The limit is go-api's: one count shared with go-api (httpapi.ValidateLimitCount)
// per window, keyed by the token's digest, by the client address for an empty
// token; a refused body costs nothing; the 429 is go-api's.
func TestValidateCompatLimitIsGoAPIsCountAndKey(t *testing.T) {
	if validateLimit != (httpapi.Limit{ID: "auth_svc_validate", Count: httpapi.ValidateLimitCount, Window: httpapi.ValidateLimitWindow}) {
		t.Fatalf("validate limit = %+v", validateLimit)
	}
	f := newValidateFixture(t, true)
	token := tokenBody(f.eddsaToken(contracts.PlatformAPIAudience))
	for i := 0; i < 3; i++ {
		f.post(f.server, `{}`, nil)
	}
	for i := 0; i < httpapi.ValidateLimitCount; i++ {
		if r := f.post(f.server, token, nil); r.Code != http.StatusOK {
			t.Fatalf("check %d: %d %s", i+1, r.Code, r.Body)
		}
	}
	expectAnswer(t, "one over the count", f.post(f.server, token, nil), http.StatusTooManyRequests,
		`{"detail":{"message":"Rate limit exceeded. Please try again later."}}`)
	expectAnswer(t, "another token has its own bucket", f.post(f.server, tokenBody(f.eddsaToken(contracts.PlatformAPIAudience)), nil),
		http.StatusOK, f.validAnswer("ada@example.com", f.org.String(), "admin"))
	for i := 0; i < httpapi.ValidateLimitCount; i++ {
		if r := f.post(f.server, tokenBody(""), nil); r.Code != http.StatusOK {
			t.Fatalf("empty token check %d: %d %s", i+1, r.Code, r.Body)
		}
	}
	if r := f.post(f.server, tokenBody(""), nil); r.Code != http.StatusTooManyRequests {
		t.Fatalf("empty token over the count: %d %s", r.Code, r.Body)
	}
	digest := validateKeyFor(t, "abc")
	if digest != "validate-token:ba7816bf8f01cfea414140de5dae2223" { // gitleaks:allow -- sha256("abc") prefix, a public test vector
		t.Fatalf("token key = %q, want go-api's validate-token:<sha256[:32]>", digest)
	}
}

// validateKeyFor is the limit key of token.
func validateKeyFor(t *testing.T, token string) string {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, PatternCompatValidate, nil)
	key, err := validateKey(request, token)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

// c8: the route mounts on its row of the embedded endpoint-profile document
// in EdDSA mode, and is a 404 on a document without it. Without the issuer it
// is not mounted (TestCompatValidateAndSwitchOrgNotMountedWithoutTheIssuer).
func TestValidateCompatC8MountsOnItsEmbeddedProfileRow(t *testing.T) {
	for _, eddsa := range []bool{true} {
		f := newValidateFixture(t, eddsa)
		f.logs.Reset()
		routes, err := CompatRoutes(f.deps())
		if err != nil {
			t.Fatal(err)
		}
		mounted := 0
		for _, route := range routes {
			if route.Method == http.MethodPost && route.Pattern == PatternCompatValidate {
				mounted++
			}
		}
		if mounted != 1 {
			t.Fatalf("eddsa=%v: the embedded document mounts the validate route %d times, want 1", eddsa, mounted)
		}
		if strings.Contains(f.logs.String(), "auth validate route not mounted") {
			t.Fatalf("eddsa=%v: a not-mounted line with the row embedded: %s", eddsa, f.logs)
		}
		f.document = []byte(`{"rows":[]}`)
		f.build()
		if r := f.post(f.server, tokenBody(f.eddsaToken(contracts.PlatformAPIAudience)), nil); r.Code != http.StatusNotFound {
			t.Fatalf("eddsa=%v: an empty document: %d %s", eddsa, r.Code, r.Body)
		}
	}
}

// The route over the production flag table (MarkResponseModels on the
// embedded document's route set) writes its valid and its refused answer
// with no writer violation.
func TestValidateCompatProductionResponseModelRaisesNoWriterViolation(t *testing.T) {
	for _, eddsa := range []bool{true} {
		f := newValidateFixture(t, eddsa)
		routes, err := CompatRoutes(f.deps())
		if err != nil {
			t.Fatal(err)
		}
		mux := http.NewServeMux()
		for _, route := range MarkResponseModels(routes) {
			mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(flaggedHandler(route)))
		}
		tokens := map[string]string{"eddsa": f.eddsaToken(contracts.PlatformAPIAudience)}
		for name, token := range tokens {
			expectNoViolation(t, fmt.Sprintf("eddsa=%v valid %s", eddsa, name), http.StatusOK, func() int {
				r := f.post(mux, tokenBody(token), nil)
				if !strings.Contains(r.Body.String(), `"valid":true`) {
					t.Errorf("eddsa=%v %s: %s, want valid true", eddsa, name, r.Body)
				}
				return r.Code
			})
		}
		expectNoViolation(t, fmt.Sprintf("eddsa=%v invalid", eddsa), http.StatusOK, func() int {
			r := f.post(mux, tokenBody("garbage"), nil)
			if !strings.Contains(r.Body.String(), `"valid":false`) {
				t.Errorf("eddsa=%v garbage: %s, want valid false", eddsa, r.Body)
			}
			return r.Code
		})
	}
}
