package oidc

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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

var errUnused = errors.New("oidc test: dependency not used by this test")

type stubIdentities struct{}

func (stubIdentities) IdentityBySubject(context.Context, ExternalKey, LegacyMatch) (Principal, error) {
	return Principal{}, errUnused
}

func (stubIdentities) EmailInUse(context.Context, string) (bool, error) { return false, errUnused }

func (stubIdentities) ProvisionFederated(context.Context, FederatedAccount) (Principal, error) {
	return Principal{}, errUnused
}

type stubMemberships struct{}

func (stubMemberships) Role(context.Context, uuid.UUID, uuid.UUID) (string, bool, error) {
	return "", false, errUnused
}

type stubSessions struct{}

func (stubSessions) Issue(context.Context, SessionRequest) (Tokens, error) {
	return Tokens{}, errUnused
}

type countingEntitlements struct {
	allowed bool
	calls   atomic.Int64
}

func (e *countingEntitlements) SSOAllowed(context.Context, uuid.UUID) (bool, error) {
	e.calls.Add(1)
	return e.allowed, nil
}

func (*countingEntitlements) DeniedDetail() any { return "SSO is not licensed" }

// scriptedFlows accepts every flow; onBegin or onConsume, when set, is the
// value Begin or Consume panics with.
type scriptedFlows struct{ onBegin, onConsume any }

func (f *scriptedFlows) Begin(context.Context, Flow) error {
	if f.onBegin != nil {
		panic(f.onBegin)
	}
	return nil
}

func (f *scriptedFlows) Consume(context.Context, [32]byte, string, uuid.UUID, time.Time) (bool, error) {
	if f.onConsume != nil {
		panic(f.onConsume)
	}
	return true, nil
}

type unitEnv struct {
	svc          *Service
	providers    *fakeProviders
	entitlements *countingEntitlements
	flows        *scriptedFlows
	reader       *sdkmetric.ManualReader
	logged       *bytes.Buffer
	now          time.Time
	secret       string
}

func newUnitEnv(t *testing.T, providers *fakeProviders) *unitEnv {
	t.Helper()
	var secret contracts.SecretString
	raw, _ := json.Marshal(uuid.NewString())
	if err := json.Unmarshal(raw, &secret); err != nil {
		t.Fatal(err)
	}
	env := &unitEnv{
		providers: providers, entitlements: &countingEntitlements{allowed: true}, flows: &scriptedFlows{},
		reader: sdkmetric.NewManualReader(), logged: &bytes.Buffer{}, now: time.Unix(1_800_000_000, 0),
		secret: secret.Reveal(),
	}
	svc, err := New(Config{
		Providers: providers, Flows: env.flows, Identities: stubIdentities{}, Memberships: stubMemberships{},
		Entitlements: env.entitlements, Sessions: stubSessions{}, StateSecret: secret,
		AppBaseURL: "https://app.example", Now: func() time.Time { return env.now },
		Logger: slog.New(slog.NewTextHandler(env.logged, nil)),
		Meter:  sdkmetric.NewMeterProvider(sdkmetric.WithReader(env.reader)).Meter("test"),
	})
	if err != nil {
		t.Fatal(err)
	}
	env.svc = svc
	return env
}

// routeCase is one SSO route as the auth-service mux serves it.
type routeCase struct {
	route    string // the auth.oidc.outcomes route label
	pattern  string
	oauth    bool
	callback bool
	byType   bool
	handler  func(*Service) http.Handler
}

var routeCases = []routeCase{
	{route: routeAuthorize, pattern: "POST /api/v1/auth/oidc/{provider_id}/authorize", handler: (*Service).AuthorizeHandler},
	{route: routeCallback, pattern: "POST /api/v1/auth/oidc/{provider_id}/callback", callback: true, handler: (*Service).CallbackHandler},
	{route: routeOAuthAuthorize, pattern: "POST /api/v1/auth/oauth/{provider_id}/authorize", oauth: true, handler: (*Service).OAuthAuthorizeHandler},
	{route: routeOAuthCallback, pattern: "POST /api/v1/auth/oauth/{provider_id}/callback", oauth: true, callback: true, handler: (*Service).OAuthCallbackHandler},
	{route: "oauth_authorize_by_type", pattern: "GET /api/v1/auth/oauth/{provider_type}/authorize", oauth: true, byType: true, handler: (*Service).OAuthAuthorizeByTypeHandler},
}

func (c routeCase) activeRow() storedRow {
	row := storedRow{ID: uuid.New(), OrgID: uuid.New(), Protocol: protocolOIDC, Status: "active",
		Config: `{"client_id":"client-1","issuer":"https://idp.example"}`}
	if c.oauth {
		row.Protocol, row.Config = "oauth_gitlab", `{"client_id":"client-1"}`
	}
	return row
}

func (c routeCase) otherProtocol() string {
	if c.oauth {
		return protocolOIDC
	}
	return "saml"
}

// request builds the route's request for row. sealed sends a callback a state
// and login-nonce cookie the service accepts, so the flow reaches the flow store.
func (env *unitEnv) request(t *testing.T, c routeCase, row storedRow, sealed bool) *http.Request {
	t.Helper()
	if c.byType {
		return httptest.NewRequest(http.MethodGet, "/api/v1/auth/oauth/gitlab/authorize?org_id="+row.OrgID.String(), nil)
	}
	kind, step := "oidc", "authorize"
	if c.oauth {
		kind = "oauth"
	}
	body := `{}`
	if c.callback {
		step, body = "callback", `{"code":"c","state":"s"}`
	}
	var cookie *http.Cookie
	if c.callback && sealed {
		token, name := env.sealCallback(t, c, row)
		body = `{"code":"c","state":"` + token + `"}`
		cookie = &http.Cookie{Name: name, Value: "cookie-1"}
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/"+kind+"/"+row.ID.String()+"/"+step, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if cookie != nil {
		req.AddCookie(cookie)
	}
	return req
}

func (env *unitEnv) sealCallback(t *testing.T, c routeCase, row storedRow) (token, cookieName string) {
	t.Helper()
	if c.oauth {
		token, state, err := sealOAuthState(env.secret, oauthFlowState{ProviderID: row.ID.String(), OrgID: row.OrgID.String(),
			RedirectURI: "https://app.example/cb", NonceHash: hashLoginNonce("cookie-1")}, env.now)
		if err != nil {
			t.Fatal(err)
		}
		return token, oauthLoginNonceCookieName(state.ID)
	}
	token, state, err := sealState(env.secret, flowState{ProviderID: row.ID.String(), OrgID: row.OrgID.String(),
		Nonce: "n-1", CodeVerifier: "v-1", RedirectURI: "https://app.example/cb", NonceHash: hashLoginNonce("cookie-1")}, env.now)
	if err != nil {
		t.Fatal(err)
	}
	return token, loginNonceCookieName(state.ID)
}

// serve runs req through the route under the shared recovery middleware, as
// the auth-service server mounts it.
func (env *unitEnv) serve(c routeCase, req *http.Request) *httptest.ResponseRecorder {
	mux := http.NewServeMux()
	mux.Handle(c.pattern, c.handler(env.svc))
	handler := httpapi.Recover(slog.New(slog.NewTextHandler(io.Discard, nil)), c.pattern)(mux)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// outcomeCounts reads auth.oidc.outcomes by its route, outcome and reason labels.
func outcomeCounts(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "auth.oidc.outcomes" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("auth.oidc.outcomes is %T", m.Data)
			}
			for _, dp := range sum.DataPoints {
				route, _ := dp.Attributes.Value("route")
				outcome, _ := dp.Attributes.Value("outcome")
				reason, _ := dp.Attributes.Value("reason")
				out[route.AsString()+"/"+outcome.AsString()+"/"+reason.AsString()] += dp.Value
			}
		}
	}
	return out
}

func detailOf(t *testing.T, rec *httptest.ResponseRecorder) any {
	t.Helper()
	var body struct {
		Detail any `json:"detail"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %q: %v", rec.Body.String(), err)
	}
	return body.Detail
}

// A provider row that changes during a request is answered from one version of
// the row: the version read first (200, or the callback's 400 for its test
// state) or the changed one (400). A malformed config on a row that is no
// longer an active row of the route's protocol never makes a 500.
func TestProviderRowChangeMidRequestIsAnsweredFromOneRow(t *testing.T) {
	for _, c := range routeCases {
		for _, change := range []string{"inactive", "protocol"} {
			for flip := 1; flip <= 3; flip++ {
				t.Run(fmt.Sprintf("%s/%s/after-read-%d", c.route, change, flip), func(t *testing.T) {
					before := c.activeRow()
					after := before
					after.Config = `[]`
					if change == "inactive" {
						after.Status = "inactive"
					} else {
						after.Protocol = c.otherProtocol()
					}
					env := newUnitEnv(t, &fakeProviders{before: before, after: after, flipAfter: flip})
					rec := env.serve(c, env.request(t, c, before, false))
					if rec.Code != http.StatusOK && rec.Code != http.StatusBadRequest {
						t.Fatalf("status = %d, want 200 or 400; body %s", rec.Code, rec.Body.String())
					}
				})
			}
		}
	}
}

// The provider checks keep go-api's order: 404 unknown provider, 402 not
// entitled, 400 other protocol, 400 not active; only then is the stored config
// decoded (a malformed one on an admitted row is a 500).
func TestProviderChecksKeepTheirOrder(t *testing.T) {
	for _, c := range routeCases {
		wrongProtocol, inactive := oidcSpec.wrongProtocol, oidcSpec.inactive
		if c.oauth {
			wrongProtocol, inactive = oauthSpec.wrongProtocol, oauthSpec.inactive
		}
		steps := []struct {
			name       string
			mutate     func(row *storedRow)
			entitled   bool
			want       int
			wantDetail any
		}{
			{"unknown", func(row *storedRow) { row.ID, row.OrgID = uuid.New(), uuid.New() }, true, http.StatusNotFound, nil},
			{"not-entitled", func(row *storedRow) { row.Protocol, row.Status, row.Config = c.otherProtocol(), "inactive", `[]` }, false, http.StatusPaymentRequired, nil},
			{"other-protocol", func(row *storedRow) { row.Protocol, row.Status, row.Config = c.otherProtocol(), "inactive", `[]` }, true, http.StatusBadRequest, wrongProtocol},
			{"inactive", func(row *storedRow) { row.Status, row.Config = "inactive", `[]` }, true, http.StatusBadRequest, inactive},
			{"malformed-config", func(row *storedRow) { row.Config = `[]` }, true, http.StatusInternalServerError, nil},
		}
		for _, s := range steps {
			t.Run(c.route+"/"+s.name, func(t *testing.T) {
				requested := c.activeRow()
				stored := requested
				s.mutate(&stored)
				env := newUnitEnv(t, &fakeProviders{before: stored})
				env.entitlements.allowed = s.entitled
				want, wantDetail := s.want, s.wantDetail
				if c.byType && (s.name == "unknown" || s.name == "other-protocol" || s.name == "inactive") {
					// The type route selects an active row of its protocol; none matches.
					want, wantDetail = http.StatusNotFound, nil
				}
				rec := env.serve(c, env.request(t, c, requested, false))
				if rec.Code != want {
					t.Fatalf("status = %d, want %d; body %s", rec.Code, want, rec.Body.String())
				}
				if wantDetail != nil {
					if got := detailOf(t, rec); got != wantDetail {
						t.Fatalf("detail = %v, want %v", got, wantDetail)
					}
				}
			})
		}
	}
}

// Every request reads its provider row once and checks the entitlement once.
func TestOneProviderReadPerRequest(t *testing.T) {
	for _, c := range routeCases {
		t.Run(c.route, func(t *testing.T) {
			row := c.activeRow()
			env := newUnitEnv(t, &fakeProviders{before: row})
			rec := env.serve(c, env.request(t, c, row, false))
			if rec.Code >= http.StatusInternalServerError {
				t.Fatalf("status = %d; body %s", rec.Code, rec.Body.String())
			}
			if n := env.providers.readCount(); n != 1 {
				t.Errorf("provider row reads = %d, want 1", n)
			}
			if n := env.entitlements.calls.Load(); n != 1 {
				t.Errorf("entitlement checks = %d, want 1", n)
			}
		})
	}
}

// A panic in a flow dependency is answered 500 by the shared recovery
// middleware and counted once as route/error/dependency_unavailable, with one
// log line that never carries the panic value.
func TestRecoveredPanicIsCountedOnce(t *testing.T) {
	for _, c := range routeCases {
		t.Run(c.route, func(t *testing.T) {
			row := c.activeRow()
			env := newUnitEnv(t, &fakeProviders{before: row})
			value := "panic-value-" + uuid.NewString()
			if c.callback {
				env.flows.onConsume = value
			} else {
				env.flows.onBegin = value
			}
			rec := env.serve(c, env.request(t, c, row, true))
			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want 500; body %s", rec.Code, rec.Body.String())
			}
			counts := outcomeCounts(t, env.reader)
			want := map[string]int64{c.route + "/error/dependency_unavailable": 1}
			if fmt.Sprint(counts) != fmt.Sprint(want) {
				t.Errorf("auth.oidc.outcomes = %v, want %v", counts, want)
			}
			logged := env.logged.String()
			if n := strings.Count(logged, "route="+c.route+" outcome=error reason=dependency_unavailable"); n != 1 {
				t.Errorf("outcome log lines = %d, want 1:\n%s", n, logged)
			}
			if strings.Contains(logged, value) {
				t.Errorf("the log carries the panic value:\n%s", logged)
			}
			if !strings.Contains(logged, "provider_id="+row.ID.String()+" ") {
				t.Errorf("the outcome log line does not name the selected provider %s:\n%s", row.ID, logged)
			}
			if attr := renderedErrorAttr(errHandlerPanic); !strings.Contains(logged, " "+attr) {
				t.Errorf("the outcome log line does not carry %q:\n%s", attr, logged)
			}
		})
	}
}

// renderedErrorAttr is logging.ErrorAttr(err) as the outcome logger's text
// handler writes it.
func renderedErrorAttr(err error) string {
	var line bytes.Buffer
	slog.New(slog.NewTextHandler(&line, &slog.HandlerOptions{ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
		if len(groups) == 0 && (a.Key == slog.TimeKey || a.Key == slog.LevelKey || a.Key == slog.MessageKey) {
			return slog.Attr{}
		}
		return a
	}})).Info("", logging.ErrorAttr(err))
	return strings.TrimSpace(line.String())
}

// The type route counts and logs under its own label, refusals included.
func TestTypeRouteCountsUnderItsOwnLabel(t *testing.T) {
	c := routeCases[len(routeCases)-1]
	row := c.activeRow()
	env := newUnitEnv(t, &fakeProviders{before: row})
	if rec := env.serve(c, env.request(t, c, row, false)); rec.Code != http.StatusOK {
		t.Fatalf("status = %d; body %s", rec.Code, rec.Body.String())
	}
	for _, target := range []string{"/api/v1/auth/oauth/bitbucket/authorize?org_id=" + row.OrgID.String(), "/api/v1/auth/oauth/gitlab/authorize"} {
		if rec := env.serve(c, httptest.NewRequest(http.MethodGet, target, nil)); rec.Code < 400 || rec.Code >= 500 {
			t.Fatalf("%s: status = %d, want a 4xx", target, rec.Code)
		}
	}
	counts := outcomeCounts(t, env.reader)
	want := map[string]int64{"oauth_authorize_by_type/success/accepted": 1, "oauth_authorize_by_type/denied/malformed": 2}
	if fmt.Sprint(counts) != fmt.Sprint(want) {
		t.Errorf("auth.oidc.outcomes = %v, want %v", counts, want)
	}
	if n := strings.Count(env.logged.String(), "route=oauth_authorize_by_type "); n != 3 {
		t.Errorf("log lines with route=oauth_authorize_by_type = %d, want 3:\n%s", n, env.logged.String())
	}
}
