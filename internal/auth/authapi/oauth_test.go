package authapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/oauthgrant"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

const (
	oauthIssuer = "https://api.example.test/oauth"
	oauthWeb    = "https://web.example.test"
	oauthAPI    = "https://acr.example.test"
	oauthMCP    = "https://mcp.example.test/mcp"
	pkceExample = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
	oauthState  = "a-client-state-value"
)

type oauthStoreFake struct {
	requests  []oauthgrant.RequestInput
	described []oauthgrant.DescribeInput
	decisions []oauthgrant.Decision
	err       error
}

func (s *oauthStoreFake) CreateRequest(_ context.Context, in oauthgrant.RequestInput) (uuid.UUID, error) {
	s.requests = append(s.requests, in)
	return uuid.New(), s.err
}

func (s *oauthStoreFake) Describe(_ context.Context, in oauthgrant.DescribeInput) (oauthgrant.Described, error) {
	s.described = append(s.described, in)
	return oauthgrant.Described{ClientID: "dev-health-web", ClientClass: "browser", Resource: oauthAPI, Audience: "acr-api",
		Scope: "acr:api", CSRF: contracts.NewSecretString("fcocs_token")}, s.err
}

func (s *oauthStoreFake) Approve(_ context.Context, in oauthgrant.Decision) (oauthgrant.Approved, error) {
	s.decisions = append(s.decisions, in)
	return oauthgrant.Approved{Code: contracts.NewSecretString("fcoac_code"), RedirectURI: oauthWeb + "/ask-dev/oauth/callback", State: oauthState,
		GrantID: uuid.New(), SessionID: uuid.New(), ClientID: "dev-health-web", ClientClass: "browser", Audience: "acr-api"}, s.err
}

func (s *oauthStoreFake) Deny(_ context.Context, in oauthgrant.Decision) (oauthgrant.Denied, error) {
	s.decisions = append(s.decisions, in)
	return oauthgrant.Denied{RedirectURI: oauthWeb + "/ask-dev/oauth/callback", State: oauthState}, s.err
}

type oauthFixture struct {
	*fixture
	store *oauthStoreFake
	mux   *http.ServeMux
	logs  *bytes.Buffer
}

// oauthRows is a document with a row per route: the rows O3c adds.
func oauthRows() []byte {
	var rows []string
	for _, r := range [][2]string{{"GET", PatternOAuthAuthorize}, {"POST", PatternOAuthAuthorize}, {"GET", PatternOAuthMetadata}, {"GET", PatternOAuthDiscovery}, {"GET", PatternOAuthJWKS}} {
		rows = append(rows, fmt.Sprintf(`{"service":%q,"method":%q,"route":%q}`, AccessService, r[0], r[1]))
	}
	return []byte(`{"rows":[` + strings.Join(rows, ",") + `]}`)
}

func oauthRegistry(t *testing.T) oauthgrant.Registry {
	t.Helper()
	registry, err := oauthgrant.ParseRegistry([]byte(`{"schema_version":"oauth-clients.v1","clients":[
		{"client_id":"dev-health-web","client_name":"Dev Health","client_class":"browser","first_party":true,"audiences":["acr-api"],"redirect_path":"/ask-dev/oauth/callback"},
		{"client_id":"desktop","client_name":"Desktop","client_class":"mcp","first_party":false,"audiences":["acr-mcp"],"redirect_uris":["https://client.example.test/cb"]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func newOAuthFixture(t *testing.T, f *fixture, edit func(*OAuthDeps)) *oauthFixture {
	t.Helper()
	limits, err := NewLimits(httpapi.NewMemoryCounters(nil), testRateLimitSecret)
	if err != nil {
		t.Fatal(err)
	}
	signer, _, _ := fileSigner(t)
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	store := &oauthStoreFake{}
	deps := OAuthDeps{Store: store, Registry: oauthRegistry(t), Limits: limits, Keys: signer, IssuerURL: oauthIssuer,
		WebOrigin: oauthWeb, ResourceACRAPI: oauthAPI, ResourceACRMCP: oauthMCP,
		Session: Deps{Sessions: f.sessions, Users: f.users, Issuer: f.issuer, Logger: logger}}
	if edit != nil {
		edit(&deps)
	}
	routes, err := oauthRoutesRows(deps, oauthRows())
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	return &oauthFixture{fixture: f, store: store, mux: mux, logs: logs}
}

func (o *oauthFixture) get(query url.Values) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, PatternOAuthAuthorize+"?"+query.Encode(), nil)
	response := httptest.NewRecorder()
	o.mux.ServeHTTP(response, request)
	return response
}

func (o *oauthFixture) post(body string, header map[string]string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, PatternOAuthAuthorize, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for k, v := range header {
		request.Header.Set(k, v)
	}
	response := httptest.NewRecorder()
	o.mux.ServeHTTP(response, request)
	return response
}

func browserQuery() url.Values {
	return url.Values{"client_id": {"dev-health-web"}, "redirect_uri": {oauthWeb + "/ask-dev/oauth/callback"}, "response_type": {"code"},
		"code_challenge": {pkceExample}, "code_challenge_method": {"S256"}, "resource": {oauthAPI}, "scope": {"acr:api"}, "state": {oauthState}}
}

func mcpQuery() url.Values {
	return url.Values{"client_id": {"desktop"}, "redirect_uri": {"https://client.example.test/cb"}, "response_type": {"code"},
		"code_challenge": {pkceExample}, "code_challenge_method": {"S256"}, "resource": {oauthMCP}, "state": {oauthState}}
}

func newPlainOAuthFixture(t *testing.T) *oauthFixture {
	t.Helper()
	return newOAuthFixture(t, newFixture(t), nil)
}

// A valid request writes one row, and the browser goes to the consent page with the
// request id only. The class, audience and scope come from the registry.
func TestAuthorizeWritesOneRowAndSendsTheBrowserToConsent(t *testing.T) {
	for name, tc := range map[string]struct {
		query                  url.Values
		class, audience, scope string
	}{
		"browser": {browserQuery(), "browser", "acr-api", "acr:api"},
		"mcp":     {mcpQuery(), "mcp", "acr-mcp", "acr:mcp"},
	} {
		t.Run(name, func(t *testing.T) {
			o := newPlainOAuthFixture(t)
			query := tc.query
			query.Set("client_class", "browser") // a metadata-supplied class cannot apply
			r := o.get(query)
			if r.Code != http.StatusFound {
				t.Fatalf("status %d, body %s", r.Code, r.Body)
			}
			location, err := url.Parse(r.Header().Get("Location"))
			if err != nil {
				t.Fatal(err)
			}
			if location.Scheme+"://"+location.Host+location.Path != oauthWeb+oauthConsentPath {
				t.Fatalf("location %s", location)
			}
			if _, err := uuid.Parse(location.Query().Get("request_id")); err != nil || len(location.Query()) != 1 {
				t.Fatalf("location query %v", location.Query())
			}
			if len(o.store.requests) != 1 {
				t.Fatalf("%d rows", len(o.store.requests))
			}
			got := o.store.requests[0]
			if got.ClientClass != tc.class || got.Audience != tc.audience || got.Scope != tc.scope || got.State != oauthState || got.CodeChallenge != pkceExample {
				t.Fatalf("row %+v", got)
			}
			if r.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("the redirect is cacheable")
			}
		})
	}
}

// Before the redirect is proven nothing goes to it: a 400 with no Location and no row.
func TestAuthorizeSendsNoRedirectBeforeTheRedirectIsProven(t *testing.T) {
	edit := func(name, value string) url.Values {
		q := browserQuery()
		q.Set(name, value)
		return q
	}
	repeated := browserQuery()
	repeated.Add("client_id", "desktop")
	twice := browserQuery()
	twice.Add("redirect_uri", oauthWeb+"/ask-dev/oauth/callback")
	missing := browserQuery()
	missing.Del("redirect_uri")
	for name, query := range map[string]url.Values{
		"an unknown client":   edit("client_id", "nobody"),
		"a metadata client":   edit("client_id", "https://evil.example.test/client.json"),
		"a trailing slash":    edit("redirect_uri", oauthWeb+"/ask-dev/oauth/callback/"),
		"an extra query":      edit("redirect_uri", oauthWeb+"/ask-dev/oauth/callback?x=1"),
		"another case":        edit("redirect_uri", oauthWeb+"/ask-dev/OAuth/callback"),
		"an unlisted host":    edit("redirect_uri", "https://evil.example.test/ask-dev/oauth/callback"),
		"another client's":    edit("redirect_uri", "https://client.example.test/cb"),
		"no redirect":         missing,
		"a repeated client":   repeated,
		"a repeated redirect": twice,
	} {
		t.Run(name, func(t *testing.T) {
			o := newPlainOAuthFixture(t)
			r := o.get(query)
			if r.Code != http.StatusBadRequest || r.Header().Get("Location") != "" || len(o.store.requests) != 0 {
				t.Fatalf("status %d, location %q, %d rows", r.Code, r.Header().Get("Location"), len(o.store.requests))
			}
		})
	}
	t.Run("unset settings refuse at request time", func(t *testing.T) {
		for name, deps := range map[string]func(*OAuthDeps){
			"no web origin": func(d *OAuthDeps) { d.WebOrigin = "" },
			"no resource":   func(d *OAuthDeps) { d.ResourceACRAPI = "" },
		} {
			o := newOAuthFixture(t, newFixture(t), deps)
			r := o.get(browserQuery())
			if r.Code != http.StatusBadRequest || r.Header().Get("Location") != "" || len(o.store.requests) != 0 {
				t.Errorf("%s: status %d, location %q", name, r.Code, r.Header().Get("Location"))
			}
		}
	})
	t.Run("no web origin refuses a fixed-URI client too", func(t *testing.T) {
		o := newOAuthFixture(t, newFixture(t), func(d *OAuthDeps) { d.WebOrigin = "" })
		if r := o.get(mcpQuery()); r.Code != http.StatusBadRequest || r.Header().Get("Location") != "" {
			t.Fatalf("status %d, location %q", r.Code, r.Header().Get("Location"))
		}
	})
	t.Run("the other audience stays servable", func(t *testing.T) {
		o := newOAuthFixture(t, newFixture(t), func(d *OAuthDeps) { d.ResourceACRAPI = "" })
		if r := o.get(mcpQuery()); r.Code != http.StatusFound {
			t.Fatalf("status %d", r.Code)
		}
	})
}

// After the redirect is proven, an error goes to it with the state and the issuer,
// and no row is written.
func TestAuthorizeErrorsGoToAProvenRedirect(t *testing.T) {
	edit := func(name, value string) url.Values {
		q := browserQuery()
		q.Set(name, value)
		return q
	}
	drop := func(name string) url.Values {
		q := browserQuery()
		q.Del(name)
		return q
	}
	repeatedScope := browserQuery()
	repeatedScope.Add("scope", "acr:api")
	for name, tc := range map[string]struct {
		query url.Values
		want  string
	}{
		"PKCE plain":         {edit("code_challenge_method", "plain"), "invalid_request"},
		"no PKCE method":     {drop("code_challenge_method"), "invalid_request"},
		"no challenge":       {drop("code_challenge"), "invalid_request"},
		"a short challenge":  {edit("code_challenge", "abc"), "invalid_request"},
		"implicit":           {edit("response_type", "token"), "unsupported_response_type"},
		"no response type":   {drop("response_type"), "unsupported_response_type"},
		"another resource":   {edit("resource", oauthMCP), "invalid_target"},
		"a resource variant": {edit("resource", oauthAPI+"/"), "invalid_target"},
		"no resource":        {drop("resource"), "invalid_target"},
		"another scope":      {edit("scope", "acr:mcp"), "invalid_scope"},
		"a repeated scope":   {repeatedScope, "invalid_scope"},
	} {
		t.Run(name, func(t *testing.T) {
			o := newPlainOAuthFixture(t)
			r := o.get(tc.query)
			if r.Code != http.StatusFound || len(o.store.requests) != 0 {
				t.Fatalf("status %d, %d rows", r.Code, len(o.store.requests))
			}
			location, err := url.Parse(r.Header().Get("Location"))
			if err != nil {
				t.Fatal(err)
			}
			got := location.Query()
			if got.Get("error") != tc.want || got.Get("state") != oauthState || got.Get("iss") != oauthIssuer || !strings.HasPrefix(location.String(), oauthWeb+"/ask-dev/oauth/callback?") {
				t.Fatalf("location %s", location)
			}
		})
	}
	t.Run("no state", func(t *testing.T) {
		o := newPlainOAuthFixture(t)
		location, err := url.Parse(o.get(drop("state")).Header().Get("Location"))
		if err != nil {
			t.Fatal(err)
		}
		if location.Query().Get("error") != "invalid_request" || location.Query().Has("state") {
			t.Fatalf("location %s", location)
		}
	})
	t.Run("a store failure", func(t *testing.T) {
		o := newPlainOAuthFixture(t)
		o.store.err = oauthgrant.ErrStore
		location, err := url.Parse(o.get(browserQuery()).Header().Get("Location"))
		if err != nil {
			t.Fatal(err)
		}
		if location.Query().Get("error") != "temporarily_unavailable" {
			t.Fatalf("location %s", location)
		}
	})
}

// The state limit is the column's: characters, not bytes. Text the column cannot
// hold is a refusal on the redirect, not a store error.
func TestAuthorizeStateIsWhatTheColumnHolds(t *testing.T) {
	for name, tc := range map[string]struct {
		state string
		want  bool
	}{
		"1024 ascii":    {strings.Repeat("a", 1024), true},
		"1025 ascii":    {strings.Repeat("a", 1025), false},
		"1024 two-byte": {strings.Repeat("é", 1024), true},
		"1025 two-byte": {strings.Repeat("é", 1025), false},
		"invalid utf-8": {"a\xffb", false},
		"a nul":         {"a\x00b", false},
		"empty":         {"", false},
	} {
		t.Run(name, func(t *testing.T) {
			o := newPlainOAuthFixture(t)
			query := browserQuery()
			query.Set("state", tc.state)
			r := o.get(query)
			if r.Code != http.StatusFound || (len(o.store.requests) == 1) != tc.want {
				t.Fatalf("status %d, %d rows, want accepted = %v", r.Code, len(o.store.requests), tc.want)
			}
			if !tc.want && !strings.Contains(r.Header().Get("Location"), "error=invalid_request") {
				t.Fatalf("location %s", r.Header().Get("Location"))
			}
		})
	}
}

func TestAuthorizeChargesThePerClientBudget(t *testing.T) {
	o := newPlainOAuthFixture(t)
	for i := range oauthClientLimit.Count {
		if r := o.get(browserQuery()); r.Code != http.StatusFound {
			t.Fatalf("request %d: status %d", i, r.Code)
		}
	}
	if r := o.get(browserQuery()); r.Code != http.StatusTooManyRequests {
		t.Fatalf("status %d", r.Code)
	}
	if r := o.get(mcpQuery()); r.Code != http.StatusFound {
		t.Fatalf("another client: status %d", r.Code)
	}
	// An unknown client has no safe identity, so it is not charged.
	before := len(o.store.requests)
	bad := browserQuery()
	bad.Set("client_id", "nobody")
	for range oauthClientLimit.Count + 1 {
		if r := o.get(bad); r.Code != http.StatusBadRequest {
			t.Fatalf("status %d", r.Code)
		}
	}
	if len(o.store.requests) != before {
		t.Fatal("an unknown client wrote a row")
	}
}

// The telemetry holds no code, token or state, and of a client id outside the
// registry only an https host.
func TestOAuthLogsHoldNoSecretAndOnlyAClientsHost(t *testing.T) {
	o := newPlainOAuthFixture(t)
	bad := browserQuery()
	bad.Set("client_id", "https://client.example.test/some/path?secret=1")
	o.get(bad)
	text := browserQuery()
	text.Set("client_id", "free-text-from-a-client")
	o.get(text)
	other := browserQuery()
	other.Set("client_id", "gopher://free-host-from-a-client/")
	o.get(other)
	o.get(browserQuery())
	headers := o.bearer(o.grant())
	o.post("action=describe&request_id="+uuid.NewString(), headers)
	o.post("action=approve&csrf_token=fcocs_token&request_id="+uuid.NewString(), headers)
	logs := o.logs.String()
	if !strings.Contains(logs, `"client":"client.example.test"`) || !strings.Contains(logs, `"client":"dev-health-web"`) {
		t.Fatalf("the positive control is missing: %s", logs)
	}
	for _, secret := range []string{oauthState, pkceExample, "fcoac_code", "fcocs_token", "some/path", "free-text-from-a-client", "free-host-from-a-client"} {
		if strings.Contains(logs, secret) {
			t.Errorf("the log holds %q", secret)
		}
	}
}

// A decision takes its session and principal from the bearer, never from the body.
func TestDecideActsAsTheBearer(t *testing.T) {
	o := newPlainOAuthFixture(t)
	headers := o.bearer(o.grant())
	request := uuid.New()
	r := o.post(url.Values{"action": {"describe"}, "request_id": {request.String()}}.Encode(), headers)
	if r.Code != http.StatusOK {
		t.Fatalf("describe: %d %s", r.Code, r.Body)
	}
	var described oauthDescription
	if err := json.Unmarshal(r.Body.Bytes(), &described); err != nil {
		t.Fatal(err)
	}
	if !described.FirstParty || described.ClientName != "Dev Health" || described.CSRFToken != "fcocs_token" {
		t.Fatalf("description %+v", described)
	}
	bound := o.store.described[0]
	if bound.WebSessionID != o.record.ID || bound.PrincipalID != o.principal || bound.RequestID != request || bound.OrganizationID == nil || *bound.OrganizationID != o.org {
		t.Fatalf("bound %+v", bound)
	}
	body := url.Values{"action": {"approve"}, "request_id": {request.String()}, "csrf_token": {"fcocs_token"}}.Encode()
	r = o.post(body, headers)
	if r.Code != http.StatusOK {
		t.Fatalf("approve: %d %s", r.Code, r.Body)
	}
	var answer oauthRedirectAnswer
	if err := json.Unmarshal(r.Body.Bytes(), &answer); err != nil {
		t.Fatal(err)
	}
	location, err := url.Parse(answer.RedirectTo)
	if err != nil {
		t.Fatal(err)
	}
	if q := location.Query(); q.Get("code") != "fcoac_code" || q.Get("state") != oauthState || q.Get("iss") != oauthIssuer {
		t.Fatalf("redirect_to %s", location)
	}
	decision := o.store.decisions[0]
	if decision.WebSessionID != o.record.ID || decision.PrincipalID != o.principal || decision.CSRF.Reveal() != "fcocs_token" {
		t.Fatalf("decision %+v", decision)
	}
	r = o.post(strings.Replace(body, "approve", "deny", 1), headers)
	if r.Code != http.StatusOK {
		t.Fatalf("deny: %d %s", r.Code, r.Body)
	}
	if err := json.Unmarshal(r.Body.Bytes(), &answer); err != nil {
		t.Fatal(err)
	}
	if location, err = url.Parse(answer.RedirectTo); err != nil || location.Query().Get("error") != "access_denied" || location.Query().Has("code") {
		t.Fatalf("redirect_to %s, %v", answer.RedirectTo, err)
	}
}

// The POST route reads both token classes: the EdDSA user access token and, where
// the row opts in, the HS256 token (acp's condition 6).
func TestDecideReadsBothTokenClasses(t *testing.T) {
	body := url.Values{"action": {"describe"}, "request_id": {uuid.NewString()}}.Encode()
	t.Run("hs256", func(t *testing.T) {
		o := newPlainOAuthFixture(t)
		if r := o.post(body, o.bearer(o.grant())); r.Code != http.StatusOK {
			t.Fatalf("%d %s", r.Code, r.Body)
		}
	})
	t.Run("user_access_token", func(t *testing.T) {
		rows, err := AuthServiceAccessRows()
		if err != nil {
			t.Fatal(err)
		}
		a := newAccessFixtureWith(t, append(rows, accessverify.Row{Service: AccessService, Profile: oauthDecideScope, AcceptsUser: true, LegacyHS256: true}), nil)
		o := newOAuthFixture(t, a.fixture, nil)
		if r := o.post(body, a.signClaims(a.claims())); r.Code != http.StatusOK {
			t.Fatalf("%d %s", r.Code, r.Body)
		}
	})
}

func TestDecideRefusesAMalformedSubmit(t *testing.T) {
	id := uuid.NewString()
	for name, body := range map[string]string{
		"no action":                 "request_id=" + id,
		"another action":            "action=consent&request_id=" + id,
		"no request":                "action=describe",
		"a request not a uuid":      "action=describe&request_id=abc",
		"an upper-case id":          "action=describe&request_id=" + strings.ToUpper(id),
		"a repeated name":           "action=describe&action=approve&request_id=" + id,
		"an unknown name":           "action=describe&request_id=" + id + "&principal_id=" + id,
		"describe with a token":     "action=describe&csrf_token=x&request_id=" + id,
		"approve with no token":     "action=approve&request_id=" + id,
		"approve with another name": "action=approve&csrf_token=t&principal_id=" + id + "&request_id=" + id,
		"deny with no token":        "action=deny&request_id=" + id,
		"a bad escape":              "action=%zz",
	} {
		t.Run(name, func(t *testing.T) {
			o := newPlainOAuthFixture(t)
			if r := o.post(body, o.bearer(o.grant())); r.Code != http.StatusBadRequest {
				t.Fatalf("status %d", r.Code)
			}
			if len(o.store.described)+len(o.store.decisions) != 0 {
				t.Fatal("the store was called")
			}
		})
	}
	for _, media := range []string{"application/json", "text/plain", "multipart/form-data; boundary=x", "not a media type"} {
		t.Run("a valid form as "+media, func(t *testing.T) {
			o := newPlainOAuthFixture(t)
			headers := o.bearer(o.grant())
			headers["Content-Type"] = media
			if r := o.post("action=describe&request_id="+id, headers); r.Code != http.StatusBadRequest {
				t.Fatalf("status %d", r.Code)
			}
		})
	}
	t.Run("a form with a charset", func(t *testing.T) {
		o := newPlainOAuthFixture(t)
		headers := o.bearer(o.grant())
		headers["Content-Type"] = "application/x-www-form-urlencoded; charset=utf-8"
		if r := o.post("action=describe&request_id="+id, headers); r.Code != http.StatusOK {
			t.Fatalf("status %d", r.Code)
		}
	})
	t.Run("no bearer", func(t *testing.T) {
		o := newPlainOAuthFixture(t)
		if r := o.post("action=describe&request_id="+id, nil); r.Code != http.StatusUnauthorized {
			t.Fatalf("status %d", r.Code)
		}
		if len(o.store.described) != 0 {
			t.Fatal("the store was called")
		}
	})
}

// A caller learns nothing of a request it does not hold: one answer for each.
func TestDecideMapsEveryStoreRefusal(t *testing.T) {
	for name, tc := range map[string]struct {
		err    error
		status int
	}{
		"no such request":                {oauthgrant.ErrRequestNotFound, http.StatusBadRequest},
		"another session":                {oauthgrant.ErrOtherSession, http.StatusBadRequest},
		"not bound":                      {oauthgrant.ErrRequestNotBound, http.StatusBadRequest},
		"a bad token":                    {oauthgrant.ErrBadCSRF, http.StatusBadRequest},
		"expired":                        {oauthgrant.ErrRequestExpired, http.StatusBadRequest},
		"decided":                        {oauthgrant.ErrAlreadyDecided, http.StatusBadRequest},
		"an actor":                       {oauthgrant.ErrActorRefused, http.StatusForbidden},
		"delegated":                      {session.ErrWebSessionDelegated, http.StatusForbidden},
		"no organization":                {oauthgrant.ErrNoOrganization, http.StatusForbidden},
		"no organization at the session": {session.ErrWebSessionNoOrganization, http.StatusForbidden},
		"not live":                       {session.ErrWebSessionNotLive, http.StatusUnauthorized},
		"the actor read":                 {oauthgrant.ErrActorUnavailable, http.StatusServiceUnavailable},
		"the store":                      {oauthgrant.ErrStore, http.StatusServiceUnavailable},
		"anything else":                  {errors.New("x"), http.StatusServiceUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			o := newPlainOAuthFixture(t)
			o.store.err = tc.err
			r := o.post("action=approve&csrf_token=t&request_id="+uuid.NewString(), o.bearer(o.grant()))
			if r.Code != tc.status {
				t.Fatalf("status %d, want %d", r.Code, tc.status)
			}
			if strings.Contains(r.Body.String(), "code") && strings.Contains(r.Body.String(), "fcoac_") {
				t.Fatal("a refusal carries a code")
			}
		})
	}
}

// The budget is principal-wide: a second address does not reset it.
func TestDecideChargesThePerPrincipalBudget(t *testing.T) {
	o := newPlainOAuthFixture(t)
	headers := o.bearer(o.grant())
	body := "action=describe&request_id=" + uuid.NewString()
	send := func(address string) int {
		request := httptest.NewRequest(http.MethodPost, PatternOAuthAuthorize, strings.NewReader(body))
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		for k, v := range headers {
			request.Header.Set(k, v)
		}
		request = request.WithContext(clientaddr.With(request.Context(), netip.MustParseAddr(address)))
		response := httptest.NewRecorder()
		o.mux.ServeHTTP(response, request)
		return response.Code
	}
	for i := range oauthPrincipalLimit.Count {
		if got := send(fmt.Sprintf("192.0.2.%d", i+1)); got != http.StatusOK {
			t.Fatalf("request %d: %d", i, got)
		}
	}
	if got := send("198.51.100.1"); got != http.StatusTooManyRequests {
		t.Fatalf("status %d", got)
	}
}

func TestOAuthRoutesMountOnlyWithEveryRowAndTheirBodyPolicies(t *testing.T) {
	f := newFixture(t)
	limits, err := NewLimits(httpapi.NewMemoryCounters(nil), testRateLimitSecret)
	if err != nil {
		t.Fatal(err)
	}
	signer, _, _ := fileSigner(t)
	deps := OAuthDeps{Store: &oauthStoreFake{}, Registry: oauthRegistry(t), Limits: limits, Keys: signer, IssuerURL: oauthIssuer,
		Session: Deps{Sessions: f.sessions, Users: f.users, Issuer: f.issuer}}
	if routes, err := OAuthRoutes(deps); err != nil || routes != nil {
		t.Fatalf("the embedded document mounted %d routes, %v", len(routes), err)
	}
	if routes, err := oauthRoutesRows(deps, bytes.Replace(oauthRows(), []byte(`"method":"POST"`), []byte(`"method":"PUT"`), 1)); err != nil || routes != nil {
		t.Fatalf("a missing row mounted %d routes, %v", len(routes), err)
	}
	noIssuer := deps
	noIssuer.IssuerURL = ""
	if routes, err := oauthRoutesRows(noIssuer, oauthRows()); err != nil || routes != nil {
		t.Fatalf("no issuer mounted %d routes, %v", len(routes), err)
	}
	routes, err := oauthRoutesRows(deps, oauthRows())
	if err != nil || len(routes) != 5 {
		t.Fatalf("%d routes, %v", len(routes), err)
	}
	for _, route := range routes {
		switch route.Method {
		case http.MethodGet:
			if !route.EmptyBody {
				t.Errorf("GET %s has no empty-body policy", route.Pattern)
			}
		case http.MethodPost:
			if route.MaxBodyBytes != 16<<10 || route.EmptyBody {
				t.Errorf("POST %s body policy %d", route.Pattern, route.MaxBodyBytes)
			}
		default:
			t.Errorf("method %s on %s", route.Method, route.Pattern)
		}
	}
	noStore := deps
	noStore.Store = nil
	if _, err := oauthRoutesRows(noStore, oauthRows()); err == nil {
		t.Fatal("no store mounted")
	}
}

func TestTheMetadataDocumentIsTheContractAndComesFromThePinnedIssuer(t *testing.T) {
	o := newPlainOAuthFixture(t)
	request := httptest.NewRequest(http.MethodGet, PatternOAuthMetadata, nil)
	request.Host = "evil.example.test"
	response := httptest.NewRecorder()
	o.mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status %d", response.Code)
	}
	var doc any
	if err := json.Unmarshal(response.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	root, err := contracts.RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	if err := contracts.Validate(root, "oauth-authorization-server.v1", doc); err != nil {
		t.Fatalf("the served document fails its schema: %v", err)
	}
	if strings.Contains(response.Body.String(), "evil") || !strings.Contains(response.Body.String(), oauthIssuer+"/authorize") {
		t.Fatalf("body %s", response.Body)
	}
}

func TestTheIssuerPathDocumentsAreTheRootDocuments(t *testing.T) {
	o := newPlainOAuthFixture(t)
	for _, path := range []string{PatternOAuthDiscovery, PatternOAuthJWKS} {
		response := httptest.NewRecorder()
		o.mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("%s: status %d", path, response.Code)
		}
	}
	response := httptest.NewRecorder()
	o.mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, PatternOAuthDiscovery, nil))
	var discovery struct {
		Issuer  string `json:"issuer"`
		JWKSURI string `json:"jwks_uri"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &discovery); err != nil {
		t.Fatal(err)
	}
	if discovery.Issuer != oauthIssuer || discovery.JWKSURI != oauthIssuer+contracts.DiscoveryJWKSPath {
		t.Fatalf("discovery %+v", discovery)
	}
}
