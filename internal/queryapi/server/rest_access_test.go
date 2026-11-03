package server

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// restFixture is one REST route (GET and POST /api/v1/home, the real mux
// pattern) behind authenticateRESTRequest, with the envelope verifier and the
// edge verifier as the route builders make them; routed when keys is set.
type restFixture struct {
	handler  http.Handler
	reader   *accessReader
	envelope string
}

func newRESTFixture(t *testing.T, store *accessStore, setup *accessSetup, legacyOn bool) *restFixture {
	t.Helper()
	verifier, priv := iaVerifier(t)
	edge, err := principal.NewEdgeVerifier(ecSecret, ecIssuer, ecAudience, newQueryAPIUserStoreFrom(store))
	if err != nil {
		t.Fatal(err)
	}
	f := &restFixture{envelope: iaEnvelope(t, priv, principal.Claims{OrgID: ecOrg.String(), Role: "admin"})}
	if setup != nil {
		f.reader = setup.reader
		if err := routeEdgeVerifier(edge, setup.opts, legacyOn); err != nil {
			t.Fatal(err)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/home", func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authenticateRESTRequest(w, r, verifier, edge, "home")
		if !ok {
			return
		}
		r = r.WithContext(authctx.WithClaims(r.Context(), claims))
		switch r.Method {
		case http.MethodGet, http.MethodPost:
			_, _ = fmt.Fprintf(w, `{"org":%q}`, claims.OrgID)
		default:
			writeRESTMethodNotAllowed(w, r, "home")
		}
	})
	f.handler = mux
	return f
}

// newQueryAPIUserStoreFrom wraps a test store the way newQueryAPIUserStore
// wraps policy.PGStore.
func newQueryAPIUserStoreFrom(store *accessStore) queryAPIUserStore {
	return queryAPIUserStore{queryAPIStoreBase: store}
}

type answer struct {
	Status int    `json:"status"`
	Body   string `json:"body"`
}

func (f *restFixture) do(method, bearer string) answer {
	req := httptest.NewRequest(method, "/api/v1/home", nil)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)
	body, _ := io.ReadAll(rec.Result().Body)
	return answer{rec.Code, strings.TrimSpace(string(body))}
}

const (
	restRefusal     = `{"detail":{"message":"Invalid or expired token"}}`
	restUnavailable = `{"detail":{"message":"Database temporarily unavailable"}}`
)

func quietStdLog(t *testing.T) {
	t.Helper()
	previous := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(previous) })
}

func TestQueryAPIRESTUserTokenPath(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	member := m.mint(ecUser, nil, nil)
	for name, tc := range map[string]struct {
		store  func(*accessStore)
		token  string
		status int
		body   string
	}{
		"member":                  {nil, member, http.StatusOK, `{"org":"` + ecOrg.String() + `"}`},
		"not a member of the org": {func(s *accessStore) { s.members = nil }, member, http.StatusUnauthorized, restRefusal},
		"inactive user":           {func(s *accessStore) { s.states[ecUser] = policy.UserState{TokenVersion: 5} }, member, http.StatusUnauthorized, restRefusal},
		"unknown user":            {func(s *accessStore) { s.found = map[uuid.UUID]bool{} }, member, http.StatusUnauthorized, restRefusal},
		"revoked session": {func(s *accessStore) {
			s.liveSessions[accessTestSession] = policy.SessionRow{PrincipalID: ecUser, Revoked: true, LegacyTokenVersion: i64(5)}
		}, member, http.StatusUnauthorized, restRefusal},
		"session of another user": {func(s *accessStore) {
			s.liveSessions[accessTestSession] = policy.SessionRow{PrincipalID: ecTarget, LegacyTokenVersion: i64(5)}
		}, member, http.StatusUnauthorized, restRefusal},
		"session predates password": {func(s *accessStore) {
			s.liveSessions[accessTestSession] = policy.SessionRow{PrincipalID: ecUser, LegacyTokenVersion: i64(4)}
		}, member, http.StatusUnauthorized, restRefusal},
		"no session":            {func(s *accessStore) { s.liveSessions = nil }, member, http.StatusUnauthorized, restRefusal},
		"other audience":        {nil, m.mint(ecUser, map[string]any{"aud": "auth-service", "scope": "auth:session"}, nil), http.StatusUnauthorized, restRefusal},
		"other issuer":          {nil, m.mint(ecUser, map[string]any{"iss": "https://other.query.test"}, nil), http.StatusUnauthorized, restRefusal},
		"expired":               {nil, m.mint(ecUser, map[string]any{"exp": accessTestNow.Unix() - 1}, nil), http.StatusUnauthorized, restRefusal},
		"unknown kid":           {nil, m.mint(ecUser, nil, map[string]any{"kid": "other"}), http.StatusUnauthorized, restRefusal},
		"users store down":      {func(s *accessStore) { s.errUserState = fmt.Errorf("down: %w", policy.ErrUnavailable) }, member, http.StatusServiceUnavailable, restUnavailable},
		"session store down":    {func(s *accessStore) { s.errSessionState = fmt.Errorf("down: %w", policy.ErrUnavailable) }, member, http.StatusServiceUnavailable, restUnavailable},
		"membership store down": {func(s *accessStore) { s.errIsMember = fmt.Errorf("down: %w", policy.ErrUnavailable) }, member, http.StatusServiceUnavailable, restUnavailable},
		"users store error":     {func(s *accessStore) { s.errUserState = errors.New("syntax error") }, member, http.StatusInternalServerError, `{"detail":"Internal Server Error"}`},
	} {
		t.Run(name, func(t *testing.T) {
			store := newAccessStore()
			if tc.store != nil {
				tc.store(store)
			}
			f := newRESTFixture(t, store, testAccess(m.keys, nil, accessTestNow), true)
			got := f.do(http.MethodGet, tc.token)
			if got.Status != tc.status || got.Body != tc.body {
				t.Fatalf("got %d %s, want %d %s", got.Status, got.Body, tc.status, tc.body)
			}
		})
	}
}

// T8: the organization rule applies to the user token as to the edge token.
func TestQueryAPIRESTOrgMissingIs403ForNonSuperuser(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	orgless := m.mint(ecUser, map[string]any{"org_id": nil}, nil)
	edgeOrgless := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), tokenVersion: 5})
	forbidden := answer{http.StatusForbidden, `{"detail":"Organization context required"}`}
	f := newRESTFixture(t, newAccessStore(), testAccess(m.keys, nil, accessTestNow), true)
	for name, token := range map[string]string{"user token": orgless, "edge token": edgeOrgless} {
		if got := f.do(http.MethodGet, token); got != forbidden {
			t.Errorf("%s with no org: got %+v, want %+v", name, got, forbidden)
		}
	}
	super := newAccessStore()
	super.states[ecUser] = policy.UserState{IsActive: true, IsSuperuser: true, TokenVersion: 5}
	f = newRESTFixture(t, super, testAccess(m.keys, nil, accessTestNow), true)
	for name, token := range map[string]string{"user token": orgless, "edge token": edgeOrgless} {
		if got := f.do(http.MethodGet, token); got.Status != http.StatusOK || got.Body != `{"org":""}` {
			t.Errorf("superuser %s with no org: got %+v, want 200 with no org", name, got)
		}
	}
}

// T5: with the issuer set, the envelope still goes to the envelope verifier,
// and the profile-routed validator counts nothing for it.
func TestQueryAPIRESTEnvelopeStillReachesTheEnvelopeVerifierWhenIssuerSet(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	f := newRESTFixture(t, newAccessStore(), testAccess(m.keys, nil, accessTestNow), true)
	if got := f.do(http.MethodGet, f.envelope); got.Status != http.StatusOK || got.Body != `{"org":"`+ecOrg.String()+`"}` {
		t.Fatalf("envelope: got %+v, want 200", got)
	}
	if got := f.reader.counts(t); len(got) != 0 {
		t.Fatalf("the envelope reached the access-token validator: %v", got)
	}
	if got := f.do(http.MethodGet, m.mint(ecUser, nil, map[string]any{"typ": "JWT"})); got.Status != http.StatusUnauthorized {
		t.Fatalf("an EdDSA token typed as the envelope but signed by another key: got %+v, want 401 from the envelope verifier", got)
	}
	if got := f.reader.counts(t); len(got) != 0 {
		t.Fatalf("a typ JWT token reached the access-token validator: %v", got)
	}
}

// The counters see one accepted user token and one accepted edge token on the
// route template, the D2 telemetry that proves when HS256 reaches zero.
func TestQueryAPIRESTCountsEachClassOnTheRouteTemplate(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	f := newRESTFixture(t, newAccessStore(), testAccess(m.keys, nil, accessTestNow), true)
	if got := f.do(http.MethodPost, m.mint(ecUser, nil, nil)); got.Status != http.StatusOK {
		t.Fatalf("user token: %+v", got)
	}
	if got := f.do(http.MethodGet, ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), tokenVersion: 5})); got.Status != http.StatusOK {
		t.Fatalf("edge token: %+v", got)
	}
	want := map[string]int64{
		accessverify.AcceptedMetric + "|class=user_access_token,profile=POST /api/v1/home,service=dev-health-query-api":     1,
		accessverify.AcceptedMetric + "|class=ops_access_token_hs256,profile=GET /api/v1/home,service=dev-health-query-api": 1,
	}
	got := f.reader.counts(t)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("counters %v, want %v", got, want)
	}
}

// T7: a JWKS that cannot be read decides nothing: 503, never a 401; an
// unknown kid with a readable JWKS stays 401.
func TestQueryAPIRESTUserTokenJWKSOutageAnswers503(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	f := newRESTFixture(t, newAccessStore(), testAccess(downKeys{}, nil, accessTestNow), true)
	if got := f.do(http.MethodGet, m.mint(ecUser, nil, nil)); got.Status != http.StatusServiceUnavailable {
		t.Fatalf("JWKS outage: got %+v, want 503", got)
	}
	f = newRESTFixture(t, newAccessStore(), testAccess(m.keys, nil, accessTestNow), true)
	if got := f.do(http.MethodGet, m.mint(ecUser, nil, map[string]any{"kid": "rotated-away"})); got.Status != http.StatusUnauthorized || got.Body != restRefusal {
		t.Fatalf("unknown kid: got %+v, want 401", got)
	}
}

// downKeys is a key source whose JWKS cannot be read.
type downKeys struct{}

func (downKeys) PublicKey(string) (ed25519.PublicKey, bool) { return nil, false }
func (downKeys) ResolveKey(string) (ed25519.PublicKey, error) {
	return nil, authverify.ErrKeySourceUnavailable
}

// ecMintEdgeTokenRaw signs an edge token with claims as given (iss, aud and
// exp added), so a claim can carry a JSON type a real minter never writes.
func ecMintEdgeTokenRaw(t *testing.T, claims map[string]any) string {
	t.Helper()
	all := jwt.MapClaims{"iss": ecIssuer, "aud": ecAudience, "exp": time.Now().Add(time.Hour).Unix()}
	for name, value := range claims {
		all[name] = value
	}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, all).SignedString([]byte(ecSecret))
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

// Q5: the issuer alone builds the verifier; its validator has no HS256
// verifier, so the edge token is refused the same 401 an unconfigured pod
// gave it, and the user token is served.
func TestQueryAPIRESTIssuerAloneServesOnlyTheUserToken(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	f := newRESTFixture(t, newAccessStore(), testAccess(m.keys, nil, accessTestNow), false)
	if got := f.do(http.MethodGet, m.mint(ecUser, nil, nil)); got.Status != http.StatusOK {
		t.Fatalf("user token: %+v", got)
	}
	if got := f.do(http.MethodGet, ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), tokenVersion: 5})); got.Status != http.StatusUnauthorized || got.Body != restRefusal {
		t.Fatalf("edge token with no edge secret: got %+v, want 401", got)
	}
	if got := f.reader.counts(t)[accessverify.RefusedMetric+"|profile=GET /api/v1/home,reason=not_configured,service=dev-health-query-api"]; got != 1 {
		t.Fatalf("the HS256 refusal is not counted not_configured: %d", got)
	}
}

// Q8 (acp, as C3 D3/D6): the answers that change with the issuer set, each
// pinned against the issuer-unset answer of the same request.
func TestQueryAPIRESTIssuerSetDrift(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	edge := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), tokenVersion: 5})
	badSub := ecMintEdgeTokenRaw(t, map[string]any{"sub": 7, "org_id": ecOrg.String(), "tv": 5, "type": "access"})
	for name, tc := range map[string]struct {
		store      func(*accessStore)
		method     string
		token      string
		unset, set answer
	}{
		"edge token, users store error that is not unavailable": {
			func(s *accessStore) { s.errUserState = errors.New("syntax error") }, http.MethodGet, edge,
			answer{http.StatusInternalServerError, `{"detail":"Internal Server Error"}`},
			answer{http.StatusServiceUnavailable, restUnavailable}},
		"edge token whose sub is not a string": {
			nil, http.MethodGet, badSub,
			answer{http.StatusInternalServerError, `{"detail":"Internal Server Error"}`},
			answer{http.StatusUnauthorized, restRefusal}},
		"edge token on a method the route does not serve": {
			nil, http.MethodPut, edge,
			answer{http.StatusMethodNotAllowed, `{"detail":"Method Not Allowed"}`},
			answer{http.StatusUnauthorized, restRefusal}},
	} {
		t.Run(name, func(t *testing.T) {
			for _, routed := range []bool{false, true} {
				store := newAccessStore()
				if tc.store != nil {
					tc.store(store)
				}
				var keys authverify.KeySource
				want := tc.unset
				if routed {
					keys, want = m.keys, tc.set
				}
				got := newRESTFixture(t, store, testAccess(keys, nil, accessTestNow), true).do(tc.method, tc.token)
				if got != want {
					t.Errorf("issuer set %v: got %+v, want %+v", routed, got, want)
				}
			}
		})
	}
}
