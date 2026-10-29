package server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

const accessTestIssuer = "https://auth.query.test"

var (
	accessTestNow     = time.Unix(1760000300, 0)
	accessTestSession = uuid.MustParse("44444444-4444-4444-8444-444444444444")
)

// accessMinter signs access-token.v1 tokens with a key of its own.
type accessMinter struct {
	priv ed25519.PrivateKey
	keys authverify.StaticKeys
}

func newAccessMinter(t *testing.T) *accessMinter {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return &accessMinter{priv: priv, keys: authverify.StaticKeys{"query-kid": pub}}
}

func wirePrincipal(id uuid.UUID) string { return "prn_" + base64.RawURLEncoding.EncodeToString(id[:]) }

// mint signs a user access token for sub in ecOrg; extra overrides claims and
// a nil value drops one. header overrides header members the same way.
func (m *accessMinter) mint(sub uuid.UUID, extra, header map[string]any) string {
	claims := map[string]any{
		"iss": accessTestIssuer, "aud": accessTokenAudienceAPI, "sub": wirePrincipal(sub), "principal_type": "human",
		"sid": sessionid.Wire(accessTestSession), "jti": strings.Repeat("j", 24),
		"iat": accessTestNow.Unix() - 60, "nbf": accessTestNow.Unix() - 60, "exp": accessTestNow.Unix() + 600,
		"org_id": ecOrg.String(), "scope": "api:access",
	}
	for name, value := range extra {
		if value == nil {
			delete(claims, name)
			continue
		}
		claims[name] = value
	}
	head := map[string]any{"alg": "EdDSA", "typ": "at+jwt", "kid": "query-kid"}
	for name, value := range header {
		if value == nil {
			delete(head, name)
			continue
		}
		head[name] = value
	}
	headerJSON, _ := json.Marshal(head)
	payload, _ := json.Marshal(claims)
	signing := base64.RawURLEncoding.EncodeToString(headerJSON) + "." + base64.RawURLEncoding.EncodeToString(payload)
	return signing + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(m.priv, []byte(signing)))
}

// accessStore is fakeEdgeStore with the auth-service session reads a user
// access token's sid check makes.
type accessStore struct {
	*fakeEdgeStore
	liveSessions    map[uuid.UUID]policy.SessionRow
	errSessionState error
	accessUserCalls int
}

func (s *accessStore) SessionState(_ context.Context, id uuid.UUID) (policy.SessionRow, bool, error) {
	if s.errSessionState != nil {
		return policy.SessionRow{}, false, s.errSessionState
	}
	row, ok := s.liveSessions[id]
	return row, ok, nil
}

// AccessUser is policy.PGStore's wider read; queryAPIUserStore must never
// reach it.
func (s *accessStore) AccessUser(context.Context, uuid.UUID) (policy.AccessUserRow, bool, error) {
	s.accessUserCalls++
	return policy.AccessUserRow{}, false, errors.New("permission denied for table users")
}

func i64(v int64) *int64 { return &v }

// pgStoreOf is the policy.PGStore inside query-api's users store.
func pgStoreOf(store policy.Store) (policy.PGStore, bool) {
	wrapped, ok := store.(queryAPIUserStore)
	if !ok {
		return policy.PGStore{}, false
	}
	pg, ok := wrapped.queryAPIStoreBase.(policy.PGStore)
	return pg, ok
}

// newAccessStore: ecUser is active at token_version 5, a member of ecOrg, and
// holds the live session accessTestSession pinned at version 5.
func newAccessStore() *accessStore {
	return &accessStore{
		fakeEdgeStore: &fakeEdgeStore{
			states:  map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, TokenVersion: 5}},
			found:   map[uuid.UUID]bool{ecUser: true},
			members: map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true},
		},
		liveSessions: map[uuid.UUID]policy.SessionRow{accessTestSession: {PrincipalID: ecUser, LegacyTokenVersion: i64(5)}},
	}
}

// accessReader collects the validator's counters.
type accessReader struct{ reader *sdkmetric.ManualReader }

// accessSetup is a validator's options with the reader of its counters; nil
// is a pod with no AUTH_ACCESS_TOKEN_ISSUER.
type accessSetup struct {
	opts   accessOptions
	reader *accessReader
}

// testAccess is the setup over keys at the clock now, issued by
// accessTestIssuer; no keys, no setup.
func testAccess(keys authverify.KeySource, denied []string, now time.Time) *accessSetup {
	if keys == nil {
		return nil
	}
	reader := sdkmetric.NewManualReader()
	return &accessSetup{opts: accessOptions{
		issuer: accessTestIssuer, keys: keys, denied: denied,
		now:   func() time.Time { return now },
		meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"),
	}, reader: &accessReader{reader: reader}}
}

// counts is every non-zero counter series: "metric|attr=v,...".
func (a *accessReader) counts(t *testing.T) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := a.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				if dp.Value == 0 {
					continue
				}
				var parts []string
				for _, kv := range dp.Attributes.ToSlice() {
					parts = append(parts, string(kv.Key)+"="+kv.Value.Emit())
				}
				out[m.Name+"|"+strings.Join(parts, ",")] = dp.Value
			}
		}
	}
	return out
}

func TestAccessClassOfSplitsTheBearerClassesByHeader(t *testing.T) {
	m := newAccessMinter(t)
	for name, tc := range map[string]struct {
		token string
		want  principal.AccessClass
	}{
		"user access token":            {m.mint(ecUser, nil, nil), principal.AccessClassUser},
		"edge token":                   {ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), tokenVersion: 5}), principal.AccessClassEdge},
		"envelope typ JWT":             {m.mint(ecUser, nil, map[string]any{"typ": "JWT"}), principal.AccessClassOther},
		"envelope without typ":         {m.mint(ecUser, nil, map[string]any{"typ": nil}), principal.AccessClassOther},
		"typ in upper case":            {m.mint(ecUser, nil, map[string]any{"typ": "AT+JWT"}), principal.AccessClassOther},
		"at+jwt under another alg":     {m.mint(ecUser, nil, map[string]any{"alg": "none"}), principal.AccessClassOther},
		"two segments":                 {"eyJhbGciOiJFZERTQSIsInR5cCI6ImF0K2p3dCJ9.e30", principal.AccessClassOther},
		"header is not JSON":           {"bm90LWpzb24.e30.c2ln", principal.AccessClassOther},
		"empty":                        {"", principal.AccessClassOther},
		"typ at+jwt with a non-string": {m.mint(ecUser, nil, map[string]any{"typ": 7}), principal.AccessClassOther},
	} {
		if got := principal.AccessClassOf(tc.token); got != tc.want {
			t.Errorf("%s: AccessClassOf = %d, want %d", name, got, tc.want)
		}
	}
}

// T1: the validator's table is every row of the query and internal listeners,
// and the MCP-only POST /query row is left out (it would collide).
func TestQueryAPIAccessRowsAreTheQueryAndInternalListenerInventory(t *testing.T) {
	rows, err := queryAPIAccessRows()
	if err != nil {
		t.Fatal(err)
	}
	byProfile := map[string]accessverify.Row{}
	for _, row := range rows {
		if row.Service != queryAPIAccessService {
			t.Errorf("row %s has service %q", row.Profile, row.Service)
		}
		byProfile[row.Profile] = row
	}
	want := map[string]struct{ user, legacy bool }{
		"GET /api/v1/home":                      {true, true},
		"POST /api/v1/home":                     {true, true},
		"POST /query":                           {true, true},
		"POST /query/proof":                     {true, true},
		"POST /query/proof-write":               {true, true},
		"POST /query/run-operation":             {true, true},
		"GET /graphql":                          {true, true},
		"POST /graphql":                         {true, true},
		"GET /buildinfo":                        {false, false},
		"GET /api/v1/meta":                      {false, false},
		"GET /metrics":                          {false, false},
		"POST /api/v1/sankey":                   {true, true},
		"GET /api/v1/people/{person_id}/metric": {true, true},
	}
	for profile, flags := range want {
		row, ok := byProfile[profile]
		if !ok {
			t.Errorf("no row for %s", profile)
			continue
		}
		if row.AcceptsUser != flags.user || row.LegacyHS256 != flags.legacy {
			t.Errorf("%s: user %v legacy %v, want %v %v", profile, row.AcceptsUser, row.LegacyHS256, flags.user, flags.legacy)
		}
	}
	for _, operatorOnly := range []string{"GET /healthz", "GET /readyz"} {
		if _, ok := byProfile[operatorOnly]; ok {
			t.Errorf("%s is an operator-listener row; the validator must not hold it", operatorOnly)
		}
	}
	users := 0
	for _, row := range rows {
		if row.AcceptsUser {
			users++
			if !row.LegacyHS256 {
				t.Errorf("%s accepts the user token without the HS256 opt-in: no query-api row is EdDSA-only yet", row.Profile)
			}
		}
	}
	if users != 35 {
		t.Errorf("%d rows accept user_access_token, want 35", users)
	}
}

func TestQueryAPIAccessRowsRefuseABadTable(t *testing.T) {
	row := func(id, method, route string) map[string]any {
		return map[string]any{"id": id, "service": queryAPIAccessService, "method": method, "route": route}
	}
	for name, rows := range map[string][]map[string]any{
		"no row":            nil,
		"two rows":          {row("POST /x [s @query,internal]", "POST", "/x"), row("POST /x [s @internal]", "POST", "/x")},
		"no listener":       {row("POST /x", "POST", "/x")},
		"no method":         {row("POST /x [s @query]", "", "/x")},
		"only mcp rows":     {row("POST /query [s @mcp]", "POST", "/query")},
		"only operator row": {row("GET /healthz [s @operator]", "GET", "/healthz")},
	} {
		raw, _ := json.Marshal(map[string]any{"rows": rows})
		if _, err := parseQueryAPIAccessRows(raw); err == nil {
			t.Errorf("%s: the table was accepted", name)
		}
	}
	if _, err := parseQueryAPIAccessRows([]byte("{")); err == nil {
		t.Error("malformed JSON was accepted")
	}
}

// T10: the profile a request carries (method + mux pattern) is a row key of
// the validator for every route that reads a bearer, as the real muxes route
// it; an unknown method is no row, never another route's row.
func TestQueryAPIRouteProfileIsMethodAndPattern(t *testing.T) {
	rows, err := queryAPIAccessRows()
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]accessverify.Row{}
	for _, row := range rows {
		keys[row.Profile] = row
	}
	type probe struct{ method, pattern, path string }
	var probes []probe
	for _, route := range RESTRoutes() {
		path := strings.NewReplacer("{person_id}", "p1", "{work_unit_id}", "w1").Replace(route.Pattern)
		probes = append(probes, probe{route.Method, route.Pattern, path})
	}
	for _, p := range []string{"/query", "/query/proof", "/query/proof-write", "/query/run-operation"} {
		probes = append(probes, probe{http.MethodPost, p, p})
	}
	probes = append(probes, probe{http.MethodGet, graphQLEdgePath, graphQLEdgePath}, probe{http.MethodPost, graphQLEdgePath, graphQLEdgePath})
	if len(probes) < 30 {
		t.Fatalf("%d probes: RESTRoutes no longer lists the REST routes", len(probes))
	}
	for _, p := range probes {
		var seen string
		mux := http.NewServeMux()
		mux.Handle(p.pattern, withRouteProfile(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			seen = policy.RouteProfileFrom(r.Context())
		})))
		mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(p.method, p.path, nil))
		row, ok := keys[seen]
		if !ok || row.AcceptsUser != row.LegacyHS256 {
			t.Errorf("%s %s: profile %q is not a row, or a row that takes the HS256 token but not the user token", p.method, p.path, seen)
		}
	}
	var seen string
	mux := http.NewServeMux()
	mux.Handle("/api/v1/home", withRouteProfile(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = policy.RouteProfileFrom(r.Context())
	})))
	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPut, "/api/v1/home", nil))
	if _, ok := keys[seen]; ok || seen != "PUT /api/v1/home" {
		t.Errorf("PUT /api/v1/home carries profile %q; want the method-and-pattern key that no row holds", seen)
	}
}

// T6: on a row without the HS256 opt-in the legacy verifier never runs, and
// the user token is refused where the row does not accept it.
func TestQueryAPIHS256OnARowWithoutLegacyNeverRunsTheLegacyVerifier(t *testing.T) {
	m := newAccessMinter(t)
	setup := testAccess(m.keys, nil, accessTestNow)
	reader := setup.reader
	calls := 0
	validator, err := newQueryAccessValidator(setup.opts, func(context.Context, string) (any, error) {
		calls++
		return &policy.User{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	edge := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), tokenVersion: 5})
	for _, profile := range []string{"GET /buildinfo", "GET /api/v1/meta", "GET /metrics", "PUT /api/v1/home"} {
		if _, err := validator.Validate(context.Background(), queryAPIAccessService, profile, edge); !errors.Is(err, accessverify.ErrRefused) {
			t.Errorf("%s: HS256 err = %v, want a refusal", profile, err)
		}
		if _, err := validator.Validate(context.Background(), queryAPIAccessService, profile, m.mint(ecUser, nil, nil)); !errors.Is(err, accessverify.ErrRefused) {
			t.Errorf("%s: user token err = %v, want a refusal", profile, err)
		}
	}
	if calls != 0 {
		t.Fatalf("the legacy verifier ran %d times on rows without the opt-in", calls)
	}
	got := reader.counts(t)
	for _, want := range []string{
		accessverify.RefusedMetric + "|profile=GET /buildinfo,reason=alg_not_allowed,service=dev-health-query-api",
		accessverify.RefusedMetric + "|profile=unknown,reason=no_profile,service=unknown",
	} {
		if got[want] == 0 {
			t.Errorf("no count for %s in %v", want, got)
		}
	}
}

// T2 (prod construction): buildEdgeVerifierFromEnv and
// buildQueryEdgeAuthenticatorFromEnv are what every REST route builder and
// the /query and /graphql builders call (TestEdgeUserStoreIsBuiltOnceAndPassedToEveryEdgeRoute,
// Build). Only AUTH_ACCESS_TOKEN_ISSUER varies.
func TestBuildersRouteByRowOnlyWithTheIssuerSet(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://nobody:none@127.0.0.1:1/none?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	users := newQueryAPIUserStore(policy.PGStore{Pool: pool})
	for name, tc := range map[string]struct {
		env    map[string]string
		built  bool
		routed bool
	}{
		"secret only":       {map[string]string{edgeJWTSecretEnvVar: edgeTestSecret}, true, false},
		"secret and issuer": {map[string]string{edgeJWTSecretEnvVar: edgeTestSecret, accessTokenIssuerEnvVar: accessTestIssuer}, true, true},
		"issuer only":       {map[string]string{accessTokenIssuerEnvVar: accessTestIssuer}, true, true},
		"issuer blank":      {map[string]string{accessTokenIssuerEnvVar: "  "}, false, false},
		"neither":           {nil, false, false},
	} {
		t.Run(name, func(t *testing.T) {
			settings := edgeSettings(tc.env)
			edge, err := buildEdgeVerifierFromEnv(settings, users)
			if err != nil || (edge != nil) != tc.built || edge.Routed() != tc.routed {
				t.Errorf("REST: verifier %v routed %v err %v; want built %v routed %v", edge != nil, edge.Routed(), err, tc.built, tc.routed)
			}
			auth, store, err := buildQueryEdgeAuthenticatorFromEnv(settings, pool)
			if err != nil || (auth != nil) != tc.built || (auth != nil && auth.Routed() != tc.routed) {
				t.Errorf("/query: authenticator %v err %v; want built %v routed %v", auth != nil, err, tc.built, tc.routed)
			}
			if tc.built {
				if _, ok := store.(queryAPIUserStore); !ok {
					t.Errorf("/query store is %T, want queryAPIUserStore", store)
				}
			}
			userStore, err := newEdgeUserStore(settings, pool)
			if err != nil || (userStore != nil) != tc.built {
				t.Errorf("users store %v err %v; want built %v", userStore != nil, err, tc.built)
			}
		})
	}
	for name, issuer := range map[string]string{"http issuer": "http://auth.query.test", "not a url": "auth"} {
		settings := edgeSettings(map[string]string{edgeJWTSecretEnvVar: edgeTestSecret, accessTokenIssuerEnvVar: issuer})
		if _, err := buildEdgeVerifierFromEnv(settings, users); err == nil {
			t.Errorf("%s: the REST verifier was built", name)
		}
		if _, _, err := buildQueryEdgeAuthenticatorFromEnv(settings, pool); err == nil {
			t.Errorf("%s: the /query authenticator was built", name)
		}
	}
}

// T9: query-api's users read of a user access token is UserState, the
// columns the query-api role is granted; the wider policy.PGStore AccessUser
// is never called.
func TestQueryAPIAccessUserReadsOnlyGrantedColumns(t *testing.T) {
	base := newAccessStore()
	store := queryAPIUserStore{queryAPIStoreBase: base}
	row, found, err := store.AccessUser(context.Background(), ecUser)
	if err != nil || !found || !row.State.IsActive || row.State.TokenVersion != 5 {
		t.Fatalf("AccessUser = %+v %v %v", row, found, err)
	}
	if row.Email != "" || row.Username != nil || row.FullName != nil {
		t.Fatalf("AccessUser read profile columns: %+v", row)
	}
	if _, found, err := store.AccessUser(context.Background(), ecTarget); err != nil || found {
		t.Fatalf("unknown user: found %v err %v", found, err)
	}
	base.errUserState = policy.ErrUnavailable
	if _, _, err := store.AccessUser(context.Background(), ecUser); !errors.Is(err, policy.ErrUnavailable) {
		t.Fatalf("a failed read is %v, want the store's error", err)
	}
	if base.accessUserCalls != 0 {
		t.Fatalf("the wrapped store's AccessUser ran %d times", base.accessUserCalls)
	}
	var _ policy.AccessUserStore = store
}
