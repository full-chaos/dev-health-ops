package apiservice

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

// AccessUser makes probeStore the EdDSA user path's store too.
func (s *probeStore) AccessUser(_ context.Context, id uuid.UUID) (policy.AccessUserRow, bool, error) {
	if s.down[id] {
		return policy.AccessUserRow{}, false, fmt.Errorf("probe store: %w", policy.ErrUnavailable)
	}
	row, ok := s.users[id]
	return policy.AccessUserRow{State: row.state, Email: row.email, Username: row.username, FullName: row.fullName}, ok, nil
}

// routedFixture is the api listener with the profile-routed validator over
// rows, an EdDSA verifier over keys, and the real HS256 legacy path.
type routedFixture struct {
	handler http.Handler
	reader  *sdkmetric.ManualReader
	auth    *policy.Authenticator
}

func newRoutedFixture(t *testing.T, store policy.Store, rows []accessverify.Row, issuer string, keys authverify.KeySource, denied []string, now time.Time) *routedFixture {
	t.Helper()
	f := &routedFixture{reader: sdkmetric.NewManualReader()}
	f.handler = probeServer(t, store, func(auth *policy.Authenticator) {
		f.auth = auth
		cfg := accessverify.Config{
			Rows:   rows,
			Legacy: legacyVerifier(auth.AuthenticateLegacy),
			Meter:  sdkmetric.NewMeterProvider(sdkmetric.WithReader(f.reader)).Meter("test"),
			Logger: quietLogger(),
			Now:    func() time.Time { return now },
		}
		if keys != nil {
			verifier, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: issuer, Audience: accessTokenAudienceAPI, Keys: keys, DeniedKids: denied})
			if err != nil {
				t.Fatal(err)
			}
			cfg.EdDSA = verifier
		}
		validator, err := accessverify.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := auth.RouteAccess(routedAccessVerify{validator}, goAPIAccessService); err != nil {
			t.Fatal(err)
		}
	})
	return f
}

func (f *routedFixture) counters(t *testing.T) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := f.reader.Collect(context.Background(), &rm); err != nil {
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

func realRows(t *testing.T) []accessverify.Row {
	t.Helper()
	rows, err := goAPIAccessRows()
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// userRows is the real table with AcceptsUser turned on for the probe
// routes: no go-api row accepts user_access_token yet (the rows are widened
// in a later leaf), so the user path is proved on a test table.
func userRows(t *testing.T) []accessverify.Row {
	t.Helper()
	rows := realRows(t)
	on := map[string]bool{}
	for _, path := range []string{probeAuthenticated, probeAdmin, probeOptional, probeSuperuser} {
		on["GET "+path] = true
	}
	for index := range rows {
		if on[rows[index].Profile] {
			rows[index].AcceptsUser = true
		}
	}
	return rows
}

const probeIssuer = "https://auth.probe.test"

var probeNow = time.Unix(1760000300, 0)

type eddsaMinter struct {
	priv ed25519.PrivateKey
	keys authverify.StaticKeys
}

func newEdDSAMinter(t *testing.T) *eddsaMinter {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return &eddsaMinter{priv: priv, keys: authverify.StaticKeys{"probe-kid": pub}}
}

func wirePrincipal(id uuid.UUID) string { return "prn_" + base64.RawURLEncoding.EncodeToString(id[:]) }

// mint signs an access-token.v1 token for sub; extra overrides claims and a
// nil value drops one.
func (m *eddsaMinter) mint(sub uuid.UUID, extra map[string]any) string {
	claims := map[string]any{
		"iss": probeIssuer, "aud": accessTokenAudienceAPI, "sub": wirePrincipal(sub), "principal_type": "human",
		"sid": sessionid.Wire(probeLiveSID), "jti": strings.Repeat("j", 24),
		"iat": probeNow.Unix() - 60, "nbf": probeNow.Unix() - 60, "exp": probeNow.Unix() + 600,
		"org_id": probeOrg.String(), "scope": "api:access",
	}
	for name, value := range extra {
		if value == nil {
			delete(claims, name)
			continue
		}
		claims[name] = value
	}
	header, _ := json.Marshal(map[string]any{"alg": "EdDSA", "typ": "at+jwt", "kid": "probe-kid"})
	payload, _ := json.Marshal(claims)
	signing := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	return signing + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(m.priv, []byte(signing)))
}

const genericRefusal = `{"detail":{"message":"Invalid or expired token"}}`

// issuerSetDrift is the accepted drift with the issuer set (acp D3, D6): the
// scope middlewares decide with the route's row, and a legacy token whose sub
// has the wrong type is a refusal.
var issuerSetDrift = map[string]struct {
	status int
	body   string
}{
	"hs256 sub not a string":                   {http.StatusUnauthorized, genericRefusal},
	"public hs256 valid":                       {http.StatusOK, `{"org":""}` + "\n"},
	"public hs256 x-org-id not a member":       {http.StatusOK, `{"org":""}` + "\n"},
	"unknown path hs256 x-org-id not a member": {http.StatusNotFound, `{"detail":"Not Found"}`},
}

// TestIssuerSetLegacyAnswersMatchTheGoldenExceptTheRuledDrift: with the
// issuer set and the FULL real row table, every HS256 answer of the fixed set
// is the issuer-unset answer, except the drift acp ruled (D3, D6).
func TestIssuerSetLegacyAnswersMatchTheGoldenExceptTheRuledDrift(t *testing.T) {
	want, err := os.ReadFile(issuerUnsetGolden)
	if err != nil {
		t.Fatal(err)
	}
	var golden []probeAnswer
	if err := json.Unmarshal(want, &golden); err != nil {
		t.Fatal(err)
	}
	byName := map[string]probeAnswer{}
	for _, answer := range golden {
		byName[answer.Name] = answer
	}
	f := newRoutedFixture(t, newProbeStore(), realRows(t), probeIssuer, newEdDSAMinter(t).keys, nil, probeNow)
	for _, got := range probeAnswers(f.handler, issuerUnsetRequests(t)) {
		if drift, ok := issuerSetDrift[got.Name]; ok {
			if got.Status != drift.status || got.Body != drift.body {
				t.Errorf("%s: %d %s, want the ruled drift %d %s", got.Name, got.Status, got.Body, drift.status, drift.body)
			}
			continue
		}
		a, _ := json.Marshal(got)
		b, _ := json.Marshal(byName[got.Name])
		if !bytes.Equal(a, b) {
			t.Errorf("%s:\n got %s\nwant %s", got.Name, a, b)
		}
	}
	counters := f.counters(t)
	if counters[accessverify.AcceptedMetric+"|class=ops_access_token_hs256,profile=GET "+probeAuthenticated+",service="+goAPIAccessService] == 0 {
		t.Error("no legacy acceptance counted on the authenticated probe row")
	}
	if counters[accessverify.RefusedMetric+"|profile=GET "+probePublic+",reason=alg_not_allowed,service="+goAPIAccessService] == 0 {
		t.Error("an HS256 token on a row without legacy_hs256 was not refused alg_not_allowed")
	}
	if counters[accessverify.RefusedMetric+"|profile=unknown,reason=no_profile,service=unknown"] == 0 {
		t.Error("an unknown path was not refused no_profile")
	}
}

// userReadCounter counts users-row reads, so a test can prove the legacy
// verifier never ran.
type userReadCounter struct {
	*probeStore
	userReads int
}

func (c *userReadCounter) UserState(ctx context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	c.userReads++
	return c.probeStore.UserState(ctx, id)
}

// TestIssuerSetHS256OnARowWithoutLegacyNeverRunsTheLegacyVerifier: the row
// decides before any validator runs; the HS256 header alone never selects
// the legacy path.
func TestIssuerSetHS256OnARowWithoutLegacyNeverRunsTheLegacyVerifier(t *testing.T) {
	store := &userReadCounter{probeStore: newProbeStore()}
	f := newRoutedFixture(t, store, realRows(t), probeIssuer, nil, nil, probeNow)
	token := probeHS256(t, probeKey, map[string]any{"sub": probeUser.String(), "org_id": probeOrg.String(), "tv": 3})
	got := probeDo(f.handler, probeRequest{"public", http.MethodGet, probePublic, bearer(token)})
	if got.Status != http.StatusOK || store.userReads != 0 {
		t.Fatalf("%d %s, users-row reads %d (want 0)", got.Status, got.Body, store.userReads)
	}
	got = probeDo(f.handler, probeRequest{"unknown", http.MethodGet, "/api/v1/nowhere", bearer(token)})
	if got.Status != http.StatusNotFound || store.userReads != 0 {
		t.Fatalf("%d %s, users-row reads %d (want 0)", got.Status, got.Body, store.userReads)
	}
}

type userCase struct {
	name    string
	path    string
	token   string
	headers map[string]string
	status  int
	check   func(t *testing.T, body map[string]any)
}

func decodeEcho(t *testing.T, raw string) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("not an echo body: %s", raw)
	}
	return out
}

// TestIssuerSetUserTokenPath is the EdDSA user path through the real api
// listener: the users row, the sid and the per-request membership role (Q4).
func TestIssuerSetUserTokenPath(t *testing.T) {
	minter := newEdDSAMinter(t)
	store := newProbeStore()
	adminSID := uuid.MustParse("00000000-0000-4000-8000-0000000000c4")
	store.sessions[adminSID] = policy.SessionRow{PrincipalID: probeAdminUser}
	memberSID := uuid.MustParse("00000000-0000-4000-8000-0000000000c5")
	store.sessions[memberSID] = policy.SessionRow{PrincipalID: probeMember}
	inactiveSID := uuid.MustParse("00000000-0000-4000-8000-0000000000c6")
	store.sessions[inactiveSID] = policy.SessionRow{PrincipalID: probeInactive}
	f := newRoutedFixture(t, store, userRows(t), probeIssuer, minter.keys, nil, probeNow)
	user := func(extra map[string]any) string { return minter.mint(probeUser, extra) }
	userWith := func(body map[string]any) map[string]any { u, _ := body["user"].(map[string]any); return u }
	cases := []userCase{
		{name: "owner-level role from the membership row", path: probeAdmin, token: user(nil), status: http.StatusOK, check: func(t *testing.T, body map[string]any) {
			u := userWith(body)
			if u["role"] != "admin" || u["org_id"] != probeOrg.String() || u["email"] != "user@example.test" || u["username"] != "user" || u["full_name"] != "Probe User" || u["token_version"] != float64(3) || u["user_id"] != probeUser.String() || body["org"] != probeOrg.String() {
				t.Errorf("identity %v", body)
			}
		}},
		{name: "member role at an admin route", path: probeAdmin, token: minter.mint(probeMember, map[string]any{"sid": sessionid.Wire(memberSID)}), status: http.StatusForbidden},
		{name: "org claim without a membership", path: probeAuthenticated, token: user(map[string]any{"org_id": probeTargetOrg.String()}), status: http.StatusUnauthorized},
		{name: "no org claim at an orgless route", path: probeAuthenticated, token: user(map[string]any{"org_id": nil}), status: http.StatusOK, check: func(t *testing.T, body map[string]any) {
			if u := userWith(body); u["org_id"] != "" || u["role"] != "" || body["org"] != "" {
				t.Errorf("identity %v", body)
			}
		}},
		{name: "no org claim at an org route", path: probeAdmin, token: user(map[string]any{"org_id": nil}), status: http.StatusForbidden},
		{name: "inactive user", path: probeAuthenticated, token: minter.mint(probeInactive, map[string]any{"sid": sessionid.Wire(inactiveSID)}), status: http.StatusUnauthorized},
		{name: "unknown user", path: probeAuthenticated, token: minter.mint(probeUnknown, nil), status: http.StatusUnauthorized},
		{name: "revoked session", path: probeAuthenticated, token: user(map[string]any{"sid": sessionid.Wire(probeRevokedSID)}), status: http.StatusUnauthorized},
		{name: "session of another principal", path: probeAuthenticated, token: user(map[string]any{"sid": sessionid.Wire(probeOtherSID)}), status: http.StatusUnauthorized},
		{name: "unknown session", path: probeAuthenticated, token: user(map[string]any{"sid": sessionid.Wire(uuid.MustParse("00000000-0000-4000-8000-0000000000c9"))}), status: http.StatusUnauthorized},
		{name: "no sid", path: probeAuthenticated, token: user(map[string]any{"sid": nil}), status: http.StatusUnauthorized},
		{name: "x-org-id of a member org", path: probeAuthenticated, token: user(nil), headers: map[string]string{"X-Org-Id": probeOtherOrg.String()}, status: http.StatusOK, check: func(t *testing.T, body map[string]any) {
			if body["org"] != probeOtherOrg.String() {
				t.Errorf("org %v", body["org"])
			}
		}},
		{name: "x-org-id of a non-member org", path: probeAuthenticated, token: user(nil), headers: map[string]string{"X-Org-Id": probeTargetOrg.String()}, status: http.StatusForbidden},
		{name: "superuser from the users row, impersonating", path: probeSuperuser, token: minter.mint(probeAdminUser, map[string]any{"sid": sessionid.Wire(adminSID), "org_id": nil}), status: http.StatusOK, check: func(t *testing.T, body map[string]any) {
			if body["impersonating"] != probeTarget.String() || body["org"] != probeTargetOrg.String() || userWith(body)["is_superuser"] != true {
				t.Errorf("impersonation %v", body)
			}
		}},
		{name: "optional route with a user token", path: probeOptional, token: user(nil), status: http.StatusOK, check: func(t *testing.T, body map[string]any) {
			if userWith(body)["user_id"] != probeUser.String() {
				t.Errorf("identity %v", body)
			}
		}},
		{name: "user token on a row that does not accept it", path: probePublic, token: user(nil), status: http.StatusOK, check: func(t *testing.T, body map[string]any) {
			if body["org"] != "" {
				t.Errorf("a refused token reached the scope: %v", body)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			headers := bearer(tc.token)
			for name, value := range tc.headers {
				headers[name] = value
			}
			got := probeDo(f.handler, probeRequest{tc.name, http.MethodGet, tc.path, headers})
			if got.Status != tc.status {
				t.Fatalf("%d %s, want %d", got.Status, got.Body, tc.status)
			}
			if tc.status == http.StatusUnauthorized && (got.Body != genericRefusal || got.Headers["Www-Authenticate"][0] != "Bearer") {
				t.Fatalf("refusal is not the generic answer: %v %s", got.Headers, got.Body)
			}
			if tc.check != nil {
				tc.check(t, decodeEcho(t, got.Body))
			}
		})
	}
	if f.counters(t)[accessverify.AcceptedMetric+"|class=user_access_token,profile=GET "+probeAdmin+",service="+goAPIAccessService] == 0 {
		t.Error("no user-token acceptance counted on the admin probe row")
	}
}

type corpusManifest struct {
	Issuer      string   `json:"issuer"`
	RemovedKids []string `json:"removed_kids"`
	RemovedKeys []struct {
		Kid string `json:"kid"`
		X   string `json:"x"`
	} `json:"removed_public_keys"`
	Vectors []struct {
		File     string `json:"file"`
		Now      int64  `json:"now"`
		Audience string `json:"verify_as_audience"`
		KeySet   string `json:"key_set"`
		Expect   string `json:"expect"`
		Reason   string `json:"reason"`
	} `json:"vectors"`
}

// corpusAtAuthService are the rows decided at auth-service, with go-api's
// decision of the same token at the same clock: the dev-health-api token in
// its window is good; expired, not yet valid, another issuer and an
// auth-service token are refused.
var corpusAtAuthService = map[string]string{
	"invalid-aud-api-token-at-auth-service.json":     "accept",
	"invalid-order-audience-over-expiry.json":        "reject",
	"invalid-order-audience-over-not-yet-valid.json": "reject",
	"invalid-order-issuer-over-audience.json":        "reject",
	"valid-auth-service.json":                        "reject",
}

// TestA2CorpusThroughGoAPI runs every vector of the signed access-token
// corpus through the api listener (Guard, scope middlewares, the
// profile-routed validator) at the vector's clock.
func TestA2CorpusThroughGoAPI(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(corpusDir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest corpusManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	published, err := authverify.NewEd25519JWKSVerifier(filepath.Join(corpusDir, "jwks.json")).Keys()
	if err != nil {
		t.Fatal(err)
	}
	subPattern := regexp.MustCompile(`"sub":"(prn_[A-Za-z0-9_-]+)"`)
	sidPattern := regexp.MustCompile(`"sid":"(ses_[A-Za-z0-9_-]+)"`)
	accepted := 0
	for _, row := range manifest.Vectors {
		t.Run(strings.TrimSuffix(row.File, ".json"), func(t *testing.T) {
			expect := row.Expect
			if row.Audience != accessTokenAudienceAPI {
				var known bool
				if expect, known = corpusAtAuthService[row.File]; !known {
					t.Fatalf("a corpus row decided at %s that this test does not place at go-api", row.Audience)
				}
			}
			keys := authverify.StaticKeys{}
			for kid, key := range published {
				keys[kid] = key
			}
			if row.KeySet == "published_and_removed" {
				for _, removed := range manifest.RemovedKeys {
					x, err := base64.RawURLEncoding.DecodeString(removed.X)
					if err != nil {
						t.Fatal(err)
					}
					keys[removed.Kid] = ed25519.PublicKey(x)
				}
			}
			vector := loadCorpusVector(t, row.File)
			store := newProbeStore()
			if sub, sid := subPattern.FindStringSubmatch(vector.PayloadJSON), sidPattern.FindStringSubmatch(vector.PayloadJSON); sub != nil && sid != nil {
				principal, okPrincipal := decodeWireID(sub[1], "prn_")
				session, okSession := decodeWireID(sid[1], "ses_")
				if okPrincipal && okSession {
					store.users[principal] = probeUserRow{state: policy.UserState{IsActive: true}, email: "corpus@example.test"}
					store.sessions[session] = policy.SessionRow{PrincipalID: principal}
					store.members[[2]uuid.UUID{principal, uuid.MustParse("00000000-0000-4000-8000-000000000001")}] = "member"
				}
			}
			f := newRoutedFixture(t, store, userRows(t), manifest.Issuer, keys, manifest.RemovedKids, time.Unix(row.Now, 0))
			got := probeDo(f.handler, probeRequest{row.File, http.MethodGet, probeAuthenticated, bearer(vector.compact())})
			if expect == "accept" {
				if got.Status != http.StatusOK {
					t.Fatalf("want accept, got %d %s", got.Status, got.Body)
				}
				accepted++
				return
			}
			if got.Status != http.StatusUnauthorized || got.Body != genericRefusal {
				t.Fatalf("want the generic 401 (%s), got %d %s", row.Reason, got.Status, got.Body)
			}
		})
	}
	if len(manifest.Vectors) < 40 || accepted < 8 {
		t.Fatalf("%d vectors, %d accepted: the corpus did not run", len(manifest.Vectors), accepted)
	}
}

func decodeWireID(wire, prefix string) (uuid.UUID, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(wire, prefix))
	if err != nil || len(raw) != 16 || !strings.HasPrefix(wire, prefix) {
		return uuid.UUID{}, false
	}
	return uuid.UUID(raw), true
}

// TestRouteTemplatesMatchWhatTheMuxRoutes: for every mounted api-listener
// route, the profile the scope middlewares see (matched before routing) is
// the r.Pattern the real mux routes the request to.
func TestRouteTemplatesMatchWhatTheMuxRoutes(t *testing.T) {
	params := regexp.MustCompile(`\{([^}]*)\}`)
	var routes []httpapi.Route
	seen := map[string]string{}
	var lastPreRoute string
	for _, route := range mountedRoutes(t) {
		if route.Listener != "api" {
			continue
		}
		key := route.Method + " " + route.Pattern
		routes = append(routes, httpapi.Route{Method: route.Method, Pattern: route.Pattern, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seen[key] = r.Pattern
		})})
	}
	recordScope := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			lastPreRoute = policy.RouteProfileFrom(r.Context())
			next.ServeHTTP(w, r)
		})
	}
	server, err := NewServer(apiTestConfig(), quietLogger(), routes, recordScope)
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, route := range routes {
		key := route.Method + " " + route.Pattern
		values := dispatchProbeValues[key]
		target := params.ReplaceAllStringFunc(route.Pattern, func(match string) string {
			if value, ok := values[match[1:len(match)-1]]; ok {
				return value
			}
			return "x"
		})
		target = strings.ReplaceAll(target, "{$}", "")
		if signatureOnlyMatch(route.Method, target) {
			// No scope middleware runs for a signature-only route.
			continue
		}
		lastPreRoute = ""
		delete(seen, key)
		probeDo(server.Handler(), probeRequest{key, route.Method, target, map[string]string{"Content-Type": "application/json"}})
		inRoute, routed := seen[key]
		if !routed {
			continue
		}
		if lastPreRoute != inRoute || inRoute != key {
			t.Errorf("%s: pre-route profile %q, mux pattern %q", key, lastPreRoute, inRoute)
		}
		checked++
	}
	if checked < 200 {
		t.Fatalf("only %d routes were reached by their probe", checked)
	}
	for name, path := range map[string]string{"unknown": "/api/v1/nowhere", "trailing slash": probeAuthenticated + "/"} {
		lastPreRoute = "unset"
		probeDo(server.Handler(), probeRequest{name, http.MethodGet, path, nil})
		if lastPreRoute != "" {
			t.Errorf("%s: pre-route profile %q, want none", name, lastPreRoute)
		}
	}
}

// TestGoAPIAccessRowsAreTheFullInventory: the validator gets every
// dev-health-ops-api row, keyed as the mux registers the route.
func TestGoAPIAccessRowsAreTheFullInventory(t *testing.T) {
	rows := realRows(t)
	ids := map[string]bool{}
	for _, row := range loadGoAPIRows(t) {
		if row["service"] == goAPIAccessService {
			ids[row["method"].(string)+" "+row["route"].(string)] = true
		}
	}
	if len(rows) != len(ids) || len(rows) < 240 {
		t.Fatalf("%d rows, the inventory has %d for %s", len(rows), len(ids), goAPIAccessService)
	}
	legacy, user := 0, 0
	for _, row := range rows {
		if row.Service != goAPIAccessService || !ids[row.Profile] {
			t.Errorf("row %+v is not an inventory row", row)
		}
		if row.LegacyHS256 {
			legacy++
		}
		if row.AcceptsUser {
			user++
		}
	}
	if legacy < 190 || user != 0 {
		t.Errorf("%d legacy rows, %d user rows", legacy, user)
	}
}

func TestGoAPIAccessRowsRefuseABadTable(t *testing.T) {
	for name, raw := range map[string]string{
		"not json":       `{`,
		"no rows":        `{"rows":[]}`,
		"no service row": `{"rows":[{"service":"dev-health-ops-billing-edge","method":"GET","route":"/health"}]}`,
		"empty route":    `{"rows":[{"service":"dev-health-ops-api","method":"GET","route":""}]}`,
		"duplicate":      `{"rows":[{"service":"dev-health-ops-api","method":"GET","route":"/a"},{"service":"dev-health-ops-api","method":"GET","route":"/a"}]}`,
	} {
		if rows, err := parseAccessRows([]byte(raw)); err == nil {
			t.Errorf("%s: accepted %v", name, rows)
		}
	}
}

// TestConfigure_RoutesTheAuthenticatorWhenIssuerIsSet: the prod construction
// path routes go-api's Authenticator; unset, it stays unrouted.
func TestConfigure_RoutesTheAuthenticatorWhenIssuerIsSet(t *testing.T) {
	for _, set := range []bool{true, false} {
		deps := configureForAccessVerify(t, set)
		if deps.Auth == nil {
			t.Fatal("no authenticator")
		}
		if deps.Auth.Routed() != set || (deps.AccessVerify != nil) != set {
			t.Errorf("issuer set %v: routed %v, validator %v", set, deps.Auth.Routed(), deps.AccessVerify != nil)
		}
	}
}

// TestRoutedAccessVerifyMapsTheValidatorForPolicy: unavailable is
// policy.ErrUnavailable, a refusal is no pass, a legacy result must be a
// policy user, and a user token's claims reach policy unchanged.
func TestRoutedAccessVerifyMapsTheValidatorForPolicy(t *testing.T) {
	minter := newEdDSAMinter(t)
	verifier, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: probeIssuer, Audience: accessTokenAudienceAPI, Keys: minter.keys})
	if err != nil {
		t.Fatal(err)
	}
	const profile = "GET /p"
	var legacyAnswer any
	var legacyErr error
	validator, err := accessverify.New(accessverify.Config{
		Rows:  []accessverify.Row{{Service: goAPIAccessService, Profile: profile, AcceptsUser: true, LegacyHS256: true}},
		EdDSA: verifier, Meter: sdkmetric.NewMeterProvider().Meter("test"), Now: func() time.Time { return probeNow },
		Legacy: func(context.Context, string) (any, error) { return legacyAnswer, legacyErr },
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter := routedAccessVerify{validator}
	ctx := context.Background()
	hs256 := probeHS256(t, probeKey, map[string]any{"sub": probeUser.String()})

	if _, err := adapter.Validate(ctx, goAPIAccessService, "GET /unknown", hs256); err == nil || errors.Is(err, policy.ErrUnavailable) {
		t.Errorf("unknown profile: %v, want a refusal", err)
	}
	legacyAnswer, legacyErr = nil, fmt.Errorf("down: %w", accessverify.ErrUnavailable)
	if _, err := adapter.Validate(ctx, goAPIAccessService, profile, hs256); !errors.Is(err, policy.ErrUnavailable) {
		t.Errorf("unavailable legacy: %v", err)
	}
	legacyAnswer, legacyErr = "not a user", nil
	if result, err := adapter.Validate(ctx, goAPIAccessService, profile, hs256); err == nil || result != nil {
		t.Errorf("a legacy answer of another type: %v %v", result, err)
	}
	want := &policy.User{UserID: probeUser.String()}
	legacyAnswer = want
	if result, err := adapter.Validate(ctx, goAPIAccessService, profile, hs256); err != nil || result.Legacy != want || result.Claims != nil {
		t.Errorf("legacy user: %+v %v", result, err)
	}
	result, err := adapter.Validate(ctx, goAPIAccessService, profile, minter.mint(probeUser, nil))
	if err != nil || result.Legacy != nil || result.Claims == nil || result.Claims.Subject != wirePrincipal(probeUser) ||
		result.Claims.SessionID != sessionid.Wire(probeLiveSID) || result.Claims.OrgID == nil || *result.Claims.OrgID != probeOrg.String() {
		t.Errorf("user token: %+v %v", result, err)
	}
}

// TestIssuerSetCallerLookupOutageIsTheListener500: a store outage behind the
// validator answers through the api listener exactly as with the issuer
// unset: OrgScope runs first and answers the generic 500 (OrgIdMiddleware
// parity); the 503 of the guard is not reached on this listener.
func TestIssuerSetCallerLookupOutageIsTheListener500(t *testing.T) {
	minter := newEdDSAMinter(t)
	store := newProbeStore()
	downSID := uuid.MustParse("00000000-0000-4000-8000-0000000000c7")
	store.sessions[downSID] = policy.SessionRow{PrincipalID: probeDown}
	legacy := probeHS256(t, probeKey, map[string]any{"sub": probeDown.String(), "org_id": probeOrg.String()})
	user := minter.mint(probeDown, map[string]any{"sid": sessionid.Wire(downSID)})
	unset := probeDo(probeServer(t, store, nil), probeRequest{"unset", http.MethodGet, probeAuthenticated, bearer(legacy)})
	if unset.Status != http.StatusInternalServerError {
		t.Fatalf("issuer unset: %d %s", unset.Status, unset.Body)
	}
	f := newRoutedFixture(t, store, userRows(t), probeIssuer, minter.keys, nil, probeNow)
	for name, token := range map[string]string{"legacy": legacy, "user": user} {
		got := probeDo(f.handler, probeRequest{name, http.MethodGet, probeAuthenticated, bearer(token)})
		a, _ := json.Marshal(probeAnswer{Status: got.Status, Headers: got.Headers, Body: got.Body})
		b, _ := json.Marshal(probeAnswer{Status: unset.Status, Headers: unset.Headers, Body: unset.Body})
		if !bytes.Equal(a, b) {
			t.Errorf("%s: issuer set answers %s, issuer unset %s", name, a, b)
		}
	}
}
