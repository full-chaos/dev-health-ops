package apiservice

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-go/authverify"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

// lookupCounts counts every read a credential can cause past the class
// decision: a signing-key read, an HS256 verification, a store read.
type lookupCounts struct {
	mu       sync.Mutex
	keys     int
	legacy   int
	store    int
	validate int
}

func (c *lookupCounts) add(field *int) {
	c.mu.Lock()
	*field++
	c.mu.Unlock()
}

func (c *lookupCounts) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.keys + c.legacy + c.store
}

func (c *lookupCounts) reset() {
	c.mu.Lock()
	c.keys, c.legacy, c.store, c.validate = 0, 0, 0, 0
	c.mu.Unlock()
}

// matrixValidator counts the calls the routed access validator receives:
// the scope may hand it a JWT of an access row, never anything else.
type matrixValidator struct {
	inner  policy.RoutedValidator
	counts *lookupCounts
}

func (v matrixValidator) Validate(ctx context.Context, service, profile, bearer string) (*policy.RoutedResult, error) {
	v.counts.add(&v.counts.validate)
	return v.inner.Validate(ctx, service, profile, bearer)
}

type matrixStore struct {
	*probeStore
	counts *lookupCounts
}

func (s matrixStore) UserState(ctx context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	s.counts.add(&s.counts.store)
	return s.probeStore.UserState(ctx, id)
}

func (s matrixStore) Membership(ctx context.Context, user, org uuid.UUID) (string, bool, error) {
	s.counts.add(&s.counts.store)
	return s.probeStore.Membership(ctx, user, org)
}

func (s matrixStore) ActiveImpersonation(ctx context.Context, admin uuid.UUID) (*policy.Impersonation, error) {
	s.counts.add(&s.counts.store)
	return s.probeStore.ActiveImpersonation(ctx, admin)
}

func (s matrixStore) SessionState(ctx context.Context, id uuid.UUID) (policy.SessionRow, bool, error) {
	s.counts.add(&s.counts.store)
	return s.probeStore.SessionState(ctx, id)
}

func (s matrixStore) AccessUser(ctx context.Context, id uuid.UUID) (policy.AccessUserRow, bool, error) {
	s.counts.add(&s.counts.store)
	return s.probeStore.AccessUser(ctx, id)
}

type matrixKeys struct {
	inner  authverify.StaticKeys
	counts *lookupCounts
}

func (k matrixKeys) PublicKey(kid string) (ed25519.PublicKey, bool) {
	k.counts.add(&k.counts.keys)
	return k.inner.PublicKey(kid)
}

const (
	matrixHS256Class  = "ops_access_token_hs256"
	matrixUserClass   = "user_access_token"
	matrixFillerBytes = 43
)

// matrixNotDriven are the classes with no request-head marker in
// credential-classes.json: the matrix has no carrier to send for them.
var matrixNotDriven = []string{
	"acr_browser_refresh_token", "acr_device_flow_code", "acr_workload_identity_exchange",
	"auth_js_browser_session", "email_verification_token", "github_app_installation_token",
	"impersonation_session", "infrastructure_deployment_credentials", "kubernetes_sa_token_exchange",
	"license_key", "link_credential", "llm_api_keys", "org_invite_token", "pagerduty_oauth_credential",
	"password_reset_token", "refresh_credential", "social_oauth_exchange", "sso_oidc_exchange",
	"sso_saml_exchange", "ops_refresh_token",
}

type matrixClass struct {
	id      string
	markers map[string][]string
}

func (c matrixClass) has(kind, name string) bool { return slices.Contains(c.markers[kind], name) }

func loadMatrixClasses(t *testing.T) []matrixClass {
	t.Helper()
	var document struct {
		Classes []struct {
			ClassID string `json:"class_id"`
			Markers []struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
			} `json:"markers"`
		} `json:"classes"`
	}
	if err := json.Unmarshal(authv1.CredentialClasses, &document); err != nil {
		t.Fatal(err)
	}
	var out []matrixClass
	for _, entry := range document.Classes {
		class := matrixClass{id: entry.ClassID, markers: map[string][]string{}}
		for _, marker := range entry.Markers {
			class.markers[marker.Kind] = append(class.markers[marker.Kind], marker.Name)
		}
		out = append(out, class)
	}
	return out
}

type matrixRig struct {
	handler http.Handler
	counts  *lookupCounts
	minter  *eddsaMinter
	rows    []map[string]any
}

var matrixParam = regexp.MustCompile(`\{[^}]*\}`)

func matrixPath(route string) string { return matrixParam.ReplaceAllString(route, "x") }

// newMatrixRig is the api listener (guarded: with each protected access row's
// own route guard, which authenticates once more per route; unguarded: the
// scope layers alone) as configureWith builds it: the real row
// tables, the real access validator over them (routed mode), the three scope
// layers, one route per api row. Only the key source, the HS256 path and the
// store are counting doubles.
func newMatrixRig(t *testing.T, guarded bool) *matrixRig {
	t.Helper()
	counts := &lookupCounts{}
	minter := newEdDSAMinter(t)
	store := matrixStore{probeStore: newProbeStore(), counts: counts}
	hs, err := edgetoken.New(probeKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(hs, store, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	eddsa, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: probeIssuer, Audience: accessTokenAudienceAPI, Keys: matrixKeys{inner: minter.keys, counts: counts}})
	if err != nil {
		t.Fatal(err)
	}
	legacy := legacyVerifier(auth.AuthenticateLegacy)
	validator, err := accessverify.New(accessverify.Config{
		Rows:  realRows(t),
		EdDSA: eddsa,
		Legacy: func(ctx context.Context, token string) (any, error) {
			counts.add(&counts.legacy)
			return legacy(ctx, token)
		},
		Meter:  sdkmetric.NewMeterProvider().Meter("test"),
		Logger: quietLogger(),
		Now:    func() time.Time { return probeNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.RouteAccess(matrixValidator{inner: routedAccessVerify{validator}, counts: counts}, goAPIAccessService); err != nil {
		t.Fatal(err)
	}
	scopeRows, err := goAPIScopeRows()
	if err != nil {
		t.Fatal(err)
	}
	scope := policy.NewScope(auth, quietLogger())
	scope.DeclaredRows(scopeRows)
	guard := policy.NewGuard(auth, quietLogger())
	rows := apiRows(t)
	var routes []httpapi.Route
	for _, row := range rows {
		var handler http.Handler = http.HandlerFunc(probeEcho)
		if guarded && row["classification"] == "protected" && matrixAcceptsAccessToken(row) {
			handler = guard.Wrap(policy.Authenticated, handler)
		}
		routes = append(routes, httpapi.Route{Method: row["method"].(string), Pattern: row["route"].(string), Handler: handler})
	}
	server, err := NewServer(apiTestConfig(), quietLogger(), routes, scope.Layers()...)
	if err != nil {
		t.Fatal(err)
	}
	return &matrixRig{handler: server.Handler(), counts: counts, minter: minter, rows: rows}
}

func matrixAcceptsAccessToken(row map[string]any) bool {
	classes := rowClasses(row)
	return slices.Contains(classes, matrixHS256Class) || slices.Contains(classes, matrixUserClass)
}

// matrixCarrier is the request head that stands for a class, built from its
// markers; ok is false for a class with no head marker.
func (rig *matrixRig) matrixCarrier(t *testing.T, class matrixClass) (map[string]string, bool) {
	t.Helper()
	filler := strings.Repeat("A", matrixFillerBytes)
	switch {
	case len(class.markers["credential_prefix"]) > 0:
		return map[string]string{"Authorization": "Bearer " + class.markers["credential_prefix"][0] + filler}, true
	case class.has("authorization_scheme", "Bearer") && class.has("jwt_header_alg", "HS256"):
		return bearer(probeHS256(t, probeKey, map[string]any{"sub": probeUser.String(), "org_id": probeOrg.String(), "role": "admin", "tv": 3})), true
	case class.has("authorization_scheme", "Bearer") && class.has("jwt_header_alg", "EdDSA"):
		typ := class.markers["jwt_header_typ"][0]
		return bearer(matrixMint(rig.minter, typ)), true
	case len(class.markers["dedicated_header"]) > 0:
		return map[string]string{class.markers["dedicated_header"][0]: probeOrg.String()}, true
	case len(class.markers["signature_header"]) > 0:
		return map[string]string{class.markers["signature_header"][0]: "sha256=" + filler}, true
	}
	return nil, false
}

func (rig *matrixRig) serve(row map[string]any, headers map[string]string) (int, int, int) {
	rig.counts.reset()
	request := httptest.NewRequest(row["method"].(string), matrixPath(row["route"].(string)), nil)
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	rig.handler.ServeHTTP(recorder, request)
	rig.counts.mu.Lock()
	validate := rig.counts.validate
	rig.counts.mu.Unlock()
	return recorder.Code, rig.counts.total(), validate
}

// matrixAccepted reports whether the row accepts the class AND the class is
// one the scope authenticates (an access token): only then may a read happen.
func matrixAccepted(row map[string]any, class string) bool {
	return (class == matrixHS256Class || class == matrixUserClass) && slices.Contains(rowClasses(row), class)
}

// TestCredentialClassMatrix drives every go-api (api listener) endpoint-profile
// row with every credential class of credential-classes.json through the
// api listener's production chain. A class the row does not accept must reach
// no key read, no HS256 verification and no store read; a class it accepts
// must resolve.
func TestCredentialClassMatrix(t *testing.T) {
	scopeRig, guardRig := newMatrixRig(t, false), newMatrixRig(t, true)
	rig := scopeRig
	classes := loadMatrixClasses(t)
	if len(classes) == 0 || len(rig.rows) == 0 {
		t.Fatal("empty matrix axis")
	}
	if want, err := parseScopeRows(authv1.GoAPIEndpointProfiles); err != nil || len(want) != len(rig.rows) {
		t.Fatalf("matrix rows %d, scope rows %d (%v)", len(rig.rows), len(want), err)
	}
	var notDriven []string
	for _, class := range classes {
		_, driven := rig.matrixCarrier(t, class)
		if !driven {
			notDriven = append(notDriven, class.id)
			continue
		}
		isJWT := class.has("authorization_scheme", "Bearer") && len(class.markers["jwt_header_alg"]) > 0
		t.Run(class.id, func(t *testing.T) {
			for _, cellRig := range []*matrixRig{scopeRig, guardRig} {
				cells, accepts := 0, 0
				carrier, _ := cellRig.matrixCarrier(t, class)
				for _, row := range cellRig.rows {
					cells++
					status, lookups, validate := cellRig.serve(row, carrier)
					if matrixAccepted(row, class.id) {
						accepts++
						if status != http.StatusOK || lookups == 0 {
							t.Errorf("%s: accepted class %s answered %d with %d lookups", row["id"], class.id, status, lookups)
						}
						continue
					}
					if cellRig == scopeRig && validate != 0 && !(isJWT && matrixAcceptsAccessToken(row)) {
						t.Errorf("%s: class %s reached the access validator %d times", row["id"], class.id, validate)
					}
					if lookups != 0 {
						t.Errorf("%s: class %s reached %d lookups (keys %d, legacy %d, store %d)", row["id"], class.id, lookups, cellRig.counts.keys, cellRig.counts.legacy, cellRig.counts.store)
					}
					if cellRig == guardRig && row["classification"] == "protected" && matrixAcceptsAccessToken(row) && status != http.StatusUnauthorized {
						t.Errorf("%s: wrong class %s answered %d, want 401", row["id"], class.id, status)
					}
				}
				t.Logf("class %s: %d cells, %d accept cells (guarded %v)", class.id, cells, accepts, cellRig == guardRig)
			}
		})
	}
	slices.Sort(notDriven)
	want := slices.Clone(matrixNotDriven)
	slices.Sort(want)
	if !slices.Equal(notDriven, want) {
		t.Errorf("classes with no carrier %v, want %v", notDriven, want)
	}
}

// matrixMint signs a token for a live user and session under the given JWT
// typ, so a token that is refused is refused for its class, not its claims.
func matrixMint(m *eddsaMinter, typ string) string {
	claims := map[string]any{
		"iss": probeIssuer, "aud": accessTokenAudienceAPI, "sub": wirePrincipal(probeUser), "principal_type": "human",
		"sid": sessionid.Wire(probeLiveSID), "jti": strings.Repeat("j", 24),
		"iat": probeNow.Unix() - 60, "nbf": probeNow.Unix() - 60, "exp": probeNow.Unix() + 600,
		"org_id": probeOrg.String(), "scope": "api:access",
	}
	header, _ := json.Marshal(map[string]any{"alg": "EdDSA", "typ": typ, "kid": "probe-kid"})
	payload, _ := json.Marshal(claims)
	signing := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	return signing + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(m.priv, []byte(signing)))
}
