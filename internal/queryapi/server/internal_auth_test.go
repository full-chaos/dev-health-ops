package server

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// CHAOS-6144: /query and /buildinfo take the identity from the internal
// headers OR the envelope, never both, and answer the same claims for the
// same identity whichever carrier states it. Everything runs through the
// real dispatch handler and the real principal.Verifier; no claims are
// built by hand on the request path.

const (
	iaIssuer   = "test-issuer"
	iaAudience = "test-audience"
	iaKID      = "test-kid"
	iaDocument = "query InternalAuthProbe { probe }"
)

func iaVerifier(t *testing.T) (*principal.Verifier, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := json.Marshal(map[string]any{"keys": []map[string]any{{
		"kty": "OKP", "crv": "Ed25519", "x": base64.RawURLEncoding.EncodeToString(pub),
		"kid": iaKID, "use": "sig", "alg": "EdDSA",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(path, doc, 0o600); err != nil {
		t.Fatal(err)
	}
	verifier, err := principal.NewVerifier(path, iaIssuer, iaAudience)
	if err != nil {
		t.Fatal(err)
	}
	return verifier, priv
}

func iaEnvelope(t *testing.T, priv ed25519.PrivateKey, claims principal.Claims) string {
	t.Helper()
	claims.SchemaVersion = principal.SupportedSchemaVersion
	claims.RegisteredClaims = jwt.RegisteredClaims{
		Issuer: iaIssuer, Audience: jwt.ClaimStrings{iaAudience}, Subject: "user-1",
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = iaKID
	signed, err := token.SignedString(priv)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

// iaDispatch builds the real /query dispatch handler around a probe that
// records the claims the resolver would see.
func iaDispatch(t *testing.T, verifier *principal.Verifier) (http.HandlerFunc, *[]authctx.Claims) {
	t.Helper()
	seen := &[]authctx.Claims{}
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{"probe": true})
	mux.Register("probe", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, _ := authctx.FromContext(r.Context())
		*seen = append(*seen, claims)
		w.WriteHeader(http.StatusOK)
	}))
	handler := newDocumentDispatchHandler(os.Getenv, mux, map[string]string{digestHex(iaDocument): "probe"}, verifier)
	return handler, seen
}

func iaPost(t *testing.T, handler http.HandlerFunc, mutate func(*http.Request)) int {
	t.Helper()
	body, err := json.Marshal(map[string]string{"query": iaDocument})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	mutate(req)
	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec.Code
}

func iaHeaders(orgID, role string, superuser, impersonating bool) func(*http.Request) {
	flag := func(b bool) string {
		if b {
			return "true"
		}
		return "false"
	}
	return func(r *http.Request) {
		r.Header.Set(internalidentity.HeaderOrgID, orgID)
		r.Header.Set(internalidentity.HeaderRole, role)
		r.Header.Set(internalidentity.HeaderSuperuser, flag(superuser))
		r.Header.Set(internalidentity.HeaderImpersonationActive, flag(impersonating))
	}
}

func TestQueryAnswersTheSameClaimsForBothCarriers(t *testing.T) {
	verifier, priv := iaVerifier(t)
	for _, identity := range []struct {
		name       string
		org, role  string
		super, imp bool
	}{
		{"member", "org-1", "member", false, false},
		{"admin", "org-2", "admin", false, false},
		{"superuser", "org-3", "owner", true, false},
		{"impersonating", "org-4", "viewer", false, true},
	} {
		t.Run(identity.name, func(t *testing.T) {
			handler, seen := iaDispatch(t, verifier)
			envelope := iaEnvelope(t, priv, principal.Claims{
				OrgID: identity.org, Role: identity.role, IsSuperuser: identity.super, ImpersonationActive: identity.imp,
			})
			if code := iaPost(t, handler, func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+envelope) }); code != http.StatusOK {
				t.Fatalf("envelope carrier: %d", code)
			}
			if code := iaPost(t, handler, iaHeaders(identity.org, identity.role, identity.super, identity.imp)); code != http.StatusOK {
				t.Fatalf("header carrier: %d", code)
			}
			if len(*seen) != 2 || (*seen)[0] != (*seen)[1] {
				t.Fatalf("carriers disagree: %+v", *seen)
			}
			want := authctx.Claims{OrgID: identity.org, Role: identity.role, IsSuperuser: identity.super, ImpersonationActive: identity.imp}
			if (*seen)[0] != want {
				t.Fatalf("got %+v want %+v", (*seen)[0], want)
			}
		})
	}
}

func TestQueryRefusesEveryAmbiguousOrMalformedCarrier(t *testing.T) {
	verifier, priv := iaVerifier(t)
	envelope := iaEnvelope(t, priv, principal.Claims{OrgID: "org-1", Role: "admin"})
	bearer := func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+envelope) }

	for _, tc := range []struct {
		name   string
		mutate func(*http.Request)
	}{
		{"no carrier", func(*http.Request) {}},
		{"both carriers", func(r *http.Request) { bearer(r); iaHeaders("org-1", "admin", false, false)(r) }},
		{"both carriers, garbage bearer", func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer garbage")
			iaHeaders("org-1", "admin", false, false)(r)
		}},
		{"org header only", func(r *http.Request) { r.Header.Set(internalidentity.HeaderOrgID, "org-1") }},
		{"role header missing", func(r *http.Request) {
			iaHeaders("org-1", "admin", false, false)(r)
			r.Header.Del(internalidentity.HeaderRole)
		}},
		{"superuser not a boolean", func(r *http.Request) {
			iaHeaders("org-1", "admin", false, false)(r)
			r.Header.Set(internalidentity.HeaderSuperuser, "1")
		}},
		{"duplicate org header", func(r *http.Request) {
			iaHeaders("org-1", "admin", false, false)(r)
			r.Header.Add(internalidentity.HeaderOrgID, "org-2")
		}},
		{"basic scheme", func(r *http.Request) { r.Header.Set("Authorization", "Basic abc") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler, seen := iaDispatch(t, verifier)
			if code := iaPost(t, handler, tc.mutate); code != http.StatusUnauthorized {
				t.Fatalf("got %d, want 401", code)
			}
			if len(*seen) != 0 {
				t.Fatalf("a refused request reached the resolver with %+v", *seen)
			}
		})
	}
}

func TestBuildInfoTakesEitherCarrierAndRefusesBoth(t *testing.T) {
	verifier, priv := iaVerifier(t)
	handler := newBuildInfoHandler(verifier)
	get := func(mutate func(*http.Request)) int {
		req := httptest.NewRequest(http.MethodGet, "/buildinfo", nil)
		mutate(req)
		rec := httptest.NewRecorder()
		handler(rec, req)
		return rec.Code
	}
	envelope := iaEnvelope(t, priv, principal.Claims{OrgID: "org-1", Role: "admin"})
	if code := get(func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+envelope) }); code != http.StatusOK {
		t.Fatalf("envelope: %d", code)
	}
	if code := get(iaHeaders("org-1", "admin", false, false)); code != http.StatusOK {
		t.Fatalf("headers: %d", code)
	}
	if code := get(func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer "+envelope)
		iaHeaders("org-1", "admin", false, false)(r)
	}); code != http.StatusUnauthorized {
		t.Fatalf("both: %d", code)
	}
	if code := get(func(*http.Request) {}); code != http.StatusUnauthorized {
		t.Fatalf("none: %d", code)
	}
}

// A route an Ingress reaches must never honour the internal headers: a
// browser can set them. authenticateRESTRequest is the guard every such
// route shares; a full header set with no bearer, and with a valid bearer
// naming another org, must not change what it answers.
func TestBrowserReachableRESTAuthIgnoresTheInternalHeaders(t *testing.T) {
	verifier, priv := iaVerifier(t)
	call := func(mutate func(*http.Request)) (int, authctx.Claims) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/meta", nil)
		mutate(req)
		rec := httptest.NewRecorder()
		claims, ok := authenticateRESTRequest(rec, req, verifier, nil, "test")
		if !ok {
			return rec.Code, authctx.Claims{}
		}
		return http.StatusOK, claims
	}

	if code, _ := call(iaHeaders("org-1", "owner", true, false)); code != http.StatusUnauthorized {
		t.Fatalf("headers alone authenticated a REST request: %d", code)
	}
	envelope := iaEnvelope(t, priv, principal.Claims{OrgID: "org-real", Role: "member"})
	code, claims := call(func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer "+envelope)
		iaHeaders("org-forged", "owner", true, false)(r)
	})
	if code != http.StatusOK || claims.OrgID != "org-real" {
		t.Fatalf("forged headers changed a REST identity: %d %+v", code, claims)
	}
}

// The internal headers are trusted only on paths no Ingress reaches. This
// pins WHICH code may read them: the identity package is imported by one
// file, and only the /query dispatch and /buildinfo call the internal
// authenticator. A REST route gaining either is a browser-settable identity.
func TestOnlyInternalPathsReadTheInternalIdentity(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	importers, callers := map[string]bool{}, map[string]bool{}
	checked := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		text := string(raw)
		if strings.Contains(text, "queryapi/internalidentity\"") {
			importers[name] = true
		}
		if strings.Contains(text, "authenticateInternalRequest(") {
			callers[name] = true
		}
	}
	if checked < 20 {
		t.Fatalf("scanned %d source files; the scan is not measuring the package", checked)
	}
	wantImporters := map[string]bool{"internal_auth.go": true}
	wantCallers := map[string]bool{"internal_auth.go": true, "query_route.go": true, "buildinfo_route.go": true}
	if !sameSet(importers, wantImporters) {
		t.Fatalf("files importing internalidentity: %v, want %v", importers, wantImporters)
	}
	if !sameSet(callers, wantCallers) {
		t.Fatalf("files calling authenticateInternalRequest: %v, want %v", callers, wantCallers)
	}
}

func sameSet(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}
