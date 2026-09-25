package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"log"
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
		// These tests speak to the handler as the INTERNAL listener would.
		*r = *r.WithContext(iaInternalCtx(r.Context()))
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
		{"org header only", func(r *http.Request) {
			*r = *r.WithContext(iaInternalCtx(r.Context()))
			r.Header.Set(internalidentity.HeaderOrgID, "org-1")
		}},
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
	wantImporters := map[string]bool{"internal_auth.go": true, "server.go": true}
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

// r1 P1 (CHAOS-6757): every carrier outcome is decided before the document
// lookup, so a refused request is 401 even for a document this router would
// 404 (unregistered). Before the fix the lookup ran first: both carriers, no
// carrier or an invalid envelope plus an unregistered document answered 404.
func TestQueryDecidesTheCarrierBeforeTheDocumentLookup(t *testing.T) {
	verifier, priv := iaVerifier(t)
	handler, seen := iaDispatch(t, verifier)
	token := iaEnvelope(t, priv, principal.Claims{OrgID: "org-1", Role: "member"})
	carriers := map[string]func(*http.Request){
		"both": func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer "+token)
			iaHeaders("org-1", "member", false, false)(r)
		},
		"none":             func(*http.Request) {},
		"invalid envelope": func(r *http.Request) { r.Header.Set("Authorization", "Bearer not-a-token") },
		"partial headers": func(r *http.Request) {
			*r = *r.WithContext(iaInternalCtx(r.Context()))
			r.Header.Set(internalidentity.HeaderOrgID, "org-1")
		},
	}
	for _, document := range []string{iaDocument, "query { notRegistered { id } }"} {
		for name, mutate := range carriers {
			body, err := json.Marshal(map[string]string{"query": document})
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
			mutate(req)
			rec := httptest.NewRecorder()
			handler(rec, req)
			if rec.Code != http.StatusUnauthorized || rec.Body.String() != "unauthorized\n" {
				t.Fatalf("carrier %q, document %q: status=%d body=%q, want 401 bare unauthorized", name, document, rec.Code, rec.Body.String())
			}
		}
	}
	if len(*seen) != 0 {
		t.Fatalf("resolver ran %d times for a refused request", len(*seen))
	}
}

// An accepted request for an unregistered document is still the 404 it was.
func TestQueryStillAnswers404ForAnUnregisteredDocumentOnceAuthenticated(t *testing.T) {
	verifier, _ := iaVerifier(t)
	handler, _ := iaDispatch(t, verifier)
	body, err := json.Marshal(map[string]string{"query": "query { notRegistered { id } }"})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
	iaHeaders("org-1", "member", false, false)(req)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("authenticated unregistered document: status=%d, want 404", rec.Code)
	}
}

// iaInternalCtx is the context a request carries after the internal listener.
func iaInternalCtx(ctx context.Context) context.Context {
	var marked context.Context
	internalidentity.Internal(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		marked = r.Context()
	})).ServeHTTP(nil, (&http.Request{}).WithContext(ctx))
	return marked
}

// CHAOS-6780: the identity headers are honoured only on the internal
// listener. The same forged four headers that grant an identity there are
// deleted, logged and counted on the public one.
func TestIdentityHeadersAreHonouredOnlyOnTheInternalListener(t *testing.T) {
	verifier, priv := iaVerifier(t)
	handler, seen := iaDispatch(t, verifier)
	public, internal := newListenerServers("127.0.0.1:0", "127.0.0.1:0", handler)
	if internal == nil {
		t.Fatal("no internal server built although an internal address was given")
	}
	post := func(server *http.Server, mutate func(*http.Request)) int {
		body, err := json.Marshal(map[string]string{"query": iaDocument})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
		mutate(req)
		rec := httptest.NewRecorder()
		server.Handler.ServeHTTP(rec, req)
		return rec.Code
	}
	// Raw headers only, no context marking: the listener alone decides.
	rawForged := func(r *http.Request) {
		for _, name := range internalidentity.Headers {
			r.Header.Set(name, "true")
		}
		r.Header.Set(internalidentity.HeaderOrgID, "org-forged")
		r.Header.Set(internalidentity.HeaderRole, "owner")
	}

	var logs bytes.Buffer
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	if code := post(public, rawForged); code != http.StatusUnauthorized {
		t.Fatalf("forged headers on the public listener: %d, want 401", code)
	}
	if len(*seen) != 0 {
		t.Fatalf("forged headers on the public listener reached the resolver: %+v", *seen)
	}
	if !strings.Contains(logs.String(), "dropped internal identity headers on the public listener") ||
		!strings.Contains(logs.String(), "path_class=query") || strings.Contains(logs.String(), "org-forged") {
		t.Fatalf("drop log = %q, want a line naming the path class and header names, never a value", logs.String())
	}
	// A bearer caller sending stray headers to the public port is served by the
	// bearer (the headers are gone, so it is no longer ambiguous).
	envelope := iaEnvelope(t, priv, principal.Claims{OrgID: "org-1", Role: "member"})
	if code := post(public, func(r *http.Request) {
		rawForged(r)
		r.Header.Set("Authorization", "Bearer "+envelope)
	}); code != http.StatusOK {
		t.Fatalf("bearer + stray headers on the public listener: %d, want 200", code)
	}
	if got := (*seen)[len(*seen)-1]; got.OrgID != "org-1" {
		t.Fatalf("public listener resolved %+v, want the bearer's org-1", got)
	}
	// The same four headers on the internal listener grant the stated identity.
	*seen = nil
	if code := post(internal, rawForged); code != http.StatusOK {
		t.Fatalf("headers on the internal listener: %d, want 200", code)
	}
	if len(*seen) != 1 || (*seen)[0].OrgID != "org-forged" {
		t.Fatalf("internal listener resolved %+v, want org-forged", *seen)
	}
}

// Unset internal address = no internal server = headers honoured nowhere.
func TestNoInternalListenerWhenTheAddressIsUnset(t *testing.T) {
	public, internal := newListenerServers("127.0.0.1:0", "", http.NotFoundHandler())
	if internal != nil || public == nil {
		t.Fatalf("public=%v internal=%v, want a public server only", public, internal)
	}
}

// A handler wired without the listener middleware refuses the headers rather
// than trusting them.
func TestHeadersOffTheInternalListenerAreRefusedAtTheHandler(t *testing.T) {
	verifier, _ := iaVerifier(t)
	handler, seen := iaDispatch(t, verifier)
	code := iaPost(t, handler, func(r *http.Request) {
		r.Header.Set(internalidentity.HeaderOrgID, "org-1")
		r.Header.Set(internalidentity.HeaderRole, "admin")
		r.Header.Set(internalidentity.HeaderSuperuser, "false")
		r.Header.Set(internalidentity.HeaderImpersonationActive, "false")
	})
	if code != http.StatusUnauthorized || len(*seen) != 0 {
		t.Fatalf("status=%d resolver=%+v, want 401 and no resolver call", code, *seen)
	}
}
