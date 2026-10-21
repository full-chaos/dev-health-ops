package authapi

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/keystore"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

const wellKnownIssuer = "https://auth.example.test"

type keySetFunc func(context.Context) (token.KeySet, error)

func (f keySetFunc) KeySet(ctx context.Context) (token.KeySet, error) { return f(ctx) }

func publicKey(t *testing.T) ed25519.PublicKey {
	t.Helper()
	public, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	return public
}

// keysOf builds a key set from kid=state pairs in order; the first "active"
// entry signs.
func keysOf(t *testing.T, states ...[2]string) (token.KeySet, map[string]ed25519.PublicKey) {
	t.Helper()
	publics := map[string]ed25519.PublicKey{}
	var keys []token.Key
	for _, pair := range states {
		public := publicKey(t)
		publics[pair[0]] = public
		keys = append(keys, token.Key{KeyID: pair[0], State: token.State(pair[1]), Algorithm: keystore.Algorithm, PublicKey: public})
	}
	set, err := token.NewKeySet(keys)
	if err != nil {
		t.Fatal(err)
	}
	return set, publics
}

func serve(t *testing.T, deps WellKnownDeps, method, path string, header http.Header) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	for _, route := range WellKnownRoutes(deps) {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	request := httptest.NewRequest(method, path, nil)
	for name, values := range header {
		request.Header[name] = values
	}
	recorder := httptest.NewRecorder()
	requestid.Middleware(mux).ServeHTTP(recorder, request)
	return recorder
}

func decode(t *testing.T, raw []byte) any {
	t.Helper()
	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	return document
}

func twoKeySet(t *testing.T) (token.KeySet, map[string]ed25519.PublicKey) {
	return keysOf(t, [2]string{"auth-2026-10", "retiring"}, [2]string{"auth-2026-11", "active"})
}

func TestJWKSServesTheActiveAndOverlapKeysAsAValidDocument(t *testing.T) {
	set, publics := twoKeySet(t)
	deps := WellKnownDeps{Keys: keySetFunc(func(context.Context) (token.KeySet, error) { return set, nil })}
	rec := serve(t, deps, http.MethodGet, PatternJWKS, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if err := contracts.Validate(repoRoot(t), "jwks.v1", decode(t, rec.Body.Bytes())); err != nil {
		t.Fatalf("served JWKS fails jwks.v1: %v", err)
	}
	var doc struct{ Keys []map[string]string }
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Keys) != 2 || doc.Keys[0]["kid"] != "auth-2026-10" || doc.Keys[1]["kid"] != "auth-2026-11" {
		t.Fatalf("keys %v, want the retiring then the active key in set order", doc.Keys)
	}
	for _, key := range doc.Keys {
		want := base64.RawURLEncoding.EncodeToString(publics[key["kid"]])
		if key["x"] != want || key["kty"] != "OKP" || key["crv"] != "Ed25519" || key["alg"] != "EdDSA" || key["use"] != "sig" {
			t.Fatalf("key %v does not describe the custody public key", key)
		}
	}
	if got := rec.Header().Get("Cache-Control"); got != "public, max-age=300" {
		t.Fatalf("Cache-Control %q", got)
	}
	if rec.Header().Get("ETag") == "" || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("headers %v", rec.Header())
	}
}

func TestJWKSNeverPublishesRetiredOrCompromisedKeys(t *testing.T) {
	set, _ := keysOf(t,
		[2]string{"auth-old", "retired"}, [2]string{"auth-bad", "compromised"},
		[2]string{"auth-next", "pending"}, [2]string{"auth-now", "active"}, [2]string{"auth-prev", "retiring"})
	deps := WellKnownDeps{Keys: keySetFunc(func(context.Context) (token.KeySet, error) { return set, nil })}
	body := serve(t, deps, http.MethodGet, PatternJWKS, nil).Body.String()
	for _, kid := range []string{"auth-old", "auth-bad"} {
		if strings.Contains(body, kid) {
			t.Fatalf("a %s key is published: %s", kid, body)
		}
	}
	for _, kid := range []string{"auth-next", "auth-now", "auth-prev"} {
		if !strings.Contains(body, kid) {
			t.Fatalf("%s is not published: %s", kid, body)
		}
	}
}

func TestJWKSFollowsRotationWithoutARestart(t *testing.T) {
	first, _ := keysOf(t, [2]string{"auth-a", "active"})
	second, _ := keysOf(t, [2]string{"auth-a", "retiring"}, [2]string{"auth-b", "active"})
	current := first
	deps := WellKnownDeps{Keys: keySetFunc(func(context.Context) (token.KeySet, error) { return current, nil })}
	before := serve(t, deps, http.MethodGet, PatternJWKS, nil)
	current = second
	after := serve(t, deps, http.MethodGet, PatternJWKS, nil)
	if before.Body.String() == after.Body.String() || !strings.Contains(after.Body.String(), "auth-b") || !strings.Contains(after.Body.String(), "auth-a") {
		t.Fatalf("rotation not served: %s -> %s", before.Body.String(), after.Body.String())
	}
	if before.Header().Get("ETag") == after.Header().Get("ETag") {
		t.Fatal("ETag did not change with the key set")
	}
}

func TestJWKSAnswers304ForAMatchingETagAndHEADHasNoBody(t *testing.T) {
	set, _ := twoKeySet(t)
	deps := WellKnownDeps{Keys: keySetFunc(func(context.Context) (token.KeySet, error) { return set, nil })}
	etag := serve(t, deps, http.MethodGet, PatternJWKS, nil).Header().Get("ETag")
	for name, header := range map[string]string{"exact": etag, "list": `"other", ` + etag, "star": "*"} {
		rec := serve(t, deps, http.MethodGet, PatternJWKS, http.Header{"If-None-Match": {header}})
		if rec.Code != http.StatusNotModified || rec.Body.Len() != 0 {
			t.Fatalf("%s: status %d body %d bytes, want 304 and none", name, rec.Code, rec.Body.Len())
		}
	}
	if rec := serve(t, deps, http.MethodGet, PatternJWKS, http.Header{"If-None-Match": {`"stale"`}}); rec.Code != http.StatusOK {
		t.Fatalf("stale ETag status %d, want 200", rec.Code)
	}
	head := serve(t, deps, http.MethodHead, PatternJWKS, nil)
	if head.Code != http.StatusOK || head.Body.Len() != 0 || head.Header().Get("Content-Length") == "" || head.Header().Get("ETag") != etag {
		t.Fatalf("HEAD status %d body %d headers %v", head.Code, head.Body.Len(), head.Header())
	}
}

func TestJWKSCustodyFailureIs503WithoutKeyMaterialAndIsNotCached(t *testing.T) {
	deps := WellKnownDeps{Keys: keySetFunc(func(context.Context) (token.KeySet, error) {
		return token.KeySet{}, errors.New("/secret/path/key.pem unreadable")
	})}
	rec := serve(t, deps, http.MethodGet, PatternJWKS, nil)
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Cache-Control") != "no-store" || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("status %d headers %v", rec.Code, rec.Header())
	}
	if strings.Contains(rec.Body.String(), "secret") || strings.Contains(rec.Body.String(), "keys") {
		t.Fatalf("body leaks: %s", rec.Body.String())
	}
	if err := contracts.Validate(repoRoot(t), contracts.ErrorSurface, decode(t, rec.Body.Bytes())); err != nil {
		t.Fatalf("503 body fails error.v1: %v", err)
	}
}

func TestJWKSNeverCarriesPrivateOrSymmetricMembers(t *testing.T) {
	set, _ := twoKeySet(t)
	deps := WellKnownDeps{Keys: keySetFunc(func(context.Context) (token.KeySet, error) { return set, nil })}
	var doc struct{ Keys []map[string]any }
	if err := json.Unmarshal(serve(t, deps, http.MethodGet, PatternJWKS, nil).Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{"kty": true, "crv": true, "alg": true, "use": true, "kid": true, "x": true}
	for _, key := range doc.Keys {
		for member := range key {
			if !allowed[member] {
				t.Fatalf("member %q is not a public Ed25519 JWK member", member)
			}
		}
	}
}

func TestDiscoveryIsServedOnlyWithAnIssuerAndMatchesTheContract(t *testing.T) {
	set, _ := twoKeySet(t)
	keys := keySetFunc(func(context.Context) (token.KeySet, error) { return set, nil })

	if rec := serve(t, WellKnownDeps{Keys: keys}, http.MethodGet, PatternDiscovery, nil); rec.Code != http.StatusNotFound {
		t.Fatalf("no issuer: status %d, want 404", rec.Code)
	}
	if rec := serve(t, WellKnownDeps{Keys: keys}, http.MethodGet, PatternJWKS, nil); rec.Code != http.StatusOK {
		t.Fatalf("no issuer: JWKS status %d, want 200", rec.Code)
	}

	deps := WellKnownDeps{Keys: keys, IssuerURL: wellKnownIssuer}
	rec := serve(t, deps, http.MethodGet, PatternDiscovery, nil)
	if rec.Code != http.StatusOK || rec.Header().Get("Cache-Control") != "public, max-age=300" {
		t.Fatalf("status %d headers %v", rec.Code, rec.Header())
	}
	if err := contracts.Validate(repoRoot(t), contracts.DiscoverySurface, decode(t, rec.Body.Bytes())); err != nil {
		t.Fatalf("served discovery fails discovery.v1: %v", err)
	}
	if _, err := contracts.ParseDiscovery(repoRoot(t), rec.Body.Bytes(), wellKnownIssuer); err != nil {
		t.Fatalf("a client pinned to the issuer refuses the document: %v", err)
	}
	if head := serve(t, deps, http.MethodHead, PatternDiscovery, nil); head.Code != http.StatusOK || head.Body.Len() != 0 {
		t.Fatalf("HEAD status %d body %d", head.Code, head.Body.Len())
	}
}
