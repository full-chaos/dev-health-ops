package authapi

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func publishedJWKS(t *testing.T, keys KeySetSource) map[string]string {
	t.Helper()
	routes := WellKnownRoutes(WellKnownDeps{Keys: keys})
	var out map[string]string
	for _, route := range routes {
		if route.Method != http.MethodGet || route.Pattern != PatternJWKS {
			continue
		}
		response := httptest.NewRecorder()
		route.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, PatternJWKS, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("jwks = %d", response.Code)
		}
		var doc struct {
			Keys []struct{ Kid, X string } `json:"keys"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &doc); err != nil {
			t.Fatal(err)
		}
		out = map[string]string{}
		for _, key := range doc.Keys {
			out[key.Kid] = key.X
		}
	}
	if out == nil {
		t.Fatal("no jwks route")
	}
	return out
}

func TestTheVerifySetIsTheJWKSSet(t *testing.T) {
	signer, dir, _ := fileSigner(t)
	for _, kid := range []string{"k3", "k4", "k5"} {
		writeEd25519Key(t, dir, kid)
	}
	writeKeySet(t, dir, "k1", "active", "k2", "pending", "k3", "retiring", "k4", "retired", "k5", "compromised")
	jwks := publishedJWKS(t, signer)
	if len(jwks) != 3 {
		t.Fatalf("the jwks publishes %d keys, want pending+active+retiring = 3", len(jwks))
	}
	resolver := publishedKeys{source: signer}
	for _, kid := range []string{"k1", "k2", "k3", "k4", "k5", "unknown"} {
		key, err := resolver.ResolveKey(kid)
		if err != nil {
			t.Fatalf("%s: %v", kid, err)
		}
		x, published := jwks[kid]
		if published != (key != nil) {
			t.Errorf("%s: published=%v but resolved=%v", kid, published, key != nil)
		}
		if published && base64.RawURLEncoding.EncodeToString(key) != x {
			t.Errorf("%s: resolved key differs from the published key", kid)
		}
	}
}

func TestATokenOfTheSuccessorKeyVerifiesAndOneOfARetiredKeyIsRefused(t *testing.T) {
	a := newAccessFixture(t)
	g := a.grant()
	underK1 := a.bearer(g)
	if r := a.do(http.MethodGet, "/v1/sessions/current", "", underK1); r.Code != http.StatusOK {
		t.Fatalf("k1 active: %d %s", r.Code, r.Body)
	}

	writeKeySet(t, a.dir, "k1", "retiring", "k2", "active")
	underK2 := a.bearer(g)
	if r := a.do(http.MethodGet, "/v1/sessions/current", "", underK2); r.Code != http.StatusOK {
		t.Fatalf("successor k2 active: %d %s", r.Code, r.Body)
	}
	if r := a.do(http.MethodGet, "/v1/sessions/current", "", underK1); r.Code != http.StatusOK {
		t.Fatalf("k1 retiring is still published and must verify: %d %s", r.Code, r.Body)
	}

	writeKeySet(t, a.dir, "k1", "retired", "k2", "active")
	expectError(t, a.do(http.MethodGet, "/v1/sessions/current", "", underK1), http.StatusUnauthorized, "bad_signature")
	if r := a.do(http.MethodGet, "/v1/sessions/current", "", underK2); r.Code != http.StatusOK {
		t.Fatalf("k2 after k1 retired: %d %s", r.Code, r.Body)
	}

	writeKeySet(t, a.dir, "k1", "compromised", "k2", "active")
	expectError(t, a.do(http.MethodGet, "/v1/sessions/current", "", underK1), http.StatusUnauthorized, "bad_signature")
}
