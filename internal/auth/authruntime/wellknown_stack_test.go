package authruntime

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

const wellKnownTestIssuer = "https://auth.example.test"

// wellKnownHandler builds the API handler through configure over a key-set
// manifest (retired, retiring and active keys), with or without an issuer.
func wellKnownHandler(t *testing.T, issuer string) http.Handler {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{"retired.pem", "retiring.pem", "active.pem"} {
		writeSigningKey(t, dir, name)
	}
	manifest := filepath.Join(dir, "keyset.json")
	body := `{"keys":[{"kid":"auth-retired","state":"retired","file":"retired.pem"},` +
		`{"kid":"auth-retiring","state":"retiring","file":"retiring.pem"},{"kid":"auth-active","state":"active","file":"active.pem"}]}`
	if err := os.WriteFile(manifest, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		authconfig.EnvDatabaseURI:        "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeySetFile:  manifest,
		authconfig.EnvRateLimitPerSecond: "100000",
		authconfig.EnvRateLimitBurst:     "100000",
		authconfig.EnvAPIAddress:         "127.0.0.1:0",
		authconfig.EnvOperatorAddress:    "127.0.0.1:1",
	}
	if issuer != "" {
		env[authconfig.EnvTokenIssuerURL] = issuer
	}
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	components, err := configure(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	t.Cleanup(func() {
		for _, component := range components {
			_ = component.Shutdown(context.Background())
		}
	})
	for _, component := range components {
		if announced, ok := component.(announcedListener); ok {
			if server, ok := announced.listener.(*httpapi.Server); ok {
				return server.Handler()
			}
		}
	}
	t.Fatal("configure built no API listener")
	return nil
}

func wellKnownGet(handler http.Handler, method, path string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(method, path, nil))
	return recorder
}

func jsonOf(t *testing.T, raw []byte) any {
	t.Helper()
	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("body is not JSON: %v: %s", err, raw)
	}
	return document
}

// TestProductionStackServesTheJWKSAndDiscovery: the binary's own configure
// serves the key set (overlap and active keys, never the retired one) and the
// discovery document, both valid against their contracts.
func TestProductionStackServesTheJWKSAndDiscovery(t *testing.T) {
	handler := wellKnownHandler(t, wellKnownTestIssuer)
	root := "../../.."

	jwks := wellKnownGet(handler, http.MethodGet, "/.well-known/jwks.json")
	if jwks.Code != http.StatusOK || jwks.Header().Get("Cache-Control") != "public, max-age=300" {
		t.Fatalf("JWKS = %d, Cache-Control %q", jwks.Code, jwks.Header().Get("Cache-Control"))
	}
	if err := contracts.Validate(root, "jwks.v1", jsonOf(t, jwks.Body.Bytes())); err != nil {
		t.Fatalf("served JWKS fails jwks.v1: %v", err)
	}
	body := jwks.Body.String()
	if !strings.Contains(body, "auth-retiring") || !strings.Contains(body, "auth-active") || strings.Contains(body, "auth-retired") {
		t.Fatalf("published keys wrong: %s", body)
	}
	if strings.Contains(body, "PRIVATE") || strings.Contains(body, `"d"`) {
		t.Fatalf("private material in the JWKS: %s", body)
	}

	discovery := wellKnownGet(handler, http.MethodGet, "/.well-known/openid-configuration")
	if discovery.Code != http.StatusOK {
		t.Fatalf("discovery = %d", discovery.Code)
	}
	if _, err := contracts.ParseDiscovery(root, discovery.Body.Bytes(), wellKnownTestIssuer); err != nil {
		t.Fatalf("discovery refused by a client pinned to the issuer: %v", err)
	}
	for _, path := range []string{"/.well-known/jwks.json", "/.well-known/openid-configuration"} {
		if head := wellKnownGet(handler, http.MethodHead, path); head.Code != http.StatusOK || head.Body.Len() != 0 {
			t.Fatalf("HEAD %s = %d with %d body bytes", path, head.Code, head.Body.Len())
		}
	}
}

func TestProductionStackServesNoDiscoveryWithoutAnIssuer(t *testing.T) {
	handler := wellKnownHandler(t, "")
	if rec := wellKnownGet(handler, http.MethodGet, "/.well-known/openid-configuration"); rec.Code != http.StatusNotFound {
		t.Fatalf("discovery without an issuer = %d, want 404", rec.Code)
	}
	if rec := wellKnownGet(handler, http.MethodGet, "/.well-known/jwks.json"); rec.Code != http.StatusOK {
		t.Fatalf("JWKS without an issuer = %d, want 200", rec.Code)
	}
}
