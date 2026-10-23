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
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/keyrotation"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/token"
	"github.com/jackc/pgx/v5/pgxpool"
)

const wellKnownTestIssuer = "https://auth.example.test"

// staticKeyStates is a signing_keys table a test sets (no database).
type staticKeyStates []token.KeyStateRow

func (s staticKeyStates) KeyStates(context.Context) ([]token.KeyStateRow, error) { return s, nil }

// useKeyStates makes configure read key states from source for this test.
func useKeyStates(t *testing.T, source token.KeyStateSource) {
	t.Helper()
	previous := keyStateSource
	keyStateSource = func(*pgxpool.Pool, authschema.ValidatedIdentifier) token.KeyStateSource { return source }
	t.Cleanup(func() { keyStateSource = previous })
}

// wellKnownHandler builds the API handler through configure over a key-set
// manifest (retired, retiring and active keys), with or without an issuer,
// and an empty key state table (custody's states stand).
func wellKnownHandler(t *testing.T, issuer string) http.Handler {
	t.Helper()
	useKeyStates(t, staticKeyStates(nil))
	return wellKnownStack(t, issuer, t.TempDir())
}

func wellKnownStack(t *testing.T, issuer, dir string) http.Handler {
	t.Helper()
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

// TestProductionStackPublishesTheDurableKeyStates: configure reads the key
// states of the rotation state machine; a key its table marks compromised
// leaves the JWKS at once and a key it does not hold is not published.
func TestProductionStackPublishesTheDurableKeyStates(t *testing.T) {
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := keyStateSource(nil, schema).(*keyrotation.Reader); !ok {
		t.Fatal("configure does not read key states from the signing_keys table")
	}
	dir := t.TempDir()
	states := &mutableKeyStates{}
	useKeyStates(t, states)
	handler := wellKnownStack(t, wellKnownTestIssuer, dir)
	custody, err := token.CustodyKeys(context.Background(), token.FileConfig{KeySetPath: filepath.Join(dir, "keyset.json")})
	if err != nil {
		t.Fatal(err)
	}
	public := map[string]token.Key{}
	for _, key := range custody {
		public[key.KeyID] = key
	}
	states.set(staticKeyStates{
		{KeyID: "auth-active", State: token.StateActive, PublicKey: public["auth-active"].PublicKey},
		{KeyID: "auth-retiring", State: token.StateCompromised, PublicKey: public["auth-retiring"].PublicKey},
	})
	jwks := wellKnownGet(handler, http.MethodGet, "/.well-known/jwks.json")
	body := jwks.Body.String()
	if jwks.Code != http.StatusOK || !strings.Contains(body, "auth-active") || strings.Contains(body, "auth-retiring") || strings.Contains(body, "auth-retired") {
		t.Fatalf("JWKS = %d %s; want only auth-active", jwks.Code, body)
	}
}

type mutableKeyStates struct {
	mu   sync.Mutex
	rows staticKeyStates
}

func (m *mutableKeyStates) set(rows staticKeyStates) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows = rows
}

func (m *mutableKeyStates) KeyStates(context.Context) ([]token.KeyStateRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rows, nil
}
