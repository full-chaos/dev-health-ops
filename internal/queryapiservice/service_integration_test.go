//go:build integration

package queryapiservice

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestConfiguredServiceReportsItsDependenciesOnTheOperatorListener runs the real
// service with real ClickHouse and PostgreSQL: the operator /readyz is 200 and lists
// one required check per dependency class on /metrics, the query listener refuses an
// unauthenticated request instead of 404ing (the routes are mounted), and when the
// registry PostgreSQL goes away /readyz turns 503 naming that one check and nothing of
// the dependency's address (CHAOS-4724).
func TestConfiguredServiceReportsItsDependenciesOnTheOperatorListener(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = ch.Close(closeCtx)
	})
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start PostgreSQL: %v", err)
	}
	pgClosed := false
	t.Cleanup(func() {
		if pgClosed {
			return
		}
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = pg.Close(closeCtx)
	})
	public, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	jwks, err := json.Marshal(map[string]any{"keys": []map[string]any{{
		"kty": "OKP", "crv": "Ed25519", "kid": "k1", "alg": "EdDSA", "use": "sig",
		"x": base64.RawURLEncoding.EncodeToString(public),
	}}})
	if err != nil {
		t.Fatal(err)
	}
	jwksPath := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(jwksPath, jwks, 0o600); err != nil {
		t.Fatal(err)
	}

	query, operator := freeAddr(t), freeAddr(t)
	r := start(t, []string{"--query-addr", query, "--http-addr", operator}, map[string]string{
		"CLICKHOUSE_URI":               ch.URI,
		"GO_API_REGISTRY_POSTGRES_URI": pg.URI,
		"GO_API_ENVELOPE_JWKS_PATH":    jwksPath,
		"GO_API_ENVELOPE_ISSUER":       "dev-health-ops-edge",
		"GO_API_ENVELOPE_AUDIENCE":     "query-api",
		"GO_API_META_ENABLED":          "true",
	})
	waitFor(t, r, "http://"+operator+"/readyz", http.StatusOK)
	if got := waitFor(t, r, "http://"+query+"/readyz", http.StatusOK); got != "ready" {
		t.Fatalf("compat /readyz on the query listener = %q, want the old ready", got)
	}
	metrics := waitFor(t, r, "http://"+operator+"/metrics", http.StatusOK)
	for _, check := range []string{"query_clickhouse", "query_postgres", "query_jwks", "query_listener"} {
		if !strings.Contains(metrics, `dev_health_runtime_check_failed{check="`+check+`"} 0`) {
			t.Errorf("/metrics lacks a passing %s check:\n%s", check, metrics)
		}
	}
	if strings.Contains(metrics, `check="query_routes"`) {
		t.Errorf("/metrics reports the not-configured check although /query is configured")
	}
	if code, _ := get(t, "http://"+query+"/api/v1/meta"); code == http.StatusNotFound {
		t.Fatalf("/api/v1/meta is unmounted although its switch and dependencies are configured")
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer closeCancel()
	if err := pg.Close(closeCtx); err != nil {
		t.Fatalf("terminate PostgreSQL: %v", err)
	}
	pgClosed = true
	deadline := time.Now().Add(30 * time.Second)
	for {
		code, body := get(t, "http://"+operator+"/readyz")
		if code == http.StatusServiceUnavailable {
			if !strings.Contains(body, `"query_postgres"`) {
				t.Fatalf("503 body %q does not name the failing check", body)
			}
			if strings.Contains(body, "127.0.0.1") || strings.Contains(body, "dial") {
				t.Fatalf("503 body %q leaks the dependency's address or error", body)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("/readyz stayed %d after the registry PostgreSQL went away: %q", code, body)
		}
		time.Sleep(200 * time.Millisecond)
	}
	// The query listener's compat /readyz names the class in the old plain text, nothing more.
	if code, body := get(t, "http://"+query+"/readyz"); code != http.StatusServiceUnavailable || body != "not ready: postgres" {
		t.Fatalf("compat /readyz on the query listener = %d %q, want 503 %q", code, body, "not ready: postgres")
	}
	// Liveness is independent of the dependency: the process stays live.
	if code, _ := get(t, "http://"+operator+"/healthz"); code != http.StatusOK {
		t.Fatalf("/healthz = %d with a dependency down: liveness and readiness must stay distinct", code)
	}
	stop(t, r)
}
