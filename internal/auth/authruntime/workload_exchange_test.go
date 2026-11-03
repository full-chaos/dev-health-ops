package authruntime

import (
	"context"
	"encoding/pem"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/workloadexchange"
)

// exchangeEnv is a configuration whose database is unreachable: a request
// that passes the shape check would need TokenReview, so these tests send
// only shape faults and never reach a store.
func exchangeEnv(t *testing.T, on bool) map[string]string {
	t.Helper()
	env := map[string]string{
		authconfig.EnvDatabaseURI:        "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile:     "/nonexistent",
		authconfig.EnvSigningKeyID:       "exchange-test",
		authconfig.EnvRateLimitPerSecond: "100000",
		authconfig.EnvRateLimitBurst:     "100000",
		authconfig.EnvAPIAddress:         "127.0.0.1:0",
		authconfig.EnvOperatorAddress:    "127.0.0.1:1",
		authconfig.EnvTokenIssuerURL:     "https://issuer.example.test",
	}
	if !on {
		return env
	}
	api := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(api.Close)
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: api.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	reviewer := filepath.Join(dir, "token")
	if err := os.WriteFile(reviewer, []byte("reviewer"), 0o600); err != nil {
		t.Fatal(err)
	}
	env[authconfig.EnvWorkloadExchange] = "true"
	env[authconfig.EnvWorkloadTrustDomain] = "cluster.example.test"
	env[authconfig.EnvWorkloadClusterIssuer] = "https://kubernetes.default.svc.cluster.local"
	env[authconfig.EnvWorkloadKubeAPIURL] = api.URL
	env[authconfig.EnvWorkloadKubeCAFile] = ca
	env[authconfig.EnvWorkloadReviewerTokenFile] = reviewer
	return env
}

func configuredAPI(t *testing.T, env map[string]string) (http.Handler, error) {
	t.Helper()
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	components, err := configure(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		for _, component := range components {
			_ = component.Shutdown(context.Background())
		}
	})
	for _, component := range components {
		if announced, ok := component.(announcedListener); ok {
			if server, ok := announced.listener.(*httpapi.Server); ok {
				return server.Handler(), nil
			}
		}
	}
	t.Fatal("configure built no API listener")
	return nil, nil
}

func exchangeShapeFault(t *testing.T, api http.Handler, method string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, workloadexchange.Pattern, strings.NewReader("grant_type=x"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	return rec
}

// Off (the default), the exchange path is not served at any method.
func TestTheWorkloadExchangeIsNotMountedByDefault(t *testing.T) {
	api, err := configuredAPI(t, exchangeEnv(t, false))
	if err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions} {
		if rec := exchangeShapeFault(t, api, method); rec.Code != http.StatusNotFound {
			t.Errorf("%s %s answered %d with the exchange off, want 404", method, workloadexchange.Pattern, rec.Code)
		}
	}
}

func TestTheWorkloadExchangeIsMountedWhenOn(t *testing.T) {
	api, err := configuredAPI(t, exchangeEnv(t, true))
	if err != nil {
		t.Fatal(err)
	}
	rec := exchangeShapeFault(t, api, http.MethodPost)
	if rec.Code != http.StatusBadRequest || rec.Body.String() != "{\"error\":\"invalid_request\"}\n" {
		t.Fatalf("POST answered %d %q, want the exchange's 400 invalid_request", rec.Code, rec.Body.String())
	}
	if rec := exchangeShapeFault(t, api, http.MethodGet); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET answered %d, want 405", rec.Code)
	}
}

// On, a CA bundle that cannot be read refuses startup; the refusal carries no
// path.
func TestTheWorkloadExchangeRefusesToStartWithoutItsCA(t *testing.T) {
	env := exchangeEnv(t, true)
	env[authconfig.EnvWorkloadKubeCAFile] = filepath.Join(t.TempDir(), "absent-ca.crt")
	_, err := configuredAPI(t, env)
	if err == nil {
		t.Fatal("configure accepted an unreadable CA bundle")
	}
	if strings.Contains(err.Error(), "absent-ca.crt") {
		t.Fatalf("the refusal carries the path: %v", err)
	}
}

// AUTH_WORKLOAD_BOOTSTRAP alone mounts the route with no Kubernetes setting:
// the bootstrap subject is served and the JWT subject is a shape fault.
func TestTheWorkloadExchangeIsMountedForTheBootstrapSubjectAlone(t *testing.T) {
	env := exchangeEnv(t, false)
	env[authconfig.EnvWorkloadBootstrap] = "true"
	api, err := configuredAPI(t, env)
	if err != nil {
		t.Fatal(err)
	}
	post := func(subjectType string) *httptest.ResponseRecorder {
		body := "grant_type=" + url.QueryEscape(workloadexchange.GrantTypeTokenExchange) +
			"&subject_token_type=" + url.QueryEscape(subjectType) + "&subject_token=not-a-bootstrap-value&audience=auth-service"
		req := httptest.NewRequest(http.MethodPost, workloadexchange.Pattern, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		api.ServeHTTP(rec, req)
		return rec
	}
	if rec := post(workloadexchange.TokenTypeWorkloadBootstrap); rec.Code != http.StatusBadRequest || rec.Body.String() != "{\"error\":\"invalid_grant\"}\n" {
		t.Fatalf("a malformed bootstrap value: %d %q, want the uniform 400 invalid_grant", rec.Code, rec.Body.String())
	}
	if rec := post(workloadexchange.TokenTypeJWT); rec.Code != http.StatusBadRequest || rec.Body.String() != "{\"error\":\"invalid_request\"}\n" {
		t.Fatalf("a JWT subject with the Kubernetes subject off: %d %q, want 400 invalid_request", rec.Code, rec.Body.String())
	}
}
