package authruntime

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

const (
	goAPITestKey    = "fixture-key-fixture-key-fixture-key-"
	goAPITestOrigin = "https://app.example"
)

type goAPIScopeStore struct{}

func (goAPIScopeStore) UserState(context.Context, uuid.UUID) (policy.UserState, bool, error) {
	return policy.UserState{IsActive: true}, true, nil
}

func (goAPIScopeStore) Membership(context.Context, uuid.UUID, uuid.UUID) (string, bool, error) {
	return "", false, nil
}

func (goAPIScopeStore) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	return nil, nil
}

// goAPIFront is go-api's real handler with the local_login proxy on and
// upstream as auth-service.
func goAPIFront(t *testing.T, upstream string) http.Handler {
	t.Helper()
	proxy, err := apiservice.ParseAuthProxyConfig(func(name string) (string, bool) {
		switch name {
		case "AUTH_PROXY_URL":
			return upstream, true
		case "AUTH_PROXY_SESSIONS", "AUTH_PROXY_LOCAL_LOGIN":
			return "true", true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	verifier, err := edgetoken.New(goAPITestKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(goAPITestKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(verifier, goAPIScopeStore{}, logger)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	routes := apiservice.Routes(apiservice.Deps{Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, logger), Verifier: verifier, Signer: signer,
		AuthProxy: proxy, Limits: httpapi.NewMemoryCounters(time.Now)}, logger)
	server, err := apiservice.NewServer(config.Config{APIAddress: "127.0.0.1:0", CORSAllowedOrigins: []string{goAPITestOrigin}}, logger, routes)
	if err != nil {
		t.Fatal(err)
	}
	return server.Handler()
}

func goAPILogin(handler http.Handler, email, password string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(`{"email":"`+email+`","password":"`+password+`"}`))
	request.RemoteAddr = "192.0.2.10:4711"
	request.Header.Set("Origin", goAPITestOrigin)
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

// loginKeyUnsetService is the real auth-service route set with the lockout
// key unset (phase 2, or the phase 3 -> 2 rollback config) behind its real
// server, plus the count of limiter hits it took.
func loginKeyUnsetService(t *testing.T) (http.Handler, *atomic.Int64) {
	t.Helper()
	hits := &atomic.Int64{}
	limits, err := authapi.NewLimits(countingProbe{hits}, testAccessSecret)
	if err != nil {
		t.Fatal(err)
	}
	cfg := loginConfig(t, false)
	routes := loginSessionRoutes(t, cfg, limits)
	options := apiServerOptions(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), routes)
	options.Address = "127.0.0.1:0"
	server, err := httpapi.NewServer(options)
	if err != nil {
		t.Fatal(err)
	}
	return server.Handler(), hits
}

type countingProbe struct{ hits *atomic.Int64 }

func (p countingProbe) Increment(context.Context, httpapi.Hit) (int64, error) {
	p.hits.Add(1)
	return 0, nil
}

func (p countingProbe) Peek(context.Context, httpapi.Hit) (int64, error) {
	p.hits.Add(1)
	return 0, nil
}

func (p countingProbe) Backend() string { return "probe" }

// A go-api that proxies local login against an auth-service pod whose lockout
// key is unset (a phase 2 pod meeting the new go-api, or the phase 3 -> 2
// rollback config) answers 503 + Retry-After: 1, never the route-missing 404.
func TestGoAPIProxiedLoginAnswers503WhileTheLockoutKeyIsUnsetAlsoOnTheRollbackConfig(t *testing.T) {
	service, hits := loginKeyUnsetService(t)
	upstream := httptest.NewServer(service)
	defer upstream.Close()
	front := goAPIFront(t, upstream.URL)
	for round := 0; round < 3; round++ {
		rec := goAPILogin(front, "a@example.com", "long-enough-1")
		if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
			t.Fatalf("round %d: status %d Retry-After %q body %q; want 503 and 1", round, rec.Code, rec.Header().Get("Retry-After"), rec.Body.String())
		}
	}
	if hits.Load() != 0 {
		t.Fatalf("auth-service limiter hit %d times; want 0", hits.Load())
	}
}
