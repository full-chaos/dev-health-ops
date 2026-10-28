package authruntime

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

const accessCheckIssuer = "https://auth.example.test"

func accessCheckConfig(t *testing.T, issuerURL string) authconfig.Config {
	t.Helper()
	env := compatEnvironment(map[string]string{
		authconfig.EnvDatabaseURI:    "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile: "/nonexistent",
		authconfig.EnvSigningKeyID:   "access-check-test",
		authconfig.EnvUsersOwned:     "true",
	})
	if issuerURL != "" {
		env[authconfig.EnvTokenIssuerURL] = issuerURL
	}
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func mount(routes []httpapi.Route) http.Handler {
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(route.Handler))
	}
	return mux
}

func answer(t *testing.T, h http.Handler, method, path, body, bearer string) (int, string) {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+bearer)
	response := httptest.NewRecorder()
	h.ServeHTTP(response, request)
	var envelope struct {
		ReasonCode string `json:"reason_code"`
	}
	_ = json.Unmarshal(response.Body.Bytes(), &envelope)
	return response.Code, envelope.ReasonCode
}

// The production constructors (sessionRoutes, credentialRoutes) build the
// access check from the signer's key set when the token issuer is set: an
// auth-service EdDSA token passes validation and reaches the session store,
// which the test leaves unreachable (503); without the check the same token is
// a 401 at validation.
func TestProductionRoutesConstructTheAccessCheck(t *testing.T) {
	dir := t.TempDir()
	writeSigningKey(t, dir, "k1.pem")
	signer := token.NewFileSigner(token.FileConfig{KeyPath: filepath.Join(dir, "k1.pem"), KeyID: "k1"})
	pool, err := pgxpool.New(context.Background(), "postgres://auth@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	minter, err := authapi.NewEdDSAIssuer(testAccessSecret, "dev-health-ops", authapi.AccessTokenAudience, signer, accessCheckIssuer)
	if err != nil {
		t.Fatal(err)
	}
	bearer, err := minter.Access(authapi.Grant{PrincipalID: uuid.New(), SessionID: uuid.New()})
	if err != nil {
		t.Fatal(err)
	}
	passwordBody := `{"current_password":"current-password-1","new_password":"Brand-new-pass-77!"}`

	for name, issuerURL := range map[string]string{"issuer set": accessCheckIssuer, "issuer unset": ""} {
		cfg := accessCheckConfig(t, issuerURL)
		sessionSet, err := sessionRoutes(cfg, logger, pool, schema, signer, mustLimits(t))
		if err != nil {
			t.Fatal(err)
		}
		credentialSet, _, err := credentialRoutes(cfg, logger, pool, schema, mustLimits(t), signer)
		if err != nil {
			t.Fatal(err)
		}
		wantStatus, wantReason := http.StatusServiceUnavailable, "store_unavailable"
		if issuerURL == "" {
			wantStatus, wantReason = http.StatusUnauthorized, ""
		}
		for route, call := range map[string]func() (int, string){
			"session current": func() (int, string) {
				return answer(t, mount(sessionSet), http.MethodGet, authapi.PatternCurrent, "", bearer)
			},
			"change password": func() (int, string) {
				return answer(t, mount(credentialSet), http.MethodPost, authapi.PatternChangePassword, passwordBody, bearer)
			},
		} {
			status, reason := call()
			if status != wantStatus || (wantReason != "" && reason != wantReason) {
				t.Errorf("%s, %s: %d %q; want %d %q", name, route, status, reason, wantStatus, wantReason)
			}
		}
	}
}
