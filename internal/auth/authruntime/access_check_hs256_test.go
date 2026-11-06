package authruntime

import (
	"context"
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
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// With the hs256_compat class and a token issuer URL set (the workload exchange needs the URL) the session routes
// run the access check, not the HS256-only path. ops_access_token_hs256 is retired: no row of the auth-service access
// table opts in, so every row refuses a valid web (HS256) session token and a tampered one, on the production route
// sets.
func TestHS256WebTokenIsRefusedOnEveryAccessRowWithAnIssuerURL(t *testing.T) {
	cfg := classConfigWith(t, "hs256_compat", map[string]string{"AUTH_SOCIAL_LINK_ROUTES": "true"})
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
	jwks := authapi.NewJWKSDocument(authapi.JWKSDocumentConfig{Keys: signer, Logger: logger})
	sessionSet, err := sessionRoutes(cfg, logger, pool, schema, signer, mustLimits(t), jwks)
	if err != nil {
		t.Fatal(err)
	}
	credentialSet, _, err := credentialRoutes(cfg, logger, pool, schema, mustLimits(t), signer)
	if err != nil {
		t.Fatal(err)
	}
	handler := mountRoutes(append(append([]httpapi.Route{}, sessionSet...), credentialSet...))
	mounted := map[string]bool{}
	for _, route := range append(append([]httpapi.Route{}, sessionSet...), credentialSet...) {
		mounted[route.Method+" "+route.Pattern] = true
	}

	web, err := authapi.NewIssuer(cfg.Session.AccessSecret.Reveal(), cfg.Session.AccessIssuer, cfg.Session.AccessAudience)
	if err != nil {
		t.Fatal(err)
	}
	valid, err := web.Access(authapi.Grant{PrincipalID: uuid.New(), SessionID: uuid.New()})
	if err != nil {
		t.Fatal(err)
	}
	tampered := valid[:len(valid)-2] + "AA"
	if tampered == valid {
		tampered = valid[:len(valid)-2] + "BB"
	}

	rows, err := authapi.AuthServiceAccessRows()
	if err != nil {
		t.Fatal(err)
	}
	bodyProbed := false
	for _, row := range rows {
		t.Run(row.Profile, func(t *testing.T) {
			if !mounted[row.Profile] {
				t.Fatalf("no route of the production sets matches the row %q: the row would be unchecked", row.Profile)
			}
			if row.Profile == validateBodyTokenProfile {
				bodyProbed = probeValidateThroughTheBody(t, handler, valid, tampered)
				return
			}
			method, path, _ := strings.Cut(row.Profile, " ")
			path = strings.ReplaceAll(path, "{id}", uuid.NewString())
			body := "{}"
			if path == authapi.PatternChangePassword {
				body = `{"current_password":"` + strings.Repeat("a", 24) + `","new_password":"` + strings.Repeat("b", 24) + `"}`
			}
			// change-password reaches its store before it answers a bad bearer, and the store is unreachable here: 503 is the only other answer.
			for name, bearer := range map[string]string{"valid": valid, "tampered": tampered} {
				status, _ := routeAnswer(t, handler, method, path, body, bearer)
				if status != http.StatusUnauthorized && !(path == authapi.PatternChangePassword && status == http.StatusServiceUnavailable) {
					t.Errorf("a %s HS256 web token answers %d, want 401", name, status)
				}
			}
		})
	}
	if !bodyProbed {
		t.Errorf("the body-token row %s was not probed through the body", validateBodyTokenProfile)
	}
}

// validateBodyTokenProfile is the one access row whose token is the JSON body
// field token, not a bearer: the bearer probe above exempts it by name.
const validateBodyTokenProfile = "POST " + authapi.PatternCompatValidate

// probeValidateThroughTheBody sends the tokens in the body: both the valid web
// token (its class is retired) and the tampered one are go-api's refusal, 200
// valid false, before any users-store read.
func probeValidateThroughTheBody(t *testing.T, handler http.Handler, valid, tampered string) bool {
	t.Helper()
	answer := func(token string) (int, string) {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, authapi.PatternCompatValidate, strings.NewReader(`{"token":"`+token+`"}`)))
		return response.Code, response.Body.String()
	}
	if status, body := answer(valid); status != http.StatusOK || !strings.Contains(body, `"valid":false`) {
		t.Errorf("a valid HS256 web token in the body answers %d %s, want 200 valid false", status, body)
	}
	if status, body := answer(tampered); status != http.StatusOK || !strings.Contains(body, `"valid":false`) {
		t.Errorf("a tampered HS256 web token in the body answers %d %s, want 200 valid false", status, body)
	}
	return true
}
