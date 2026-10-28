package authruntime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
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

func mountRoutes(routes []httpapi.Route) http.Handler {
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(route.Handler))
	}
	return mux
}

func routeAnswer(t *testing.T, h http.Handler, method, path, body, bearer string) (int, string) {
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
	passwordBody, err := json.Marshal(map[string]string{"current_password": strings.Repeat("a", 24), "new_password": strings.Repeat("b", 24)})
	if err != nil {
		t.Fatal(err)
	}

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
				return routeAnswer(t, mountRoutes(sessionSet), http.MethodGet, authapi.PatternCurrent, "", bearer)
			},
			"change password": func() (int, string) {
				return routeAnswer(t, mountRoutes(credentialSet), http.MethodPost, authapi.PatternChangePassword, string(passwordBody), bearer)
			},
		} {
			status, reason := call()
			if status != wantStatus || (wantReason != "" && reason != wantReason) {
				t.Errorf("%s, %s: %d %q; want %d %q", name, route, status, reason, wantStatus, wantReason)
			}
		}
	}
}

type kidLog struct{ kids []string }

func (k *kidLog) RecordSigningKid(_ context.Context, _ uuid.UUID, kid string) error {
	k.kids = append(k.kids, kid)
	return nil
}

func tokenAlg(t *testing.T, compact string) string {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(strings.Split(compact, ".")[0])
	if err != nil {
		t.Fatal(err)
	}
	var header struct {
		Alg string `json:"alg"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		t.Fatal(err)
	}
	return header.Alg
}

// The password routes mint the access token of the configured class and
// record the signing kid of the session, like the session routes.
func TestCredentialIssuerFollowsTheConfiguredAccessClass(t *testing.T) {
	dir := t.TempDir()
	writeSigningKey(t, dir, "k1.pem")
	signer := token.NewFileSigner(token.FileConfig{KeyPath: filepath.Join(dir, "k1.pem"), KeyID: "k1"})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for class, want := range map[string]struct{ alg, kid string }{"hs256_compat": {"HS256", ""}, "eddsa": {"EdDSA", "k1"}} {
		kids := &kidLog{}
		issuer, err := credentialIssuer(classConfig(t, class), logger, signer, kids)
		if err != nil {
			t.Fatal(err)
		}
		compact, err := issuer.AccessContext(context.Background(), authapi.Grant{PrincipalID: uuid.New(), SessionID: uuid.New()})
		if err != nil {
			t.Fatalf("%s: %v", class, err)
		}
		if got := tokenAlg(t, compact); got != want.alg {
			t.Errorf("%s: the password routes minted alg %s, want %s", class, got, want.alg)
		}
		if len(kids.kids) != 1 || kids.kids[0] != want.kid {
			t.Errorf("%s: recorded kids %v, want [%q]", class, kids.kids, want.kid)
		}
	}
}

// The production builder: under the eddsa class (audience auth-service) the legacy
// HS256 verifier checks go-api's audience; under hs256_compat it checks the
// configured audience, as before.
func TestTheLegacyAudienceOfTheProductionIssuerFollowsTheClass(t *testing.T) {
	cfg := classConfig(t, "eddsa")
	mint := func(audience string) string {
		hs, err := authapi.NewIssuer(cfg.Session.AccessSecret.Reveal(), cfg.Session.AccessIssuer, audience)
		if err != nil {
			t.Fatal(err)
		}
		compact, err := hs.Access(authapi.Grant{PrincipalID: uuid.New(), SessionID: uuid.New()})
		if err != nil {
			t.Fatal(err)
		}
		return compact
	}
	signer := &stubSigner{}
	eddsa, err := newAccessIssuer(cfg, signer)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := eddsa.Verify(mint(contracts.PlatformAPIAudience)); err != nil {
		t.Errorf("eddsa class: a go-api HS256 token is refused: %v", err)
	}
	if _, _, err := eddsa.Verify(mint("auth-service")); err == nil {
		t.Error("eddsa class: an HS256 token of the EdDSA audience is accepted")
	}
	hs, err := newAccessIssuer(classConfig(t, "hs256_compat"), signer)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := hs.Verify(mint("auth-service")); err != nil {
		t.Errorf("hs256_compat: the configured audience is refused: %v", err)
	}
	if _, _, err := hs.Verify(mint(contracts.PlatformAPIAudience)); err == nil {
		t.Error("hs256_compat: a token of another audience is accepted")
	}
}
