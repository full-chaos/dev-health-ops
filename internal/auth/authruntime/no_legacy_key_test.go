package authruntime

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

const noLegacyKeyRateLimitKey = "no-legacy-key-rate-limit-key-0123456789"

func noLegacyKeyConfig(t *testing.T) authconfig.Config {
	t.Helper()
	env := compatEnvironment(map[string]string{
		authconfig.EnvDatabaseURI:         "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile:      "/nonexistent",
		authconfig.EnvSigningKeyID:        "no-legacy-key-test",
		authconfig.EnvUsersOwned:          "true",
		authconfig.EnvAccessTokenClass:    authconfig.AccessClassEdDSA,
		authconfig.EnvAccessTokenAudience: "dev-health-api",
		authconfig.EnvTokenIssuerURL:      accessCheckIssuer,
		authconfig.EnvRateLimitKey:        noLegacyKeyRateLimitKey,
	})
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatalf("eddsa with no HS256 key does not load: %v", err)
	}
	return cfg
}

func TestEdDSAWithoutTheLegacyKeyKeysTheLimiterFromTheRateLimitKey(t *testing.T) {
	if got := rateLimitSecret(noLegacyKeyConfig(t)); got != noLegacyKeyRateLimitKey {
		t.Fatal("the limiter is not keyed from AUTH_RATE_LIMIT_KEY")
	}
}

// The production builders with no AUTH_JWT_SECRET_KEY under eddsa: the session
// routes mount, an EdDSA bearer reaches the (unreachable) session store, an
// HS256 bearer is a 401 and no route panics, and the validate route is
// mounted (rule 1: legacy served it; rule 3: a missing key is no unmount).
func TestEdDSASessionRoutesBuildAndServeWithoutTheLegacyKey(t *testing.T) {
	cfg := noLegacyKeyConfig(t)
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
	issuer, err := newAccessIssuer(cfg, signer)
	if err != nil {
		t.Fatalf("the production issuer does not build without the legacy key: %v", err)
	}
	routes, err := sessionRoutes(cfg, logger, pool, schema, signer, mustLimits(t), nil)
	if err != nil {
		t.Fatalf("the session routes do not build without the legacy key: %v", err)
	}
	validate := 0
	for _, route := range routes {
		if route.Pattern == authapi.PatternCompatValidate {
			validate++
		}
	}
	if validate != 1 {
		t.Fatalf("the validate route is mounted %d times without the legacy key, want once", validate)
	}
	grant := authapi.Grant{PrincipalID: uuid.New(), SessionID: uuid.New()}
	if _, err := issuer.Access(grant); err != nil {
		t.Fatalf("the production issuer mints no access token without the legacy key: %v", err)
	}
	minter, err := authapi.NewEdDSAIssuer(authapi.AccessTokenAudience, signer, accessCheckIssuer)
	if err != nil {
		t.Fatal(err)
	}
	eddsa, err := minter.Access(grant)
	if err != nil {
		t.Fatal(err)
	}
	legacy := legacyAccess(t, testAccessSecret, "dev-health-ops", "dev-health-api", grant)
	handler := mountRoutes(routes)
	if status, reason := routeAnswer(t, handler, http.MethodGet, authapi.PatternCurrent, "", eddsa); status != http.StatusServiceUnavailable || reason != "store_unavailable" {
		t.Errorf("EdDSA bearer: %d %q; want 503 store_unavailable", status, reason)
	}
	if status, _ := routeAnswer(t, handler, http.MethodGet, authapi.PatternCurrent, "", legacy); status != http.StatusUnauthorized {
		t.Errorf("HS256 bearer: %d; want 401", status)
	}
}

// Product rules 1 and 3: the production builder mounts POST /api/v1/auth/validate
// under eddsa with or without AUTH_JWT_SECRET_KEY (the routed check needs no HS256
// verifier), and logs no not-mounted line either way.
func TestTheValidateRouteMountsWithOrWithoutTheLegacyKey(t *testing.T) {
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
	const line = "auth validate route not mounted"
	for name, cfg := range map[string]authconfig.Config{
		"no legacy key": noLegacyKeyConfig(t),
		"legacy key":    accessCheckConfig(t, accessCheckIssuer),
	} {
		var logs bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		routes, err := sessionRoutes(cfg, logger, pool, schema, signer, mustLimits(t), nil)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		mounted := 0
		for _, route := range routes {
			if route.Method == http.MethodPost && route.Pattern == authapi.PatternCompatValidate {
				mounted++
			}
		}
		lines := 0
		scanner := bufio.NewScanner(&logs)
		for scanner.Scan() {
			var record struct {
				Msg   string `json:"msg"`
				Route string `json:"route"`
			}
			if json.Unmarshal(scanner.Bytes(), &record) == nil && strings.HasPrefix(record.Msg, line) {
				if record.Route != authapi.PatternCompatValidate {
					t.Errorf("%s: the line names route %q, want %q", name, record.Route, authapi.PatternCompatValidate)
				}
				lines++
			}
		}
		want := [2]int{1, 0}
		if mounted != want[0] || lines != want[1] {
			t.Errorf("%s: validate mounted %d times, %d not-mounted lines; want %d and %d", name, mounted, lines, want[0], want[1])
		}
	}
}
