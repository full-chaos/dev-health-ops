package authruntime

import (
	"context"
	"io"
	"log/slog"
	"net/http"
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
// run the access check, not the HS256-only path. Every row of the auth-service access table must still pass a valid
// web (HS256) session token and refuse a tampered one, on the production route sets.
func TestHS256WebTokenPassesEveryAccessRowWithAnIssuerURL(t *testing.T) {
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
	sessionSet, err := sessionRoutes(cfg, logger, pool, schema, signer, mustLimits(t))
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
	for _, row := range rows {
		t.Run(row.Profile, func(t *testing.T) {
			if !mounted[row.Profile] {
				t.Fatalf("no route of the production sets matches the row %q: the row would be unchecked", row.Profile)
			}
			method, path, _ := strings.Cut(row.Profile, " ")
			path = strings.ReplaceAll(path, "{id}", uuid.NewString())
			body := "{}"
			if path == authapi.PatternChangePassword {
				body = `{"current_password":"` + strings.Repeat("a", 24) + `","new_password":"` + strings.Repeat("b", 24) + `"}`
			}
			if status, reason := routeAnswer(t, handler, method, path, body, valid); status == http.StatusUnauthorized {
				t.Errorf("a valid HS256 web token is refused: %d %q", status, reason)
			}
			// change-password reaches its store before it answers a bad bearer, and the store is unreachable here: 503 is the only other answer.
			status, _ := routeAnswer(t, handler, method, path, body, tampered)
			if status != http.StatusUnauthorized && !(path == authapi.PatternChangePassword && status == http.StatusServiceUnavailable) {
				t.Errorf("a tampered HS256 web token answers %d, want 401", status)
			}
		})
	}
}
