//go:build integration

package lockorder

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/servicecred"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func randomText(n int) (string, error) {
	raw := make([]byte, n)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// startAuthPlane serves auth-service's compatibility routes (go-api's refresh,
// the session issue route) as auth-service wires them (authruntime
// compatRoutes), over the auth stores on the runtime role.
func (e *env) startAuthPlane(ctx context.Context, schema authschema.ValidatedIdentifier, logger *slog.Logger, jwtKey string) error {
	server, err := e.authPlaneOn(e.runtime, schema, logger, jwtKey)
	if err != nil {
		return err
	}
	e.authServer = server
	// The same routes on a one-connection pool (AUTH_DATABASE_MAX_CONNS=1 is a
	// valid setting): a refresh must never need a second connection while its
	// exchange transaction holds the only one.
	cfg := e.runtime.Config().Copy()
	cfg.MaxConns = 1
	if e.runtimeOne, err = pgxpool.NewWithConfig(ctx, cfg); err != nil {
		return fmt.Errorf("one-connection pool: %w", err)
	}
	closers = append(closers, e.runtimeOne.Close)
	e.authServerOne, err = e.authPlaneOn(e.runtimeOne, schema, logger, jwtKey)
	return err
}

func (e *env) authPlaneOn(pool *pgxpool.Pool, schema authschema.ValidatedIdentifier, logger *slog.Logger, jwtKey string) (*httptest.Server, error) {
	successor, err := randomText(32)
	if err != nil {
		return nil, err
	}
	sessions, err := session.New(session.Config{Pool: pool, Schema: schema, Logger: logger, UsersOwned: true,
		SuccessorKey: contracts.NewSecretString(successor)})
	if err != nil {
		return nil, fmt.Errorf("session store: %w", err)
	}
	issuer, err := authapi.NewIssuer(jwtKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		return nil, fmt.Errorf("issuer: %w", err)
	}
	limitKey, err := randomText(32)
	if err != nil {
		return nil, err
	}
	limits, err := authapi.NewLimits(httpapi.NewMemoryCounters(nil), limitKey)
	if err != nil {
		return nil, fmt.Errorf("limits: %w", err)
	}
	runtime := pool
	routes, err := authapi.CompatRoutes(authapi.CompatDeps{
		Sessions: sessions, Users: authapi.PGUsers{Pool: runtime}, Issuer: issuer, Logger: logger,
		Projector: e.identity, ServiceCredentials: servicecred.PG{Pool: runtime},
		AppendAudit: func(ctx context.Context, event audit.AuditEvent) error {
			return audit.Append(ctx, runtime, schema, event)
		},
		Limits: limits,
	})
	if err != nil {
		return nil, fmt.Errorf("compat routes: %w", err)
	}
	v1, err := authapi.Routes(authapi.Deps{Sessions: sessions, Users: authapi.PGUsers{Pool: runtime}, Issuer: issuer, Logger: logger})
	if err != nil {
		return nil, fmt.Errorf("v1 routes: %w", err)
	}
	routes = append(routes, v1...)
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	server := httptest.NewServer(mux)
	closers = append(closers, server.Close)
	return server, nil
}

// seedIssueClient stores one session issue credential (go-api's
// svc_authsess_ token) and returns go-api's issue client for authServer.
func (e *env) seedIssueClient(ctx context.Context) (*sessionissue.Client, error) {
	secret, err := randomText(32)
	if err != nil {
		return nil, err
	}
	token := "svc_authsess_" + secret
	sum := sha256.Sum256([]byte(token))
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.internal_service_credentials
	(id, service_name, token_hash, token_prefix, scopes, created_at)
VALUES ($1, $2, $3, $4, $5::json, now())`, uuid.New(), servicecred.ServiceSessionIssuer, hex.EncodeToString(sum[:]),
		token[:len("svc_authsess_")+4], `["`+servicecred.ScopeSessionsIssue+`"]`); err != nil {
		return nil, fmt.Errorf("seed the issue credential: %w", err)
	}
	base, err := url.Parse(e.authServer.URL)
	if err != nil {
		return nil, err
	}
	one, err := url.Parse(e.authServerOne.URL)
	if err != nil {
		return nil, err
	}
	if e.issueOne, err = sessionissue.New(one, token, 5*time.Second); err != nil {
		return nil, err
	}
	return sessionissue.New(base, token, 5*time.Second)
}
