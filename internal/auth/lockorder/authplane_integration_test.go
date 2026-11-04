//go:build integration

package lockorder

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/issueworkload"
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
	plane, err := issueworkload.New("https://auth.lockorder.example.test")
	if err != nil {
		return err
	}
	e.issuePlane = plane
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
	workload, err := authapi.NewSessionIssueWorkload(authapi.SessionIssueWorkloadConfig{
		IssuerURL: e.issuePlane.Issuer, Keys: e.issuePlane, Subject: e.issuePlane.Subject,
	})
	if err != nil {
		return nil, fmt.Errorf("session issue workload: %w", err)
	}
	routes, err := authapi.CompatRoutes(authapi.CompatDeps{
		Sessions: sessions, Users: authapi.PGUsers{Pool: runtime}, Issuer: issuer, Logger: logger,
		Projector: e.identity, IssueWorkload: workload,
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

// seedIssueClient stores go-api's workload principal and returns go-api's
// issue client for authServer, which presents the go-api workload token.
func (e *env) seedIssueClient(ctx context.Context) (*sessionissue.Client, error) {
	if err := e.issuePlane.Seed(ctx, e.admin); err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp("", "lockorder-issue-")
	if err != nil {
		return nil, err
	}
	closers = append(closers, func() { _ = os.RemoveAll(dir) })
	workload, stop, err := e.issuePlane.Client(dir)
	if err != nil {
		return nil, err
	}
	closers = append(closers, stop)
	base, err := url.Parse(e.authServer.URL)
	if err != nil {
		return nil, err
	}
	one, err := url.Parse(e.authServerOne.URL)
	if err != nil {
		return nil, err
	}
	if e.issueOne, err = sessionissue.NewWorkload(one, workload, 5*time.Second); err != nil {
		return nil, err
	}
	return sessionissue.NewWorkload(base, workload, 5*time.Second)
}
