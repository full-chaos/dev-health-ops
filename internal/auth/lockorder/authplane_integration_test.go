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
	successor, err := randomText(32)
	if err != nil {
		return err
	}
	sessions, err := session.New(session.Config{Pool: e.runtime, Schema: schema, Logger: logger, UsersOwned: true,
		SuccessorKey: contracts.NewSecretString(successor)})
	if err != nil {
		return fmt.Errorf("session store: %w", err)
	}
	issuer, err := authapi.NewIssuer(jwtKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		return fmt.Errorf("issuer: %w", err)
	}
	limitKey, err := randomText(32)
	if err != nil {
		return err
	}
	limits, err := authapi.NewLimits(httpapi.NewMemoryCounters(nil), limitKey)
	if err != nil {
		return fmt.Errorf("limits: %w", err)
	}
	runtime := e.runtime
	routes, err := authapi.CompatRoutes(authapi.CompatDeps{
		Sessions: sessions, Users: authapi.PGUsers{Pool: runtime}, Issuer: issuer, Logger: logger,
		Projector: e.identity, ServiceCredentials: servicecred.PG{Pool: runtime},
		AppendAudit: func(ctx context.Context, event audit.AuditEvent) error {
			return audit.Append(ctx, runtime, schema, event)
		},
		Limits: limits,
	})
	if err != nil {
		return fmt.Errorf("compat routes: %w", err)
	}
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	e.authServer = httptest.NewServer(mux)
	closers = append(closers, e.authServer.Close)
	return nil
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
	return sessionissue.New(base, token, 5*time.Second)
}
