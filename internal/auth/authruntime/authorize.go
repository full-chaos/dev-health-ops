package authruntime

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/decisionstate"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// authorizeProfileRows is the endpoint-profile document the authorize routes
// are looked up in; nil is the embedded document. Tests plant a document.
var authorizeProfileRows []byte

// sessionRefreshTTL is the one refresh lifetime of the process: the session stores and the decision reader
// read session liveness with it, so the two cannot disagree on when a session without a credential ends.
func sessionRefreshTTL() time.Duration {
	return session.DefaultRefreshTTL
}

// authorizeRoutes builds the policy decision point routes over the signer's
// published key set. A route mounts only when its endpoint-profile row exists.
// Without AUTH_AUTHORIZE_CALLER_SUBJECTS the caller gate is nil and refuses
// every caller; without AUTH_TOKEN_ISSUER_URL no principal reference resolves.
//
// decisions is the decision cache with the feed on, nil with it off: the routes then decide without a cache and
// decisionstate collects nothing.
func authorizeRoutes(cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier,
	signer authapi.AccessSigner, decisions *decisionFeed) ([]httpapi.Route, error) {
	state, err := decisionstate.New(decisionstate.Config{Pool: pool, Schema: schema, Users: pgUsers(cfg, pool, schema), RefreshTTL: sessionRefreshTTL(),
		CollectDependencies: decisions != nil})
	if err != nil {
		return nil, err
	}
	deps := authapi.AuthorizeDeps{State: state, Evaluator: authorization.NewEvaluator(), ProfileRows: authorizeProfileRows, Logger: logger,
		CallerLimit: authapi.PDPCallerLimit{PerSecond: cfg.PDPCaller.PerSecond, Burst: cfg.PDPCaller.Burst},
		Audit: func(ctx context.Context, events []audit.AuditEvent) error {
			return audit.AppendAll(ctx, pool, schema, events...)
		}}
	if decisions != nil {
		deps.Cache, deps.Ready = decisions.cache, decisions.ready
	}
	if cfg.TokenIssuerURL != "" {
		keys, ok := signer.(authapi.KeySetSource)
		if !ok {
			return nil, fmt.Errorf("authruntime: the authorize routes need a signer that reads the key set")
		}
		if deps.References, err = authapi.NewPrincipalReferences(authapi.PrincipalReferencesConfig{IssuerURL: cfg.TokenIssuerURL, Keys: keys}); err != nil {
			return nil, err
		}
		if len(cfg.AuthorizeCallerSubjects) > 0 {
			deps.Callers, err = authapi.NewAuthorizeCallers(authapi.AuthorizeCallersConfig{
				IssuerURL: cfg.TokenIssuerURL, Keys: keys, Subjects: cfg.AuthorizeCallerSubjects,
			})
			if err != nil {
				return nil, fmt.Errorf("authruntime: %s: %w", authconfig.EnvAuthorizeCallerSubjects, err)
			}
		}
	}
	return authapi.AuthorizeRoutes(deps)
}
