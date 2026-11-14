package authruntime

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
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
// decisionstate collects nothing. wrap is AuthorizeRoutesWithoutFeed's; auth-service passes none.
func authorizeRoutes(cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier,
	signer authapi.AccessSigner, decisions *decisionFeed, wrap ...func(authapi.DecisionState) authapi.DecisionState) ([]httpapi.Route, error) {
	reader, err := decisionstate.New(decisionstate.Config{Pool: pool, Schema: schema, Users: pgUsers(cfg, pool, schema), RefreshTTL: sessionRefreshTTL(),
		CollectDependencies: decisions != nil})
	if err != nil {
		return nil, err
	}
	var state authapi.DecisionState = reader
	for _, w := range wrap {
		if w != nil {
			state = w(state)
		}
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
		if deps.References, err = authapi.NewPrincipalReferences(authapi.PrincipalReferencesConfig{IssuerURL: cfg.TokenIssuerURL, Keys: keys,
			WebApprover: webApproverReferences(cfg, logger)}); err != nil {
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

// webApproverReferences builds the web approver reference kind (CHAOS-11207),
// or nil when it is off. Off with only part of its settings is one ERROR line
// that names the missing settings (never a value); auth-service starts either
// way (product rule 3).
func webApproverReferences(cfg authconfig.Config, logger *slog.Logger) *authapi.WebApproverReferences {
	w := cfg.AuthorizeWebApprover
	var missing []string
	for _, setting := range []struct{ name, value string }{
		{authconfig.EnvAuthorizeWebApproverIssuer, w.Issuer},
		{authconfig.EnvAuthorizeWebApproverJWKSFile, w.JWKSFile},
		{authconfig.EnvAuthorizeWebApproverCaller, w.Caller},
	} {
		if setting.value == "" {
			missing = append(missing, setting.name)
		}
	}
	if len(missing) == 3 {
		return nil
	}
	if w.Caller != "" && !slices.Contains(cfg.AuthorizeCallerSubjects, w.Caller) {
		missing = append(missing, authconfig.EnvAuthorizeWebApproverCaller+" in "+authconfig.EnvAuthorizeCallerSubjects)
	}
	if len(missing) > 0 {
		logger.Error("web approver reference off: settings missing", "missing", strings.Join(missing, ","))
		return nil
	}
	references, err := authapi.NewWebApproverReferences(authapi.WebApproverReferencesConfig{Issuer: w.Issuer, JWKSFile: w.JWKSFile, Caller: w.Caller})
	if err != nil {
		logger.Error("web approver reference off: settings invalid", "error", err.Error())
		return nil
	}
	logger.Info("web approver reference on")
	return references
}

// AuthorizeRoutesWithoutFeed is authorizeRoutes with the revision feed off (no decision cache): the production builder
// of the decision point routes, for a test decision point over a test database. wrap, when set, wraps the decision
// state the routes read (a test plants a defect in it); nil reads the state as auth-service does.
func AuthorizeRoutesWithoutFeed(cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier,
	signer authapi.AccessSigner, wrap func(authapi.DecisionState) authapi.DecisionState) ([]httpapi.Route, error) {
	return authorizeRoutes(cfg, logger, pool, schema, signer, nil, wrap)
}
