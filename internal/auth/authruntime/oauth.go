package authruntime

import (
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/acrmint"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/oauthgrant"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// oauthRoutes builds the OAuth authorization server's routes. They need the token
// issuer, the key set and the shared limits; without one of them nothing mounts.
// authapi.OAuthRoutes also mounts nothing until the endpoint-profile rows exist, and
// on prod the routes stay dark behind go-api's default-off AUTH_PROXY_OAUTH and the
// NetworkPolicy (CHAOS-12251, the corrected section 4).
func oauthRoutes(
	cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier,
	sessions *session.Store, issuer *authapi.Issuer, limits *authapi.Limits, signer authapi.AccessSigner,
) ([]httpapi.Route, error) {
	keys, ok := signer.(authapi.KeySetSource)
	if cfg.TokenIssuerURL == "" || !ok || limits == nil {
		return nil, nil
	}
	registry, err := oauthgrant.LoadRegistry()
	if err != nil {
		return nil, err
	}
	store, err := oauthgrant.New(oauthgrant.Config{
		Pool: pool, Schema: schema, Sessions: sessions, Logger: logger,
		Actors: acrmint.PGImpersonation{Pool: pool, Sessions: sessions},
	})
	if err != nil {
		return nil, err
	}
	return authapi.OAuthRoutes(authapi.OAuthDeps{
		Store: store, Registry: registry, Limits: limits, Keys: keys,
		Session:   authapi.Deps{Sessions: sessions, Users: pgUsers(cfg, pool, schema), Issuer: issuer, Logger: logger},
		IssuerURL: cfg.TokenIssuerURL, WebOrigin: cfg.OAuthWebOrigin,
		ResourceACRAPI: cfg.OAuthResourceACRAPI, ResourceACRMCP: cfg.OAuthResourceACRMCP,
	})
}
