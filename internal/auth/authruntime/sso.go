package authruntime

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/oidc"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
	"github.com/full-chaos/dev-health-ops/internal/platform/fernet"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// ssoRoutes mounts the federated sign-in routes with just-in-time
// provisioning off: no MembershipJIT is set on either service, so a sign-in
// that resolves no account is refused and writes nothing. A protocol whose
// settings are incomplete mounts no route.
func ssoRoutes(
	cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier,
	identities *identity.Store, deps authapi.CompatDeps,
) ([]httpapi.Route, error) {
	if !cfg.SSO.OIDCEnabled() && !cfg.SSO.SAMLEnabled() {
		logger.Info("federated sign-in routes are not mounted: their settings are incomplete")
		return nil, nil
	}
	sessions, err := authapi.NewSSOSessions(deps)
	if err != nil {
		return nil, err
	}
	var routes []httpapi.Route
	if cfg.SSO.OIDCEnabled() {
		service, err := oidcService(cfg, logger, pool, schema, identities, sessions)
		if err != nil {
			return nil, err
		}
		routes = append(routes, oidcRoutes(service, sessions)...)
	} else {
		logger.Info("OIDC and OAuth sign-in routes are not mounted: their settings are incomplete")
	}
	if cfg.SSO.SAMLEnabled() {
		service, err := samlService(cfg, logger, pool, schema, identities)
		if err != nil {
			return nil, err
		}
		routes = append(routes, samlRoutes(service, sessions, logger)...)
	} else {
		logger.Info("SAML sign-in routes are not mounted: their settings are incomplete")
	}
	return routes, nil
}

// providerSecrets is the oidc.SecretDecrypter over the settings key; every
// plaintext it returns is registered with the log redactor.
type providerSecrets struct{ cipher fernet.Cipher }

func (d providerSecrets) Decrypt(stored string) (string, error) {
	plain, err := d.cipher.Open(stored)
	if err != nil {
		return "", err
	}
	secrets.RegisterDecrypted(plain)
	return string(plain), nil
}

func oidcService(
	cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier,
	identities *identity.Store, sessions authapi.SSOSessions,
) (*oidc.Service, error) {
	flows, err := oidc.NewPGFlowStore(pool, schema)
	if err != nil {
		return nil, err
	}
	cipher, err := fernet.New(cfg.SSO.EncryptionKey.Reveal(), cfg.SSO.EncryptionSalt)
	if err != nil {
		return nil, errors.New("authruntime: the provider secret key is unusable")
	}
	return oidc.New(oidc.Config{
		Providers:    oidc.PGProviders{Pool: pool},
		Flows:        flows,
		Identities:   oidc.StoreIdentities{Store: identities, Pool: pool, Schema: schema},
		Memberships:  oidc.PGMemberships{Pool: pool},
		Entitlements: oidc.LicensingEntitlements{Pool: pool},
		Sessions:     sessions.OIDC(),
		Secrets:      providerSecrets{cipher},
		StateSecret:  contracts.NewSecretString(cfg.SSO.StateKey.Reveal()),
		AppBaseURL:   cfg.SSO.AppBaseURL,
		Logger:       logger,
	})
}

func oidcRoutes(service *oidc.Service, sessions authapi.SSOSessions) []httpapi.Route {
	refusal := sessions.TransportRefusal("")
	return []httpapi.Route{
		{Method: http.MethodPost, Pattern: "/api/v1/auth/oidc/{provider_id}/authorize", Handler: service.AuthorizeHandler(), ErrorWriter: refusal},
		{Method: http.MethodPost, Pattern: "/api/v1/auth/oidc/{provider_id}/callback", Handler: service.CallbackHandler(), ErrorWriter: refusal},
		{Method: http.MethodPost, Pattern: "/api/v1/auth/oauth/{provider_id}/authorize", Handler: service.OAuthAuthorizeHandler(), ErrorWriter: refusal},
		{Method: http.MethodGet, Pattern: "/api/v1/auth/oauth/{provider_id}/authorize", Handler: byProviderType(service.OAuthAuthorizeByTypeHandler()), ErrorWriter: refusal},
		{Method: http.MethodPost, Pattern: "/api/v1/auth/oauth/{provider_id}/callback", Handler: service.OAuthCallbackHandler(), ErrorWriter: refusal},
	}
}

// byProviderType serves the by-type authorize route on the provider_id
// wildcard: net/http refuses two wildcard names at one path, and the handler
// reads the segment as provider_type.
func byProviderType(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.SetPathValue("provider_type", r.PathValue("provider_id"))
		next.ServeHTTP(w, r)
	})
}

func samlService(
	cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier,
	identities *identity.Store,
) (*saml.Service, error) {
	store, err := saml.NewPGStore(pool, schema)
	if err != nil {
		return nil, err
	}
	base := cfg.SSO.AppBaseURL
	return &saml.Service{
		Providers:    store,
		Entitlements: saml.LicensingEntitlements{DB: pool},
		Flows:        store,
		Uses:         store,
		Store:        samlIdentities{identities},
		Memberships:  store,
		BaseURL:      func() string { return base },
		Logger:       logger,
	}, nil
}

func samlRoutes(service *saml.Service, sessions authapi.SSOSessions, logger *slog.Logger) []httpapi.Route {
	handlers := saml.Handlers{Service: service, Sessions: sessions.SAML(), Logger: logger}
	refusal := sessions.TransportRefusal("saml")
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: "/api/v1/auth/saml/{provider_id}/metadata", Handler: http.HandlerFunc(handlers.Metadata), ErrorWriter: refusal},
		{Method: http.MethodPost, Pattern: "/api/v1/auth/saml/{provider_id}/initiate", Handler: http.HandlerFunc(handlers.Initiate), ErrorWriter: refusal},
		{Method: http.MethodPost, Pattern: "/api/v1/auth/saml/{provider_id}/acs", Handler: http.HandlerFunc(handlers.ACS), ErrorWriter: refusal},
	}
}

// samlIdentities is the saml.Store over the shared identity store.
type samlIdentities struct{ store *identity.Store }

func (a samlIdentities) IdentityBySubject(ctx context.Context, id saml.Identity, legacy *saml.LegacyMatch) (saml.Principal, bool, error) {
	var match *identity.LegacyMatch
	if legacy != nil {
		org := legacy.MembershipOrg
		match = &identity.LegacyMatch{AuthProvider: legacy.AuthProvider, MembershipOrg: &org, Subject: legacy.Subject}
	}
	principal, found, err := a.store.IdentityBySubject(ctx, id.Issuer, identity.Provider(id.Provider), id.Subject, match)
	switch {
	case errors.Is(err, identity.ErrMultipleUsers):
		return saml.Principal{}, false, saml.ErrIdentityAmbiguous
	case errors.Is(err, identity.ErrLegacyNotMember):
		return saml.Principal{}, false, saml.ErrNotOrgMember
	case err != nil:
		return saml.Principal{}, false, err
	}
	return saml.Principal{ID: principal.ID, Disabled: !principal.IsActive || principal.DisabledAt != nil}, found, nil
}

func (samlIdentities) NormalizeEmail(raw string) (string, error) { return identity.NormalizeEmail(raw) }

func (a samlIdentities) PrincipalByEmail(ctx context.Context, email string) (saml.EmailOwner, bool, error) {
	principal, err := a.store.ByEmail(ctx, email)
	switch {
	case errors.Is(err, identity.ErrMultipleUsers):
		return saml.EmailOwner{}, true, nil
	case err != nil:
		return saml.EmailOwner{}, false, err
	case principal == nil:
		return saml.EmailOwner{}, false, nil
	}
	method := "local"
	if principal.AuthProvider != nil {
		method = *principal.AuthProvider
	}
	return saml.EmailOwner{PrincipalID: principal.ID, Method: method}, true, nil
}
