package authruntime

import (
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/oidc"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// samlJIT is the SAML membership seam, nil with AUTH_SSO_JIT off.
func samlJIT(cfg authconfig.Config, logger *slog.Logger, identities *identity.Store) saml.MembershipJIT {
	if !cfg.SSO.JIT {
		return nil
	}
	return saml.PGJIT{Identities: identities, Logger: logger}
}

// oidcJIT is the OIDC membership seam, nil with AUTH_SSO_JIT off.
func oidcJIT(cfg authconfig.Config, logger *slog.Logger, identities *identity.Store, sessions *session.Store, sso authapi.SSOSessions) oidc.MembershipJIT {
	if !cfg.SSO.JIT {
		return nil
	}
	return oidc.PGJIT{Identities: identities, Sessions: sessions, LoginEvent: sso.LoginEvent, Mint: sso.MintOIDC, Logger: logger}
}

// oidcSeam builds the OIDC seam once the session issuer exists.
type oidcSeam func(authapi.SSOSessions) oidc.MembershipJIT

func oidcSeamOf(cfg authconfig.Config, logger *slog.Logger, identities *identity.Store, sessions *session.Store) oidcSeam {
	return func(sso authapi.SSOSessions) oidc.MembershipJIT {
		return oidcJIT(cfg, logger, identities, sessions, sso)
	}
}
