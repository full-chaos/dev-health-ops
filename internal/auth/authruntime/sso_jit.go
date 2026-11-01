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

// samlJIT is the SAML membership seam of cfg.
func samlJIT(authconfig.Config, *identity.Store) saml.MembershipJIT { return nil }

// oidcJIT is the OIDC membership seam of cfg.
func oidcJIT(authconfig.Config, *slog.Logger, *identity.Store, *session.Store, authapi.SSOSessions) oidc.MembershipJIT {
	return nil
}
