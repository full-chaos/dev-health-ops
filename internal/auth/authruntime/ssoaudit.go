package authruntime

import (
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/ssoaudit"
)

// providerFailure is go-api's provider failure record (recordSSOFailure) for the OAuth and SAML sign-ins.
func providerFailure(pool *pgxpool.Pool) ssoaudit.ProviderFailure {
	return ssoaudit.PGProviderFailure{DB: pool}
}
