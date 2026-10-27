package identity

import (
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/edgetokenmint"
)

// servicePlaceholderIDs are the fixed ids of the proof service principals (edgetokenmint). Only these rows can be
// a placeholder; any other row of the same shape is an ordinary row of a provider without an identity.
var servicePlaceholderIDs = []uuid.UUID{
	uuid.MustParse(edgetokenmint.ProvePrincipalID),
	uuid.MustParse(edgetokenmint.AdminProofPrincipalID),
}

// IsServiceProvider reports a users row of the service provider (edgetokenmint.ServiceAuthProvider), whatever its
// other columns: such a row never gets a password, a reset link or a local login.
func IsServiceProvider(provider *string) bool {
	return provider != nil && *provider == edgetokenmint.ServiceAuthProvider
}

// ServiceProviderSQL is IsServiceProvider over the public.users row aliased alias; false (never NULL) for a NULL
// provider.
func ServiceProviderSQL(alias string) string {
	return fmt.Sprintf("coalesce(%s.auth_provider, '') = '%s'", alias, edgetokenmint.ServiceAuthProvider)
}

// IsServicePlaceholder is the one definition of a non-interactive placeholder in public.users: one of the fixed proof
// principal ids, provider service, no password hash and no provider id. It can sign in by no method, so it needs
// no external identity.
func IsServicePlaceholder(id uuid.UUID, provider, passwordHash, providerID *string) bool {
	known := false
	for _, placeholder := range servicePlaceholderIDs {
		known = known || id == placeholder
	}
	return known && IsServiceProvider(provider) &&
		(passwordHash == nil || *passwordHash == "") &&
		(providerID == nil || *providerID == "")
}

// ServicePlaceholderSQL is IsServicePlaceholder as a predicate over the public.users row aliased alias; never NULL.
func ServicePlaceholderSQL(alias string) string {
	ids := make([]string, len(servicePlaceholderIDs))
	for i, id := range servicePlaceholderIDs {
		ids[i] = "'" + id.String() + "'::uuid"
	}
	return fmt.Sprintf("(%[1]s.id IN (%[2]s) AND %[3]s AND coalesce(%[1]s.password_hash, '') = '' AND coalesce(%[1]s.auth_provider_id, '') = '')",
		alias, strings.Join(ids, ", "), ServiceProviderSQL(alias))
}
