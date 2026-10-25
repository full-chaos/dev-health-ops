package identity

import "fmt"

// ServicePlaceholderProvider is the public.users.auth_provider of a non-interactive placeholder row.
const ServicePlaceholderProvider = "service"

// IsServicePlaceholder is the one definition of a non-interactive placeholder in public.users: provider "service"
// with no password hash and no provider id. It can sign in by no method, so it needs no external identity.
func IsServicePlaceholder(provider, passwordHash, providerID *string) bool {
	return provider != nil && *provider == ServicePlaceholderProvider &&
		(passwordHash == nil || *passwordHash == "") &&
		(providerID == nil || *providerID == "")
}

// ServicePlaceholderSQL is IsServicePlaceholder as a predicate over the public.users row aliased alias.
func ServicePlaceholderSQL(alias string) string {
	return fmt.Sprintf("(%[1]s.auth_provider = '%[2]s' AND coalesce(%[1]s.password_hash, '') = '' AND coalesce(%[1]s.auth_provider_id, '') = '')",
		alias, ServicePlaceholderProvider)
}
