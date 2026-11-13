package authconfig

import "github.com/full-chaos/dev-health-ops/internal/platform/secrets"

// EnvSSOProvidersOwned is the SSO providers switch: auth-service writes public.sso_providers (the internal provider
// routes) and its runtime role holds the grants for it.
const EnvSSOProvidersOwned = "AUTH_SSO_PROVIDERS_OWNED"

// SSOProvidersOwnedFromLookup reads AUTH_SSO_PROVIDERS_OWNED: unset or empty is false, anything but true or false is
// refused. The grants pass and auth-service use it so every process agrees on the switch.
func SSOProvidersOwnedFromLookup(lookup secrets.LookupEnv) (bool, error) {
	return ownedSwitch(lookup, EnvSSOProvidersOwned)
}
