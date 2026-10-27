package identity

import "fmt"

// ssoCarrierScopeSQL is the one carrier set of an OIDC or SAML key in both switch states: the go-api accounts of the
// row aliased alias that carry the stored subject and are unbound, bound to the key's provider row (the uuid
// parameter providerParam), or bound to a row that is gone or of another protocol. An account bound to another live
// provider row of its protocol is that provider's identity (pgmigrate revision 0155) and is not read. Never NULL.
func ssoCarrierScopeSQL(alias, providerParam string) string {
	return fmt.Sprintf(`(%[1]s.sso_provider_id IS NULL OR %[1]s.sso_provider_id = %[2]s::uuid
       OR NOT EXISTS (SELECT 1 FROM public.sso_providers p WHERE p.id = %[1]s.sso_provider_id AND p.protocol = %[1]s.auth_provider))`,
		alias, providerParam)
}

// ssoBoundToKeySQL holds when the carrier aliased alias is bound (isSSOBound) to the key's provider row; only such a
// carrier is the key's identity. Never NULL.
func ssoBoundToKeySQL(alias, providerParam string) string {
	return fmt.Sprintf(`(%[2]s AND coalesce(%[1]s.sso_provider_id = %[3]s::uuid, false))`, alias, ssoBoundSQL(alias), providerParam)
}
