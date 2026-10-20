// Package oidc is the auth-service OpenID Connect SSO flow: authorize, then a
// callback that validates the IdP's ID token and resolves it to a principal.
//
// An identity is keyed by (issuer, provider, subject) only, where the provider
// is the sso_providers row the flow ran through. An e-mail address
// never selects or links an account: a verified IdP e-mail that belongs to an
// existing account is a conflict, and an unverified one is refused. The flow
// state is sealed (AES-256-GCM) and single-use. New accounts go through the
// identity store's federated provisioning, which stays off until the users
// cutover. The handlers here are mounted by the auth-service runtime.
package oidc
