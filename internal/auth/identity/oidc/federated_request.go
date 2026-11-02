package oidc

import "github.com/full-chaos/dev-health-ops/internal/auth/identity"

// provisionRequest is the identity store's request for a new federated account.
func provisionRequest(account FederatedAccount) identity.ProvisionRequest {
	return identity.ProvisionRequest{
		Key:   identity.ExternalKey{Issuer: account.Key.Issuer, Provider: identity.Provider(account.Key.Provider), Subject: account.Key.Subject},
		Email: account.Email, FullName: account.FullName, Actor: identity.SelfSignUp(account.RequestID),
		LegacyProvider: ProviderKind, LegacySubject: account.Key.Subject, LegacyFullName: account.LegacyFullName,
	}
}
