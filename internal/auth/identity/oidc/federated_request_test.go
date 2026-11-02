package oidc

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestProvisionRequestLegacyProviderIsTheLoginProtocol(t *testing.T) {
	for _, provider := range []string{"oidc", "github", "gitlab", "google"} {
		t.Run(provider, func(t *testing.T) {
			req := provisionRequest(FederatedAccount{
				Key:   ExternalKey{Issuer: "https://idp.example.test", Provider: uuid.NewString(), Subject: "sub-1"},
				Email: "p@example.com", LegacyProvider: provider,
			})
			if req.LegacyProvider != provider {
				t.Fatalf("LegacyProvider = %q, want %q", req.LegacyProvider, provider)
			}
			if req.LegacySubject != "sub-1" {
				t.Fatalf("LegacySubject = %q, want sub-1", req.LegacySubject)
			}
		})
	}
}

func TestResolveNewAccountCarriesTheLoginProtocolToTheJIT(t *testing.T) {
	for _, provider := range []string{"oidc", "github", "gitlab", "google"} {
		t.Run(provider, func(t *testing.T) {
			jit := &resolveJIT{}
			svc := &Service{cfg: Config{Identities: &resolveIdentities{err: ErrIdentityNotFound}, Memberships: resolveMembers{}, Sessions: &resolveSessions{}, MembershipJIT: jit}}
			row := Provider{ID: uuid.New(), OrgID: uuid.New(), AutoProvision: true, AllowedDomains: []string{"example.com"}}
			v := validated{profile: profile{Email: "p@example.com", EmailVerified: true}, legacyAuthProvider: provider}
			if _, failure := svc.resolve(context.Background(), row, v, nil); failure != nil {
				t.Fatalf("resolve: %v", failure)
			}
			if len(jit.new) != 1 || jit.new[0].LegacyProvider != provider {
				t.Fatalf("jit accounts %+v, want LegacyProvider %q", jit.new, provider)
			}
		})
	}
}
