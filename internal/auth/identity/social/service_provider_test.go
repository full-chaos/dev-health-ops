package social

import (
	"context"
	"errors"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

func TestSignInToRefusesAServiceProviderPrincipalAsDisabled(t *testing.T) {
	reason, err := signInTo(identity.Principal{ID: uuid.New(), IsActive: true})
	if err != nil || reason != ReasonAccepted {
		t.Fatalf("normal principal: %v %v, want accepted", reason, err)
	}
	disabledReason, disabledErr := signInTo(identity.Principal{ID: uuid.New(), IsActive: true, DisabledAt: ptrTime()})
	reason, err = signInTo(identity.Principal{ID: uuid.New(), IsActive: true, ServiceProvider: true})
	if !errors.Is(err, ErrPrincipalDisabled) || reason != ReasonPrincipalDeactivated || reason != disabledReason || !errors.Is(disabledErr, err) {
		t.Fatalf("service-provider principal: %v %v, want the disabled answer %v %v", reason, err, disabledReason, disabledErr)
	}
}

func ptrTime() *time.Time { t := time.Now(); return &t }

type linkStore struct {
	Store
	principal identity.Principal
	have      []identity.Identity
	linked    int
}

func (s *linkStore) IdentityBySubject(context.Context, string, identity.Provider, string, *identity.LegacyMatch) (identity.Principal, bool, error) {
	return s.principal, true, nil
}
func (s *linkStore) ListIdentities(context.Context, uuid.UUID) ([]identity.Identity, error) {
	return s.have, nil
}
func (s *linkStore) LinkIdentity(context.Context, identity.ExternalKey, uuid.UUID, identity.Actor) (uuid.UUID, error) {
	s.linked++
	return uuid.New(), nil
}

type linkProfiles struct{ key identity.ExternalKey }

func (linkProfiles) Configured(Provider) bool { return true }
func (p linkProfiles) Issuer(Provider) string { return p.key.Issuer }
func (p linkProfiles) Fetch(context.Context, Provider, contracts.SecretString) (Profile, error) {
	return Profile{ExternalKey: p.key}, nil
}

func TestLinkRefusesAServiceProviderPrincipalEvenForAnIdentityItAlreadyHolds(t *testing.T) {
	key := identity.ExternalKey{Issuer: "https://github.com", Provider: identity.ProviderGitHub, Subject: "42"}
	for name, tc := range map[string]struct {
		serviceProvider bool
		wantErr         error
	}{
		"normal principal":           {false, nil},
		"service-provider principal": {true, ErrPrincipalDisabled},
	} {
		t.Run(name, func(t *testing.T) {
			id := uuid.New()
			store := &linkStore{principal: identity.Principal{ID: id, IsActive: true, ServiceProvider: tc.serviceProvider},
				have: []identity.Identity{{ID: uuid.New(), Issuer: key.Issuer, Provider: key.Provider, Subject: key.Subject}}}
			svc, err := New(Config{Store: store, Providers: linkProfiles{key: key}})
			if err != nil {
				t.Fatal(err)
			}
			_, err = svc.Link(context.Background(), SignedIn{PrincipalID: id, AuthTime: time.Now()}, GitHub, contracts.NewSecretString("tok"))
			if !errors.Is(err, tc.wantErr) || (tc.wantErr == nil && err != nil) {
				t.Fatalf("Link = %v, want %v", err, tc.wantErr)
			}
			if store.linked != 0 {
				t.Fatalf("LinkIdentity called %d times", store.linked)
			}
		})
	}
}
