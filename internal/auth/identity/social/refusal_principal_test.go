package social

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// scriptStore answers each Store call from a script, per call number.
type scriptStore struct {
	stubStore
	mu        sync.Mutex
	lookups   []func() (identity.Principal, bool, error)
	byEmail   []func() (*identity.Principal, error)
	provision error
	password  bool
}

func (s *scriptStore) IdentityBySubject(context.Context, string, identity.Provider, string, *identity.LegacyMatch) (identity.Principal, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.lookups[0]
	if len(s.lookups) > 1 {
		s.lookups = s.lookups[1:]
	}
	return next()
}

func (s *scriptStore) ByEmail(context.Context, string) (*identity.Principal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.byEmail[0]
	if len(s.byEmail) > 1 {
		s.byEmail = s.byEmail[1:]
	}
	return next()
}

func (s *scriptStore) HasPassword(context.Context, uuid.UUID) (bool, error) { return s.password, nil }

func (s *scriptStore) ListIdentities(context.Context, uuid.UUID) ([]identity.Identity, error) {
	return nil, nil
}

func (s *scriptStore) ProvisionFederated(context.Context, identity.ProvisionRequest) (uuid.UUID, error) {
	return uuid.Nil, s.provision
}

func notFound() (identity.Principal, bool, error) { return identity.Principal{}, false, nil }

func noAccount() (*identity.Principal, error) { return nil, nil }

func exchangeWith(t *testing.T, store Store) (ExchangeResult, error) {
	t.Helper()
	f := newFakeProviders(t)
	svc, err := New(Config{Store: store, Providers: f.client()})
	if err != nil {
		t.Fatal(err)
	}
	tok := f.token("refusal", account{provider: GitHub, id: 11, email: "refusal@example.com", verified: true, public: true})
	return svc.Exchange(context.Background(), GitHub, tok, nil)
}

// Every refusal of Exchange that concerns a principal it found names it.
func TestEveryExchangeRefusalNamesThePrincipalItConcerns(t *testing.T) {
	linked, owner := uuid.New(), uuid.New()
	providerLinked := func() (identity.Principal, bool, error) {
		return identity.Principal{ID: linked}, false, identity.ErrProviderAlreadyLinked
	}
	ownerByEmail := func() (*identity.Principal, error) { return &identity.Principal{ID: owner}, nil }
	cases := []struct {
		name  string
		store *scriptStore
		want  uuid.UUID
		is    error
	}{
		{"the subject's account holds another identity at the provider",
			&scriptStore{lookups: []func() (identity.Principal, bool, error){providerLinked}, byEmail: []func() (*identity.Principal, error){noAccount}},
			linked, ErrProviderLinked},
		{"the email owner's lookup finds another identity at the provider",
			&scriptStore{lookups: []func() (identity.Principal, bool, error){notFound, providerLinked}, byEmail: []func() (*identity.Principal, error){ownerByEmail}, password: true},
			linked, ErrProviderLinked},
		{"an email held by an account with a password and no provider",
			&scriptStore{lookups: []func() (identity.Principal, bool, error){notFound}, byEmail: []func() (*identity.Principal, error){ownerByEmail}, password: true},
			owner, nil},
		{"an email taken while provisioning",
			&scriptStore{lookups: []func() (identity.Principal, bool, error){notFound}, byEmail: []func() (*identity.Principal, error){noAccount, ownerByEmail},
				provision: identity.ErrEmailTaken, password: true},
			owner, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result, err := exchangeWith(t, c.store)
			var conflict *ConflictError
			switch {
			case c.is != nil && !errors.Is(err, c.is):
				t.Fatalf("err = %v, want %v", err, c.is)
			case c.is == nil && !errors.As(err, &conflict):
				t.Fatalf("err = %v, want an email collision", err)
			}
			if result.PrincipalID != c.want {
				t.Fatalf("PrincipalID = %v, want %v", result.PrincipalID, c.want)
			}
		})
	}
}
