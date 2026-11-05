package social

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

type spExchangeStore struct {
	scriptStore
	provisioned atomic.Int32
	linked      atomic.Int32
}

func (s *spExchangeStore) ProvisionFederated(ctx context.Context, r identity.ProvisionRequest) (uuid.UUID, error) {
	s.provisioned.Add(1)
	return s.scriptStore.ProvisionFederated(ctx, r)
}

func (s *spExchangeStore) LinkIdentity(context.Context, identity.ExternalKey, uuid.UUID, identity.Actor) (uuid.UUID, error) {
	s.linked.Add(1)
	return uuid.New(), nil
}

// A service-provider principal never signs in through Exchange, by either lookup (the first, or the re-lookup that
// settles a taken email), and Exchange writes nothing for it.
func TestExchangeRefusesAServiceProviderPrincipalWithNoSideEffect(t *testing.T) {
	sp := uuid.New()
	servicePrincipal := func() (identity.Principal, bool, error) {
		return identity.Principal{ID: sp, IsActive: true, ServiceProvider: true}, true, nil
	}
	normal := func() (identity.Principal, bool, error) {
		return identity.Principal{ID: sp, IsActive: true}, true, nil
	}
	taken := func() (*identity.Principal, error) { return &identity.Principal{ID: uuid.New()}, nil }
	cases := []struct {
		name    string
		lookups []func() (identity.Principal, bool, error)
		byEmail []func() (*identity.Principal, error)
		wantErr error
	}{
		{"first lookup finds it", []func() (identity.Principal, bool, error){servicePrincipal}, []func() (*identity.Principal, error){noAccount}, ErrPrincipalDisabled},
		{"settling lookup finds it", []func() (identity.Principal, bool, error){notFound, servicePrincipal}, []func() (*identity.Principal, error){taken}, ErrPrincipalDisabled},
		{"control: normal principal signs in", []func() (identity.Principal, bool, error){normal}, []func() (*identity.Principal, error){noAccount}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &spExchangeStore{scriptStore: scriptStore{lookups: tc.lookups, byEmail: tc.byEmail}}
			res, err := exchangeWith(t, store)
			if !errors.Is(err, tc.wantErr) || (tc.wantErr == nil && err != nil) {
				t.Fatalf("Exchange error = %v, want %v", err, tc.wantErr)
			}
			if res.PrincipalID != sp {
				t.Fatalf("PrincipalID = %v, want %v", res.PrincipalID, sp)
			}
			if res.Created || store.provisioned.Load() != 0 || store.linked.Load() != 0 {
				t.Fatalf("created=%v provisioned=%d linked=%d, want no side effect", res.Created, store.provisioned.Load(), store.linked.Load())
			}
		})
	}
}
