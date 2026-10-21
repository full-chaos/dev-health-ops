package social

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// ownerStore finds no identity and one local account, ownerID, for every
// email.
type ownerStore struct {
	stubStore
	ownerID uuid.UUID
}

func (s ownerStore) ByEmail(context.Context, string) (*identity.Principal, error) {
	local := "local"
	return &identity.Principal{ID: s.ownerID, AuthProvider: &local}, nil
}

// An email collision names the account that holds the email: the refusal
// concerns that principal, and its caller audits it.
func TestAnEmailCollisionNamesTheAccountThatHoldsTheEmail(t *testing.T) {
	f := newFakeProviders(t)
	owner := uuid.New()
	svc, err := New(Config{Store: ownerStore{ownerID: owner}, Providers: f.client()})
	if err != nil {
		t.Fatal(err)
	}
	tok := f.token("collision", account{provider: GitHub, id: 7, email: "held@example.com", verified: true, public: true})
	result, err := svc.Exchange(context.Background(), GitHub, tok, nil)
	var conflict *ConflictError
	if !errors.As(err, &conflict) || conflict.ExistingMethod != "local" {
		t.Fatalf("err = %v, want the local account's collision", err)
	}
	if result.PrincipalID != owner {
		t.Fatalf("PrincipalID = %v, want the account that holds the email %v", result.PrincipalID, owner)
	}
}
