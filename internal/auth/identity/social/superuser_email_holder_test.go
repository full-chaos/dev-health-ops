package social

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// heldEmailStore: owner holds every email; byKey, when set, is the account the identity key resolves to. The
// embedded nil Store makes any other call (a provision, a link) panic.
type heldEmailStore struct {
	stubStore
	owner   identity.Principal
	byKey   *identity.Principal
	byEmail int
}

func (s *heldEmailStore) IdentityBySubject(context.Context, string, identity.Provider, string, *identity.LegacyMatch) (identity.Principal, bool, error) {
	if s.byKey != nil {
		return *s.byKey, true, nil
	}
	return identity.Principal{}, false, nil
}

func (s *heldEmailStore) ByEmail(context.Context, string) (*identity.Principal, error) {
	s.byEmail++
	owner := s.owner
	return &owner, nil
}

// A social exchange whose identity holds no account and whose verified email an account holds is refused with the
// one generic 409 and writes nothing. identity.Principal carries no account class, so a platform admin's account
// gets the same answer as any other: the email is never a link to an existing account.
func TestExchangeRefusesAnEmailAPlatformAdminHoldsAsItRefusesAnyAccount(t *testing.T) {
	local, github := "local", "github"
	answers := map[string]string{}
	for name, provider := range map[string]*string{"platform admin, password": &local, "member, github": &github} {
		f := newFakeProviders(t)
		store := &heldEmailStore{owner: identity.Principal{ID: uuid.New(), IsActive: true, AuthProvider: provider}}
		svc, err := New(Config{Store: store, Providers: f.client()})
		if err != nil {
			t.Fatal(err)
		}
		tok := f.token("held-8984", account{provider: GitHub, id: 8984, email: "Root.Admin@example.com", verified: true, public: true})
		result, err := svc.Exchange(context.Background(), GitHub, tok, nil)
		var conflict *ConflictError
		if !errors.As(err, &conflict) || result.Created {
			t.Fatalf("%s: Exchange = %+v, %v; want the email conflict and no account", name, result, err)
		}
		if store.byEmail == 0 {
			t.Fatalf("%s: the exchange never read the email owner: the cell did not reach the check", name)
		}
		rec := httptest.NewRecorder()
		writeError(rec, GitHub, err)
		want := `{"detail":{"message":"` + ConflictMessage + `","reason":"identity_conflict"}}`
		if rec.Code != http.StatusConflict || rec.Body.String() != want {
			t.Fatalf("%s: %d %s, want 409 %s", name, rec.Code, rec.Body.String(), want)
		}
		answers[name] = rec.Body.String()
	}
	if answers["platform admin, password"] != answers["member, github"] {
		t.Fatalf("platform admin holder answer %s, other holder answer %s; want one answer", answers["platform admin, password"], answers["member, github"])
	}
}

// A platform admin whose account holds the identity still signs in by it, without an email read.
func TestExchangeOfAPlatformAdminByItsIdentityStillSignsIn(t *testing.T) {
	f := newFakeProviders(t)
	admin := identity.Principal{ID: uuid.New(), IsActive: true}
	store := &heldEmailStore{byKey: &admin}
	svc, err := New(Config{Store: store, Providers: f.client()})
	if err != nil {
		t.Fatal(err)
	}
	tok := f.token("admin-8984", account{provider: GitHub, id: 8985, email: "root.admin@example.com", verified: true, public: true})
	result, err := svc.Exchange(context.Background(), GitHub, tok, nil)
	if err != nil || result.PrincipalID != admin.ID || result.Created {
		t.Fatalf("Exchange = %+v, %v; want the platform admin %v signed in", result, err, admin.ID)
	}
	if store.byEmail != 0 {
		t.Fatalf("the identity sign-in read the email owner %d times", store.byEmail)
	}
}
