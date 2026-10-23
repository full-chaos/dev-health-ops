package authapi

import (
	"context"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/social"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

// fakeLinkDomain is an in-memory social domain with the ownership rules of
// social.Service: a proof binds to the caller's principal only, a proof
// bound elsewhere is a conflict, an id is removable by its owner only, and
// the last sign-in method stays.
type fakeLinkDomain struct {
	mu      sync.Mutex
	calls   int
	whos    []social.SignedIn
	proofs  map[string]uuid.UUID // token -> principal that already holds the identity
	owners  map[uuid.UUID]uuid.UUID
	methods map[uuid.UUID]int // principal -> sign-in methods left
}

func (f *fakeLinkDomain) Exchange(context.Context, social.Provider, contracts.SecretString, *string) (social.ExchangeResult, error) {
	return social.ExchangeResult{}, nil
}

func (f *fakeLinkDomain) Link(_ context.Context, who social.SignedIn, _ social.Provider, token contracts.SecretString) (identity.Identity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.whos = append(f.whos, who)
	holder, bound := f.proofs[token.Reveal()]
	if token.Reveal() == "bad" {
		return identity.Identity{}, social.ErrProviderRefused
	}
	if bound && holder != who.PrincipalID {
		return identity.Identity{}, identity.ErrIdentityConflict
	}
	id := uuid.New()
	f.owners[id] = who.PrincipalID
	f.methods[who.PrincipalID]++
	return identity.Identity{ID: id, Provider: "github"}, nil
}

func (f *fakeLinkDomain) Unlink(_ context.Context, who social.SignedIn, id uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.whos = append(f.whos, who)
	if owner, ok := f.owners[id]; !ok || owner != who.PrincipalID {
		return identity.ErrNotFound
	}
	if f.methods[who.PrincipalID] <= 1 {
		return identity.ErrLastSignInMethod
	}
	delete(f.owners, id)
	f.methods[who.PrincipalID]--
	return nil
}

func linkFixture(t *testing.T) (*fixture, *fakeLinkDomain) {
	t.Helper()
	return linkFixtureWith(t, httpapi.NewMemoryCounters(nil))
}

func linkFixtureWith(t *testing.T, counters httpapi.CounterStore) (*fixture, *fakeLinkDomain) {
	t.Helper()
	f := newFixture(t)
	domain := &fakeLinkDomain{proofs: map[string]uuid.UUID{}, owners: map[uuid.UUID]uuid.UUID{}, methods: map[uuid.UUID]int{f.principal: 1}}
	routes, err := SocialLinkRoutes(SocialLinkDeps{Sessions: f.sessions, Users: f.users, Issuer: f.issuer,
		Logger: slog.New(slog.NewJSONHandler(f.logs, nil)), Social: domain, Limits: mustLimits(t, counters)})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(route.Handler))
	}
	f.server = mux
	return f, domain
}

const linkBody = `{"provider":"github","provider_access_token":"good"}`

func TestSocialLinkRoutesAreUnmountedWithoutASocialDomain(t *testing.T) {
	f := newFixture(t)
	routes, err := SocialLinkRoutes(SocialLinkDeps{Sessions: f.sessions, Users: f.users, Issuer: f.issuer})
	if err != nil || len(routes) != 0 {
		t.Fatalf("routes %d err %v; want none", len(routes), err)
	}
}

func TestSocialLinkNeedsABearerSessionAndNeverReadsACookie(t *testing.T) {
	f, domain := linkFixture(t)
	cookie := map[string]string{"Cookie": "session=abc; access_token=abc"}
	for name, header := range map[string]map[string]string{"none": nil, "cookie only": cookie, "malformed": {"Authorization": "Bearer"}, "wrong scheme": {"Authorization": "Basic abc"}} {
		for _, call := range []struct{ method, path, body string }{
			{http.MethodPost, PatternSocialLink, linkBody},
			{http.MethodDelete, "/v1/identities/social/" + uuid.NewString(), ""},
		} {
			response := f.do(call.method, call.path, call.body, header)
			if response.Code != http.StatusUnauthorized {
				t.Errorf("%s %s: status %d, want 401", name, call.method, response.Code)
			}
		}
	}
	if domain.calls != 0 {
		t.Fatalf("domain reached %d times without a bearer", domain.calls)
	}
}

func TestSocialLinkRefusesARevokedSessionBearer(t *testing.T) {
	f, domain := linkFixture(t)
	headers := f.bearer(f.grant())
	revoked := time.Now()
	record := f.sessions.records[f.record.ID]
	record.RevokedAt = &revoked
	f.sessions.records[f.record.ID] = record
	if response := f.do(http.MethodPost, PatternSocialLink, linkBody, headers); response.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", response.Code)
	}
	if domain.calls != 0 {
		t.Fatal("domain reached with a revoked session")
	}
}

func TestSocialLinkActsForTheBearersPrincipalAndSessionTime(t *testing.T) {
	f, domain := linkFixture(t)
	response := f.do(http.MethodPost, PatternSocialLink, linkBody, f.bearer(f.grant()))
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s", response.Code, response.Body)
	}
	if len(domain.whos) != 1 || domain.whos[0].PrincipalID != f.principal || !domain.whos[0].AuthTime.Equal(f.record.AuthTime) {
		t.Fatalf("domain saw %+v, want the bearer's principal and its session auth time", domain.whos)
	}
}

func TestSocialLinkOfAProofBoundToAnotherPrincipalIsAConflictAndWritesNothing(t *testing.T) {
	f, domain := linkFixture(t)
	domain.proofs["good"] = uuid.New()
	response := f.do(http.MethodPost, PatternSocialLink, linkBody, f.bearer(f.grant()))
	if response.Code != http.StatusConflict {
		t.Fatalf("status %d, want 409", response.Code)
	}
	if len(domain.owners) != 0 || domain.methods[f.principal] != 1 {
		t.Fatal("a refused link wrote")
	}
}

func TestSocialLinkRefusesAProviderProofTheProviderRejects(t *testing.T) {
	f, domain := linkFixture(t)
	body := `{"provider":"github","provider_access_token":"bad"}`
	if response := f.do(http.MethodPost, PatternSocialLink, body, f.bearer(f.grant())); response.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", response.Code)
	}
	if len(domain.owners) != 0 {
		t.Fatal("a refused proof wrote")
	}
}

func TestSocialUnlinkKeepsTheLastSignInMethodAndHidesAForeignIdentity(t *testing.T) {
	f, domain := linkFixture(t)
	headers := f.bearer(f.grant())
	own := uuid.New()
	domain.owners[own] = f.principal
	foreign := uuid.New()
	domain.owners[foreign] = uuid.New()

	if response := f.do(http.MethodDelete, "/v1/identities/social/"+foreign.String(), "", headers); response.Code != http.StatusNotFound {
		t.Fatalf("foreign id: status %d, want 404", response.Code)
	}
	if _, kept := domain.owners[foreign]; !kept {
		t.Fatal("a foreign identity was removed")
	}
	if response := f.do(http.MethodDelete, "/v1/identities/social/"+own.String(), "", headers); response.Code != http.StatusConflict {
		t.Fatalf("last method: status %d, want 409", response.Code)
	}
	if _, kept := domain.owners[own]; !kept {
		t.Fatal("the last sign-in method was removed")
	}
	domain.methods[f.principal] = 2
	if response := f.do(http.MethodDelete, "/v1/identities/social/"+own.String(), "", headers); response.Code != http.StatusNoContent {
		t.Fatalf("status %d, want 204", response.Code)
	}
	if _, kept := domain.owners[own]; kept {
		t.Fatal("the identity stayed after an accepted unlink")
	}
}

func TestSocialLinkRoutesRequireTheirCollaborators(t *testing.T) {
	if _, err := SocialLinkRoutes(SocialLinkDeps{Social: &fakeLinkDomain{}}); err == nil {
		t.Fatal("routes built without sessions, users and an issuer")
	}
}
