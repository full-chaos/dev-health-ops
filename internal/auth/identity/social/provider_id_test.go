package social

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

const invalidUserIDBody = `{"detail":{"message":"The provider returned an invalid user id"}}`

// idCountingStore panics on any store call but the two sign-in lookups, and counts those: a refused id reaches no
// lookup and no write.
type idCountingStore struct {
	stubStore
	lookups int
}

func (s *idCountingStore) IdentityBySubject(context.Context, string, identity.Provider, string, *identity.LegacyMatch) (identity.Principal, bool, error) {
	s.lookups++
	return identity.Principal{}, false, nil
}

func (s *idCountingStore) ByEmail(context.Context, string) (*identity.Principal, error) {
	s.lookups++
	return nil, nil
}

// TestProviderUserIDClassesAnswerLikeGoAPI pins, per id class, go-api's answer (internal/api/oauthprovider
// ErrMissingProviderID and identity.RefuseInvalidProviderID -> 400; internal/api/session/social.go). Classes go-api
// answers otherwise stay on today's answer: a GitHub/GitLab non-integer id (go-api signs in with str(id)) and a
// non-string Google id (go-api 500) are 401 here.
func TestProviderUserIDClassesAnswerLikeGoAPI(t *testing.T) {
	long := `"` + strings.Repeat("x", 1025) + `"`
	cases := []struct {
		provider Provider
		name     string
		rawID    string
		want     int
	}{
		{GitHub, "absent", "-", 400}, {GitHub, "null", "null", 400}, {GitHub, "empty string", `""`, 400},
		{GitHub, "float", "1.5", 401}, {GitHub, "negative", "-1", 401}, {GitHub, "exponent", "1e3", 401},
		{GitHub, "text", `"abc"`, 401}, {GitHub, "boolean", "true", 401}, {GitHub, "leading zero", "01", 401},
		{GitLab, "absent", "-", 400}, {GitLab, "null", "null", 400}, {GitLab, "empty string", `""`, 400},
		{GitLab, "float", "1.5", 401}, {GitLab, "negative", "-1", 401}, {GitLab, "exponent", "1e3", 401},
		{GitLab, "text", `"abc"`, 401}, {GitLab, "boolean", "true", 401}, {GitLab, "leading zero", "01", 401},
		{Google, "absent", "-", 400}, {Google, "null", "null", 400}, {Google, "empty string", `""`, 400},
		{Google, "leading space", `" g1"`, 400}, {Google, "trailing space", `"g1 "`, 400},
		{Google, "control character", `"g\u00001"`, 400}, {Google, "over 1024 bytes", long, 400},
		{Google, "number", "1.5", 401}, {Google, "boolean", "true", 401},
	}
	for _, c := range cases {
		t.Run(string(c.provider)+" "+c.name, func(t *testing.T) {
			f := newFakeProviders(t)
			store := &idCountingStore{}
			svc, err := New(Config{Store: store, Providers: f.client()})
			if err != nil {
				t.Fatal(err)
			}
			googleID := "g1"
			if decoded := ""; c.provider == Google && json.Unmarshal([]byte(c.rawID), &decoded) == nil && decoded != "" {
				googleID = decoded
			}
			tok := f.token("tok", account{provider: c.provider, id: 7, googleID: googleID, email: "a@example.com", verified: true, public: true, rawID: c.rawID})
			rec := httptest.NewRecorder()
			Handlers{Service: svc, Auth: noAuth{}}.Exchange(rec, httptest.NewRequest(http.MethodPost, "/",
				strings.NewReader(`{"provider":"`+string(c.provider)+`","provider_access_token":"`+tok.Reveal()+`"}`)))
			if rec.Code != c.want {
				t.Fatalf("status %d %s, want %d", rec.Code, rec.Body, c.want)
			}
			if c.want == 400 && rec.Body.String() != invalidUserIDBody {
				t.Fatalf("body %q, want exactly %q", rec.Body.String(), invalidUserIDBody)
			}
			if c.want == 401 && rec.Body.String() == invalidUserIDBody {
				t.Fatalf("a %s id answers the invalid-user-id body", c.name)
			}
			if store.lookups != 0 {
				t.Fatalf("%d store lookups before the id was refused", store.lookups)
			}
		})
	}
}

// TestRefusedTokensAndAudienceStayUnauthorized: the id rule moves no other refusal.
func TestRefusedTokensAndAudienceStayUnauthorized(t *testing.T) {
	f := newFakeProviders(t)
	svc, err := New(Config{Store: &idCountingStore{}, Providers: f.client()})
	if err != nil {
		t.Fatal(err)
	}
	h := Handlers{Service: svc, Auth: noAuth{}}
	other := f.token("other", account{provider: GitHub, id: 8, email: "b@example.com", verified: true})
	mismatch := f.token("mismatch", account{provider: GitHub, id: 7, email: "a@example.com", verified: true, profileOf: other.Reveal()})
	foreign := f.token("foreign", account{provider: GitHub, id: 9, email: "c@example.com", verified: true, clientID: otherClientID})
	for name, tok := range map[string]string{"bad token": "unknown", "audience mismatch": mismatch.Reveal(), "another client": foreign.Reveal()} {
		rec := httptest.NewRecorder()
		h.Exchange(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"provider":"github","provider_access_token":"`+tok+`"}`)))
		if rec.Code != http.StatusUnauthorized || rec.Body.String() == invalidUserIDBody {
			t.Errorf("%s: %d %s, want 401", name, rec.Code, rec.Body)
		}
	}
}

// TestLinkRefusesAnInvalidProviderSubject: Link answers the Exchange answer, with no link written.
func TestLinkRefusesAnInvalidProviderSubject(t *testing.T) {
	key := identity.ExternalKey{Issuer: SocialIssuer(Google), Provider: identity.ProviderGoogle, Subject: " g1"}
	store := &linkStore{}
	svc, err := New(Config{Store: store, Providers: linkProfiles{key: key}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.Link(context.Background(), SignedIn{PrincipalID: uuid.New(), AuthTime: svc.now()}, Google, secretOf("tok"))
	if err == nil {
		t.Fatal("Link accepted an invalid subject")
	}
	if status, reason := Classify(err); status != http.StatusBadRequest || reason != ReasonMalformed {
		t.Fatalf("Classify = %d %q, want 400 malformed", status, reason)
	}
	if store.linked != 0 {
		t.Fatalf("LinkIdentity called %d times", store.linked)
	}
}
