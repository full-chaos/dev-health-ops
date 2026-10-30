package social

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

const emailMarker = "leakmarker"

const (
	unusableEmailBody   = `{"detail":{"message":"The provider returned an unusable email address"}}`
	unverifiedEmailBody = `{"detail":{"message":"The provider has not verified this email address"}}`
)

// providerEmailForms are addresses the shared provider email rule refuses, with the answer go-api's social login
// gives a verified profile carrying each. A form that decodes to U+FFFD (a lone surrogate escape or invalid UTF-8
// after encoding/json) is no verified address, as go-api's surrogate rule makes it.
func providerEmailForms() map[string]struct {
	email          string
	verifiedStatus int
	verifiedBody   string
} {
	type form = struct {
		email          string
		verifiedStatus int
		verifiedBody   string
	}
	return map[string]form{
		"nul":              {emailMarker + "\x00@example.test", http.StatusBadRequest, unusableEmailBody},
		"control":          {emailMarker + "\x01@example.test", http.StatusBadRequest, unusableEmailBody},
		"del":              {emailMarker + "\x7f@example.test", http.StatusBadRequest, unusableEmailBody},
		"c1":               {emailMarker + "\u0085x@example.test", http.StatusBadRequest, unusableEmailBody},
		"replacement char": {emailMarker + "�@example.test", http.StatusForbidden, unverifiedEmailBody},
		"invalid utf8":     {emailMarker + "\xff@example.test", http.StatusForbidden, unverifiedEmailBody},
	}
}

// orderStore holds one principal for every identity key when linked is set, and counts each store call the email
// rule must precede.
type orderStore struct {
	Store
	linked                          *identity.Principal
	lookups, byEmail, writes, lists int
}

func (s *orderStore) IdentityBySubject(context.Context, string, identity.Provider, string, *identity.LegacyMatch) (identity.Principal, bool, error) {
	s.lookups++
	if s.linked != nil {
		return *s.linked, true, nil
	}
	return identity.Principal{}, false, nil
}

func (s *orderStore) ByEmail(context.Context, string) (*identity.Principal, error) {
	s.byEmail++
	return nil, nil
}

func (s *orderStore) ProvisionFederated(context.Context, identity.ProvisionRequest) (uuid.UUID, error) {
	s.writes++
	return uuid.New(), nil
}

func (s *orderStore) LinkIdentity(context.Context, identity.ExternalKey, uuid.UUID, identity.Actor) (uuid.UUID, error) {
	s.writes++
	return uuid.New(), nil
}

func (s *orderStore) ListIdentities(context.Context, uuid.UUID) ([]identity.Identity, error) {
	s.lists++
	return nil, nil
}

func (s *orderStore) calls() int { return s.lookups + s.byEmail + s.writes + s.lists }

type emailProfiles struct{ profile Profile }

func (emailProfiles) Configured(Provider) bool { return true }
func (p emailProfiles) Issuer(Provider) string { return p.profile.Issuer }
func (p emailProfiles) Fetch(context.Context, Provider, contracts.SecretString) (Profile, error) {
	return p.profile, nil
}

func emailProfile(email string, verified bool) Profile {
	return Profile{ExternalKey: identity.ExternalKey{Issuer: "https://github.com", Provider: identity.ProviderGitHub, Subject: "42"},
		Email: email, EmailVerified: verified, Username: "u"}
}

type socialAnswer struct {
	status int
	body   string
	store  *orderStore
	logs   string
	writes int32
}

func serveSocial(t *testing.T, link bool, profile Profile, linked bool) socialAnswer {
	t.Helper()
	who := SignedIn{PrincipalID: uuid.New(), AuthTime: time.Now()}
	store := &orderStore{}
	if linked {
		store.linked = &identity.Principal{ID: who.PrincipalID, IsActive: true}
	}
	var logs strings.Builder
	svc, err := New(Config{Store: store, Providers: emailProfiles{profile: profile}, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	sessions := &sessionRecorder{}
	h := Handlers{Service: svc, Sessions: sessions, Auth: signedInAs{who: &who}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"provider":"github","provider_access_token":"tok"}`))
	if link {
		h.Link(rec, req)
	} else {
		h.Exchange(rec, req)
	}
	return socialAnswer{status: rec.Code, body: rec.Body.String(), store: store, logs: logs.String(), writes: sessions.written.Load()}
}

// The provider email rule runs before the identity lookup on exchange and link: a linked identity with a refused
// address is refused like an unknown one, byte for byte, and no store call runs.
func TestARefusedProviderEmailIsAnsweredBeforeAnyLookup(t *testing.T) {
	for name, form := range providerEmailForms() {
		for _, verified := range []bool{true, false} {
			for _, link := range []bool{false, true} {
				route := "exchange"
				if link {
					route = "link"
				}
				label := name + "/" + route
				if !verified {
					label += "/unverified"
				}
				t.Run(label, func(t *testing.T) {
					wantStatus, wantBody := form.verifiedStatus, form.verifiedBody
					if !verified {
						wantStatus, wantBody = http.StatusForbidden, unverifiedEmailBody
					}
					profile := emailProfile(form.email, verified)
					known := serveSocial(t, link, profile, true)
					unknown := serveSocial(t, link, profile, false)
					if known.status != unknown.status || known.body != unknown.body {
						t.Fatalf("linked %d %s, unknown %d %s: the answers differ", known.status, known.body, unknown.status, unknown.body)
					}
					for _, a := range []socialAnswer{known, unknown} {
						if a.status != wantStatus || a.body != wantBody {
							t.Fatalf("answer %d %s, want %d %s", a.status, a.body, wantStatus, wantBody)
						}
						if n := a.store.calls(); n != 0 || a.writes != 0 {
							t.Fatalf("%d store calls and %d sessions ran before the refusal", n, a.writes)
						}
						if strings.Contains(a.logs, emailMarker) {
							t.Fatal("the email value is in the logs")
						}
					}
				})
			}
		}
	}
}

// A usable address still reaches the lookup (control).
func TestAUsableProviderEmailReachesTheLookup(t *testing.T) {
	for _, email := range []string{"a@example.test", "\ta@example.test", ""} {
		for _, link := range []bool{false, true} {
			a := serveSocial(t, link, emailProfile(email, true), true)
			if a.store.lookups == 0 {
				t.Fatalf("%q link=%v: no lookup ran (%d %s)", email, link, a.status, a.body)
			}
		}
	}
}
