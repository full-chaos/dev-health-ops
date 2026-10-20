package social

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// googleAccessTokenServer answers tokeninfo with the documented access-token
// shape (issued_to, audience, user_id) and userinfo with the v2 profile.
func googleAccessTokenServer(t *testing.T, audience, tokenUser, profileUser string) *ProviderClient {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tokeninfo", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil || r.PostForm.Get("access_token") != "google-access" {
			http.Error(w, `{"error":"invalid_token"}`, http.StatusBadRequest)
			return
		}
		reply(w, map[string]any{"issued_to": audience, "audience": audience, "user_id": tokenUser,
			"scope": "openid email profile", "expires_in": 3599, "access_type": "online"})
	})
	mux.HandleFunc("GET /userinfo", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer google-access" {
			http.Error(w, "{}", http.StatusUnauthorized)
			return
		}
		reply(w, map[string]any{"id": profileUser, "email": "g@example.com", "verified_email": true, "name": "G"})
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	creds := NewClientCredentials(testClientID, testClientSecret)
	return &ProviderClient{
		HTTP:      server.Client(),
		Endpoints: Endpoints{GoogleTokenInfo: server.URL + "/tokeninfo", GoogleUserInfo: server.URL + "/userinfo"},
		Clients:   map[Provider]ClientCredentials{Google: creds},
	}
}

func TestGoogleAccessTokenTokeninfoShape(t *testing.T) {
	ctx := context.Background()
	t.Run("the documented access-token response is accepted", func(t *testing.T) {
		c := googleAccessTokenServer(t, testClientID, "1077", "1077")
		p, err := c.Fetch(ctx, Google, secretOf("google-access"))
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		if p.Subject != "1077" || p.Issuer != SocialIssuer(Google) || !p.EmailVerified {
			t.Fatalf("profile %+v", p)
		}
	})
	t.Run("a token issued to another client is refused", func(t *testing.T) {
		c := googleAccessTokenServer(t, otherClientID, "1077", "1077")
		var perr *ProviderError
		if _, err := c.Fetch(ctx, Google, secretOf("google-access")); !errors.As(err, &perr) || perr.Reason != ReasonAudience {
			t.Fatalf("err = %v, want an audience refusal", err)
		}
	})
	t.Run("a token of another user than the profile is refused", func(t *testing.T) {
		c := googleAccessTokenServer(t, testClientID, "1077", "2088")
		var perr *ProviderError
		if _, err := c.Fetch(ctx, Google, secretOf("google-access")); !errors.As(err, &perr) || perr.Reason != ReasonAudience {
			t.Fatalf("err = %v, want an audience refusal", err)
		}
	})
}

// unlinkStore holds one identity of one principal.
type unlinkStore struct {
	stubStore
	principal uuid.UUID
	identity  identity.Identity
	unlinkErr error
}

func (s unlinkStore) ListIdentities(_ context.Context, principal uuid.UUID) ([]identity.Identity, error) {
	if principal != s.principal {
		return nil, nil
	}
	return []identity.Identity{s.identity}, nil
}

func (s unlinkStore) UnlinkIdentity(context.Context, identity.ExternalKey, uuid.UUID, identity.Actor) error {
	return s.unlinkErr
}

type signedInAs struct{ who *SignedIn }

func (a signedInAs) SignedIn(*http.Request) (SignedIn, bool) {
	if a.who == nil {
		return SignedIn{}, false
	}
	return *a.who, true
}

func TestUnlinkHandlerAnswersInGoAPIShape(t *testing.T) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	owner := uuid.New()
	held := identity.Identity{ID: uuid.New(), Issuer: SocialIssuer(GitHub), Provider: GitHub, Subject: "42"}
	fresh := &SignedIn{PrincipalID: owner, AuthTime: now.Add(-time.Minute)}
	stale := &SignedIn{PrincipalID: owner, AuthTime: now.Add(-time.Hour)}
	other := &SignedIn{PrincipalID: uuid.New(), AuthTime: now.Add(-time.Minute)}
	cases := []struct {
		name      string
		who       *SignedIn
		id        string
		unlinkErr error
		status    int
		body      string
	}{
		{"owner", fresh, held.ID.String(), nil, http.StatusNoContent, ""},
		{"no session", nil, held.ID.String(), nil, http.StatusUnauthorized, `{"detail":"Not authenticated"}`},
		{"stale sign-in", stale, held.ID.String(), nil, http.StatusForbidden, `{"detail":"Sign in again to change linked accounts"}`},
		{"another principal", other, held.ID.String(), nil, http.StatusNotFound, `{"detail":"Identity not found"}`},
		{"not an id", fresh, "not-a-uuid", nil, http.StatusNotFound, `{"detail":"Identity not found"}`},
		{"last sign-in method", fresh, held.ID.String(), identity.ErrLastSignInMethod, http.StatusConflict, `{"detail":"Cannot remove the last sign-in method"}`},
		{"held by another principal at unlink", fresh, held.ID.String(), identity.ErrNotOwner, http.StatusNotFound, `{"detail":"Identity not found"}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFakeProviders(t)
			svc, err := New(Config{Store: unlinkStore{principal: owner, identity: held, unlinkErr: c.unlinkErr},
				Providers: f.client(), Now: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			h := Handlers{Service: svc, Auth: signedInAs{who: c.who}}
			req := httptest.NewRequest(http.MethodDelete, "/v1/identities/social/"+c.id, nil)
			req.SetPathValue("id", c.id)
			rec := httptest.NewRecorder()
			h.Unlink(rec, req)
			if rec.Code != c.status || strings.TrimSpace(rec.Body.String()) != c.body {
				t.Fatalf("got %d %q, want %d %q", rec.Code, rec.Body.String(), c.status, c.body)
			}
			if c.body != "" {
				var detail map[string]string
				if err := json.Unmarshal(rec.Body.Bytes(), &detail); err != nil || rec.Header().Get("Content-Type") != "application/json" {
					t.Fatalf("body %q, content type %q", rec.Body.String(), rec.Header().Get("Content-Type"))
				}
			}
		})
	}
}

func TestJSONParsersRefuseTrailingData(t *testing.T) {
	t.Run("provider response", func(t *testing.T) {
		for _, body := range []string{`{"a":1} garbage`, `{"a":1}{"a":2}`, `{"a":1} 1`} {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(body))
			}))
			c := &ProviderClient{HTTP: server.Client()}
			var out map[string]any
			err := c.do(context.Background(), request{method: http.MethodGet, url: server.URL, step: "probe"}, &out)
			server.Close()
			var perr *ProviderError
			if !errors.As(err, &perr) || perr.Reason != ReasonInvalidCredentials {
				t.Errorf("%q: err = %v, want a refusal", body, err)
			}
		}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("{\"a\":1}\n \t"))
		}))
		defer server.Close()
		var out map[string]any
		if err := (&ProviderClient{HTTP: server.Client()}).do(context.Background(), request{method: http.MethodGet, url: server.URL, step: "probe"}, &out); err != nil {
			t.Errorf("trailing white space refused: %v", err)
		}
	})
	t.Run("exchange request body", func(t *testing.T) {
		f := newFakeProviders(t)
		svc, err := New(Config{Store: stubStore{}, Providers: f.client()})
		if err != nil {
			t.Fatal(err)
		}
		h := Handlers{Service: svc}
		for _, body := range []string{`{"provider":"github","provider_access_token":"x"} garbage`, `{"provider":"github","provider_access_token":"x"}{}`} {
			rec := httptest.NewRecorder()
			h.Exchange(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)))
			if rec.Code != http.StatusUnprocessableEntity {
				t.Errorf("%q: status %d, want 422", body, rec.Code)
			}
		}
	})
}
