package social

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

func TestReasonsAreAuthOutcomeCodes(t *testing.T) {
	known := map[string]bool{}
	for _, code := range contracts.AuthOutcomeReasonCodes {
		known[code] = true
	}
	for _, r := range allReasons {
		if !known[string(r)] {
			t.Errorf("reason %q is not an auth-outcome.v1 reason code", r)
		}
	}
}

func TestParseProviderAcceptsOnlyTheThree(t *testing.T) {
	for _, s := range []string{"github", "gitlab", "google"} {
		p, ok := ParseProvider(s)
		if !ok || SocialIssuer(p) == "" {
			t.Errorf("ParseProvider(%q) = %q, %v", s, p, ok)
		}
	}
	for _, s := range []string{"", "GitHub", "github ", "saml", "oidc", "microsoft"} {
		if _, ok := ParseProvider(s); ok {
			t.Errorf("ParseProvider(%q) accepted", s)
		}
	}
}

func TestProfileFetchFollowsEachProvider(t *testing.T) {
	ctx := context.Background()
	f := newFakeProviders(t)
	c := f.client()
	cases := []struct {
		name         string
		acct         account
		wantSubject  string
		wantVerified bool
	}{
		{"github public email", account{provider: GitHub, id: 42, email: "a@example.com", public: true}, "42", true},
		{"github primary verified", account{provider: GitHub, id: 43, email: "b@example.com", verified: true}, "43", true},
		{"github unverified only", account{provider: GitHub, id: 44, email: "c@example.com"}, "44", false},
		{"gitlab confirmed", account{provider: GitLab, id: 45, email: "d@example.com", verified: true}, "45", true},
		{"gitlab unconfirmed", account{provider: GitLab, id: 46, email: "e@example.com"}, "46", false},
		{"google verified", account{provider: Google, googleID: "108", email: "f@example.com", verified: true}, "108", true},
		{"google unverified", account{provider: Google, googleID: "109", email: "g@example.com"}, "109", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := c.Fetch(ctx, tc.acct.provider, f.token(t.Name(), tc.acct))
			if err != nil {
				t.Fatalf("Fetch: %v", err)
			}
			if p.Subject != tc.wantSubject || p.EmailVerified != tc.wantVerified || p.Issuer != SocialIssuer(tc.acct.provider) {
				t.Fatalf("profile %+v", p)
			}
			if !tc.wantVerified && tc.acct.provider == GitHub && p.Email != "" {
				t.Fatalf("an unverified GitHub address was returned: %q", p.Email)
			}
		})
	}
}

func TestFetchRefusesTokensOfAnotherClient(t *testing.T) {
	ctx := context.Background()
	f := newFakeProviders(t)
	c := f.client()
	for _, p := range Providers() {
		t.Run(string(p), func(t *testing.T) {
			tok := f.token(t.Name(), account{provider: p, id: 7, googleID: "7", email: "x@example.com", verified: true, clientID: otherClientID})
			_, err := c.Fetch(ctx, p, tok)
			var perr *ProviderError
			if !errors.As(err, &perr) {
				t.Fatalf("err = %v, want a ProviderError", err)
			}
			if perr.Reason != ReasonAudience && perr.Reason != ReasonInvalidCredentials {
				t.Fatalf("reason = %q", perr.Reason)
			}
		})
	}
}

func TestFetchDoesNotFollowRedirectsOrLeakTheToken(t *testing.T) {
	var hits int
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++ }))
	t.Cleanup(target.Close)
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	t.Cleanup(redirect.Close)
	creds := NewClientCredentials(testClientID, testClientSecret)
	c := &ProviderClient{
		Endpoints: Endpoints{GitHubAPI: redirect.URL, GitLabBase: redirect.URL, GoogleUserInfo: redirect.URL, GoogleTokenInfo: redirect.URL},
		Clients:   map[Provider]ClientCredentials{GitHub: creds, GitLab: creds, Google: creds},
	}
	const secret = "tok-must-not-appear"
	for _, p := range Providers() {
		_, err := c.Fetch(context.Background(), p, secretOf(secret))
		if err == nil {
			t.Fatalf("%s: a redirect was accepted", p)
		}
		if strings.Contains(err.Error(), secret) || strings.Contains(fmt.Sprintf("%+v %#v", err, c), testClientSecret) {
			t.Fatalf("%s: a credential reached an error or printed form: %v", p, err)
		}
	}
	if hits != 0 {
		t.Fatalf("redirect target hit %d times", hits)
	}
}

func TestFetchRefusesNonIntegerIDs(t *testing.T) {
	for _, s := range []string{"1.0", "1e3", "-1", "01", ""} {
		if _, ok := integerID(json.Number(s)); ok {
			t.Errorf("integerID(%q) accepted", s)
		}
	}
	if got, ok := integerID("12345678901234567890"); !ok || got != "12345678901234567890" {
		t.Errorf("a large id = %q, %v", got, ok)
	}
}

// stubStore finds no identity and one local account for every email.
type stubStore struct{ Store }

func (stubStore) IdentityBySubject(context.Context, string, identity.Provider, string, *identity.LegacyMatch) (identity.Principal, bool, error) {
	return identity.Principal{}, false, nil
}

func (stubStore) ByEmail(context.Context, string) (*identity.Principal, error) {
	local := "local"
	return &identity.Principal{ID: uuid.New(), AuthProvider: &local}, nil
}

func TestHandlersAnswerInGoAPIShape(t *testing.T) {
	f := newFakeProviders(t)
	var logs bytes.Buffer
	svc, err := New(Config{Store: stubStore{}, Providers: f.client(), Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	h := Handlers{Service: svc, Auth: noAuth{}}
	tok := f.token("handler", account{provider: GitHub, id: 1, email: "a@example.com", public: true})
	cases := []struct {
		body string
		want int
	}{
		{`{"provider":"github","provider_access_token":"` + tok.Reveal() + `"}`, http.StatusConflict},
		{`{"provider":"github","provider_access_token":"unknown-token"}`, http.StatusUnauthorized},
		{`{"provider":"saml","provider_access_token":"x"}`, http.StatusUnprocessableEntity},
		{`{"provider":"github"}`, http.StatusUnprocessableEntity},
		{`not json`, http.StatusUnprocessableEntity},
	}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		h.Exchange(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(c.body)))
		if rec.Code != c.want || !strings.HasPrefix(rec.Body.String(), `{"detail":`) {
			t.Errorf("%s: %d %s, want %d", c.body, rec.Code, rec.Body, c.want)
		}
	}
	rec := httptest.NewRecorder()
	h.Link(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`)))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("link without a session: %d", rec.Code)
	}
	if strings.Contains(logs.String(), tok.Reveal()) || strings.Contains(logs.String(), "unknown-token") {
		t.Fatalf("a provider token reached the log: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "reason_code=email_collision") {
		t.Fatalf("no reason code in the log: %s", logs.String())
	}
}

type noAuth struct{}

func (noAuth) SignedIn(*http.Request) (SignedIn, bool) { return SignedIn{}, false }

func TestFreshnessBound(t *testing.T) {
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	f := newFakeProviders(t)
	svc, err := New(Config{Store: stubStore{}, Providers: f.client(), Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	for _, c := range []struct {
		age  time.Duration
		want bool
	}{{0, true}, {10 * time.Minute, true}, {10*time.Minute + time.Second, false}, {-2 * time.Minute, false}} {
		_, err := svc.fresh(SignedIn{PrincipalID: id, AuthTime: now.Add(-c.age)})
		if (err == nil) != c.want {
			t.Errorf("age %v: err = %v", c.age, err)
		}
	}
}

func TestGitLabIssuerIsTheFixedSocialIssuer(t *testing.T) {
	for _, base := range []string{DefaultEndpoints.GitLabBase, "https://git.example.com/"} {
		c := &ProviderClient{Endpoints: Endpoints{GitLabBase: base}}
		if got := c.Issuer(GitLab); got != "https://gitlab.com" {
			t.Errorf("GitLab issuer at %q = %q, want https://gitlab.com", base, got)
		}
	}
	if got := (&ProviderClient{}).Issuer(GitHub); got != "https://github.com" {
		t.Errorf("GitHub issuer = %q", got)
	}
}

// failingStore answers the email lookup with a database error whose detail
// carries an email.
type failingStore struct{ stubStore }

func (failingStore) ByEmail(context.Context, string) (*identity.Principal, error) {
	return nil, &pgconn.PgError{Code: "23505", Message: "duplicate key value violates unique constraint",
		Detail: "Key (email_lower)=(store-detail@example.com) already exists.", ConstraintName: "users_email_lower_key"}
}

func TestStoreErrorTextIsNeverLogged(t *testing.T) {
	f := newFakeProviders(t)
	var logs bytes.Buffer
	svc, err := New(Config{Store: failingStore{}, Providers: f.client(), Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	tok := f.token(t.Name(), account{provider: GitHub, id: 2, email: "store-detail@example.com", public: true})
	if _, err := svc.Exchange(context.Background(), GitHub, tok, nil); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	out := logs.String()
	if strings.Contains(out, "store-detail@example.com") || strings.Contains(out, "duplicate key") {
		t.Fatalf("store error text reached the log: %s", out)
	}
	if !strings.Contains(out, "error.class=postgres") || !strings.Contains(out, "error.code=23505") {
		t.Fatalf("no error class and SQLSTATE in the log: %s", out)
	}
}
